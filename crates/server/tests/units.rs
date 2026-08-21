//! Server-side units that need no broker: the arithmetic and the naming that the
//! live tests would only exercise incidentally.

use gate_core::TargetSpec;
use gate_server::edge;
use serde_json::json;

fn spec(v: serde_json::Value) -> TargetSpec {
    serde_json::from_value(v).expect("parse")
}

/// The window is what keeps the bottleneck queue shallow, and it is sized from what
/// the node can admit inside ONE lease — so it has to be read off a rate that is
/// actually the node's, not one that is per key.
#[test]
fn the_relay_window_is_the_nodes_own_rate_and_never_a_per_key_one() {
    // The plan's `ip` node: the most generous budget is 1500 per 10s = 150/s, and two
    // lease-windows of it is 300. Not the tightest (the daily average, 39/s): what
    // binds inside one lease is whichever budget has room, and sizing on the average
    // would starve the gate and make the relay the limiter.
    let ip = spec(json!({
        "name": "airbnb.ip", "version": 1,
        "budgets": [
            { "id": "ip-10s", "cap": 1500, "periodSeconds": 10, "alignment": "rolling",
              "confidence": "inferred" },
            { "id": "ip-1d", "cap": 3375000, "periodSeconds": 86400, "alignment": "rolling",
              "confidence": "inferred" }
        ],
        "lanes": [{ "name": "default", "cap": "ceiling", "concurrency": 8, "default": true }],
        "cost": { "field": "httpCost", "default": 1, "max": 100 },
        "pacing": { "leaseSeconds": 1, "batch": 200 }
    }));
    assert_eq!(edge::window_for(&ip), 300);

    // A node limited only PER KEY has no rate of its own: 100 photo deletions per
    // listing per week is not one item per six thousand seconds of node throughput.
    // Sizing on it collapsed the window to a single item, and because the depth it is
    // compared against is the whole node's, one item waiting anywhere stopped
    // everything.
    let photos = spec(json!({
        "name": "airbnb.photos", "version": 1,
        "shardBy": "entity", "shards": 8,
        "budgets": [
            { "id": "photo-del-weekly", "cap": 100, "periodSeconds": 604800,
              "alignment": "rolling", "scope": ["entity"], "maxKeys": 20000,
              "confidence": "inferred" }
        ],
        "lanes": [{ "name": "default", "cap": "ceiling", "concurrency": 8, "default": true }],
        "cost": { "field": "httpCost", "default": 1, "max": 1 },
        "pacing": { "leaseSeconds": 1, "batch": 200 }
    }));
    assert_eq!(
        edge::window_for(&photos),
        200 * 8,
        "a per-key-limited node is paced by its shards, not by the relay"
    );

    // A class node with no budget at all is the same argument: it paces nothing.
    let class = spec(json!({
        "name": "airbnb.prices", "version": 1,
        "budgets": [],
        "lanes": [{ "name": "default", "cap": "ceiling", "concurrency": 8, "default": true }],
        "cost": { "field": "httpCost", "default": 1, "max": 100 },
        "pacing": { "leaseSeconds": 1, "batch": 50 }
    }));
    assert_eq!(edge::window_for(&class), 50);
}

/// One group per EDGE, not per node: two edges out of one node would otherwise share
/// a cursor and split the stream between them, and each edge is supposed to carry the
/// whole of it to its own destination.
#[test]
fn a_relay_group_is_named_for_the_edge_it_serves() {
    assert_eq!(
        edge::group_of("channel-manager", "airbnb", "messages", "ip"),
        "gate.edge.channel-manager.airbnb.messages.ip"
    );
    assert_ne!(
        edge::group_of("a", "g", "x", "z"),
        edge::group_of("a", "g", "y", "z")
    );
}

/// A `store: kv` ceiling paces the node exactly as a gate-held one does — it is merely
/// enforced from a local lease instead of from the state document. Excluding it sent a node
/// whose only total-rate bound is a shared ceiling down the "nothing paces this" path, and
/// its queue was then allowed to run deep: the shallow-window property that makes priority
/// real, quietly lost.
#[test]
fn a_shared_ceiling_paces_a_node_like_any_other() {
    let shared = spec(json!({
        "name": "airbnb.ip", "version": 1,
        "budgets": [
            { "id": "egress", "cap": 1000, "periodSeconds": 10, "alignment": "calendar",
              "store": "kv", "confidence": "inferred" }
        ],
        "lanes": [{ "name": "default", "cap": "ceiling", "concurrency": 8, "default": true }],
        "cost": { "field": "httpCost", "default": 1, "max": 1 },
        "pacing": { "leaseSeconds": 1, "batch": 200 }
    }));
    // 1000 per 10s = 100/s, two lease-windows of it.
    assert_eq!(edge::window_for(&shared), 200);
}

// ---------------------------------------------------------------------- eta

use gate_core::Alignment;
use gate_server::{eta, gate};

/// A round instant with a window boundary that is not on it: 20s into the
/// minute, so a minute-long calendar window has 40s left to run.
const T: i64 = 1_700_000_000_000;

/// The whole reason the refill half of an ETA reads the DECLARATION and not the
/// meter.
///
/// A window with nothing left in it is measuring zero admissions per second, and
/// zero per second answers "never" — at exactly the moment somebody is asking,
/// because a window is only exhausted while work is piling up behind it. What is
/// true is that the counter zeroes at the edge and the whole cap is available the
/// instant it does, which the spec knows and no measurement can see.
#[test]
fn an_exhausted_window_answers_with_its_edge_and_not_with_infinity() {
    let s = eta::admits(Alignment::Calendar, 100.0, 60, 100.0, 50.0, T);
    assert_eq!(s.seconds, Some(40.0), "40s of this minute are left to run");
    assert_eq!(s.resets_at, T + 40_000);

    // And the same window with room in it admits the backlog now.
    let s = eta::admits(Alignment::Calendar, 100.0, 60, 20.0, 50.0, T);
    assert_eq!(s.seconds, Some(0.0));
    assert_eq!(
        s.resets_at,
        T + 40_000,
        "the edge is a fact about the window"
    );
}

/// A backlog bigger than the cap does not fit in one window, and the answer is
/// the edge plus a whole window for every further cap-worth — not the edge, and
/// not the backlog divided by an average rate.
#[test]
fn a_backlog_deeper_than_the_cap_waits_out_whole_windows() {
    // 100 free at the edge, 100 in the window after it, and the last 50 in the
    // one after that: two edges away.
    let s = eta::admits(Alignment::Calendar, 100.0, 60, 100.0, 250.0, T);
    assert_eq!(s.seconds, Some(40.0 + 120.0));

    // Exactly a cap-worth still lands in the first window: ceil() must not buy
    // an extra one.
    let s = eta::admits(Alignment::Calendar, 100.0, 60, 100.0, 100.0, T);
    assert_eq!(s.seconds, Some(40.0));
}

/// A rolling window has no edge to wait for: its carried tail decays
/// continuously, so room comes back continuously and the rate to divide by is
/// the declared one.
#[test]
fn a_rolling_window_refills_at_the_rate_it_declares() {
    // 100 per 10s is 10/s; 50 units short of room is five seconds of it.
    let s = eta::admits(Alignment::Rolling, 100.0, 10, 100.0, 50.0, T);
    assert_eq!(s.seconds, Some(5.0));

    // Room now is room now, in either alignment.
    assert_eq!(
        eta::admits(Alignment::Rolling, 100.0, 10, 0.0, 50.0, T).seconds,
        Some(0.0)
    );
}

/// Lanes DIVIDE a ceiling rather than replicate it, so an ETA that answered off
/// the whole cap would promise capacity the asking lane's own gate is going to
/// refuse. The share is applied to the cap, and halving it doubles the wait.
#[test]
fn a_lane_is_paced_by_its_share_and_not_by_the_ceiling() {
    let whole = eta::admits(Alignment::Rolling, 100.0, 10, 100.0, 100.0, T);
    let half = eta::admits(Alignment::Rolling, 50.0, 10, 50.0, 100.0, T);
    assert_eq!(whole.seconds, Some(10.0));
    assert_eq!(half.seconds, Some(20.0));
}

/// A cap of nothing is never refilled into something, and a product can render
/// "we cannot say" — where it would render an infinity as a number.
#[test]
fn a_cap_that_admits_nothing_says_so_rather_than_guessing() {
    assert_eq!(
        eta::admits(Alignment::Calendar, 0.0, 60, 0.0, 1.0, T).seconds,
        None
    );
    // Nothing waiting is nothing to wait for, even against a spent cap.
    assert_eq!(
        eta::admits(Alignment::Calendar, 100.0, 60, 100.0, 0.0, T).seconds,
        Some(0.0)
    );
}

/// The gate's consumer group is NOT its query id.
///
/// `RunOptions` leaves `consumer_group` unset and the SDK derives
/// `streams.{query_id}` from it. Naming the query id instead would ask the
/// broker about a group that has no cursor — and a group with no cursor owes its
/// whole retained range, so the ETA would report every message ever pushed as
/// waiting for budget, plausibly, for ever.
#[test]
fn the_gates_group_is_the_one_the_stream_runtime_derives() {
    let s = spec(json!({
        "application": "channel-manager", "name": "airbnb", "version": 1,
        "budgets": [{ "id": "ip", "cap": 100, "periodSeconds": 10, "alignment": "rolling",
                      "confidence": "inferred" }],
        "lanes": [{ "name": "bulk", "cap": "ceiling", "concurrency": 8, "default": true }],
        "cost": { "field": "httpCost", "default": 1, "max": 1 }
    }));
    assert_eq!(s.query_id("bulk"), "gate.channel-manager.airbnb.bulk");
    assert_eq!(
        gate::consumer_group(&s, "bulk"),
        "streams.gate.channel-manager.airbnb.bulk"
    );
}
