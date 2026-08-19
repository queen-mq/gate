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
