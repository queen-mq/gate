//! Server-side units that need no broker: the naming and the arithmetic the live
//! tests would only exercise incidentally.
//!
//! What went, and why:
//!
//! * `the_relay_window_is_the_nodes_own_rate_and_never_a_per_key_one` and
//!   `a_shared_ceiling_paces_a_node_like_any_other` — there is no relay window.
//!   It existed to keep the destination's push queue shallow so that ARRIVAL
//!   ORDER at a merge would honour priority. With no merge and no arrival-order
//!   priority there is nothing to keep shallow: the destination's own budget is
//!   the pacing, and backlog on an interior queue is exactly where held work
//!   belongs.
//! * `a_relay_group_is_named_for_the_edge_it_serves` — one group per (path,
//!   node) now, not one per edge. Replaced below.
//! * `the_gates_group_is_the_one_the_stream_runtime_derives` — there is no
//!   stream runtime and no derived group name to get wrong. The property it
//!   protected (a group with no cursor owes its whole retained range, so a
//!   misspelling reports every message ever pushed as waiting) is protected by
//!   minting every name in one function, which `gate-core`'s `tests/plan.rs`
//!   pins.
//! * the five `eta::admits` tests — ported, with their fixtures and their fixed
//!   instant, into `gate_server::eta`'s own `#[cfg(test)]` module, because the
//!   function moved with them.

use gate_core::plan::{self, PlanOpts};
use gate_core::GraphDoc;
use serde_json::json;

fn doc(v: serde_json::Value) -> GraphDoc {
    serde_json::from_value(v).expect("parse")
}

/// A stage's group carries the path AND the node, because two paths sharing a
/// node is pub-sub: each group gets every message, and one group per node would
/// make them split the stream instead.
#[test]
fn a_stage_group_is_named_for_the_path_and_the_node() {
    assert_eq!(
        plan::stage_group("channel", "airbnb", "messages", "ip"),
        "gate.channel.airbnb.messages.ip"
    );
    assert_ne!(
        plan::stage_group("a", "g", "x", "z"),
        plan::stage_group("a", "g", "y", "z"),
    );
}

/// The per-stage worker count comes from the BUDGET, not from the partitions.
///
/// A rate limiter's drain never needs to exceed its own cap: the cap bounds the
/// admissible work by construction, so lanes beyond what it can feed are parked
/// polls that can never have work to do. The old rule — `max(4, partitions)` —
/// is a THROUGHPUT rule, which assumes the queue is what bounds you. Stage,
/// measured: about 200 gate consumers parked for a system whose largest declared
/// budget is 200 items a second.
///
///     workers = clamp(ceil(cap_rate_per_sec / LANE_CAPACITY), 1, partitions)
#[test]
fn the_worker_count_comes_from_the_budget_and_not_from_the_partitions() {
    let node = |count: i64, time_ms: i64| {
        doc(json!({
          "application": "a", "graph": "g", "version": 1,
          "nodes": { "n": { "ingress": { "queue": "theirs.in" },
                            "budgets": [{ "id": "b", "count": count, "timeMs": time_ms }],
                            "egress": "theirs.out" } },
          "paths": [{ "name": "main", "nodes": ["n"] }]
        }))
    };
    let with_partitions = |d: &gate_core::GraphDoc, n: u32| {
        let mut partitions = std::collections::BTreeMap::new();
        partitions.insert("theirs.in".to_string(), n);
        gate_core::compile_with(
            d,
            &PlanOpts {
                partitions,
                ..Default::default()
            },
        )
        .stages[0]
            .concurrency
    };

    // 200 per second against a lane that drains a thousand: ONE, and the
    // sixteen partitions it is spread over do not change that. This is the
    // shape of every real graph we run.
    assert_eq!(with_partitions(&node(200, 1000), 16), 1);
    // The same ceiling expressed over a longer window is the same rate.
    assert_eq!(with_partitions(&node(12_000, 60_000), 16), 1);

    // 20k per second needs twenty lanes and is given what the ordering has:
    // sixteen partitions, sixteen lanes.
    assert_eq!(with_partitions(&node(20_000, 1000), 16), 16);
    // ...and four, on four. A lane with no partition to claim finds nothing,
    // for ever.
    assert_eq!(with_partitions(&node(20_000, 1000), 4), 4);
    // Exactly at the lane capacity is one lane; a single item more is two.
    assert_eq!(with_partitions(&node(1000, 1000), 16), 1);
    assert_eq!(with_partitions(&node(1001, 1000), 16), 2);

    // A node with no budget at all has no rate to divide. (`node-budget`
    // refuses the document; the compiler still may not divide by nothing.)
    let bare = doc(json!({
      "application": "a", "graph": "g", "version": 1,
      "nodes": { "n": { "ingress": { "queue": "theirs.in" }, "egress": "theirs.out" } },
      "paths": [{ "name": "main", "nodes": ["n"] }]
    }));
    assert_eq!(with_partitions(&bare, 16), 1, "no budget, one worker");
    // A budget of ZERO is a rate of zero, and the floor holds: a stage with no
    // workers is a queue nobody drains, and the thing that admits nothing here
    // is the budget, which needs a lane to say so.
    assert_eq!(with_partitions(&node(0, 1000), 16), 1, "zero, one worker");

    // The migration's pass-through is a SENTINEL, not a measurement. A million a
    // second means "this node limits nothing", and dividing it by a lane would
    // ask for a thousand lanes on behalf of a class node that paces no traffic
    // of its own.
    let class_node = doc(json!({
      "application": "a", "graph": "g", "version": 1,
      "nodes": { "n": { "ingress": { "queue": "theirs.in" }, "egress": "theirs.out",
                        "budgets": [{ "id": plan::PASSTHROUGH_BUDGET_ID,
                                      "count": 1_000_000, "timeMs": 1000, "subWindows": 1 }] } },
      "paths": [{ "name": "main", "nodes": ["n"] }]
    }));
    assert_eq!(
        with_partitions(&class_node, 16),
        1,
        "a pass-through is not a rate"
    );

    // A per-key budget is not a rate the node has — 100 photo deletions per
    // listing per week says nothing about how fast the node drains — so the
    // node's own budget is what decides, exactly as it does for the batch.
    let scoped = doc(json!({
      "application": "a", "graph": "g", "version": 1,
      "nodes": { "n": { "ingress": { "queue": "theirs.in" }, "egress": "theirs.out",
                        "budgets": [
                          { "id": "node", "count": 20000, "timeMs": 1000 },
                          { "id": "per-listing", "count": 100, "timeMs": 604800000,
                            "subWindows": 1, "scopeBy": "payload.listingId" }
                        ] } },
      "paths": [{ "name": "main", "nodes": ["n"] }]
    }));
    assert_eq!(
        with_partitions(&scoped, 16),
        16,
        "the per-key budget is not a rate"
    );
}

/// A path's SHARE is part of its ceiling, so it is part of its worker count: a
/// path that may spend a quarter of the counter needs a quarter of the lanes.
#[test]
fn a_paths_share_is_part_of_the_rate_its_workers_are_derived_from() {
    let d = doc(json!({
      "application": "a", "graph": "g", "version": 1,
      "nodes": {
        "e1": { "ingress": true, "budgets": [{ "id": "x", "count": 100, "timeMs": 1000 }] },
        "e2": { "ingress": true, "budgets": [{ "id": "y", "count": 100, "timeMs": 1000 }] },
        "n":  { "budgets": [{ "id": "b", "count": 8000, "timeMs": 1000 }], "egress": "out" }
      },
      "paths": [
        { "name": "top", "priority": 0, "share": 1.0,  "nodes": ["e1", "n"] },
        { "name": "low", "priority": 1, "share": 0.25, "nodes": ["e2", "n"] }
      ]
    }));
    let p = gate_core::compile(&d);
    let at = |path: &str| p.stage(path, "n").expect("stage").concurrency;
    assert_eq!(at("top"), 8, "8000/s over lanes of 1000");
    assert_eq!(
        at("low"),
        2,
        "a quarter of the counter is a quarter of the lanes"
    );
}

/// The lane capacity is a knob, because "what one lane drains" is a property of
/// the deployment and not of the declaration.
#[test]
fn the_lane_capacity_is_what_divides_the_ceiling() {
    let d = doc(json!({
      "application": "a", "graph": "g", "version": 1,
      "nodes": { "n": { "ingress": true,
                        "budgets": [{ "id": "b", "count": 4000, "timeMs": 1000 }],
                        "egress": "out" } },
      "paths": [{ "name": "main", "nodes": ["n"] }]
    }));
    let workers = |capacity: u32| {
        gate_core::compile_with(
            &d,
            &PlanOpts {
                lane_capacity: capacity,
                ..Default::default()
            },
        )
        .stages[0]
            .concurrency
    };
    assert_eq!(workers(plan::LANE_CAPACITY), 4);
    assert_eq!(
        workers(500),
        8,
        "a more pessimistic lane wants more of them"
    );
    assert_eq!(workers(4000), 1);
}

/// A node may declare its own batch and its own worker count, and they win over
/// the fleet defaults.
#[test]
fn a_node_may_size_its_own_stage() {
    let d = doc(json!({
      "application": "a", "graph": "g", "version": 1,
      "nodes": { "n": { "ingress": true, "batch": 25, "concurrency": 3,
                        "budgets": [{ "id": "b", "count": 100, "timeMs": 1000 }],
                        "egress": "out" } },
      "paths": [{ "name": "main", "nodes": ["n"] }]
    }));
    let p = gate_core::compile(&d);
    assert_eq!(p.stages[0].batch, 25);
    assert_eq!(p.stages[0].concurrency, 3);
}

/// A claim is sized to what one sub-window admits, and a PER-KEY budget has no
/// say in it.
///
/// This is v1's `the_relay_window_is_the_nodes_own_rate_and_never_a_per_key_one`
/// asked of the mechanism that replaced it. Both halves matter and both are
/// measured defects:
///
/// * the CLAMP — settling a claim in full re-arms its partition in about seven
///   milliseconds and settling a prefix of one costs the whole lease, so a claim
///   that routinely exceeds what the window admits would pace itself by the
///   lease, which is the thing this design set out to remove;
/// * the SCOPED EXCLUSION — a node limited only per key has no rate of its own.
///   *100 photo deletions per listing per week is not one item per six thousand
///   seconds of node throughput.* Sizing on it collapsed the claim to a single
///   item, and because the depth it was compared against was the whole node's,
///   one item waiting anywhere stopped everything.
#[test]
fn a_claim_is_sized_by_the_nodes_own_rate_and_never_by_a_per_key_one() {
    // Clamped: 40 per sub-window at cost 1 is 40, under the declared 200.
    let tight = doc(json!({
      "application": "a", "graph": "g", "version": 1,
      "nodes": { "n": { "ingress": true,
                        "budgets": [{ "id": "b", "count": 40, "timeMs": 1000 }],
                        "egress": "out" } },
      "paths": [{ "name": "main", "nodes": ["n"] }]
    }));
    assert_eq!(gate_core::compile(&tight).stages[0].batch, 40);

    // At cost 4 an item, the same window admits ten of them.
    let costly = doc(json!({
      "application": "a", "graph": "g", "version": 1,
      "nodes": { "n": { "ingress": true, "cost": { "path": "payload.w", "default": 4, "max": 40 },
                        "budgets": [{ "id": "b", "count": 40, "timeMs": 1000 }],
                        "egress": "out" } },
      "paths": [{ "name": "main", "nodes": ["n"] }]
    }));
    assert_eq!(gate_core::compile(&costly).stages[0].batch, 10);

    // The share is part of the ceiling, so it is part of the claim: half of a
    // 40-per-second counter is twenty.
    let shared = doc(json!({
      "application": "a", "graph": "g", "version": 1,
      "nodes": { "e1": { "ingress": true, "budgets": [{ "id": "x", "count": 1000, "timeMs": 1000 }] },
                 "e2": { "ingress": true, "budgets": [{ "id": "y", "count": 1000, "timeMs": 1000 }] },
                 "n":  { "budgets": [{ "id": "b", "count": 40, "timeMs": 1000 }], "egress": "out" } },
      "paths": [{ "name": "top", "priority": 0, "share": 1.0,  "nodes": ["e1", "n"] },
                { "name": "low", "priority": 1, "share": 0.5, "nodes": ["e2", "n"] }]
    }));
    let p = gate_core::compile(&shared);
    let at = |path: &str| p.stage(path, "n").expect("stage").batch;
    assert_eq!(at("top"), 40);
    assert_eq!(at("low"), 20, "half the ceiling is half the claim");

    // And a per-key budget is excluded: 100 per WEEK per listing must not shrink
    // the claim to one item. The node's own 500-per-second budget is what sizes
    // it, and 500 is above the declared 200, so the declaration stands.
    let per_key = doc(json!({
      "application": "a", "graph": "g", "version": 1,
      "nodes": { "n": { "ingress": true,
                        "budgets": [
                          { "id": "node", "count": 500, "timeMs": 1000 },
                          { "id": "per-listing", "count": 100, "timeMs": 604800000,
                            "subWindows": 1, "scopeBy": "payload.listingId" }
                        ],
                        "egress": "out" } },
      "paths": [{ "name": "main", "nodes": ["n"] }]
    }));
    assert_eq!(
        gate_core::compile(&per_key).stages[0].batch,
        gate_core::DEFAULT_BATCH,
        "a batch of two hundred messages across two hundred different keys spends one unit of \
         each: sizing on a per-key allowance is how a node stops draining"
    );
}

/// `null` rather than infinity when nothing is moving: "we cannot say" is an
/// honest answer and a product can render it, where a number that means for ever
/// would be rendered as a number.
#[test]
fn a_drain_estimate_without_a_rate_says_so() {
    use gate_server::api::console::eta;
    assert_eq!(
        eta(0, None),
        Some(0),
        "nothing waiting is nothing to wait for"
    );
    assert_eq!(eta(100, None), None, "no measured rate, no answer");
    assert_eq!(
        eta(100, Some(0.0)),
        None,
        "and a zero rate is not an answer either"
    );
    assert_eq!(eta(100, Some(10.0)), Some(10));
}

/// The knobs are read once, and the defaults are the ones the design names.
#[test]
fn the_knobs_default_to_what_the_design_says() {
    let k = gate_server::knobs::Knobs::default();
    assert_eq!(k.batch, 200, "the per-claim batch");
    assert_eq!(
        k.lease_seconds, 10,
        "a WORK lease, not a pacing quantum — and short, because it is what a deferred tail waits"
    );
    assert_eq!(k.poll_timeout.as_secs(), 30, "the parked long-poll window");
    assert_eq!(
        k.max_park.as_secs(),
        30,
        "a TIME budget, not a count of parks: releasing costs about a minute, so a worker waiting          its turn on a one-second window must be able to wait more than three of them"
    );
    assert_eq!(
        k.retry_limit, 3,
        "the DLQ is back: v1 had to disarm it because it paced by nacking"
    );
    assert_eq!(
        k.max_push_body,
        8 * 1024 * 1024,
        "four times axum's 2 MiB default, which was the real ceiling on every push \
         until 2026-09-04 because nothing here ever set one"
    );
}

/// `forwarded / commits` is THE number that explains a stage's throughput: the
/// destination partition takes one row lock per transaction whoever holds it, so
/// items-per-transaction is the multiplier on everything the workers do in
/// parallel. In v1 it sat near 1; here it should sit near the batch.
#[test]
fn the_stage_view_reports_items_per_commit() {
    let c = gate_server::obs::StageCounters::default();
    assert!(
        c.view()["itemsPerCommit"].is_null(),
        "no commits is not a ratio of zero"
    );
    c.forwarded.store(400, std::sync::atomic::Ordering::Relaxed);
    c.commits.store(2, std::sync::atomic::Ordering::Relaxed);
    assert_eq!(c.view()["itemsPerCommit"], json!(200.0));
}

/// The refusal ring is bounded and drop-oldest: a flush that misses a pass loses
/// the OLDEST denials and never blocks the hot path.
#[test]
fn the_trace_ring_drops_the_oldest_and_never_grows() {
    let t = gate_server::obs::Traces::default();
    for i in 0..(gate_server::obs::TRACE_RING + 50) {
        t.push(gate_server::obs::Trace {
            at: i as i64,
            application: "a".into(),
            graph: "g".into(),
            node: "n".into(),
            path: "p".into(),
            op: String::new(),
            outcome: "denied",
            budget_id: Some("b".into()),
        });
    }
    assert_eq!(t.len(), gate_server::obs::TRACE_RING);
    let recent = t.recent(None, 1);
    assert_eq!(
        recent[0].at,
        (gate_server::obs::TRACE_RING + 49) as i64,
        "newest first"
    );
    assert!(t.recent(Some("admitted"), 10).is_empty(), "denials only");
}

// ----------------------------------------------------------- the body limit

/// A push carries a BATCH and everything else carries a document, so only the
/// push routes get the raised body limit.
///
/// The limit is asserted through the ROUTER rather than by reading the knob back,
/// because the knob was never the thing that was wrong: axum applies a 2 MiB
/// default to every route unless a layer says otherwise, and for the life of this
/// service nothing did. A test that reads `knobs().max_push_body` would have
/// passed just as happily on the day a caller was being refused.
///
/// No broker is needed and none is reached: a body over the limit is rejected by
/// the extractor, so the handler never runs.
mod body_limit {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use std::sync::Arc;
    use tower::ServiceExt;

    fn app() -> gate_server::api::Shared {
        // Deliberately unreachable: nothing in these cases gets far enough to
        // speak to it, and a test that needed a broker would belong in live.rs.
        let queen = queen_mq::Queen::connect(queen_mq::Config::new("http://127.0.0.1:1"))
            .expect("the client is constructed, not connected");
        Arc::new(gate_server::api::App::new(
            queen,
            "http://127.0.0.1:1".into(),
        ))
    }

    fn body_of(bytes: usize) -> Body {
        Body::from(vec![b'x'; bytes])
    }

    const MIB: usize = 1024 * 1024;

    #[tokio::test]
    async fn a_push_accepts_a_body_axums_default_would_have_refused() {
        let res = gate_server::api::router(app())
            .oneshot(
                Request::post("/v1/apps/channel-go/graphs/google/nodes/hotel/push")
                    .header("content-type", "application/json")
                    .body(body_of(3 * MIB))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_ne!(
            res.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "a 3 MiB push was refused for its size: this is the 2 MiB default that \
             cost a caller a week of undelivered pushes, and raising it is the point"
        );
    }

    #[tokio::test]
    async fn a_push_over_the_ceiling_is_still_refused() {
        let over = gate_server::knobs::knobs().max_push_body + MIB;
        let res = gate_server::api::router(app())
            .oneshot(
                Request::post("/v1/apps/channel-go/graphs/google/nodes/hotel/push")
                    .header("content-type", "application/json")
                    .body(body_of(over))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            res.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "the ceiling is a ceiling: a body past it must be refused, or the limit \
             is memory nobody is bounding"
        );
    }

    #[tokio::test]
    async fn a_document_route_keeps_the_default() {
        // Declaring a graph is a document, not a batch. Raising its ceiling would
        // buy nothing and would let a caller hand a 512 MiB pod a body per
        // request that no declaration has ever needed.
        let res = gate_server::api::router(app())
            .oneshot(
                Request::put("/v1/apps/channel-go/graphs/google")
                    .header("content-type", "application/json")
                    .body(body_of(3 * MIB))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            res.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "a document route accepted 3 MiB: the raise must be scoped to the push \
             routes, not applied to the whole surface"
        );
    }
}
