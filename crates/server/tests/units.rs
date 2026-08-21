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

/// The per-stage worker count defaults from the SOURCE's partition width, which
/// is a fact Gate reads from the broker rather than one it chooses. More workers
/// than partitions is harmless — the extras find nothing and park; fewer is a
/// throughput ceiling.
#[test]
fn concurrency_defaults_from_the_sources_partition_width() {
    let d = doc(json!({
      "application": "a", "graph": "g", "version": 1,
      "nodes": { "n": { "ingress": { "queue": "theirs.in" },
                        "budgets": [{ "id": "b", "count": 100, "timeMs": 1000 }],
                        "egress": "theirs.out" } },
      "paths": [{ "name": "main", "nodes": ["n"] }]
    }));

    // Nothing observed: the declared default.
    let bare = gate_core::compile(&d);
    assert_eq!(bare.stages[0].concurrency, plan::DEFAULT_INGRESS_PARTITIONS);

    // Observed at the broker: that, floored at four.
    let mut partitions = std::collections::BTreeMap::new();
    partitions.insert("theirs.in".to_string(), 2u32);
    let narrow = gate_core::compile_with(
        &d,
        &PlanOpts {
            partitions,
            ..Default::default()
        },
    );
    assert_eq!(narrow.stages[0].concurrency, 4, "floored, never zero");

    let mut partitions = std::collections::BTreeMap::new();
    partitions.insert("theirs.in".to_string(), 32u32);
    let wide = gate_core::compile_with(
        &d,
        &PlanOpts {
            partitions,
            ..Default::default()
        },
    );
    assert_eq!(wide.stages[0].concurrency, 32);
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
