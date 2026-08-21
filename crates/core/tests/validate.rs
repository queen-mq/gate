//! Every rule, and the failure it buys.
//!
//! Fixture-mutation style, ported from v1: take a document that validates clean,
//! break exactly one thing, and assert the rule name. The rule names are what
//! the caller sees, so they are API and they are asserted on here.
//!
//! Rules deleted with v1 and the reason each one went — every one policed an
//! invariant that is now structural, or a resource v2 does not allocate:
//!
//! * `default-lane`, `lane-unique`, `lane-concurrency`, `lane-floor` x2,
//!   `lane-shares` x4 — lanes DIVIDED a ceiling because each was its own
//!   partition with its own counters. There is one counter now, so N ceilings
//!   cannot oversubscribe it; the 93/s-against-50 and 7131-against-5000 defects
//!   are not expressible.
//! * `max-keys`, `store-fits`, `kv-scope`, `kv-match`, `kv-chunk` x2,
//!   `shard-count` x3, `shard-scope`, `shard-entry` — cardinality is Postgres
//!   rows with a TTL, not entries in a document Gate re-reads whole every cycle,
//!   and there is no capacity lease in front of a shared budget any more.
//! * `batch-fits`, `pacing`, `lease-beats-window` — the lease is a work lease
//!   and pacing is the budget window; there is no pacing quantum to beat.
//! * `admitted-partitions` x2, `edge-fanout`, `relay-lane`, `edge-unique`,
//!   `edge-self`, `cost-monotonic`, `budget-once`, `consume-terminal`,
//!   `path-length` (the 3-hop wall), `relay-parallelism` — the admitted ring,
//!   the merge relay and the smear that made a long path a vague one are all
//!   gone with the counter-funnel.
//! * `retry-cost`, `retry-entry`, `breach-when`, `breach-attempts` — see
//!   `migrate`: a v1 document carrying `breach[]` is REFUSED with a pointer, not
//!   quietly accepted with the policy dropped.
//! * `kv-rolling` — kv was always a fixed window; that is now the only window
//!   there is, and it is documented rather than warned about per budget.

mod common;

use common::{airbnb, rules};
use gate_core::doc::{Egress, Ingress, PathElem};
use gate_core::{validate, warnings, GraphDoc};

/// The single most valuable test in the file: the flagship fixture must validate
/// clean, in the new vocabulary. If it cannot, the schema is wrong.
#[test]
fn the_airbnb_fixture_validates_clean() {
    let problems = validate(&airbnb());
    assert!(problems.is_empty(), "{problems:#?}");
}

/// ...and warns about exactly one thing: that a fan-out multiplies what the
/// vendor sees. A fixture with a fan-out that warned about nothing would mean
/// the rule was not wired.
#[test]
fn the_airbnb_fixture_warns_only_that_a_fanout_multiplies() {
    let w = warnings(&airbnb());
    assert_eq!(rules(&w), vec!["fanout-multiplies"], "{w:#?}");
}

#[test]
fn the_rrl_fixture_validates_clean_and_warns_about_nothing() {
    let doc = common::rrl();
    assert!(validate(&doc).is_empty());
    assert!(warnings(&doc).is_empty());
}

fn broken(f: impl FnOnce(&mut GraphDoc)) -> Vec<&'static str> {
    let mut doc = airbnb();
    f(&mut doc);
    rules(&validate(&doc))
}

// -------------------------------------------------------------------- naming

#[test]
fn application_is_one_lowercase_segment() {
    assert!(broken(|d| d.application = "Channel Manager".into()).contains(&"application"));
}

#[test]
fn a_graph_name_carries_no_dot() {
    assert!(broken(|d| d.graph = "airbnb.eu".into()).contains(&"graph-name"));
}

#[test]
fn a_node_name_that_overflows_a_queue_name_is_refused() {
    let long = "n".repeat(60);
    let got = broken(|d| {
        let n = d.nodes.remove("audit").unwrap();
        d.nodes.insert(long.clone(), n);
        if let Some(p) = d.paths.iter_mut().find(|p| p.name == "photos") {
            p.nodes[1] = PathElem::FanOut(vec!["ip".into(), long.clone()]);
        }
    });
    assert!(got.contains(&"node-name"), "{got:?}");
}

#[test]
fn two_paths_of_one_name_would_share_a_cursor() {
    let got = broken(|d| d.paths[1].name = "prices".into());
    assert!(got.contains(&"path-name"), "{got:?}");
}

// --------------------------------------------------------------- graph shape

#[test]
fn a_path_that_visits_an_undeclared_node_is_refused() {
    assert!(
        broken(|d| d.paths[0].nodes[1] = PathElem::One("nowhere".into())).contains(&"path-node")
    );
}

#[test]
fn a_cycle_would_re_pay_every_budget_on_the_way_round() {
    let got = broken(|d| {
        d.paths[0].nodes = vec![
            PathElem::One("prices".into()),
            PathElem::One("ip".into()),
            PathElem::One("prices".into()),
        ]
    });
    assert!(got.contains(&"acyclic"), "{got:?}");
}

#[test]
fn a_path_must_start_at_a_node_work_can_enter_by() {
    assert!(broken(|d| d.nodes.get_mut("prices").unwrap().ingress = None).contains(&"path-entry"));
}

#[test]
fn a_path_must_end_where_work_can_leave() {
    assert!(broken(|d| d.nodes.get_mut("ip").unwrap().egress = None).contains(&"path-terminal"));
}

#[test]
fn a_node_no_path_visits_can_never_hold_work() {
    let got = broken(|d| {
        d.nodes.insert("stray".into(), d.nodes["audit"].clone());
    });
    assert!(got.contains(&"node-orphan"), "{got:?}");
}

#[test]
fn a_fanout_of_one_is_not_a_fanout() {
    assert!(
        broken(|d| d.paths[2].nodes[1] = PathElem::FanOut(vec!["ip".into()]))
            .contains(&"fanout-branch")
    );
}

#[test]
fn a_fanout_must_be_the_last_hop() {
    let got = broken(|d| {
        d.paths[2].nodes = vec![
            PathElem::FanOut(vec!["prices".into(), "photos".into()]),
            PathElem::One("ip".into()),
        ]
    });
    assert!(got.contains(&"fanout-terminal"), "{got:?}");
}

// ------------------------------------------------------------------- budgets

#[test]
fn a_node_with_no_budget_is_a_queue_with_extra_steps() {
    assert!(broken(|d| d.nodes.get_mut("audit").unwrap().budgets.clear()).contains(&"node-budget"));
}

/// A node with only per-key budgets has no denominator for the ETA and no lever
/// for the breaker.
#[test]
fn a_node_needs_at_least_one_unscoped_budget() {
    let got = broken(|d| {
        let n = d.nodes.get_mut("photos").unwrap();
        n.budgets.retain(|b| b.scope_by.is_some());
    });
    assert!(got.contains(&"node-unscoped-budget"), "{got:?}");
}

#[test]
fn a_budget_that_cannot_admit_anything_never_will() {
    assert!(
        broken(|d| d.nodes.get_mut("audit").unwrap().budgets[0].count = 0)
            .contains(&"budget-count")
    );
}

#[test]
fn the_window_floor_is_a_hundred_milliseconds() {
    assert!(
        broken(|d| d.nodes.get_mut("audit").unwrap().budgets[0].time_ms = 50)
            .contains(&"budget-window")
    );
}

#[test]
fn one_id_declared_twice_would_spend_one_counter() {
    let got = broken(|d| {
        let n = d.nodes.get_mut("photos").unwrap();
        let dup = n.budgets[0].clone();
        n.budgets.push(dup);
    });
    assert!(got.contains(&"budget-unique"), "{got:?}");
}

#[test]
fn more_sub_windows_than_count_enforces_the_sub_window_count() {
    let got = broken(|d| {
        let b = &mut d.nodes.get_mut("audit").unwrap().budgets[0];
        b.count = 10;
        b.sub_windows = Some(50);
    });
    assert!(got.contains(&"subwindow-fits"), "{got:?}");
}

#[test]
fn sub_windows_has_a_range() {
    assert!(
        broken(|d| d.nodes.get_mut("audit").unwrap().budgets[0].sub_windows = Some(0))
            .contains(&"subwindow-range")
    );
}

/// The one that matters most. An item costing more than a sub-window can never
/// be admitted, blocks the head of its partition for ever, and never reaches a
/// DLQ because lease expiry charges no retry.
#[test]
fn an_item_that_cannot_fit_a_window_is_refused_at_declare_time() {
    let got = broken(|d| {
        d.nodes.get_mut("audit").unwrap().cost = gate_core::Cost::Fixed(5000);
    });
    assert!(got.contains(&"cost-fits"), "{got:?}");
}

#[test]
fn a_max_below_the_default_makes_the_default_inadmissible() {
    let got = broken(|d| {
        d.nodes.get_mut("prices").unwrap().cost = gate_core::Cost::Path(gate_core::CostPath {
            path: "payload.rooms".into(),
            default: 9,
            max: Some(4),
        })
    });
    assert!(got.contains(&"cost-max"), "{got:?}");
}

/// The counter is an integer on this wire, which v1's `f64` cost was not.
#[test]
fn a_cost_below_one_is_not_expressible() {
    assert!(
        broken(|d| d.nodes.get_mut("audit").unwrap().cost = gate_core::Cost::Fixed(0))
            .contains(&"cost-integer")
    );
}

#[test]
fn a_cost_path_is_a_payload_path() {
    let got = broken(|d| {
        d.nodes.get_mut("audit").unwrap().cost = gate_core::Cost::Path(gate_core::CostPath {
            path: "rooms".into(),
            default: 1,
            max: Some(1),
        })
    });
    assert!(got.contains(&"cost-path"), "{got:?}");
}

#[test]
fn a_scope_path_is_a_payload_path() {
    let got = broken(|d| {
        d.nodes.get_mut("photos").unwrap().budgets[1].scope_by = Some("listingId".into())
    });
    assert!(got.contains(&"scope-path"), "{got:?}");
}

/// One counter, two declarations that disagree: one of them is a lie about what
/// it enforces.
#[test]
fn a_shared_key_declared_twice_must_agree() {
    let got = broken(|d| {
        let b = d.nodes.get_mut("audit").unwrap().budgets[0].clone();
        let mut b = b;
        b.id = Some("audit-shared".into());
        b.shared_key = Some("egress-ip".into());
        b.count = 7;
        b.time_ms = 10_000;
        d.nodes.get_mut("audit").unwrap().budgets.push(b);
    });
    assert!(got.contains(&"shared-conflict"), "{got:?}");
}

#[test]
fn an_empty_when_op_charges_nothing() {
    assert!(
        broken(|d| d.nodes.get_mut("photos").unwrap().budgets[1].when_op = Some(vec![]))
            .contains(&"whenop-empty")
    );
}

/// A guess must never look like a measurement.
#[test]
fn documented_means_source_and_as_of() {
    assert!(
        broken(|d| d.nodes.get_mut("prices").unwrap().budgets[0].source = None)
            .contains(&"provenance")
    );
}

// -------------------------------------------------------------------- shares

#[test]
fn a_share_is_a_fraction_not_a_rate() {
    assert!(broken(|d| d.paths[1].share = Some(1.5)).contains(&"share-range"));
}

/// The top priority must be able to reach the whole ceiling, or the headroom
/// above every other path's share belongs to nobody.
#[test]
fn the_top_priority_must_reach_the_ceiling() {
    assert!(broken(|d| d.paths[0].share = Some(0.9)).contains(&"share-top"));
}

/// Priority is expressed as the ceiling, so a lower priority with a larger
/// share is the opposite of what was asked for.
#[test]
fn a_lower_priority_may_not_have_a_larger_share() {
    let got = broken(|d| {
        d.paths[1].share = Some(0.5);
        d.paths[2].share = Some(0.9);
    });
    assert!(got.contains(&"share-order"), "{got:?}");
}

#[test]
fn a_share_that_rounds_below_the_item_cost_could_never_admit() {
    let got = broken(|d| d.paths[2].share = Some(0.01));
    assert!(got.contains(&"share-rounds-out"), "{got:?}");
}

// ----------------------------------------------------------------- ownership

/// Two consumers of one queue in different groups each get every message, which
/// doubles what leaves.
#[test]
fn two_nodes_may_not_name_one_ingress_queue() {
    let got = broken(|d| {
        d.nodes.get_mut("prices").unwrap().ingress = Some(Ingress::Named(gate_core::IngressSpec {
            queue: Some("channel.airbnb.messages.in".into()),
            partitions: None,
            http: None,
            shed: None,
        }))
    });
    assert!(got.contains(&"ingress-owner"), "{got:?}");
}

#[test]
fn an_ingress_queue_claimed_elsewhere_in_the_fleet_is_refused() {
    let facts = gate_core::ExternalFacts {
        ingress_owners: vec![(
            "channel.airbnb.messages.in".into(),
            "channel/other".into(),
            "in".into(),
        )],
        ..Default::default()
    };
    let got = rules(&gate_core::validate_with(&airbnb(), &facts));
    assert!(got.contains(&"ingress-owner"), "{got:?}");
}

// ------------------------------------------------------------------ warnings

/// A kv TTL is whole seconds, so a window declared under one is enforced at one
/// — tighter than declared, never looser, but slower. It is a trade and not a
/// mistake, so it is a warning that names both numbers.
#[test]
fn a_sub_second_window_warns_rather_than_refusing() {
    let mut doc = airbnb();
    doc.nodes.get_mut("audit").unwrap().budgets[0].time_ms = 200;
    assert!(validate(&doc).is_empty(), "{:#?}", validate(&doc));
    let w = warnings(&doc);
    assert!(rules(&w).contains(&"window-sub-second"), "{w:#?}");
    assert!(
        w.iter()
            .any(|p| p.detail.contains("200ms") && p.detail.contains("2000 per 1s")),
        "the warning must name both numbers: {w:#?}"
    );
}

#[test]
fn rounding_down_more_than_two_percent_is_said_out_loud() {
    let mut doc = airbnb();
    let b = &mut doc.nodes.get_mut("audit").unwrap().budgets[0];
    b.count = 95;
    b.time_ms = 10_000;
    b.sub_windows = Some(10);
    let w = warnings(&doc);
    assert!(rules(&w).contains(&"subwindow-rounding"), "{w:#?}");
}

/// A boundary warning is about a subdivision chosen FOR the caller. A budget
/// that declares its own `subWindows` has made the trade knowingly — a weekly
/// per-listing quota has a week-long window and no amount of subdividing changes
/// that.
#[test]
fn the_boundary_warning_is_only_for_a_defaulted_subdivision() {
    let mut doc = airbnb();
    let b = &mut doc.nodes.get_mut("audit").unwrap().budgets[0];
    b.count = 2000;
    b.time_ms = 30_000;
    b.sub_windows = None;
    // 30s / 30 sub-windows is a one-second sub-window: nothing to say.
    assert!(!rules(&warnings(&doc)).contains(&"window-boundary"));

    let b = &mut doc.nodes.get_mut("audit").unwrap().budgets[0];
    b.count = 3;
    b.time_ms = 30_000;
    assert!(rules(&warnings(&doc)).contains(&"window-boundary"));
}

#[test]
fn a_user_owned_ingress_queue_with_a_retention_is_named() {
    let mut queues = std::collections::BTreeMap::new();
    queues.insert(
        "channel.airbnb.messages.in".to_string(),
        gate_core::QueueFacts {
            exists: true,
            partitions: 8,
            retention: Some("2 hours".into()),
        },
    );
    let facts = gate_core::ExternalFacts {
        queues,
        ..Default::default()
    };
    let w = gate_core::warnings_with(&airbnb(), &facts);
    assert!(rules(&w).contains(&"ingress-retention"), "{w:#?}");
}

#[test]
fn one_partition_is_one_order_and_must_not_be_a_surprise() {
    let mut queues = std::collections::BTreeMap::new();
    queues.insert(
        "channel.airbnb.messages.in".to_string(),
        gate_core::QueueFacts {
            exists: true,
            partitions: 1,
            retention: None,
        },
    );
    let facts = gate_core::ExternalFacts {
        queues,
        ..Default::default()
    };
    let w = gate_core::warnings_with(&airbnb(), &facts);
    assert!(rules(&w).contains(&"single-partition"), "{w:#?}");
}

#[test]
fn a_shared_egress_queue_is_legal_and_named() {
    let facts = gate_core::ExternalFacts {
        egress_owners: vec![("channel.airbnb.out".into(), "channel/legacy".into())],
        ..Default::default()
    };
    let w = gate_core::warnings_with(&airbnb(), &facts);
    assert!(rules(&w).contains(&"egress-owner"), "{w:#?}");
}

// ------------------------------------------------------------------ shorthand

#[test]
fn egress_accepts_a_bare_queue_name_or_a_group() {
    let doc = airbnb();
    assert_eq!(
        doc.nodes["audit"].egress.as_ref().unwrap().queue(),
        "channel.airbnb.audit"
    );
    assert_eq!(doc.nodes["audit"].egress.as_ref().unwrap().group(), None);
    assert_eq!(
        doc.nodes["ip"].egress.as_ref().unwrap().group(),
        Some("channel-workers")
    );
    let e: Egress = serde_json::from_str(r#""q.out""#).unwrap();
    assert_eq!(e.queue(), "q.out");
}

#[test]
fn ingress_true_is_a_queue_gate_owns() {
    let doc = airbnb();
    assert!(doc.nodes["prices"].ingress.as_ref().unwrap().is_owned());
    assert!(!doc.nodes["messages"].ingress.as_ref().unwrap().is_owned());
    // The front door defaults on for a queue Gate made and off for one the
    // application already pushes to with its own SDK.
    assert!(doc.nodes["prices"].ingress.as_ref().unwrap().http());
    assert!(!doc.nodes["messages"].ingress.as_ref().unwrap().http());
}
