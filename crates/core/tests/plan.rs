//! The compiler. Every name, every stage, every ceiling.
//!
//! This file replaces v1's `tests/engine.rs`. The engine it tested does not
//! exist — Postgres does the counting — so the properties that survived moved
//! here and to `tests/validate.rs`:
//!
//! * *cost is weighted, not counted* → `cost_is_a_payload_path`;
//! * *a glob matches a whole segment* → `when_op_matches_a_whole_segment`;
//! * *the spec round-trips / two teams may both own an `airbnb`* →
//!   `the_document_round_trips` and `identity_is_the_pair`.
//!
//! The window tests went with the window: `calendar`, `rolling`, the saturation
//! property and the token-bucket property all assert an arithmetic Gate no
//! longer owns.

mod common;

use common::{airbnb, rrl};
use gate_core::plan::{self, QueueKind};
use gate_core::{compile, GraphDoc};

#[test]
fn the_document_round_trips() {
    let doc = airbnb();
    let json = serde_json::to_string(&doc).unwrap();
    let back: GraphDoc = serde_json::from_str(&json).unwrap();
    assert_eq!(doc, back);
}

/// v1's rule and v1's reason: the identity of a graph is the PAIR. Two teams may
/// both have something they call `airbnb`, and they are not the same thing.
#[test]
fn identity_is_the_pair() {
    let a = airbnb();
    let mut b = airbnb();
    b.application = "other".into();
    assert_ne!(a.key(), b.key());
    assert_eq!(a.key(), "channel/airbnb");
}

/// A document a NEWER build wrote must be unreadable by an older one, or the
/// store's `complete: false` is a lie and a reconcile pass silently downgrades a
/// configuration.
#[test]
fn an_unknown_field_is_a_parse_error() {
    let bad = r#"{"application":"a","graph":"g","version":1,"nodes":{},"paths":[],"whatIsThis":1}"#;
    assert!(serde_json::from_str::<GraphDoc>(bad).is_err());
}

#[test]
fn an_omitted_application_lands_in_default() {
    let doc: GraphDoc =
        serde_json::from_str(r#"{"graph":"g","version":1,"nodes":{},"paths":[]}"#).unwrap();
    assert_eq!(doc.application, "default");
}

// ------------------------------------------------------------------- stages

/// The seven stages of the design's §4.2 table, exactly.
#[test]
fn the_airbnb_graph_compiles_to_seven_stages() {
    let p = compile(&airbnb());
    let mut got: Vec<String> = p
        .stages
        .iter()
        .map(|s| {
            format!(
                "{}|{}|{}|{}|{}",
                s.path,
                s.source,
                s.group,
                s.node,
                s.destinations
                    .iter()
                    .map(|d| d.queue.as_str())
                    .collect::<Vec<_>>()
                    .join("+")
            )
        })
        .collect();
    got.sort();

    let mut want = vec![
        "prices|gate.channel.airbnb.prices.ingress|gate.channel.airbnb.prices.prices|prices|gate.channel.airbnb.ip.in",
        "prices|gate.channel.airbnb.ip.in|gate.channel.airbnb.prices.ip|ip|channel.airbnb.out",
        "messages|channel.airbnb.messages.in|gate.channel.airbnb.messages.messages|messages|gate.channel.airbnb.ip.in",
        "messages|gate.channel.airbnb.ip.in|gate.channel.airbnb.messages.ip|ip|channel.airbnb.out",
        "photos|gate.channel.airbnb.photos.ingress|gate.channel.airbnb.photos.photos|photos|gate.channel.airbnb.ip.in+gate.channel.airbnb.audit.in",
        "photos|gate.channel.airbnb.ip.in|gate.channel.airbnb.photos.ip|ip|channel.airbnb.out",
        "photos|gate.channel.airbnb.audit.in|gate.channel.airbnb.photos.audit|audit|channel.airbnb.audit",
    ];
    want.sort();
    assert_eq!(got, want);
}

/// One group per (path, node), not per node. Two paths sharing a node is
/// pub-sub: each group gets EVERY message, which is the documented, intended
/// semantics — and it is why the path is in the group name.
#[test]
fn every_group_name_is_unique_per_path_and_node() {
    let p = compile(&airbnb());
    let mut groups: Vec<&str> = p.stages.iter().map(|s| s.group.as_str()).collect();
    groups.sort_unstable();
    let n = groups.len();
    groups.dedup();
    assert_eq!(groups.len(), n, "a group name is shared: {groups:?}");
}

/// Five queues, and only the interior queues a stage actually reads. A queue
/// nothing writes and nothing drains is a topology's ghost.
#[test]
fn the_plan_provisions_five_queues_and_owns_three() {
    let p = compile(&airbnb());
    let mut names: Vec<(&str, QueueKind)> =
        p.queues.iter().map(|q| (q.name.as_str(), q.kind)).collect();
    names.sort();
    assert_eq!(
        names,
        vec![
            ("channel.airbnb.audit", QueueKind::Egress),
            ("channel.airbnb.messages.in", QueueKind::UserIngress),
            ("channel.airbnb.out", QueueKind::Egress),
            ("gate.channel.airbnb.audit.in", QueueKind::Interior),
            ("gate.channel.airbnb.ip.in", QueueKind::Interior),
            (
                "gate.channel.airbnb.photos.ingress",
                QueueKind::OwnedIngress
            ),
            (
                "gate.channel.airbnb.prices.ingress",
                QueueKind::OwnedIngress
            ),
        ]
    );
}

/// Three stages read `ip.in`, so each one must recognise and settle the
/// messages that belong to the other two — a below-cursor-cheap skip, one ack
/// per foreign batch, no push and no budget charge.
#[test]
fn a_shared_interior_queue_turns_the_foreign_check_on() {
    let p = compile(&airbnb());
    for s in &p.stages {
        let want = s.source == "gate.channel.airbnb.ip.in";
        assert_eq!(
            s.check_foreign, want,
            "{}/{} reads {}",
            s.path, s.node, s.source
        );
    }
}

/// The first hop reads an ingress queue, where a producer that has never heard
/// of Gate is the writer. Nothing there carries a `_gate` stamp Gate wrote, so
/// nothing there can be foreign — and the check must not be on, or a payload
/// that happens to carry a `_gate` key would be silently dropped.
#[test]
fn a_first_hop_never_checks_for_foreign_messages() {
    let p = compile(&airbnb());
    assert!(p.stages.iter().filter(|s| s.first_hop).count() == 3);
    assert!(p.stages.iter().all(|s| !s.first_hop || !s.check_foreign));
}

// -------------------------------------------------------------- identity

/// A fan-out's branches must not carry one transaction id, or a later
/// convergence dedups one of them away.
#[test]
fn a_fanout_derives_a_distinct_id_per_branch() {
    let p = compile(&airbnb());
    let s = p.stage("photos", "photos").unwrap();
    assert_eq!(s.destinations.len(), 2);
    assert!(s.destinations.iter().all(|d| d.derive_id));
    let labels: Vec<&str> = s.destinations.iter().map(|d| d.label.as_str()).collect();
    assert_eq!(labels, vec!["photos/ip", "photos/audit"]);

    let a = gate_core::derive("parent", labels[0]);
    let b = gate_core::derive("parent", labels[1]);
    assert_ne!(a, b);
    assert_eq!(a, gate_core::derive("parent", labels[0]), "deterministic");
}

/// §7's middle arm, and the hole it closes: three stages push into `ip.in` and
/// three into `channel.airbnb.out`, so reusing the upstream id there would
/// collapse two messages that entered by different paths carrying the same one.
#[test]
fn a_convergence_derives_even_without_a_fanout() {
    let p = compile(&airbnb());
    let s = p.stage("prices", "prices").unwrap();
    assert_eq!(s.destinations.len(), 1, "not a fan-out");
    assert!(
        s.destinations[0].derive_id,
        "three stages push into ip.in, so the id must be derived"
    );

    // And where exactly one stage pushes into a queue, the upstream id is
    // carried through — that is the exactly-once mechanism under redelivery.
    let single = compile(&rrl());
    let only = &single.stages[0];
    assert_eq!(only.destinations.len(), 1);
    assert!(!only.destinations[0].derive_id);
}

// ---------------------------------------------------------------- ceilings

/// Priority is a per-path `max` on ONE counter. The top half of `ip` is an
/// exact, atomic reserve that only `prices` can reach — held by the same row
/// lock that does the counting, with no scheduler and no depth probe anywhere.
#[test]
fn every_share_is_a_ceiling_on_one_counter() {
    let p = compile(&airbnb());
    let ip = p.node("ip").unwrap();
    let ten = ip.budgets.iter().find(|b| b.id == "ip-10s").unwrap();

    assert_eq!(ten.count_sub, 150, "1500 over ten one-second sub-windows");
    assert_eq!(ten.window_sub_seconds, 1);

    assert_eq!(ten.max_for(ip.shares["prices"]), 150);
    assert_eq!(ten.max_for(ip.shares["messages"]), 113);
    assert_eq!(ten.max_for(ip.shares["photos"]), 75);

    // One key, so the ceilings overlap rather than divide. This is the property
    // v1's exhaustive `every_spec_that_validates_clean_divides_exactly_one_ceiling`
    // policed with four rules and a 6x5x5x22 property test, and it is structural
    // here: the 7131-against-a-declared-5000 defect is not expressible.
    let keys: Vec<&str> = ip.budgets.iter().map(|b| b.key.as_str()).collect();
    assert_eq!(
        keys,
        vec![
            "b:channel:shared:egress-ip",
            "b:channel:shared:egress-ip-hour"
        ]
    );
}

/// Monotone in priority: a lower priority never gets a larger ceiling.
#[test]
fn ceilings_are_monotone_in_priority() {
    let doc = airbnb();
    let p = compile(&doc);
    for np in p.nodes.values() {
        let mut by_priority: Vec<(u32, f64)> = np
            .shares
            .iter()
            .filter_map(|(name, s)| doc.path(name).map(|d| (d.priority, *s)))
            .collect();
        by_priority.sort_by_key(|(prio, _)| *prio);
        for w in by_priority.windows(2) {
            assert!(
                w[0].1 >= w[1].1 - 1e-9,
                "node {}: priority {} at {} then {} at {}",
                np.name,
                w[0].0,
                w[0].1,
                w[1].0,
                w[1].1
            );
        }
    }
}

/// A share is a fraction of a SHARED counter. Where one path crosses a node
/// alone there is nothing to share, and a fraction there would be capacity
/// nobody can reach.
#[test]
fn a_sole_occupant_gets_the_whole_ceiling() {
    let p = compile(&airbnb());
    assert_eq!(p.node("messages").unwrap().shares["messages"], 1.0);
    assert_eq!(p.node("photos").unwrap().shares["photos"], 1.0);
    assert_eq!(p.node("audit").unwrap().shares["photos"], 1.0);
    // And at the node they meet, the declared fractions bite.
    assert_eq!(p.node("ip").unwrap().shares["messages"], 0.75);
}

/// Equal steps by priority rank, top rank always 1.0.
#[test]
fn default_shares_are_equal_steps_by_rank() {
    let d = plan::default_shares(&[0, 5, 5, 9]);
    assert_eq!(d[&0], 1.0);
    assert!((d[&5] - 2.0 / 3.0).abs() < 1e-9);
    assert!((d[&9] - 1.0 / 3.0).abs() < 1e-9);
}

// -------------------------------------------------------------- subdivision

/// Rounding is always DOWN, in both terms, so the enforced ceiling is at or
/// below the declared one. Enforcing tighter than declared is the safe
/// direction; enforcing looser is a vendor block.
#[test]
fn subdivision_always_rounds_down() {
    assert_eq!(plan::subdivide(1000, 10_000, 10), (100, 1));
    assert_eq!(plan::subdivide(105, 10_000, 10), (10, 1));
    assert_eq!(plan::subdivide(7, 10_000, 10), (1, 1));
}

/// A kv TTL is whole seconds with a minimum of one, so a window declared under a
/// second is enforced as `count` per SECOND — tighter than declared, never
/// looser, but slower.
#[test]
fn a_sub_second_window_is_enforced_at_one_second() {
    let (count_sub, window) = plan::subdivide(50, 200, 1);
    assert_eq!((count_sub, window), (50, 1));
}

/// N may never exceed the count, or `count_sub` floors to 1 and the budget
/// enforces N per window instead of `count` per window.
#[test]
fn the_default_subdivision_never_outruns_the_count() {
    assert_eq!(plan::default_sub_windows(5, 60_000), 5);
    assert_eq!(plan::default_sub_windows(600, 60_000), 60);
    assert_eq!(plan::default_sub_windows(100, 1_000), 1);
}

// ------------------------------------------------------------------- naming

/// Every name is minted in exactly one function, for a measured reason: the
/// broker answers a group with NO cursor with the queue's whole retained range,
/// so an ETA built on a misspelt group reports every message ever pushed as
/// waiting for budget — plausibly, and for ever.
#[test]
fn names_are_minted_in_one_place() {
    assert_eq!(plan::namespace("channel"), "gate.channel");
    assert_eq!(
        plan::owned_ingress_queue("channel", "airbnb", "prices"),
        "gate.channel.airbnb.prices.ingress"
    );
    assert_eq!(
        plan::interior_queue("channel", "airbnb", "ip"),
        "gate.channel.airbnb.ip.in"
    );
    assert_eq!(
        plan::stage_group("channel", "airbnb", "photos", "ip"),
        "gate.channel.airbnb.photos.ip"
    );
    assert_eq!(
        plan::budget_key("channel", "airbnb", "photos", "per-listing"),
        "b:channel:airbnb:photos:per-listing"
    );
    assert_eq!(
        plan::shared_budget_key("channel", "egress-ip"),
        "b:channel:shared:egress-ip"
    );
    assert_eq!(
        plan::breaker_key("channel", "airbnb", "ip"),
        "brk:channel:airbnb:ip"
    );
}

/// A scoped budget is one row per value with its own TTL — 200,000 live
/// listings is 200,000 Postgres rows, where v1 needed 64 shards, 64 gate
/// runners, 64 partition leases and 64 state documents to hold the same thing.
#[test]
fn a_scoped_budget_keys_on_the_value() {
    let p = compile(&airbnb());
    let b = p
        .node("photos")
        .unwrap()
        .budgets
        .iter()
        .find(|b| b.id == "per-listing")
        .unwrap();
    assert!(b.is_scoped());
    assert_eq!(
        b.key_for(Some("l-42")),
        "b:channel:airbnb:photos:per-listing:l-42"
    );
    assert_eq!(b.key_for(None), "b:channel:airbnb:photos:per-listing");
}

// ---------------------------------------------------------------------- cost

#[test]
fn cost_is_a_payload_path() {
    let cost = gate_core::Cost::Path(gate_core::CostPath {
        path: "payload.rooms".into(),
        default: 1,
        max: Some(50),
    });
    let item = serde_json::json!({ "rooms": 7 });
    assert_eq!(gate_core::cost_of(&cost, &item).unwrap(), 7);

    // Absent, non-numeric and fractional all fall back to the default.
    assert_eq!(
        gate_core::cost_of(&cost, &serde_json::json!({})).unwrap(),
        1
    );
    assert_eq!(
        gate_core::cost_of(&cost, &serde_json::json!({"rooms": "many"})).unwrap(),
        1
    );
    assert_eq!(
        gate_core::cost_of(&cost, &serde_json::json!({"rooms": 2.5})).unwrap(),
        1
    );

    // Above the ceiling is NOT tolerated: an item that cannot fit a window can
    // never be admitted and would park the head of its partition for ever.
    assert!(gate_core::cost_of(&cost, &serde_json::json!({"rooms": 51})).is_err());
}

#[test]
fn a_payload_path_must_start_at_the_payload_root() {
    assert!(gate_core::ok_payload_path("payload.a"));
    assert!(gate_core::ok_payload_path("payload.a.b"));
    assert!(!gate_core::ok_payload_path("payload"));
    assert!(!gate_core::ok_payload_path("data.a"));
    assert!(!gate_core::ok_payload_path("payload."));
    // `_gate` is Gate's own stamp and must stay unaddressable from a document.
    assert!(
        gate_core::resolve(&serde_json::json!({"_gate": {"path": "x"}}), "_gate.path").is_none()
    );
}

#[test]
fn when_op_matches_a_whole_segment() {
    let pats = vec!["listing.*".to_string()];
    assert!(gate_core::op_matches(&pats, "listing.create"));
    assert!(!gate_core::op_matches(&pats, "listings.create"));
    assert!(!gate_core::op_matches(&pats, "listing"));
    assert!(gate_core::op_matches(&["*".to_string()], "anything"));
}

// -------------------------------------------------------------- version bumps

#[test]
fn changing_a_count_is_a_hot_change() {
    let old = airbnb();
    let mut new = airbnb();
    new.nodes.get_mut("prices").unwrap().budgets[0].count = 250;
    assert!(!gate_core::needs_version_bump(&old, &new));
}

#[test]
fn changing_a_budget_id_refounds_a_counter() {
    let old = airbnb();
    let mut new = airbnb();
    new.nodes.get_mut("prices").unwrap().budgets[0].id = Some("prices-fast".into());
    assert!(gate_core::needs_version_bump(&old, &new));
}

#[test]
fn removing_a_path_strands_an_interior_queue() {
    let old = airbnb();
    let mut new = airbnb();
    new.paths.retain(|p| p.name != "photos");
    new.nodes.remove("audit");
    assert!(gate_core::needs_version_bump(&old, &new));
}
