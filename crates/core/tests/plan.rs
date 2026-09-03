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
    assert_eq!(
        labels,
        vec!["channel/airbnb/photos/ip", "channel/airbnb/photos/audit"],
        "the application and the graph are in the label because `converging` can only count ONE \
         plan's stages: without them two graphs can mint the same id for two different messages"
    );

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

    // And an INTERIOR queue exactly one stage pushes into carries the upstream
    // id through — that is the exactly-once mechanism under redelivery, and it
    // is what settled point 4 asks for.
    let two_hop = p.stage("prices", "ip").unwrap();
    assert!(
        two_hop.destinations.iter().all(|d| d.terminal),
        "the `ip` stage is terminal in this fixture"
    );
    let chain: gate_core::GraphDoc = serde_json::from_str(
        r#"{"application":"a","graph":"g","version":1,
            "nodes":{"one":{"ingress":true,"budgets":[{"id":"b","count":100,"timeMs":1000}]},
                     "two":{"budgets":[{"id":"b","count":100,"timeMs":1000}],"egress":"a.g.out"}},
            "paths":[{"name":"main","nodes":["one","two"]}]}"#,
    )
    .unwrap();
    let chain = compile(&chain);
    let hop = &chain.stage("main", "one").unwrap().destinations[0];
    assert!(!hop.terminal);
    assert!(
        !hop.derive_id,
        "one writer, one reader, no fan-out: the upstream id is carried through"
    );

    // A TERMINAL push always derives, whatever the count says. `converging` sees
    // one plan, so two graphs that both name one egress queue each counted one
    // and each reused the upstream id — and one producer event id entering both
    // graphs then dedup-collapses on arrival and is silently lost.
    let single = compile(&rrl());
    let only = &single.stages[0];
    assert_eq!(only.destinations.len(), 1);
    assert!(only.destinations[0].terminal);
    assert!(
        only.destinations[0].derive_id,
        "a terminal push derives, so two graphs sharing an egress queue cannot collapse each \
         other's messages"
    );
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

/// The enforced rate is at or below the declared one. Enforcing tighter than
/// declared is the safe direction; enforcing LOOSER is a vendor block, which is
/// the failure this whole service exists to prevent.
///
/// This test used to be called `subdivision_always_rounds_down` and it pinned
/// the leak as if it were the rule. Rounding down in both terms is not one
/// property but two opposite ones: the count is a NUMERATOR, so rounding it
/// down is tighter, and the window is a DENOMINATOR, so rounding it down is
/// looser. The third line below was `(1, 1)` — one call per second against a
/// declared 0.7 — and it was green.
#[test]
fn the_enforced_rate_is_never_above_the_declared_one() {
    // A clean division is exact, and stays exact: 1000 per 10s is 100 per 1s.
    assert_eq!(plan::subdivide(1000, 10_000, 10), (100, 1));
    // 105 per 10s over ten sub-windows: the count floors to 10, and 10 per 1s
    // would be 10/s against a declared 10.5/s — under it, so one second stands.
    assert_eq!(plan::subdivide(105, 10_000, 10), (10, 1));
    // More sub-windows than count. `count_sub` floors to 1, and a one-second
    // window would enforce 1/s against a declared 0.7/s: 43% MORE than the
    // ceiling the caller wrote. Two seconds is 0.5/s, which is under it.
    assert_eq!(plan::subdivide(7, 10_000, 10), (1, 2));
}

/// The exact case from the report, spelled out in the numbers an operator would
/// check by hand.
///
/// 200,000 calls an hour, smoothed over 2000 sub-windows. Each carries 100, and
/// each window is 1800ms — which floored to ONE second, so the limiter enforced
/// 100 per second where 55.6 per second was declared. Not a rounding artefact:
/// very nearly twice the vendor's ceiling, on a document that validates clean
/// and that anybody might write.
#[test]
fn a_window_that_is_not_a_whole_number_of_seconds_rounds_up_and_not_down() {
    let (count_sub, window) = plan::subdivide(200_000, 3_600_000, 2000);
    assert_eq!(count_sub, 100, "200000 over 2000 sub-windows");
    assert_eq!(
        window, 2,
        "1800ms is not expressible as a whole number of seconds, and the choice between one and          two is the choice between 100/s and 50/s against a declared 55.6/s"
    );
    // The declared rate, and the enforced one, as integers so the comparison is
    // exact rather than a float that happens to agree.
    assert!(
        count_sub * 3_600_000 <= 200_000 * 1000 * window,
        "{count_sub} per {window}s is above 200000 per 3600s"
    );
}

/// And the property over a spread of shapes, because the two cases above are
/// the ones that were noticed.
///
/// `count_sub / window_sub_seconds <= count / (time_ms / 1000)`, cross-multiplied
/// so it is integer arithmetic and not a float comparison that rounds its way to
/// agreement. Every shape here is one somebody could declare: sub-second windows,
/// windows that divide evenly and windows that do not, a single window, more
/// sub-windows than count, and the daily ceilings that made the flagship
/// documents fail to migrate at all. The sub-second window needs no exception:
/// stretching it to a whole second is already the tight direction.
#[test]
fn no_shape_of_budget_is_enforced_above_what_it_declared() {
    let counts = [1i64, 2, 5, 7, 100, 105, 999, 1000, 4_500_000];
    let periods = [
        200i64, 1_000, 10_000, 30_000, 60_000, 300_000, 3_600_000, 86_400_000,
    ];
    let subs = [1u32, 2, 3, 4, 10, 60, 150, 1800, 2000, 3600];

    let mut checked = 0usize;
    for &count in &counts {
        for &time_ms in &periods {
            for &n in &subs {
                let (count_sub, window) = plan::subdivide(count, time_ms, n);
                assert!(
                    count_sub >= 1,
                    "a sub-window that admits nothing admits nothing for ever"
                );
                assert!(
                    window >= 1,
                    "a kv TTL is whole seconds with a minimum of one"
                );
                // One inequality, no special cases — including the sub-second
                // window, where stretching to a whole second is already the
                // tight direction and so satisfies the same test.
                assert!(
                    (count_sub as i128) * (time_ms as i128)
                        <= (count as i128) * 1000 * (window as i128),
                    "count {count} over {time_ms}ms in {n} sub-windows is enforced as \
                     {count_sub} per {window}s, which is ABOVE the declared rate"
                );
                checked += 1;
            }
        }
    }
    assert!(
        checked > 500,
        "the spread must actually cover something: {checked}"
    );
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

// ------------------------------------------------------- the assumed discount

/// The `assumed` discount is **wired and not switched on**, and that is a
/// deliberate holding position rather than an oversight.
///
/// v1 defined `ASSUMED_FACTOR = 0.7`, unit-tested it, documented it in the
/// README — *"an assumed cap is enforced at 70% of what it claims"* — and never
/// applied it: `effective_cap` had no caller. Shipping a third release that
/// documents a discount it does not apply is the option that is not available,
/// so the arithmetic is here and one field turns it on. Turning it on changes
/// what every existing `assumed` budget admits, which is a product decision
/// (design §16.3) and not one to make on the way past.
#[test]
fn an_assumed_budget_is_enforced_at_its_declared_count_until_somebody_says_otherwise() {
    let mut doc = airbnb();
    doc.nodes.get_mut("audit").unwrap().budgets[0].confidence = gate_core::Confidence::Assumed;

    let plain = compile(&doc);
    assert_eq!(
        plain.node("audit").unwrap().budgets[0].count_sub,
        2000,
        "the default build enforces exactly what the declaration says"
    );

    let discounted = gate_core::compile_with(
        &doc,
        &plan::PlanOpts {
            assumed_factor: gate_core::ASSUMED_FACTOR,
            ..Default::default()
        },
    );
    assert_eq!(
        discounted.node("audit").unwrap().budgets[0].count_sub,
        1400,
        "and the discount is one field away, not one rewrite away"
    );
    // A documented budget is never discounted, whatever the factor says.
    assert_eq!(discounted.node("prices").unwrap().budgets[0].count_sub, 100);
}

// ------------------------------------------------- where a new group starts

/// A hop that reads a queue only Gate's own relay writes is seeded at the TAIL.
///
/// The rule in one line: `source_is_interior` is true exactly where the stage's
/// source is its node's interior queue. In `airbnb` that is the four hops into
/// `ip` and `audit`, and nothing else.
#[test]
fn a_hop_that_reads_a_gate_written_interior_queue_is_seeded_at_the_tail() {
    let p = compile(&airbnb());
    let mut interior: Vec<String> = p
        .stages
        .iter()
        .filter(|s| s.source_is_interior)
        .map(|s| format!("{}/{} <- {}", s.path, s.node, s.source))
        .collect();
    interior.sort();
    assert_eq!(
        interior,
        vec![
            "messages/ip <- gate.channel.airbnb.ip.in",
            "photos/audit <- gate.channel.airbnb.audit.in",
            "photos/ip <- gate.channel.airbnb.ip.in",
            "prices/ip <- gate.channel.airbnb.ip.in",
        ]
    );
    // And the derivation, stated as the plan sees it rather than as a name:
    // interior exactly where the source is the node's own interior queue.
    for s in &p.stages {
        let np = p.node(&s.node).expect("every stage has a node");
        assert_eq!(
            s.source_is_interior,
            s.source == np.interior_queue,
            "{}/{} reads {}",
            s.path,
            s.node,
            s.source
        );
    }
}

/// A first hop on a queue the APPLICATION owns keeps the whole retained backlog.
///
/// A producer that has never heard of Gate writes it, so what is waiting there
/// is real work the limiter exists to pace — the original "never `new`" rule,
/// unchanged.
#[test]
fn a_first_hop_on_a_user_owned_ingress_queue_keeps_the_whole_backlog() {
    let p = compile(&airbnb());
    let s = p.stage("messages", "messages").expect("the messages stage");
    assert_eq!(s.source, "channel.airbnb.messages.in");
    assert_eq!(
        p.queue(&s.source).map(|q| q.kind),
        Some(QueueKind::UserIngress)
    );
    assert!(
        !s.source_is_interior,
        "a user's queue is never seeded at the tail"
    );
}

/// A first hop on GATE's own HTTP ingress queue keeps the whole backlog too.
///
/// Gate created it, but a caller's `POST` is what writes it, and two paths
/// entering at one node is pub-sub by design — so the same rule applies as for a
/// queue the application made.
#[test]
fn a_first_hop_on_gates_own_http_ingress_queue_keeps_the_whole_backlog() {
    let p = compile(&airbnb());
    for (path, node) in [("prices", "prices"), ("photos", "photos")] {
        let s = p.stage(path, node).expect("the entry stage");
        assert_eq!(s.source, format!("gate.channel.airbnb.{node}.ingress"));
        assert_eq!(
            p.queue(&s.source).map(|q| q.kind),
            Some(QueueKind::OwnedIngress)
        );
        assert!(
            !s.source_is_interior,
            "{path}/{node} reads a front door, not an interior queue"
        );
    }
}

/// A node may own a user ingress queue AND be a downstream hop of another path.
///
/// The two are different queues and get different answers: the hop that ENTERS
/// through the user's queue keeps the backlog, and the hop that arrives by relay
/// reads the interior queue and starts at the tail. Owning an ingress does not
/// make a node's interior queue any less Gate-written.
#[test]
fn a_node_that_owns_a_user_ingress_still_reads_an_interior_queue_downstream_of_it() {
    let doc: GraphDoc = serde_json::from_str(
        r#"
        {
          "application": "app", "graph": "g", "version": 1,
          "nodes": {
            "x": { "ingress": true,
                   "budgets": [{ "id": "x", "count": 100, "timeMs": 1000 }] },
            "t": { "ingress": { "queue": "app.t.in" },
                   "budgets": [{ "id": "t", "count": 100, "timeMs": 1000 }],
                   "egress": "app.g.out" }
          },
          "paths": [
            { "name": "direct", "nodes": ["t"] },
            { "name": "via",    "nodes": ["x", "t"] }
          ]
        }
        "#,
    )
    .expect("parse");
    assert_eq!(
        gate_core::validate(&doc),
        vec![],
        "the shape under test must be a legal one"
    );

    let p = compile(&doc);
    let direct = p.stage("direct", "t").expect("the direct entry");
    assert_eq!(direct.source, "app.t.in");
    assert!(
        !direct.source_is_interior,
        "entering by the user's own queue keeps the backlog"
    );

    let via = p.stage("via", "t").expect("the relayed hop");
    assert_eq!(via.source, "gate.app.g.t.in");
    assert!(
        via.source_is_interior,
        "arriving by relay reads a queue only Gate writes"
    );
}

/// The 2026-09-02 incident, as a compiled plan.
///
/// A path added to a running graph through a node three other paths already
/// terminate at. Its new group on that node's interior queue must start at the
/// tail: everything already there was relayed by — and stamped with — one of the
/// other paths, so the new one can never need any of it, and reading it would
/// mean acking frames whose transaction hashes the broker has long since purged.
#[test]
fn a_path_added_through_an_existing_terminal_node_starts_at_the_tail() {
    let before: GraphDoc = serde_json::from_str(VRBO).expect("parse");
    let after: GraphDoc =
        serde_json::from_str(&VRBO.replace(r#""paths": ["#, PATHS_WITH_REVIEWS)).expect("parse");

    let old = compile(&before);
    let new = compile(&after);
    assert_eq!(old.stages.len() + 2, new.stages.len(), "one path, two hops");

    let added = new.stage("reviews", "partner").expect("the new hop");
    assert_eq!(added.source, "gate.channel-go.vrbo.partner.in");
    assert_eq!(added.group, "gate.channel-go.vrbo.reviews.partner");
    assert!(
        added.source_is_interior,
        "the queue holds the other paths' frames and nothing this path can use"
    );
    // The entry of the new path is a front door and keeps `All`: a caller may
    // well have been pushing at it before the declare landed.
    let entry = new.stage("reviews", "reviews").expect("the new entry");
    assert!(!entry.source_is_interior);
    // And nothing about the paths that were already running changed.
    for s in &old.stages {
        let same = new.stage(&s.path, &s.node).expect("still there");
        assert_eq!(same.source_is_interior, s.source_is_interior);
        assert_eq!(same.group, s.group);
    }
}

const VRBO: &str = r#"
{
  "application": "channel-go", "graph": "vrbo", "version": 1,
  "nodes": {
    "push":       { "ingress": true, "budgets": [{ "id": "p", "count": 100, "timeMs": 1000 }] },
    "promotions": { "ingress": true, "budgets": [{ "id": "m", "count": 100, "timeMs": 1000 }] },
    "content":    { "ingress": true, "budgets": [{ "id": "c", "count": 100, "timeMs": 1000 }] },
    "reviews":    { "ingress": true, "budgets": [{ "id": "r", "count": 100, "timeMs": 1000 }] },
    "partner":    { "budgets": [{ "id": "x", "count": 100, "timeMs": 1000 }],
                    "egress": "channel-go.vrbo.out" }
  },
  "paths": [
    { "name": "push",       "nodes": ["push",       "partner"] },
    { "name": "promotions", "nodes": ["promotions", "partner"] },
    { "name": "content",    "nodes": ["content",    "partner"] }
  ]
}
"#;

const PATHS_WITH_REVIEWS: &str = r#""paths": [
    { "name": "reviews", "nodes": ["reviews", "partner"] },"#;
