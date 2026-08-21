//! v1 documents in, v2 documents out — and every field that was mapped or
//! ignored named in a warning.
//!
//! The acceptance criterion (design §15F): every v1 document is accepted,
//! mapped, and answered 200 with warnings, EXCEPT one carrying `breach[]`, which
//! is refused with a pointer. And a round trip of each of the README's v1
//! examples must produce a document that validates clean.

#![allow(deprecated)]

mod common;

use common::rules;
use gate_core::migrate;
use gate_core::v1;

/// The README's standalone target, verbatim.
const V1_TARGET: &str = r#"
{
  "application": "channel-manager",
  "name": "airbnb",
  "version": 1,
  "budgets": [
    { "id": "api", "cap": 3000, "periodSeconds": 60, "alignment": "calendar",
      "confidence": "documented", "source": "portal docs", "asOf": "2026-08-18" }
  ],
  "lanes": [
    { "name": "urgent", "cap": "ceiling", "concurrency": 8 },
    { "name": "bulk", "cap": "ceiling-minus-measured", "concurrency": 16,
      "floor": 0.5, "default": true }
  ],
  "cost": { "field": "httpCost", "default": 1, "max": 5 },
  "pacing": { "leaseSeconds": 5, "batch": 250 },
  "admitted": { "partitionBy": "connection", "partitions": 8 }
}
"#;

/// The README's graph, verbatim, minus the breach rule — which is the one thing
/// that is refused rather than mapped, and has its own test below.
const V1_GRAPH: &str = r#"
{
  "application": "channel-manager",
  "name": "airbnb",
  "version": 1,
  "nodes": {
    "prices":   { "entry": true, "budgets": [],
                  "cost": { "field": "httpCost", "default": 1, "max": 100 } },
    "messages": { "entry": true,
                  "budgets": [ { "id": "msg-post", "cap": 100, "periodSeconds": 60,
                                 "alignment": "rolling", "confidence": "documented",
                                 "source": "portal docs", "asOf": "2026-05-19" } ],
                  "cost": { "field": "httpCost", "default": 1, "max": 1 } },
    "photos":   { "entry": true, "shardBy": "entity", "shards": 64,
                  "budgets": [ { "id": "photo-del-weekly", "cap": 100,
                                 "periodSeconds": 604800, "alignment": "rolling",
                                 "scope": ["entity"], "maxKeys": 200000,
                                 "confidence": "documented", "source": "portal docs",
                                 "asOf": "2026-05-19" } ],
                  "cost": { "field": "httpCost", "default": 1, "max": 1 } },
    "ip":       { "budgets": [ { "id": "ip-10s", "cap": 1500, "periodSeconds": 10,
                                 "alignment": "rolling", "confidence": "documented",
                                 "source": "portal docs", "asOf": "2026-05-19" } ],
                  "cost": { "field": "httpCost", "default": 1, "max": 100 },
                  "admitted": { "partitionBy": "connection", "partitions": 64 } }
  },
  "edges":   [ { "from": "prices", "to": "ip", "priority": 0 },
               { "from": "messages", "to": "ip", "priority": 1 },
               { "from": "photos", "to": "ip", "priority": 1 } ],
  "consume": [ "ip" ]
}
"#;

#[test]
fn a_v1_target_becomes_a_one_node_graph_that_validates() {
    let spec: v1::TargetSpec = serde_json::from_str(V1_TARGET).unwrap();
    let m = migrate::from_v1_target(&spec).unwrap();
    let problems = gate_core::validate(&m.doc);
    assert!(problems.is_empty(), "{problems:#?}");
    assert_eq!(m.doc.nodes.len(), 1);
    assert!(
        !m.warnings.is_empty(),
        "a mapped document is never a silent success"
    );
}

/// §12.4's stability promise, which is the whole reason a migration is a
/// migration and not a rewrite: the queue the application's consumers are
/// already popping does not move.
#[test]
fn the_terminal_queue_name_survives_the_migration() {
    let spec: v1::TargetSpec = serde_json::from_str(V1_TARGET).unwrap();
    let m = migrate::from_v1_target(&spec).unwrap();
    assert_eq!(
        m.doc.nodes["airbnb"].egress.as_ref().unwrap().queue(),
        "gate.channel-manager.airbnb.admitted.bulk"
    );
}

#[test]
fn a_v1_graph_maps_its_chains_to_paths_and_validates() {
    let spec: v1::GraphSpec = serde_json::from_str(V1_GRAPH).unwrap();
    let m = migrate::from_v1_graph(&spec).unwrap();
    let problems = gate_core::validate(&m.doc);
    assert!(problems.is_empty(), "{problems:#?}");

    // Three entries, three chains, three paths — each two hops into `ip`.
    let mut names: Vec<&str> = m.doc.paths.iter().map(|p| p.name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, vec!["messages", "photos", "prices"]);
    assert!(m.doc.paths.iter().all(|p| p.nodes.len() == 2));

    // The edge priorities came across, which is what the shares are ranked by.
    assert_eq!(m.doc.path("prices").unwrap().priority, 0);
    assert_eq!(m.doc.path("messages").unwrap().priority, 1);
}

/// v1 let a class node declare no budget. v2 requires one, so the mapping
/// declares a pass-through — which limits nothing, exactly as before — and says
/// so rather than inventing a ceiling nobody asked for.
#[test]
fn a_class_node_with_no_budget_gets_a_passthrough_and_a_warning() {
    let spec: v1::GraphSpec = serde_json::from_str(V1_GRAPH).unwrap();
    let m = migrate::from_v1_graph(&spec).unwrap();
    assert_eq!(m.doc.nodes["prices"].budgets.len(), 1);
    assert_eq!(
        m.doc.nodes["prices"].budgets[0].id.as_deref(),
        Some("passthrough")
    );
    assert!(rules(&m.warnings).contains(&"node-budget"));
}

#[test]
fn every_dropped_v1_field_is_named_in_a_warning() {
    let spec: v1::GraphSpec = serde_json::from_str(V1_GRAPH).unwrap();
    let m = migrate::from_v1_graph(&spec).unwrap();
    let got = rules(&m.warnings);
    for want in [
        "alignment",
        "scope",
        "maxKeys",
        "pacing",
        "admitted",
        "shards",
        "consume",
        "edges",
    ] {
        assert!(got.contains(&want), "missing `{want}` in {got:?}");
    }
}

/// A scoped budget's whole subsystem — `shardBy`, `shards`, `maxKeys` — is a
/// no-op now, and the per-key limit becomes one Postgres row per value.
#[test]
fn a_sharded_scoped_budget_becomes_one_counter_per_value() {
    let spec: v1::GraphSpec = serde_json::from_str(V1_GRAPH).unwrap();
    let m = migrate::from_v1_graph(&spec).unwrap();
    let b = &m.doc.nodes["photos"].budgets[0];
    assert_eq!(b.scope_by.as_deref(), Some("payload.entity"));
}

/// `breach[]` keeps its BOUND and loses its trigger, and the warning has to say
/// which is which.
///
/// It was a refusal until re-entry shipped (§16.6 option 2), because silently
/// dropping a bounded-retry policy is the migration failure that gets discovered
/// by a livelock. Now the bound has a home — `maxAttempts` on the document,
/// enforced by `POST .../reenter` — and §12.1's promise applies again: mapped
/// and warned about, never a 422 for having been written last year.
#[test]
fn breach_rules_keep_their_bound_and_say_that_the_trigger_moved() {
    let mut spec: v1::GraphSpec = serde_json::from_str(V1_GRAPH).unwrap();
    spec.breach.push(v1::BreachRule {
        when: v1::BreachWhen {
            status: Some(429),
            outcome: None,
        },
        retry_to: "origin-entry".into(),
        max_attempts: 5,
    });
    let m = migrate::from_v1_graph(&spec).expect("mapped, not refused");
    assert_eq!(
        m.doc.max_attempts,
        Some(5),
        "the bound is the half that survives"
    );
    let detail = m
        .warnings
        .iter()
        .find(|p| p.rule == "breach")
        .map(|p| p.detail.clone())
        .expect("named in a warning");
    assert!(
        detail.contains("reenter"),
        "the caller has to be told where the trigger went: {detail}"
    );
    assert!(
        detail.contains("backoff"),
        "and that the aggregate half is the breaker: {detail}"
    );
    // The document it produces must still be a legal v2 document.
    assert!(gate_core::validate(&m.doc).is_empty(), "{:#?}", m.doc);
}

/// The re-entry bound is per document, and an absent `breach[]` leaves it
/// unstated rather than inventing one.
#[test]
fn a_document_without_breach_rules_states_no_bound() {
    let spec: v1::GraphSpec = serde_json::from_str(V1_GRAPH).unwrap();
    let m = migrate::from_v1_graph(&spec).unwrap();
    assert_eq!(m.doc.max_attempts, None);
    assert!(!rules(&m.warnings).contains(&"breach"));
    assert_eq!(
        gate_core::compile(&m.doc).max_attempts,
        gate_core::plan::DEFAULT_MAX_ATTEMPTS
    );
}

/// `ceiling-minus-measured` is `share: 1 - floor`, and the floor has to be READ.
///
/// It meant "take what the higher lanes are not using, but never less than
/// `floor` of the ceiling", so what it RESERVED for the higher lanes is
/// `1 - floor`. Mapping every derived lane to 1.00 is not oversubscription —
/// there is one counter — but it is the exact opposite of the reserve §3.6
/// promises: the top path's headroom stops being a reserve the moment the low
/// path can spend the counter to the ceiling too.
#[test]
fn a_derived_lane_cap_maps_to_the_reserve_its_floor_asked_for() {
    let spec: v1::TargetSpec = serde_json::from_str(
        r#"{"application":"a","name":"t","version":1,
            "budgets":[{"id":"b","cap":1000,"periodSeconds":10,"alignment":"calendar",
                        "confidence":"inferred"}],
            "lanes":[{"name":"urgent","cap":"ceiling","concurrency":4,"default":true},
                     {"name":"bulk","cap":"ceiling-minus-measured","concurrency":4,"floor":0.3}],
            "cost":{"field":"w","default":1,"max":1}}"#,
    )
    .unwrap();
    let m = migrate::from_v1_target(&spec).unwrap();
    let share = |name: &str| {
        m.doc
            .paths
            .iter()
            .find(|p| p.name == name)
            .and_then(|p| p.share)
            .unwrap_or(-1.0)
    };
    assert_eq!(
        share("urgent"),
        1.0,
        "share-top: the first lane reaches all"
    );
    assert_eq!(
        share("bulk"),
        0.7,
        "1 - floor, rounded to the nearest 0.05 — not 1.0, which is what a floor nobody read gives"
    );
    let detail = m
        .warnings
        .iter()
        .find(|p| p.rule == "lane-cap")
        .map(|p| p.detail.clone())
        .expect("the resolved number is named");
    assert!(detail.contains("0.70"), "{detail}");
}

/// v1's cost was an `f64`; `kv.incr`'s delta is an `i64` on this wire. Rounding
/// is UP, so a migrated ceiling never admits something the old one refused.
#[test]
fn a_fractional_cost_is_rounded_up_and_named() {
    let spec: v1::TargetSpec = serde_json::from_str(
        r#"{"application":"a","name":"t","version":1,
            "budgets":[{"id":"b","cap":100,"periodSeconds":1,"alignment":"calendar",
                        "confidence":"inferred"}],
            "lanes":[{"name":"default","cap":"ceiling","concurrency":4,"default":true}],
            "cost":{"field":"w","default":1.5,"max":2.5}}"#,
    )
    .unwrap();
    let m = migrate::from_v1_target(&spec).unwrap();
    assert_eq!(m.doc.nodes["t"].cost.default_value(), 2);
    assert_eq!(m.doc.nodes["t"].cost.max(), 3);
    assert!(rules(&m.warnings).contains(&"cost"));
}
