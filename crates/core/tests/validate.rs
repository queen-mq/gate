use gate_core::*;
use serde_json::json;

fn parse(v: serde_json::Value) -> TargetSpec {
    serde_json::from_value(v).expect("parse")
}

fn base() -> serde_json::Value {
    json!({
        "name": "t", "version": 1,
        "budgets": [{ "id": "b", "cap": 100, "periodSeconds": 60,
                      "alignment": "calendar", "confidence": "inferred" }],
        "lanes": [{ "name": "bulk", "cap": "ceiling", "concurrency": 4, "default": true }],
        "cost": { "field": "c", "default": 1, "max": 1 },
        "pacing": { "leaseSeconds": 1, "batch": 200 }
    })
}

#[test]
fn a_sound_spec_has_no_problems() {
    assert_eq!(validate(&parse(base())), vec![]);
}

#[test]
fn an_item_that_cannot_fit_any_cap_is_rejected_at_declare_time() {
    let mut v = base();
    v["cost"]["max"] = json!(500);
    let problems = validate(&parse(v));
    assert!(problems.iter().any(|p| p.rule == "cost-fits"), "{problems:?}");
}

#[test]
fn exactly_one_lane_must_be_default() {
    let mut v = base();
    v["lanes"] = json!([
        { "name": "urgent", "cap": "ceiling", "concurrency": 4 },
        { "name": "bulk", "cap": "ceiling", "concurrency": 4 }
    ]);
    assert!(validate(&parse(v)).iter().any(|p| p.rule == "default-lane"));
}

#[test]
fn a_scoped_budget_must_declare_its_cardinality() {
    let mut v = base();
    v["budgets"][0]["scope"] = json!(["entity"]);
    assert!(validate(&parse(v)).iter().any(|p| p.rule == "max-keys"));
}

#[test]
fn high_cardinality_does_not_belong_in_the_gate_state() {
    let mut v = base();
    v["budgets"][0]["scope"] = json!(["entity"]);
    v["budgets"][0]["maxKeys"] = json!(200_000);
    assert!(validate(&parse(v.clone())).iter().any(|p| p.rule == "store-fits"));
    v["budgets"][0]["store"] = json!("kv");
    assert!(!validate(&parse(v)).iter().any(|p| p.rule == "store-fits"));
}

#[test]
fn documented_without_a_source_is_not_documented() {
    let mut v = base();
    v["budgets"][0]["confidence"] = json!("documented");
    assert!(validate(&parse(v)).iter().any(|p| p.rule == "provenance"));
}

#[test]
fn a_batch_smaller_than_the_budget_would_be_the_real_limiter() {
    let mut v = base();
    v["budgets"][0] = json!({ "id": "fast", "cap": 400, "periodSeconds": 1,
                              "alignment": "rolling", "confidence": "inferred" });
    v["pacing"] = json!({ "leaseSeconds": 1, "batch": 50 });
    assert!(validate(&parse(v)).iter().any(|p| p.rule == "batch-fits"));
}

#[test]
fn changing_a_period_re_founds_the_state_and_needs_a_version() {
    let old = parse(base());
    let mut v = base();
    v["budgets"][0]["periodSeconds"] = json!(120);
    assert!(needs_version_bump(&old, &parse(v)));
}

#[test]
fn changing_only_a_cap_is_a_hot_change() {
    let old = parse(base());
    let mut v = base();
    v["budgets"][0]["cap"] = json!(50);
    assert!(!needs_version_bump(&old, &parse(v)));
}

#[test]
fn an_assumed_cap_is_enforced_below_what_it_claims() {
    assert_eq!(effective_cap(100.0, Confidence::Documented), 100.0);
    assert_eq!(effective_cap(100.0, Confidence::Assumed), 70.0);
}

#[test]
fn rolling_on_kv_warns_because_kv_is_a_fixed_window() {
    let mut v = base();
    v["budgets"][0]["store"] = json!("kv");
    v["budgets"][0]["alignment"] = json!("rolling");
    assert!(warnings(&parse(v)).iter().any(|p| p.rule == "kv-rolling"));
}

#[test]
fn two_lanes_cannot_both_claim_the_whole_ceiling() {
    // The rule that came out of a load run: each lane is its own partition with
    // its own counters, so two ceilings enforce the ceiling twice.
    let mut v = base();
    v["lanes"] = json!([
        { "name": "urgent", "cap": "ceiling", "concurrency": 4, "default": true },
        { "name": "bulk", "cap": "ceiling", "concurrency": 4 }
    ]);
    assert!(validate(&parse(v)).iter().any(|p| p.rule == "lane-shares"));
}

#[test]
fn reservations_may_not_oversubscribe_the_ceiling() {
    let mut v = base();
    v["lanes"] = json!([
        { "name": "a", "cap": "share:0.7", "concurrency": 4, "default": true },
        { "name": "b", "cap": "share:0.6", "concurrency": 4 }
    ]);
    assert!(validate(&parse(v)).iter().any(|p| p.rule == "lane-shares"));
}

#[test]
fn a_derived_lane_needs_a_floor_or_it_admits_nothing() {
    let mut v = base();
    v["lanes"] = json!([
        { "name": "urgent", "cap": "ceiling", "concurrency": 4, "default": true },
        { "name": "bulk", "cap": "ceiling-minus-measured", "concurrency": 4 }
    ]);
    assert!(validate(&parse(v)).iter().any(|p| p.rule == "lane-floor"));
}

#[test]
fn shares_divide_the_ceiling_rather_than_replicating_it() {
    let mut v = base();
    v["lanes"] = json!([
        { "name": "urgent", "cap": "ceiling", "concurrency": 4, "default": true },
        { "name": "bulk", "cap": "ceiling-minus-measured", "concurrency": 4, "floor": 0.25 }
    ]);
    let s = parse(v);
    assert_eq!(validate(&s), vec![]);
    let urgent = s.lane_share("urgent", None);
    let bulk = s.lane_share("bulk", None);
    assert!((urgent - 0.75).abs() < 1e-9, "urgent got {urgent}");
    assert!((bulk - 0.25).abs() < 1e-9, "bulk got {bulk}");
    assert!((urgent + bulk - 1.0).abs() < 1e-9, "shares must sum to the ceiling");
}

#[test]
fn a_lease_as_long_as_the_window_warns_because_it_beats_against_it() {
    // Measured: 200/s over a 1s window with a 1s lease held 152/s; the same
    // ceiling over a 10s window held 205/s.
    let v = base(); // 60s window, 1s lease — five times over, so quiet
    assert!(!warnings(&parse(v)).iter().any(|p| p.rule == "lease-beats-window"));

    let mut v = base();
    v["budgets"][0]["periodSeconds"] = json!(1);
    v["pacing"] = json!({ "leaseSeconds": 1, "batch": 200 });
    let w = warnings(&parse(v));
    assert!(w.iter().any(|p| p.rule == "lease-beats-window"), "{w:?}");
    assert!(
        w.iter().any(|p| p.detail.contains("lease floor is one second")),
        "a one-second window should say it cannot do better"
    );
}

#[test]
fn shares_never_oversubscribe_whatever_the_meter_says() {
    // The property a load run broke: with the meter live, `ceiling` subtracted
    // static reservations while `ceiling-minus-measured` subtracted measured
    // spend, and neither knew what the other had taken. 5000 per ten seconds
    // carried 7131.
    let mut v = base();
    v["lanes"] = json!([
        { "name": "urgent", "cap": "ceiling", "concurrency": 4, "default": true },
        { "name": "bulk", "cap": "ceiling-minus-measured", "concurrency": 8, "floor": 0.5 }
    ]);
    let s = parse(v);
    for i in 0..=20 {
        let m = i as f64 / 20.0;
        let urgent = s.lane_share("urgent", None);
        let bulk = s.lane_share("bulk", Some(m));
        assert!(
            urgent + bulk <= 1.0 + 1e-9,
            "measured {m}: urgent {urgent} + bulk {bulk} = {} over the ceiling",
            urgent + bulk
        );
        assert!(bulk >= 0.5 - 1e-9, "measured {m}: bulk fell below its floor at {bulk}");
    }
}

#[test]
fn a_derived_lane_gets_the_residual_and_the_meter_can_only_shrink_it() {
    // What the name promises — take back an idle neighbour's capacity — is not
    // implementable here: each lane is its own partition, there is no channel
    // to tell a borrower to give capacity back, and an idle neighbour is still
    // entitled to its allocation the moment it wakes. So the measurement may
    // shrink this lane and never grow it past its residual.
    let mut v = base();
    v["lanes"] = json!([
        { "name": "urgent", "cap": "ceiling", "concurrency": 4, "default": true },
        { "name": "bulk", "cap": "ceiling-minus-measured", "concurrency": 8, "floor": 0.2 }
    ]);
    let s = parse(v);
    let urgent = s.lane_share("urgent", None);
    assert!((urgent - 0.8).abs() < 1e-9, "urgent should hold its allocation, got {urgent}");
    // An idle neighbour does NOT hand its share over.
    assert!((s.lane_share("bulk", Some(0.0)) - 0.2).abs() < 1e-9);
    // And a busy one cannot push this lane below its floor.
    assert!((s.lane_share("bulk", Some(1.0)) - 0.2).abs() < 1e-9);
}

/// The application is in the queue name AND in queen's own namespace field.
/// The name is a convention this codebase invented; the namespace is a thing
/// the broker keeps, and only the second one shows up in queen's console.
#[test]
fn namespace_is_per_application() {
    let mut v = base();
    v["application"] = json!("finance");
    v["name"] = json!("stripe");
    let spec = parse(v);
    assert_eq!(spec.namespace(), "gate.finance");
    assert_eq!(spec.push_queue(), "gate.finance.stripe.push");
    assert_eq!(spec.calls_queue(), "gate.finance.stripe.calls");
    assert_eq!(
        spec.admitted_queue("urgent"),
        "gate.finance.stripe.admitted.urgent"
    );
    // Two applications, same target name: different namespaces, and nothing
    // they own can collide.
    let mut w = base();
    w["application"] = json!("channel-manager");
    w["name"] = json!("stripe");
    let other = parse(w);
    assert_ne!(spec.namespace(), other.namespace());
    assert_ne!(spec.push_queue(), other.push_queue());
}
