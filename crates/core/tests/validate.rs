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

    // This test used to end by moving the budget to kv and asserting the spec
    // then passed — which is how the defect got a test of its own. The kv pool
    // keys on `(application, id)` and DROPS the scope, so the per-key limit
    // silently became one shared counter. The escape hatch for cardinality is
    // sharding, not kv.
    let mut on_kv = v.clone();
    on_kv["budgets"][0]["store"] = json!("kv");
    let problems = validate(&parse(on_kv));
    assert!(
        problems.iter().any(|p| p.rule == "kv-scope"),
        "a scoped budget on kv must be refused, not recommended: {problems:?}"
    );

    // Sharded, the same 200,000 keys fit: 64 documents of 3,125.
    let mut sharded = v.clone();
    sharded["shardBy"] = json!("entity");
    sharded["shards"] = json!(64);
    assert_eq!(validate(&parse(sharded)), vec![]);
}

#[test]
fn a_kv_budget_may_not_carry_a_match_either() {
    // The pool is spent from a local lease before any op is looked at, so a
    // selector on it charges every op and reads as though it selected.
    let mut v = base();
    v["budgets"][0]["store"] = json!("kv");
    v["budgets"][0]["match"] = json!({ "op": ["message.post"] });
    assert!(validate(&parse(v)).iter().any(|p| p.rule == "kv-match"));
}

#[test]
fn a_kv_budget_too_small_to_chunk_would_deadlock_at_zero() {
    // chunk = cap / periodSeconds, topped up below chunk/2. At cap < 2 x period
    // the chunk is one, half of it is zero, and the top-up never fires.
    let mut v = base();
    v["budgets"][0] = json!({ "id": "shared", "cap": 100, "periodSeconds": 60,
                              "alignment": "calendar", "store": "kv",
                              "confidence": "inferred" });
    assert!(validate(&parse(v.clone())).iter().any(|p| p.rule == "kv-chunk"));

    v["budgets"][0]["cap"] = json!(120);
    assert!(!validate(&parse(v)).iter().any(|p| p.rule == "kv-chunk"));
}

#[test]
fn a_ceiling_nobody_claims_is_a_ceiling_enforced_twice() {
    // T6: with no lane on `ceiling`, `ceiling-minus-measured` reads the residual
    // as its own — so two derived lanes each claim everything the other did not
    // reserve. This validated clean until the rule existed.
    let mut v = base();
    v["lanes"] = json!([
        { "name": "urgent", "cap": "ceiling-minus-measured", "concurrency": 4, "floor": 0.1,
          "default": true },
        { "name": "bulk", "cap": "ceiling-minus-measured", "concurrency": 4, "floor": 0.1 }
    ]);
    let s = parse(v);
    let problems = validate(&s);
    assert!(problems.iter().any(|p| p.rule == "lane-shares"), "{problems:?}");
    // And the reason, measured: the shares add up to nearly two ceilings.
    let total: f64 = s.lanes.iter().map(|l| s.lane_share(&l.name, None)).sum();
    assert!(total > 1.0 + 1e-9, "the defect this refuses should be visible: {total}");
}

#[test]
fn reservations_that_sum_to_one_need_no_taker() {
    // The fix the refusal recommends: either a lane claims the ceiling, or the
    // reservations close it themselves.
    let mut v = base();
    v["lanes"] = json!([
        { "name": "urgent", "cap": "share:0.4", "concurrency": 4, "default": true },
        { "name": "bulk", "cap": "share:0.6", "concurrency": 4 }
    ]);
    assert_eq!(validate(&parse(v)), vec![]);
}

/// The property the code's comments claimed and nothing held: whatever a spec
/// declares, if it validates clean its lanes divide ONE ceiling.
#[test]
fn every_spec_that_validates_clean_divides_exactly_one_ceiling() {
    let policies = ["ceiling", "ceiling-minus-measured", "share:0.25", "share:0.5",
                    "share:0.75", "absolute:10"];
    let floors = [0.0, 0.1, 0.25, 0.5, 1.0];
    let mut clean = 0usize;
    for a in policies {
        for b in policies {
            for fa in floors {
                for fb in floors {
                    let mut v = base();
                    v["lanes"] = json!([
                        { "name": "urgent", "cap": a, "concurrency": 4, "floor": fa,
                          "default": true },
                        { "name": "bulk", "cap": b, "concurrency": 4, "floor": fb }
                    ]);
                    let s = parse(v);
                    if !validate(&s).is_empty() {
                        continue;
                    }
                    clean += 1;
                    // Every measurement, including none: the meter may only
                    // shrink a derived lane, so `None` is the worst case.
                    for m in 0..=20 {
                        let measured = Some(m as f64 / 20.0);
                        for measured in [None, measured] {
                            let total: f64 = s
                                .lanes
                                .iter()
                                .map(|l| s.lane_share(&l.name, measured))
                                .sum();
                            assert!(
                                total <= 1.0 + 1e-9,
                                "`{a}`(floor {fa}) + `{b}`(floor {fb}) at measured {measured:?} \
                                 divide {total} ceilings"
                            );
                        }
                    }
                }
            }
        }
    }
    assert!(clean > 5, "the property is vacuous if almost nothing validates: {clean}");
}

#[test]
fn a_sharded_target_may_not_hold_an_unscoped_budget() {
    // G7: one counter per shard is the cap enforced `shards` times.
    let mut v = base();
    v["shardBy"] = json!("entity");
    v["shards"] = json!(8);
    let problems = validate(&parse(v.clone()));
    assert!(problems.iter().any(|p| p.rule == "shard-scope"), "{problems:?}");

    v["budgets"][0]["scope"] = json!(["entity"]);
    v["budgets"][0]["maxKeys"] = json!(1000);
    assert_eq!(validate(&parse(v)), vec![]);
}

#[test]
fn shards_are_partitions_so_the_count_is_bounded_and_named() {
    let mut v = base();
    v["shards"] = json!(8);
    assert!(validate(&parse(v)).iter().any(|p| p.rule == "shard-count"));

    let mut v = base();
    v["shardBy"] = json!("entity");
    v["shards"] = json!(100_000);
    v["budgets"][0]["scope"] = json!(["entity"]);
    v["budgets"][0]["maxKeys"] = json!(1000);
    assert!(validate(&parse(v)).iter().any(|p| p.rule == "shard-count"));
}

#[test]
fn re_sharding_re_founds_the_counters_and_needs_a_version() {
    let mut a = base();
    a["shardBy"] = json!("entity");
    a["shards"] = json!(8);
    a["budgets"][0]["scope"] = json!(["entity"]);
    a["budgets"][0]["maxKeys"] = json!(1000);
    let mut b = a.clone();
    b["shards"] = json!(16);
    assert!(needs_version_bump(&parse(a.clone()), &parse(b)));
    // The same shape is not a migration.
    assert!(!needs_version_bump(&parse(a.clone()), &parse(a)));
}

#[test]
fn a_shard_is_a_partition_and_a_key_lands_in_exactly_one() {
    let mut v = base();
    v["shardBy"] = json!("entity");
    v["shards"] = json!(64);
    v["budgets"][0]["scope"] = json!(["entity"]);
    v["budgets"][0]["maxKeys"] = json!(200_000);
    let s = parse(v);
    assert_eq!(s.shard_count(), 64);
    // Stable, and the same number the gate's own partitioner computes.
    let a = s.shard_of("listing-1");
    assert_eq!(a, s.shard_of("listing-1"));
    assert_eq!(s.push_partition("bulk", a), format!("bulk:{a}"));
    assert!((0..64).all(|i| s.lane_partitions("bulk").contains(&format!("bulk:{i}"))));
    // Spread, not a single bucket: a hash that piles up serialises everything.
    let buckets: std::collections::HashSet<u32> =
        (0..500).map(|i| s.shard_of(&format!("listing-{i}"))).collect();
    assert!(buckets.len() > 40, "500 keys landed in only {} of 64 shards", buckets.len());

    // Unsharded, the partition IS the lane — which is why nothing about an
    // existing target changes.
    let plain = parse(base());
    assert_eq!(plain.shard_count(), 1);
    assert_eq!(plain.push_partition("bulk", 0), "bulk");
}

#[test]
fn a_graph_node_is_a_target_whose_name_carries_a_dot() {
    let mut v = base();
    v["name"] = json!("airbnb.messages");
    assert_eq!(validate(&parse(v)), vec![]);

    let mut bad = base();
    bad["name"] = json!("airbnb..messages");
    assert!(validate(&parse(bad)).iter().any(|p| p.rule == "name"));

    let mut bad = base();
    bad["name"] = json!("Airbnb.Messages");
    assert!(validate(&parse(bad)).iter().any(|p| p.rule == "name"));
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

#[test]
fn a_kv_budget_must_lease_enough_for_one_item() {
    // `cost-fits` compares an item against the whole cap, which is right for a budget
    // the gate holds in memory. A kv budget is spent from a LEASE of one second's
    // rate, so an item costing more than that lease can never be admitted and blocks
    // the head of its lane for ever — the very failure cost-fits exists to prevent.
    let mut v = base();
    v["budgets"][0] = json!({ "id": "shared", "cap": 1000, "periodSeconds": 60,
                              "alignment": "calendar", "store": "kv",
                              "confidence": "inferred" });
    v["cost"] = json!({ "field": "c", "default": 1, "max": 100 });
    let problems = validate(&parse(v.clone()));
    assert!(problems.iter().any(|p| p.rule == "kv-chunk"), "{problems:?}");
    assert!(
        !problems.iter().any(|p| p.rule == "cost-fits"),
        "cost-fits sees a cap of 1000 and is happy; the lease is the real limit: {problems:?}"
    );

    // A rate of 100 per second leases 100 per top-up, which fits the item.
    v["budgets"][0]["periodSeconds"] = json!(10);
    assert!(!validate(&parse(v)).iter().any(|p| p.rule == "kv-chunk"));
}
