use gate_core::*;
use serde_json::{json, Value};

fn budget(id: &str, cap: f64, period: i64, alignment: Alignment) -> Budget {
    Budget {
        id: id.into(),
        cap,
        period_seconds: period,
        alignment,
        matcher: None,
        scope: vec![],
        max_keys: None,
        store: Store::Gate,
        confidence: Confidence::Documented,
        source: Some("test".into()),
        as_of: Some("2026-08-18".into()),
    }
}

fn spec(budgets: Vec<Budget>) -> TargetSpec {
    TargetSpec {
        application: "app".into(),
        name: "t".into(),
        version: 1,
        egress: None,
        budgets,
        lanes: vec![Lane {
            name: "bulk".into(),
            cap: CapPolicy::Ceiling,
            concurrency: 1,
            floor: 0.0,
            default: true,
        }],
        cost: Cost { field: "cost".into(), default: 1.0, max: 1.0 },
        pacing: Pacing::default(),
        admitted: Admitted::default(),
        shard_by: None,
        shards: None,
    }
}


fn item(op: &str, cost: f64) -> Item {
    Item { op: op.into(), cost, scope: vec![] }
}

#[test]
fn a_calendar_window_admits_exactly_its_cap_then_refuses() {
    let s = spec(vec![budget("m", 5.0, 60, Alignment::Calendar)]);
    let mut st = Value::Null;
    let now = 1_000_000;
    for i in 0..5 {
        assert!(decide(&s, None, &mut st, now, &item("x", 1.0)).is_admit(), "call {i}");
    }
    match decide(&s, None, &mut st, now, &item("x", 1.0)) {
        Decision::Deny(d) => {
            assert_eq!(d.budget_id, "m");
            assert_eq!(d.reason, Reason::Limit);
            assert!(d.retry_after_ms > 0 && d.retry_after_ms <= 60_000);
        }
        Decision::Admit => panic!("admitted over cap"),
    }
}

#[test]
fn a_calendar_window_starts_empty_after_it_rotates() {
    let s = spec(vec![budget("m", 2.0, 60, Alignment::Calendar)]);
    let mut st = Value::Null;
    let t0 = 60_000;
    assert!(decide(&s, None, &mut st, t0, &item("x", 2.0)).is_admit());
    assert!(!decide(&s, None, &mut st, t0, &item("x", 1.0)).is_admit());
    // Next minute: the row is recycled, not created, and it starts at zero.
    assert!(decide(&s, None, &mut st, t0 + 60_000, &item("x", 2.0)).is_admit());
}

#[test]
fn a_rolling_window_lets_the_previous_window_decay() {
    // 10 per second, and the second starts on a boundary.
    let s = spec(vec![budget("r", 10.0, 1, Alignment::Rolling)]);
    let mut st = Value::Null;
    let t0 = 5_000_000;
    for _ in 0..10 {
        assert!(decide(&s, None, &mut st, t0, &item("x", 1.0)).is_admit());
    }
    assert!(!decide(&s, None, &mut st, t0, &item("x", 1.0)).is_admit());
    // Half of the next window in, half of the old spend still counts, so half
    // the cap is free — not the whole of it, which is what a fixed window would
    // wrongly give back at the edge.
    let mid = t0 + 1_500;
    let mut admitted = 0;
    for _ in 0..10 {
        if decide(&s, None, &mut st, mid, &item("x", 1.0)).is_admit() {
            admitted += 1;
        }
    }
    assert_eq!(admitted, 5, "expected half the cap back at the halfway point");
}

#[test]
fn a_rolling_window_holds_the_ceiling_under_saturation() {
    // The property that matters: hammered flat out, no ten-second window may
    // carry meaningfully more than the cap. A token bucket with burst = cap
    // fails this by a factor of two on the first window, which is why this is
    // a sliding-window counter instead.
    let cap = 100usize;
    let s = spec(vec![budget("r", cap as f64, 10, Alignment::Rolling)]);
    let mut st = Value::Null;
    let mut admitted_at: Vec<i64> = Vec::new();
    for ms in 0..60_000i64 {
        if decide(&s, None, &mut st, ms, &item("x", 1.0)).is_admit() {
            admitted_at.push(ms);
        }
    }
    // Worst ten-second window anywhere in the run.
    let mut worst = 0usize;
    for (i, start) in admitted_at.iter().enumerate() {
        let n = admitted_at[i..]
            .iter()
            .take_while(|t| **t - start < 10_000)
            .count();
        worst = worst.max(n);
    }
    assert!(
        worst <= cap,
        "a 10s window carried {worst} admissions against a cap of {cap}"
    );
    // And it is not throttling to nothing: the long-run rate is the ceiling.
    assert!(
        admitted_at.len() >= cap * 5,
        "only {} admitted in 60s against {}/10s",
        admitted_at.len(),
        cap
    );
}

#[test]
fn nested_windows_all_must_admit_and_the_tightest_binds() {
    let s = spec(vec![
        budget("wide", 1000.0, 60, Alignment::Calendar),
        budget("tight", 3.0, 60, Alignment::Calendar),
    ]);
    let mut st = Value::Null;
    let now = 120_000;
    for _ in 0..3 {
        assert!(decide(&s, None, &mut st, now, &item("x", 1.0)).is_admit());
    }
    match decide(&s, None, &mut st, now, &item("x", 1.0)) {
        Decision::Deny(d) => assert_eq!(d.budget_id, "tight"),
        Decision::Admit => panic!("admitted over the tightest cap"),
    }
}

#[test]
fn a_denial_charges_nothing_at_all() {
    // The two-pass property. `wide` matches and would have admitted; because
    // `tight` refuses, `wide` must be left untouched — a partial charge here is
    // budget lost forever, since a denied message's state is discarded anyway.
    let s = spec(vec![
        budget("wide", 1000.0, 60, Alignment::Calendar),
        budget("tight", 1.0, 60, Alignment::Calendar),
    ]);
    let mut st = Value::Null;
    let now = 180_000;
    assert!(decide(&s, None, &mut st, now, &item("x", 1.0)).is_admit());
    let after_one = st.clone();
    assert!(!decide(&s, None, &mut st, now, &item("x", 1.0)).is_admit());
    assert_eq!(st, after_one, "a denied item mutated state");
}

#[test]
fn cost_is_weighted_not_counted() {
    let s = spec(vec![budget("m", 10.0, 60, Alignment::Calendar)]);
    let mut st = Value::Null;
    let now = 240_000;
    assert!(decide(&s, None, &mut st, now, &item("x", 7.0)).is_admit());
    assert!(!decide(&s, None, &mut st, now, &item("x", 4.0)).is_admit());
    assert!(decide(&s, None, &mut st, now, &item("x", 3.0)).is_admit());
}

#[test]
fn an_item_costing_more_than_a_cap_is_unsatisfiable_not_merely_denied() {
    let s = spec(vec![budget("m", 5.0, 60, Alignment::Calendar)]);
    let mut st = Value::Null;
    match decide(&s, None, &mut st, 0, &item("x", 6.0)) {
        Decision::Deny(d) => assert_eq!(d.reason, Reason::Unsatisfiable),
        Decision::Admit => panic!("admitted an item bigger than the cap"),
    }
}

#[test]
fn match_selects_on_op_and_a_bare_budget_takes_everything() {
    let mut messaging = budget("messaging", 1.0, 60, Alignment::Calendar);
    messaging.matcher = Some(Match { op: vec!["messaging.send".into()] });
    let s = spec(vec![budget("all", 100.0, 60, Alignment::Calendar), messaging]);
    let mut st = Value::Null;
    let now = 300_000;
    assert!(decide(&s, None, &mut st, now, &item("messaging.send", 1.0)).is_admit());
    // The messaging budget is spent; a different op still goes through.
    assert!(!decide(&s, None, &mut st, now, &item("messaging.send", 1.0)).is_admit());
    assert!(decide(&s, None, &mut st, now, &item("listing.update", 1.0)).is_admit());
}

#[test]
fn a_glob_matches_on_a_whole_segment() {
    let m = Match { op: vec!["listing.*".into()] };
    assert!(m.matches("listing.create"));
    assert!(m.matches("listing.rooms"));
    assert!(!m.matches("listings.create"));
    assert!(!m.matches("listing"));
}

#[test]
fn scope_gives_one_counter_per_key() {
    let mut b = budget("per-host", 1.0, 3600, Alignment::Calendar);
    b.scope = vec![Dim::Host];
    b.max_keys = Some(100);
    let s = spec(vec![b]);
    let mut st = Value::Null;
    let now = 400_000;
    let mk = |host: &str| Item {
        op: "x".into(),
        cost: 1.0,
        scope: vec![(Dim::Host, host.into())],
    };
    assert!(decide(&s, None, &mut st, now, &mk("a")).is_admit());
    assert!(!decide(&s, None, &mut st, now, &mk("a")).is_admit());
    assert!(decide(&s, None, &mut st, now, &mk("b")).is_admit());
}

#[test]
fn a_missing_scope_value_is_a_refusal_not_a_zero() {
    let mut b = budget("per-host", 10.0, 60, Alignment::Calendar);
    b.scope = vec![Dim::Host];
    b.max_keys = Some(100);
    let s = spec(vec![b]);
    let mut st = Value::Null;
    match decide(&s, None, &mut st, 0, &item("x", 1.0)) {
        Decision::Deny(d) => assert_eq!(d.reason, Reason::MissingScope(Dim::Host)),
        Decision::Admit => panic!("counted against a key it did not have"),
    }
}

#[test]
fn a_lane_cap_composes_with_the_target_budgets() {
    let s = spec(vec![budget("wide", 10_000.0, 60, Alignment::Calendar)]);
    let mut st = Value::Null;
    let now = 500_000;
    // Two per second, with a one second lease.
    for _ in 0..2 {
        assert!(decide(&s, Some(2.0), &mut st, now, &item("x", 1.0)).is_admit());
    }
    assert!(!decide(&s, Some(2.0), &mut st, now, &item("x", 1.0)).is_admit());
}

#[test]
fn utilisation_reads_the_same_in_both_alignments() {
    let cal = budget("c", 10.0, 60, Alignment::Calendar);
    let roll = budget("r", 10.0, 60, Alignment::Rolling);
    let s = spec(vec![cal.clone(), roll.clone()]);
    let mut st = Value::Null;
    let now = 600_000;
    for _ in 0..5 {
        assert!(decide(&s, None, &mut st, now, &item("x", 1.0)).is_admit());
    }
    assert!((utilisation(&cal, &st, "-", now) - 0.5).abs() < 1e-9);
    assert!((utilisation(&roll, &st, "-", now) - 0.5).abs() < 1e-9);
}

#[test]
fn the_spec_round_trips_through_json() {
    let raw = json!({
        "application": "channel-manager",
        "name": "airbnb",
        "version": 3,
        "budgets": [{
            "id": "ip-10s", "cap": 2000, "periodSeconds": 10,
            "alignment": "rolling", "confidence": "documented",
            "source": "developer.withairbnb.com", "asOf": "2026-05-19"
        }],
        "lanes": [{ "name": "bulk", "cap": "ceiling-minus-measured",
                    "concurrency": 24, "floor": 0.2, "default": true }],
        "cost": { "field": "httpCost", "default": 1, "max": 400 },
        "pacing": { "leaseSeconds": 1, "batch": 200 }
    });
    let s: TargetSpec = serde_json::from_value(raw).expect("parse");
    assert_eq!(s.application, "channel-manager");
    assert_eq!(s.key(), "channel-manager/airbnb");
    assert_eq!(s.budgets[0].alignment, Alignment::Rolling);
    assert_eq!(s.lanes[0].cap, CapPolicy::CeilingMinusMeasured);
    let back = serde_json::to_value(&s).unwrap();
    let again: TargetSpec = serde_json::from_value(back).unwrap();
    assert_eq!(s, again);
}

#[test]
fn alignment_has_no_default_so_omitting_it_fails_loudly() {
    let raw = json!({
        "name": "t", "version": 1,
        "budgets": [{ "id": "b", "cap": 10, "periodSeconds": 60, "confidence": "inferred" }],
        "lanes": [{ "name": "bulk", "cap": "ceiling", "concurrency": 1, "default": true }],
        "cost": { "field": "c", "default": 1, "max": 1 }
    });
    assert!(serde_json::from_value::<TargetSpec>(raw).is_err());
}

#[test]
fn an_omitted_application_lands_in_default_so_the_concept_stays_invisible() {
    let raw = json!({
        "name": "t", "version": 1,
        "budgets": [{ "id": "b", "cap": 10, "periodSeconds": 60,
                      "alignment": "calendar", "confidence": "inferred" }],
        "lanes": [{ "name": "bulk", "cap": "ceiling", "concurrency": 1, "default": true }],
        "cost": { "field": "c", "default": 1, "max": 1 }
    });
    let s: TargetSpec = serde_json::from_value(raw).expect("parse");
    assert_eq!(s.application, "default");
}

#[test]
fn two_teams_may_both_own_something_called_airbnb() {
    // The identity is the pair. Without that, the second team's declare would
    // silently take over the first's queues, gate state and stored spec.
    let mut a = spec(vec![budget("b", 10.0, 60, Alignment::Calendar)]);
    a.application = "channel-manager".into();
    a.name = "airbnb".into();
    let mut b = a.clone();
    b.application = "finance".into();

    assert_ne!(a.key(), b.key());
    assert_ne!(a.push_queue(), b.push_queue());
    assert_ne!(a.query_id("bulk"), b.query_id("bulk"));
    assert_ne!(a.calls_queue(), b.calls_queue());
    assert_eq!(a.push_queue(), "gate.channel-manager.airbnb.push");
}

/// Nothing pruned the state document. `maxKeys` is checked once, at declare
/// time, and never again — so a budget keyed on a listing wrote one cell per
/// listing and kept every one of them for ever, in a document that is re-read
/// whole on every cycle.
#[test]
fn cells_whose_window_has_closed_stop_being_carried() {
    let mut b = budget("weekly", 100.0, 60, Alignment::Rolling);
    b.scope = vec![Dim::Entity];
    b.max_keys = Some(1000);
    let s = spec(vec![b.clone()]);

    let mut state = json!({});
    let period = 60_000i64;
    // Three listings spend in window W.
    for key in ["listing-1", "listing-2", "listing-3"] {
        let it = Item {
            op: "photo.delete".into(),
            cost: 1.0,
            scope: vec![(Dim::Entity, key.to_string())],
        };
        assert!(decide(&s, None, &mut state, period * 10, &it).is_admit());
    }
    assert_eq!(key_count(&b, &state), 3);

    // Two periods on, one live listing spends again. The three stale cells can no
    // longer change any decision — a rolling window reads its own window and the
    // one before — so they go.
    let live = Item {
        op: "photo.delete".into(),
        cost: 1.0,
        scope: vec![(Dim::Entity, "listing-9".to_string())],
    };
    assert!(decide(&s, None, &mut state, period * 12, &live).is_admit());
    assert_eq!(key_count(&b, &state), 1, "stale cells: {state}");
    assert!(state["b"]["weekly"].get("listing-9").is_some());

    // And the previous window is NOT stale: a rolling budget carries its tail.
    let mut state = json!({});
    let it = Item {
        op: "photo.delete".into(),
        cost: 40.0,
        scope: vec![(Dim::Entity, "listing-1".to_string())],
    };
    assert!(decide(&s, None, &mut state, period * 10, &it).is_admit());
    let next_window = Item { cost: 1.0, ..it.clone() };
    assert!(decide(&s, None, &mut state, period * 11, &next_window).is_admit());
    assert_eq!(
        key_count(&b, &state),
        1,
        "the previous window still decides, so its cell must survive"
    );
    assert!(utilisation(&b, &state, "listing-1", period * 11) > 0.0);
}

/// The sweep runs once per cycle, not once per message: the gate's clock is
/// sampled per cycle, so a whole batch shares it.
#[test]
fn the_sweep_costs_one_pass_per_cycle_and_leaves_the_live_batch_alone() {
    let s = spec(vec![budget("b", 100.0, 60, Alignment::Rolling)]);
    let mut state = json!({});
    let now = 60_000i64 * 5;
    for _ in 0..50 {
        assert!(decide(&s, None, &mut state, now, &item("x", 1.0)).is_admit());
    }
    assert_eq!(state["b"]["b"]["-"]["u"], json!(50.0));
    // The cycle clock is remembered in a plain field, because `__` belongs to the
    // stream runtime.
    assert_eq!(state["t"], json!(now));
}

/// A budget the spec no longer declares can never be read again: nothing asks
/// for a counter by a name that is not in the spec, so the slot is dead weight.
#[test]
fn a_budget_that_left_the_spec_takes_its_counters_with_it() {
    let s = spec(vec![budget("b", 100.0, 60, Alignment::Rolling)]);
    let mut state = json!({ "b": { "b": { "-": { "w": 5.0, "u": 1.0, "p": 0.0 } },
                                   "gone": { "-": { "w": 5.0, "u": 90.0, "p": 0.0 } } } });
    assert!(decide(&s, None, &mut state, 60_000 * 5, &item("x", 1.0)).is_admit());
    assert!(state["b"].get("gone").is_none(), "{state}");
    assert!(state["b"].get("b").is_some());
}

/// A scoped budget has no `-` cell, so asking for one reported 0% while the
/// budget was refusing work. The worst key is the honest number.
#[test]
fn the_busiest_key_is_what_a_scoped_budget_is_spending() {
    let mut b = budget("per-listing", 10.0, 60, Alignment::Calendar);
    b.scope = vec![Dim::Entity];
    b.max_keys = Some(100);
    let s = spec(vec![b.clone()]);
    let mut state = json!({});
    let now = 60_000i64 * 3;
    for (key, n) in [("a", 2), ("b", 9)] {
        for _ in 0..n {
            let it = Item {
                op: "x".into(),
                cost: 1.0,
                scope: vec![(Dim::Entity, key.to_string())],
            };
            assert!(decide(&s, None, &mut state, now, &it).is_admit());
        }
    }
    assert_eq!(utilisation(&b, &state, "-", now), 0.0);
    assert!((utilisation_max(&b, &state, now) - 0.9).abs() < 1e-9);
    assert_eq!(key_count(&b, &state), 2);
}

/// The plan's phase-5 test: per-shard state stays under its key bound across windows.
///
/// A shard IS a state document — one per push partition, each with its own gate runner and
/// its own single writer — so the bound that matters is per document, and the sweep is what
/// holds it there over time. Nothing combined sharding with expiry.
///
/// Two things are narrower than they first look, and the test says both rather than
/// asserting a bound that is not there:
///
/// * `maxKeys / shards` is a MEAN. A hash ring does not promise an even split, so one
///   document can sit above that figure while the declaration is honoured in aggregate.
/// * a ROLLING budget reads its own window and the one before it, so a document holds up to
///   two windows' worth of keys. `maxKeys` is therefore a statement about the live set, not
///   about a single window.
///
/// What the sweep guarantees, and what the failure was, is the third assertion: the live set
/// stops growing. Before it, a document gained one cell per key seen and never gave one back.
#[test]
fn a_sharded_budget_holds_its_key_bound_across_windows() {
    const SHARDS: u32 = 8;
    const MAX_KEYS: u64 = 400;
    // Two windows of these have to fit inside the declaration, because two windows is what
    // a rolling budget can still read.
    const KEYS_PER_WINDOW: usize = 150;
    let mut b = budget("per-listing", 5.0, 60, Alignment::Rolling);
    b.scope = vec![Dim::Entity];
    b.max_keys = Some(MAX_KEYS);
    let mut s = spec(vec![b.clone()]);
    s.shard_by = Some(Dim::Entity);
    s.shards = Some(SHARDS);
    // The declared shape is the one the validator accepts, or this proves nothing.
    assert_eq!(validate(&s), vec![]);

    // One document per shard, exactly as the runners hold them.
    let mut states: Vec<Value> = (0..SHARDS).map(|_| json!({})).collect();
    let period = 60_000i64;
    let mut steady = None;

    // Five windows of traffic, a fresh set of listings in each. Nothing ever revisits an old
    // key, which is the pattern that used to grow the document without bound — `maxKeys` is
    // checked once at declare time and never again.
    for window in 0..5i64 {
        for i in 0..KEYS_PER_WINDOW {
            let key = format!("listing-{window}-{i}");
            let shard = s.shard_of(&key) as usize;
            let item = Item {
                op: "photo.delete".into(),
                cost: 1.0,
                scope: vec![(Dim::Entity, key)],
            };
            // Denials are expected — five per listing per minute — and charge nothing.
            let _ = decide(&s, None, &mut states[shard], period * (10 + window), &item);
        }

        // Every key lands in exactly one document: the shards partition the live set rather
        // than each holding a copy of it, which is the whole single-writer argument.
        let total: usize = states.iter().map(|st| key_count(&b, st)).sum();
        let live_windows = (window + 1).min(2) as usize;
        assert_eq!(
            total,
            KEYS_PER_WINDOW * live_windows,
            "window {window}: the documents hold something other than the live set"
        );
        assert!(
            total <= MAX_KEYS as usize,
            "window {window}: {total} keys across the shards, above the {MAX_KEYS} declared"
        );

        // From the second window on it is a steady state, not a staircase.
        if window >= 1 {
            match steady {
                None => steady = Some(total),
                Some(before) => assert_eq!(
                    total, before,
                    "window {window}: the documents grew with history instead of holding the live set"
                ),
            }
        }
    }

    // And what survives is the last two windows, not an accumulation of every window that
    // ever ran.
    for (shard, state) in states.iter().enumerate() {
        let cells = state["b"]["per-listing"].as_object().cloned().unwrap_or_default();
        assert!(
            cells
                .keys()
                .all(|k| k.starts_with("listing-3-") || k.starts_with("listing-4-")),
            "shard {shard} is still carrying a closed window's keys: {:?}",
            cells.keys().take(3).collect::<Vec<_>>()
        );
    }
}
