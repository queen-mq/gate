use gate_core::{compile, needs_version_bump, validate, GraphDoc, Watch};
use serde_json::json;

fn doc() -> GraphDoc {
    serde_json::from_value(json!({
        "application": "test", "graph": "observe", "version": 1,
        "watch": {"durationSeconds": 86400},
        "nodes": {"limit": {"ingress": {"shed": true, "partitions": 4}, "egress": "test.out"}},
        "paths": [{"name": "main", "nodes": ["limit"]}]
    }))
    .unwrap()
}

#[test]
fn watch_needs_no_caps_and_always_enables_history_without_shedding() {
    let watching = doc();
    assert!(validate(&watching).is_empty());
    let plan = compile(&watching);
    assert!(plan.nodes["limit"].budgets.is_empty());
    assert!(!plan.nodes["limit"].ingress_shed);
    assert_eq!(plan.counters_window_seconds, Some(60));
    assert_eq!(plan.stages[0].concurrency, 4);
    assert_eq!(plan.stages[0].batch, 200);
    let mut enforced = watching;
    enforced.watch = None;
    assert!(validate(&enforced).iter().any(|p| p.rule == "node-budget"));
}

#[test]
fn watch_retains_candidate_limits_without_charging_them_or_changing_cursors() {
    let mut watching = doc();
    watching.nodes.get_mut("limit").unwrap().budgets = serde_json::from_value(json!([
        {"id": "candidate", "count": 1, "timeMs": 60000, "scopeBy": "payload.account"}
    ]))
    .unwrap();
    assert!(validate(&watching).is_empty());
    let watch_plan = compile(&watching);
    assert!(watch_plan.nodes["limit"].budgets.is_empty());
    let mut enforced = watching.clone();
    enforced.watch = None;
    enforced.nodes.get_mut("limit").unwrap().budgets[0].scope_by = None;
    let enforced_plan = compile(&enforced);
    assert_eq!(watch_plan.stages[0].source, enforced_plan.stages[0].source);
    assert_eq!(watch_plan.stages[0].group, enforced_plan.stages[0].group);
    assert_eq!(
        watch_plan.stages[0].destinations,
        enforced_plan.stages[0].destinations
    );
    assert!(needs_version_bump(&watching, &enforced));
    assert!(needs_version_bump(&enforced, &watching));
    assert!(!needs_version_bump(&watching, &watching));
    watching.nodes.get_mut("limit").unwrap().budgets[0].count = 0;
    assert!(validate(&watching).iter().any(|p| p.rule == "budget-count"));
}

#[test]
fn watch_period_is_bounded_and_expiry_does_not_activate_limits() {
    let mut watching = doc();
    for seconds in [0, 60, 119, gate_core::MAX_WATCH_SECONDS + 1] {
        watching.watch.as_mut().unwrap().duration_seconds = seconds;
        assert!(validate(&watching)
            .iter()
            .any(|p| p.rule == "watch-duration"));
    }
    watching.watch = Some(Watch {
        duration_seconds: 120,
        started_at: Some(1000),
    });
    assert_eq!(watching.watch.as_ref().unwrap().ends_at(), Some(121000));
    assert!(compile(&watching).nodes["limit"].budgets.is_empty());
    let serialized = serde_json::to_string(&watching).unwrap();
    assert_eq!(
        serde_json::from_str::<GraphDoc>(&serialized).unwrap(),
        watching
    );
    watching.watch = None;
    assert!(!serde_json::to_string(&watching).unwrap().contains("watch"));
}
