//! Console diagnostics and opt-in, minute-sampled queue history.
use crate::api::{find, ok, ApiResult, Fail, Shared};
use crate::registry::GraphRuntime;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::Ordering;
use std::time::Duration;

pub fn spawn(app: Shared) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(10));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last_minute = -1;
        let mut last_hour = -1;
        loop {
            tick.tick().await;
            let live: HashSet<_> = app.registry.all().iter().map(|g| g.key()).collect();
            app.conditions.lock().retain(|key, _| {
                live.contains(&key.split('/').take(2).collect::<Vec<_>>().join("/"))
            });
            let Some(history) = &app.history else {
                continue;
            };
            let recorders = app.observations.lock().clone();
            let mut done = HashSet::new();
            for rec in recorders {
                match history.flush_observation(&rec).await {
                    Ok(())
                        if (rec.closed_at.load(Ordering::Relaxed) > 0
                            || crate::now_ms() >= rec.until)
                            && rec.pending(i64::MAX).is_empty() =>
                    {
                        done.insert(rec.id.clone());
                    }
                    Ok(()) => {}
                    Err(error) => tracing::warn!(%error, "could not flush watch samples"),
                }
            }
            app.observations.lock().retain(|r| !done.contains(&r.id));
            let minute = crate::now_ms() / 60_000;
            if minute == last_minute {
                continue;
            }
            last_minute = minute;
            for graph in app.registry.all() {
                if graph.plan.counters_window_seconds.is_none() {
                    continue;
                }
                for node in graph.plan.nodes.keys() {
                    let body = match tokio::time::timeout(
                        Duration::from_secs(8),
                        sample_node(&app, &graph, node),
                    )
                    .await
                    {
                        Ok(Ok(body)) => body,
                        _ => json!({"available":false}),
                    };
                    if let Err(error) = history
                        .snapshot(
                            &graph.doc.application,
                            &graph.doc.graph,
                            node,
                            crate::now_ms(),
                            &body,
                        )
                        .await
                    {
                        tracing::warn!(%error, "could not persist queue sample");
                    }
                }
            }
            if minute / 60 != last_hour {
                history.prune_insights().await;
                last_hour = minute / 60;
            }
        }
    })
}

async fn sample_node(app: &Shared, rt: &GraphRuntime, name: &str) -> Result<Value, String> {
    let node = &rt.plan.nodes[name];
    let mut pending = 0u64;
    let mut oldest = 0i64;
    let mut age_known = true;
    let mut positions = BTreeMap::new();
    for stage in rt.stages_of_node(name) {
        let depth = app
            .depths
            .pending_of_group(&app.queen, &stage.stage.source, &stage.stage.group)
            .await
            .map_err(|e| e.to_string())?;
        let waiting: u64 = depth.values().sum();
        pending = pending.saturating_add(waiting);
        let group = app
            .queen
            .admin()
            .consumer_group(&stage.stage.group)
            .await
            .map_err(|e| e.to_string())?;
        let parts = group
            .get(&stage.stage.source)
            .and_then(|v| v["partitions"].as_array());
        if let Some(parts) = parts {
            for p in parts {
                if let (Some(part), Some(consumed), Some(lag)) = (
                    p["partition"].as_str(),
                    p["totalConsumed"].as_i64(),
                    p["offsetLag"].as_i64(),
                ) {
                    positions.insert(
                        format!("{}/{}/{}", stage.stage.source, stage.stage.group, part),
                        consumed.saturating_add(lag),
                    );
                }
            }
        }
        if waiting > 0 {
            // Missing group cursors must not turn a nonempty queue into age zero.
            match parts.and_then(|ps| {
                ps.iter()
                    .filter(|p| p["offsetLag"].as_u64().unwrap_or(0) > 0)
                    .filter_map(|p| p["timeLagSeconds"].as_i64())
                    .max()
            }) {
                Some(age) => oldest = oldest.max(age),
                None => age_known = false,
            }
        }
    }
    let mut workers = 0u64;
    if let Some(queue) = &node.egress_queue {
        let depth = if let Some(group) = &node.egress_group {
            app.depths.pending_of_group(&app.queen, queue, group).await
        } else {
            app.depths.pending(&app.queen, queue).await
        }
        .map_err(|e| e.to_string())?;
        workers = depth.values().sum();
        if workers > 0 {
            let age = if let Some(group) = &node.egress_group {
                let detail = app
                    .queen
                    .admin()
                    .consumer_group(group)
                    .await
                    .map_err(|e| e.to_string())?;
                detail
                    .get(queue)
                    .and_then(|v| v["partitions"].as_array())
                    .and_then(|ps| {
                        ps.iter()
                            .filter(|p| p["offsetLag"].as_u64().unwrap_or(0) > 0)
                            .filter_map(|p| p["timeLagSeconds"].as_i64())
                            .max()
                    })
            } else {
                let detail = app
                    .queen
                    .admin()
                    .queue(queue)
                    .await
                    .map_err(|e| e.to_string())?;
                detail["partitions"].as_array().and_then(|ps| {
                    ps.iter()
                        .filter(|p| p["stats"]["pending"].as_u64().unwrap_or(0) > 0)
                        .filter_map(|p| p["oldestMessage"].as_str())
                        .filter_map(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                        .map(|t| (crate::now_ms() - t.timestamp_millis()).max(0) / 1000)
                        .max()
                })
            };
            let age = match age {
                Some(age) => Some(age),
                None => {
                    let detail = app
                        .queen
                        .admin()
                        .queue(queue)
                        .await
                        .map_err(|e| e.to_string())?;
                    matching_queue_age(&detail, &depth, crate::now_ms())
                }
            };
            match age {
                Some(age) => oldest = oldest.max(age),
                None => age_known = false,
            }
        }
    }
    // A shared interior queue contains foreign-path frames. Its position is not
    // this node's demand; avoid attributing those frames as incoming work.
    let single_ingress = node.ingress_queue.is_some() && rt.stages_of_node(name).count() == 1;
    Ok(json!({"available":true,"before":pending,"workers":workers,
        "oldestSeconds":if age_known {Some(oldest.max(0))} else {None},
        "positions":if single_ingress {Some(positions)} else {None}, "version":rt.doc.version}))
}

#[derive(Deserialize)]
pub struct Range {
    #[serde(default = "default_minutes")]
    minutes: i64,
}
fn default_minutes() -> i64 {
    60
}
pub async fn timeline(
    State(app): State<Shared>,
    Path((application, graph)): Path<(String, String)>,
    Query(range): Query<Range>,
) -> ApiResult {
    let rt = find(&app, &application, &graph)?;
    let history = app.history.as_ref().ok_or_else(|| {
        Fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "History is not configured.".into(),
        )
    })?;
    let minutes = range.minutes.clamp(5, 1440);
    let mut data = history
        .insights(&application, &graph, minutes)
        .await
        .map_err(history_error)?;
    let mut relayed = BTreeMap::new();
    for node in rt.plan.nodes.keys() {
        relayed.insert(
            node,
            history
                .rollups(&application, &format!("{graph}.{node}"), minutes)
                .await
                .map_err(history_error)?,
        );
    }
    data["relayed"] = json!(relayed);
    data["enabled"] = json!(rt.plan.counters_window_seconds.is_some());
    data["resolutionSeconds"] = json!(60);
    ok(data)
}
fn history_error(error: String) -> Fail {
    Fail(
        StatusCode::BAD_GATEWAY,
        format!("Could not read history: {error}"),
    )
}

pub async fn diagnostics(
    State(app): State<Shared>,
    Path((application, graph)): Path<(String, String)>,
) -> ApiResult {
    let rt = find(&app, &application, &graph)?;
    let now = crate::now_ms();
    let mut issues = Vec::new();
    if !app.broker_health().await.reachable {
        issues.push(issue(
            "broker",
            None,
            None,
            "Broker unavailable",
            "Gate cannot read Queen. Check broker health, networking and credentials.",
            "bad",
        ));
    } else {
        if !rt.is_running() {
            issues.push(issue(
                "stopped",
                None,
                None,
                "Relay stopped",
                "Redeclare the graph after checking the Gate service logs.",
                "bad",
            ));
        }
        match tokio::time::timeout(Duration::from_secs(8), crate::api::declare::view(&app,&rt)).await {
            Ok(Ok(view)) => {
                for node in view["nodes"].as_array().into_iter().flatten() {
                    let name = node["node"].as_str().unwrap_or("");
                    let before = node["waiting_for_budget"].as_u64().unwrap_or(0);
                    let workers = node["waiting_for_workers"].as_u64().unwrap_or(0);
                    let stages: Vec<_> = rt.stages_of_node(name).collect();
                    let mut diagnosed = false;
                    for stage in &stages {
                        if stage.wedge.read().as_ref().is_some_and(|w| w.escalated) {
                            issues.push(issue("cursor",Some(name),Some(&stage.stage.path),"Cursor is not advancing",&format!("Repeated acknowledgement failures on {} / {}. Inspect the Gate logs and Queen cursor before seeking or replaying messages.",stage.stage.source,stage.stage.group),"bad"));
                            diagnosed = true;
                        }
                    }
                    if node["breaker"]["until"].as_i64().is_some_and(|until| until > now) {
                        issues.push(issue("backoff",Some(name),None,"Provider backoff active","Wait for Retry-After to expire; inspect the provider response before lifting the backoff.","warn"));
                        diagnosed = true;
                    }
                    if before > 0 && !diagnosed {
                        let recent = stages.iter().find_map(|s| s.last_refusal.read().clone().filter(|(_,at)| now.saturating_sub(*at) < 15_000));
                        let exhausted = node["budgets"].as_array().into_iter().flatten().any(|b| b["utilisation"].as_f64().is_some_and(|v| v >= 1.0) && b["expiresAt"].as_i64().is_some_and(|t|t>now));
                        if rt.doc.watch.is_none() && (recent.is_some() || exhausted) {
                            let mut item = issue("budget",Some(name),None,"Rate limit is holding traffic","Wait for the counter window to reset. Review the quota and path share before raising the limit.","info");
                            item["budget"] = json!(recent.map(|(id,_)|id));
                            issues.push(item);
                        } else {
                            issues.push(issue("relay",Some(name),None,"Work is waiting for the relay","No current budget refusal explains this backlog. Check relay logs, active partitions and concurrency.","warn"));
                        }
                    }
                    if workers > 0 {
                        issues.push(issue("workers",Some(name),None,"Admitted work is waiting for workers",&format!("{} items are waiting on {}. Check the application's consumers, their group and processing errors.",workers,node["egressQueue"].as_str().unwrap_or("the outgoing queue")),"info"));
                    }
                }
            }
            _ => issues.push(issue("read",None,None,"Queue state unavailable","Queen health responded, but queue or budget reads failed. Check broker logs and permissions; backlog is unknown.","bad")),
        }
    }
    let prefix = format!("{application}/{graph}/");
    let mut active = HashSet::new();
    let mut conditions = app.conditions.lock();
    for issue in &mut issues {
        let key = format!(
            "{prefix}{}/{}/{}",
            issue["kind"], issue["node"], issue["path"]
        );
        active.insert(key.clone());
        issue["since"] = json!(*conditions.entry(key).or_insert(now));
    }
    conditions.retain(|key, _| !key.starts_with(&prefix) || active.contains(key));
    ok(
        json!({"at":now,"issues":issues,"durationScope":"First detected on this Gate replica; resets after a restart or recovery."}),
    )
}
fn issue(
    kind: &str,
    node: Option<&str>,
    path: Option<&str>,
    title: &str,
    action: &str,
    severity: &str,
) -> Value {
    json!({"kind":kind,"node":node,"path":path,"title":title,"action":action,"severity":severity})
}

// Queue-level timestamps can stand in for an uninitialised group only when
// every partition's pending count agrees. Another group's oldest is not ours.
fn matching_queue_age(
    detail: &Value,
    depth: &std::collections::HashMap<String, u64>,
    now: i64,
) -> Option<i64> {
    let parts = detail["partitions"].as_array()?;
    let mut oldest = 0;
    for (name, pending) in depth.iter().filter(|(_, n)| **n > 0) {
        let part = parts.iter().find(|p| p["name"].as_str() == Some(name))?;
        if part["stats"]["pending"].as_u64()? != *pending {
            return None;
        }
        let stamp = chrono::DateTime::parse_from_rfc3339(part["oldestMessage"].as_str()?).ok()?;
        oldest = oldest.max((now - stamp.timestamp_millis()).max(0) / 1000);
    }
    Some(oldest)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn another_groups_timestamp_is_not_reported_as_our_oldest() {
        let detail = json!({"partitions":[{"name":"p0","stats":{"pending":3},"oldestMessage":"2026-10-09T00:00:00Z"}]});
        let now = chrono::DateTime::parse_from_rfc3339("2026-10-09T00:01:00Z")
            .unwrap()
            .timestamp_millis();
        assert_eq!(
            matching_queue_age(
                &detail,
                &std::collections::HashMap::from([("p0".into(), 3)]),
                now
            ),
            Some(60)
        );
        assert_eq!(
            matching_queue_age(
                &detail,
                &std::collections::HashMap::from([("p0".into(), 2)]),
                now
            ),
            None
        );
    }
}
