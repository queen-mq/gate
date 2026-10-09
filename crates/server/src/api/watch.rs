//! Observation uses durable, completed minute buckets, never replica uptime.
use axum::extract::{Path, State};
use axum::http::StatusCode;
use serde_json::{json, Value};

use crate::api::{find, ok, ApiResult, Fail, Shared};

pub async fn summary(
    State(app): State<Shared>,
    Path((application, graph)): Path<(String, String)>,
) -> ApiResult {
    let rt = find(&app, &application, &graph)?;
    let watch = rt
        .doc
        .watch
        .as_ref()
        .ok_or_else(|| Fail(StatusCode::CONFLICT, "This graph is not in watch.".into()))?;
    let history = app.history.as_ref().ok_or_else(|| {
        Fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "Watch history is unavailable. Restore Gate's PostgreSQL history connection.".into(),
        )
    })?;
    let now = crate::now_ms();
    let start = watch.started_at.ok_or_else(|| {
        Fail(
            StatusCode::CONFLICT,
            "This watch has no start time. Declare it again to start observation.".into(),
        )
    })?;
    let end = watch.ends_at().unwrap_or(start);
    let (since, until) = complete_minutes(start, now.min(end));
    let minutes = (until - since) / 60_000;
    let mut nodes = Vec::new();
    for name in rt.plan.nodes.keys() {
        let rows = history
            .watch_minutes(&application, &format!("{graph}.{name}"), since, until)
            .await
            .map_err(|e| {
                Fail(
                    StatusCode::BAD_GATEWAY,
                    format!("could not read watch history: {e}"),
                )
            })?;
        nodes.push(json!({"node": name, "traffic": traffic(&rows, minutes)}));
    }
    ok(json!({
        "startedAt": start, "endsAt": end, "ready": now >= end,
        "minutes": minutes, "nodes": nodes,
        "resolutionSeconds": 60,
    }))
}

fn complete_minutes(start: i64, end: i64) -> (i64, i64) {
    let since = start.saturating_add(59_999) / 60_000 * 60_000;
    // A malformed stored duration cannot make the history query unbounded.
    let end = end.min(start.saturating_add(i64::from(gate_core::MAX_WATCH_SECONDS) * 1000));
    (since, (end / 60_000 * 60_000).max(since))
}

fn traffic(rows: &[(i64, f64)], minutes: i64) -> Value {
    let admitted: i64 = rows.iter().map(|(items, _)| *items).sum();
    let cost: f64 = rows.iter().map(|(_, cost)| *cost).sum();
    let peak: f64 = rows.iter().map(|(_, cost)| *cost).fold(0.0, f64::max);
    json!({
        "admitted": admitted, "cost": cost,
        "averageCostPerSecond": if minutes > 0 { Some(cost / (minutes * 60) as f64) } else { None },
        "peakCostPerMinute": if minutes > 0 { Some(peak) } else { None },
        "recordedMinutes": rows.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_and_pre_session_minutes_are_excluded() {
        assert_eq!(complete_minutes(60_001, 239_999), (120_000, 180_000));
        assert_eq!(complete_minutes(60_000, 120_000), (60_000, 120_000));
        assert_eq!(complete_minutes(60_001, 60_002), (120_000, 120_000));
        assert!(
            complete_minutes(60_000, i64::MAX).1
                <= 60_000 + i64::from(gate_core::MAX_WATCH_SECONDS) * 1000
        );
    }

    #[test]
    fn average_includes_idle_minutes_and_no_sample_is_not_a_zero_rate() {
        let result = traffic(&[(60, 120.0), (30, 60.0)], 10);
        assert_eq!(result["admitted"], 90);
        assert_eq!(result["averageCostPerSecond"], 0.3);
        assert_eq!(result["peakCostPerMinute"], 120.0);
        assert_eq!(result["recordedMinutes"], 2);
        assert!(traffic(&[], 0)["averageCostPerSecond"].is_null());
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Candidate {
    node: String,
    count: i64,
    time_ms: i64,
    sub_windows: Option<u32>,
    #[serde(default = "simulation_minutes")]
    minutes: i64,
}
fn simulation_minutes() -> i64 {
    60
}

/// Read-only, viewer-accessible: this never writes a budget or touches a queue.
pub async fn simulate(
    State(app): State<Shared>,
    Path((application, graph)): Path<(String, String)>,
    axum::extract::Query(candidate): axum::extract::Query<Candidate>,
) -> ApiResult {
    let rt = find(&app, &application, &graph)?;
    let watch = rt.doc.watch.as_ref().ok_or_else(|| {
        Fail(
            StatusCode::CONFLICT,
            "Simulation requires a target in Watch.".into(),
        )
    })?;
    let node = rt
        .doc
        .nodes
        .get(&candidate.node)
        .ok_or_else(|| Fail(StatusCode::NOT_FOUND, "Unknown node.".into()))?;
    let cost = match node.cost {
        gate_core::Cost::Fixed(cost) => cost,
        _ => return Err(Fail(StatusCode::UNPROCESSABLE_ENTITY,"Simulation currently needs a fixed cost per item. Variable costs cannot be reconstructed from aggregate samples.".into())),
    };
    if rt.stages_of_node(&candidate.node).count() != 1
        || rt
            .stages_of_node(&candidate.node)
            .any(|s| s.stage.share != 1.0)
    {
        return Err(Fail(StatusCode::UNPROCESSABLE_ENTITY,"Simulation currently supports one path with a full share. Multiple path shares need a joint replay.".into()));
    }
    if candidate.count < 1
        || candidate.count > 1_000_000_000
        || !(100..=2_592_000_000).contains(&candidate.time_ms)
        || candidate
            .sub_windows
            .is_some_and(|n| n == 0 || n > 3600 || i64::from(n) > candidate.count)
    {
        return Err(Fail(StatusCode::UNPROCESSABLE_ENTITY,"Use a positive allowance up to 1 billion, a window from 100 ms to 30 days, and 1–3600 subdivisions no greater than the allowance.".into()));
    }
    let sub = candidate.sub_windows.unwrap_or_else(|| {
        gate_core::plan::default_sub_windows(candidate.count, candidate.time_ms)
    });
    let (cap, seconds) = gate_core::plan::subdivide(candidate.count, candidate.time_ms, sub);
    if cost < 1 || cost > cap {
        return Err(Fail(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Each item's cost must fit in the candidate's effective sub-window.".into(),
        ));
    }
    let history = app.history.as_ref().ok_or_else(|| {
        Fail(
            StatusCode::SERVICE_UNAVAILABLE,
            "Watch history is unavailable.".into(),
        )
    })?;
    let until = crate::now_ms().min(watch.ends_at().unwrap_or(0)) / 1000 * 1000;
    let since =
        (until - candidate.minutes.clamp(1, 1440) * 60_000).max(watch.started_at.unwrap_or(until));
    let (rows, coverage) = history
        .observation_data(
            &application,
            &graph,
            &candidate.node,
            watch.started_at.unwrap_or(0),
            since,
            until,
        )
        .await
        .map_err(|e| Fail(StatusCode::BAD_GATEWAY, e))?;
    if rows
        .iter()
        .any(|(_, row)| row.items.checked_mul(cost) != Some(row.cost))
    {
        return Err(Fail(StatusCode::CONFLICT,"Item cost changed during this observation. Select a shorter range with a constant cost.".into()));
    }
    let start = coverage["since"].as_i64().unwrap_or(until);
    let end = coverage["until"].as_i64().unwrap_or(until);
    let mut result = replay(&rows, start, end, cost, cap, seconds);
    result["coverage"] = coverage;
    result["effective"] =
        json!({"count":cap,"seconds":seconds,"subWindows":sub,"costPerItem":cost});
    result["node"] = json!(candidate.node);
    result["hasKnownGaps"] =
        json!(result["coverage"]["gapSeconds"] != 0 || result["coverage"]["lostItems"] != 0);
    ok(result)
}

/// A single global, first-charge-anchored TTL counter, as used by the broker.
/// One-second buckets assume simultaneous arrivals and FIFO within the node.
/// Work already waiting at the start and traffic from other targets are excluded.
fn replay(
    rows: &[(i64, crate::observation::Sample)],
    start: i64,
    end: i64,
    cost: i64,
    cap: i64,
    seconds: i64,
) -> Value {
    let mut index = 0;
    let mut backlog = 0i64;
    let mut peak = 0i64;
    let mut total = 0i64;
    let mut delayed = 0i64;
    let mut used = 0i64;
    let mut expires = start;
    let mut series = Vec::new();
    let mut peak_in_minute = 0i64;
    for at in (start..end).step_by(1000) {
        if at >= expires {
            used = 0;
        }
        let arrived = if index < rows.len() && rows[index].0 == at {
            let n = rows[index].1.items;
            index += 1;
            n
        } else {
            0
        };
        total = total.saturating_add(arrived);
        let available = (cap - used) / cost;
        delayed = delayed.saturating_add(
            arrived
                .saturating_sub(available.saturating_sub(backlog).max(0))
                .max(0),
        );
        backlog = backlog.saturating_add(arrived);
        let admitted = backlog.min(available);
        if admitted > 0 && used == 0 {
            expires = at.saturating_add(seconds.saturating_mul(1000));
        }
        used += admitted * cost;
        backlog -= admitted;
        peak = peak.max(backlog);
        peak_in_minute = peak_in_minute.max(backlog);
        if at % 60_000 == 59_000 || at + 1000 >= end {
            series.push(json!({"t":at,"backlog":backlog,"peak":peak_in_minute}));
            peak_in_minute = 0;
        }
    }
    let drain_seconds = if backlog == 0 {
        0
    } else {
        let per_window = cap / cost;
        ((expires - end).max(0) + 999) / 1000 + ((backlog - 1) / per_window).saturating_mul(seconds)
    };
    json!({"items":total,"delayedItems":delayed,"delayedPercent":if total>0 {Some(delayed as f64*100.0/total as f64)} else {None},"peakBacklog":peak,"remainingBacklog":backlog,"drainSeconds":drain_seconds,"series":series})
}

#[cfg(test)]
mod simulation_tests {
    use super::*;
    use crate::observation::Sample;
    #[test]
    fn replay_counts_each_delayed_item_once_and_drains_idle_seconds() {
        let out = replay(
            &[(
                0,
                Sample {
                    items: 25,
                    cost: 25,
                },
            )],
            0,
            3000,
            1,
            10,
            1,
        );
        assert_eq!(out["items"], 25);
        assert_eq!(out["delayedItems"], 15);
        assert_eq!(out["peakBacklog"], 15);
        assert_eq!(out["remainingBacklog"], 0);
        assert_eq!(out["drainSeconds"], 0);
    }
    #[test]
    fn ttl_starts_on_first_charge_and_item_cost_is_indivisible() {
        let out = replay(&[(5000, Sample { items: 5, cost: 15 })], 0, 6000, 3, 10, 10);
        assert_eq!(out["delayedItems"], 2);
        assert_eq!(out["remainingBacklog"], 2);
        assert_eq!(out["drainSeconds"], 9);
        assert_eq!(
            replay(&[], 0, 60_000, 1, 10, 1)["delayedPercent"],
            Value::Null
        );
    }
}
