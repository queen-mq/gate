//! What the console reads.
//!
//! Same shapes as v1 wherever a shape still has a source, because the Vue app is
//! not the thing being rewritten. Three fields that v1 hardcoded are computed
//! here for the first time: `queen.reachable` and `queen.version` are PROBED,
//! and `budgets_stale` is derived from each budget's `asOf` against a ninety-day
//! horizon. `admitted_per_sec` is `null` when the counters stream is off rather
//! than a lifetime average dressed up as a rate — which fixes the divergence
//! where this field and `/v1/apps/:app/metrics`'s field of the same name meant
//! different things.

use std::collections::{BTreeSet, HashMap};

use axum::extract::Path as AxPath;
use axum::extract::{Query, State};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::api::{ok, ApiResult, Shared};

// ------------------------------------------------------------------ overview

pub async fn overview(State(app): State<Shared>) -> ApiResult {
    let graphs = app.registry.all();
    let health = app.broker_health().await;
    let now = crate::now_ms();

    let (mut admitted, mut denied) = (0u64, 0u64);
    let mut assumed = 0usize;
    let mut stale = 0usize;
    let mut counters_on = false;

    for g in &graphs {
        for s in &g.stages {
            let c = &s.counters;
            admitted += c.admitted.load(std::sync::atomic::Ordering::Relaxed);
            denied += c.deferred.load(std::sync::atomic::Ordering::Relaxed)
                + c.released.load(std::sync::atomic::Ordering::Relaxed);
        }
        counters_on |= g.plan.counters_window_seconds.is_some();
        for node in g.doc.nodes.values() {
            for b in &node.budgets {
                if b.confidence == gate_core::Confidence::Assumed {
                    assumed += 1;
                }
                if is_stale(b.as_of.as_deref(), now) {
                    stale += 1;
                }
            }
        }
    }

    ok(json!({
        "queen": {
            "reachable": health.reachable,
            "url": app.queen_url,
            "version": health.version,
        },
        "targets": graphs.len(),
        "graphs": graphs.len(),
        "admitted_total": admitted,
        "denied_total": denied,
        // Null, not a lifetime average. A running counter divided by an uptime
        // the caller does not know is a number that is wrong in a way nobody can
        // see.
        "admitted_per_sec": if counters_on { rate_of(&app, now).await } else { Value::Null },
        "budgets_assumed": assumed,
        "budgets_stale": stale,
    }))
}

/// Ninety days. A vendor's published limit does not go off like milk, but a
/// number nobody has looked at since the spring is a number worth a badge.
const STALE_DAYS: i64 = 90;

fn is_stale(as_of: Option<&str>, now_ms: i64) -> bool {
    let Some(s) = as_of else { return false };
    match crate::budget::parse_instant(&format!("{s}T00:00:00Z"))
        .or_else(|| crate::budget::parse_instant(s))
    {
        Some(at) => now_ms - at > STALE_DAYS * 86_400_000,
        // Unparseable is not stale — it is a different complaint, and reporting
        // it here would put a badge on a typo.
        None => false,
    }
}

async fn rate_of(app: &Shared, now: i64) -> Value {
    let Some(h) = app.history.as_ref() else {
        return Value::Null;
    };
    let mut total = 0.0f64;
    for g in app.registry.all() {
        for s in &g.stages {
            total += h
                .rate_per_sec(
                    &g.doc.application,
                    &format!("{}.{}", g.doc.graph, s.stage.node),
                    &s.stage.path,
                    now,
                )
                .await;
        }
    }
    json!(total)
}

// -------------------------------------------------------------------- lists

pub async fn list_apps(State(st): State<Shared>) -> ApiResult {
    let out: Vec<Value> = st
        .registry
        .applications()
        .into_iter()
        .map(|a| {
            let gs = st.registry.of_app(&a);
            let (mut adm, mut den) = (0u64, 0u64);
            for g in &gs {
                for s in &g.stages {
                    adm += s
                        .counters
                        .admitted
                        .load(std::sync::atomic::Ordering::Relaxed);
                    den += s
                        .counters
                        .deferred
                        .load(std::sync::atomic::Ordering::Relaxed);
                }
            }
            json!({ "application": a, "targets": gs.len(), "admitted": adm, "denied": den })
        })
        .collect();
    ok(json!(out))
}

/// One row per graph, in the console's target vocabulary. A node IS a target,
/// and a one-node graph is what a "target" was.
pub async fn list_targets(State(app): State<Shared>) -> ApiResult {
    let now = crate::now_ms();
    let mut out = Vec::new();

    for g in app.registry.all() {
        // The backlog of everything that has not been admitted yet: every
        // ingress queue this graph reads, under the group that reads it.
        let mut backlog = 0u64;
        for s in g.stages.iter().filter(|s| s.stage.first_hop) {
            backlog += app
                .depths
                .pending_of_group(&app.queen, &s.stage.source, &s.stage.group)
                .await
                .values()
                .sum::<u64>();
        }

        let (mut adm, mut den) = (0u64, 0u64);
        for s in &g.stages {
            adm += s
                .counters
                .admitted
                .load(std::sync::atomic::Ordering::Relaxed);
            den += s
                .counters
                .deferred
                .load(std::sync::atomic::Ordering::Relaxed);
        }

        // The worst counter across every unscoped budget of every node: a graph
        // is as close to refusing as the counter closest to its ceiling.
        let mut worst = (String::new(), 0.0f64, 0i64, 0i64, false, 0i64);
        for np in g.plan.nodes.values() {
            let keys: Vec<String> = np.unscoped().map(|b| b.key.clone()).collect();
            let states = app.budgets.read(&keys).await.unwrap_or_default();
            for b in np.unscoped() {
                let ceiling = b.max_for(np.widest_share());
                if ceiling <= 0 {
                    continue;
                }
                let v = states
                    .iter()
                    .find(|s| s.key == b.key)
                    .map(|s| s.value)
                    .unwrap_or(0);
                let u = v as f64 / ceiling as f64;
                if u >= worst.1 {
                    let assumed = g
                        .doc
                        .nodes
                        .get(&np.name)
                        .map(|n| {
                            n.budgets.iter().any(|d| {
                                d.id.as_deref() == Some(b.id.as_str())
                                    && d.confidence == gate_core::Confidence::Assumed
                            })
                        })
                        .unwrap_or(false);
                    // The period is the SUB-window, because that is what the
                    // ceiling beside it is: `worst_cap` is `count_sub * share`,
                    // so pairing it with the declared `timeMs` would report a
                    // rate ten times too low for a budget with ten sub-windows.
                    worst = (b.id.clone(), u, v, ceiling, assumed, b.window_sub_seconds);
                }
            }
        }

        let assumed_budgets = g
            .doc
            .nodes
            .values()
            .flat_map(|n| n.budgets.iter())
            .filter(|b| b.confidence == gate_core::Confidence::Assumed)
            .count();

        out.push(json!({
            "application": g.doc.application,
            "name": g.doc.graph,
            "version": g.doc.version,
            "graph": g.doc.graph,
            "running": g.is_running(),
            "lanes": g.doc.paths.iter().map(|p| json!({ "name": p.name })).collect::<Vec<_>>(),
            "paths": g.doc.paths.iter().map(|p| json!({ "name": p.name })).collect::<Vec<_>>(),
            "budgets_total": g.doc.nodes.values().map(|n| n.budgets.len()).sum::<usize>(),
            "assumed_budgets": assumed_budgets,
            "worst_budget_id": worst.0,
            "worst_used": worst.2,
            "worst_cap": worst.3,
            "worst_period_seconds": worst.5,
            // Computed, where v1 hardcoded `false`.
            "worst_assumed": worst.4,
            "admitted": adm,
            "denied": den,
            "state": if den > 0 { "pacing" } else { "flowing" },
            "backlog": backlog,
            "at": now,
        }));
    }
    ok(json!(out))
}

pub async fn list_graphs(State(st): State<Shared>) -> ApiResult {
    let mut out = Vec::new();
    for g in st.registry.all() {
        let mut nodes = Vec::new();
        for (name, np) in &g.plan.nodes {
            let mut waiting = 0u64;
            let mut admitted = 0u64;
            let mut denied = 0u64;
            for s in g.stages_of_node(name) {
                waiting += st
                    .depths
                    .pending_of_group(&st.queen, &s.stage.source, &s.stage.group)
                    .await
                    .values()
                    .sum::<u64>();
                admitted += s
                    .counters
                    .admitted
                    .load(std::sync::atomic::Ordering::Relaxed);
                denied += s
                    .counters
                    .deferred
                    .load(std::sync::atomic::Ordering::Relaxed);
            }
            nodes.push(json!({
                "name": name,
                "target": format!("{}.{}", g.doc.graph, name),
                "entry": np.ingress_queue.is_some(),
                "consume": np.egress_queue.is_some(),
                "running": g.is_running(),
                "budgets": np.budgets.len(),
                "paths": gate_core::plan::paths_through(&g.plan, name),
                "shares": np.shares,
                "waiting_for_budget": waiting,
                "admitted": admitted,
                "denied": denied,
            }));
        }
        out.push(json!({
            "application": g.doc.application,
            "name": g.doc.graph,
            "version": g.doc.version,
            "nodes": nodes,
            "edges": gate_core::plan::edges(&g.doc).iter()
                .map(|(a, b)| json!({ "from": a, "to": b })).collect::<Vec<_>>(),
            "paths": g.doc.paths.iter().map(|p| json!({
                "name": p.name, "priority": p.priority, "share": p.share,
                "hops": gate_core::plan::hop_names(p),
            })).collect::<Vec<_>>(),
            "forwarded": g.stages.iter()
                .map(|s| s.counters.forwarded.load(std::sync::atomic::Ordering::Relaxed))
                .sum::<u64>(),
        }));
    }
    ok(json!(out))
}

// ------------------------------------------------------------------ history

#[derive(Deserialize)]
pub struct FlowQuery {
    #[serde(default)]
    pub minutes: Option<usize>,
}

/// Every application on one axis: how close each one came to its own ceiling,
/// minute by minute.
///
/// Applications have different ceilings — 150/s here, 20/s there — so raw
/// admissions cannot share a y-axis without the big one burying the small one.
/// Utilisation can, and it is also the question a dashboard is asked: not "how
/// much did we send" but "how close are we to being refused".
pub async fn flow(State(app): State<Shared>, Query(q): Query<FlowQuery>) -> ApiResult {
    let now = crate::now_ms();
    let minutes = q.minutes.unwrap_or(120).clamp(1, 1440) as i64;

    let Some(h) = app.history.as_ref() else {
        return ok(json!({ "minutes": [], "applications": [], "durable": false }));
    };

    // The ceiling per node, from the DECLARATION rather than from the data: it
    // is what the counter enforces, and a node that admitted nothing this minute
    // still has one.
    let mut ceiling: HashMap<(String, String), f64> = HashMap::new();
    for g in app.registry.all() {
        for (name, np) in &g.plan.nodes {
            let per_min = np
                .unscoped()
                .map(|b| b.count_sub as f64 * 60.0 / b.window_sub_seconds.max(1) as f64)
                .fold(f64::INFINITY, f64::min);
            if per_min.is_finite() && per_min > 0.0 {
                ceiling.insert(
                    (
                        g.doc.application.clone(),
                        format!("{}.{}", g.doc.graph, name),
                    ),
                    per_min,
                );
            }
        }
    }

    struct Cell {
        utilisation: f64,
        target: String,
        admitted: i64,
        ceiling: f64,
        total: i64,
    }
    let mut cells: HashMap<(String, i64), Cell> = HashMap::new();
    let mut minute_set: BTreeSet<i64> = BTreeSet::new();

    for (application, target, minute, admitted) in h.flow(minutes, now).await {
        minute_set.insert(minute);
        let cap = ceiling.get(&(application.clone(), target.clone())).copied();
        let u = cap.map_or(0.0, |c| admitted as f64 / c);
        let e = cells.entry((application.clone(), minute)).or_insert(Cell {
            utilisation: 0.0,
            target: target.clone(),
            admitted: 0,
            ceiling: cap.unwrap_or(0.0),
            total: 0,
        });
        e.total += admitted;
        if cap.is_some() && u >= e.utilisation {
            e.utilisation = u;
            e.target = target;
            e.admitted = admitted;
            e.ceiling = cap.unwrap_or(0.0);
        }
    }

    let minutes_axis: Vec<i64> = minute_set.into_iter().collect();
    let apps: BTreeSet<String> = cells.keys().map(|(a, _)| a.clone()).collect();
    let series: Vec<Value> = apps
        .iter()
        .map(|a| {
            let points: Vec<Value> = minutes_axis
                .iter()
                .map(|t| match cells.get(&(a.clone(), *t)) {
                    Some(c) => json!({
                        "t": t, "utilisation": c.utilisation, "target": c.target,
                        "admitted": c.admitted, "ceiling": c.ceiling, "total_admitted": c.total,
                    }),
                    // A minute an application did not appear in is a minute it
                    // admitted nothing, which is a real zero and not a gap.
                    None => {
                        json!({ "t": t, "utilisation": 0.0, "admitted": 0, "total_admitted": 0 })
                    }
                })
                .collect();
            json!({ "application": a, "points": points })
        })
        .collect();

    ok(json!({
        "minutes": minutes_axis,
        "applications": series,
        // "the counters stream is on AND history is configured" — either half
        // missing means the chart has no source, and an empty chart that does
        // not say so reads as "nothing happened".
        "durable": app.registry.all().iter().any(|g| g.plan.counters_window_seconds.is_some()),
    }))
}

#[derive(Deserialize)]
pub struct RollupQuery {
    pub target: String,
    #[serde(default)]
    pub application: Option<String>,
    #[serde(default)]
    pub minutes: Option<usize>,
}

pub async fn rollups(State(app): State<Shared>, Query(q): Query<RollupQuery>) -> ApiResult {
    let Some(h) = app.history.as_ref() else {
        return ok(json!([]));
    };
    let (a, t) = match q.target.split_once('/') {
        Some((a, t)) => (a.to_string(), t.to_string()),
        None => (
            q.application
                .clone()
                .unwrap_or_else(gate_core::default_application),
            q.target.clone(),
        ),
    };
    ok(json!(
        h.rollups(&a, &t, q.minutes.unwrap_or(120) as i64).await
    ))
}

#[derive(Deserialize)]
pub struct TraceQuery {
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Refusals, from the in-process ring, plus whatever has been flushed.
///
/// Strictly poorer than v1's, which wrote one row per decision in the same
/// transaction as the ack. There is no ack any more, so there is no atomicity to
/// inherit and no `cost_actual` to compare — see the design's §16.5. What is
/// kept is the interesting event: the denial.
pub async fn traces(State(app): State<Shared>, Query(q): Query<TraceQuery>) -> ApiResult {
    let limit = q.limit.unwrap_or(100);
    let mut out: Vec<Value> = app
        .traces
        .recent(q.outcome.as_deref(), limit)
        .iter()
        .map(|t| t.view())
        .collect();
    if out.len() < limit {
        if let Some(h) = app.history.as_ref() {
            out.extend(
                h.traces(q.outcome.as_deref(), (limit - out.len()) as i64)
                    .await,
            );
        }
    }
    ok(json!(out))
}

#[derive(Deserialize)]
pub struct LimitQuery {
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Fleet-wide, and that is the improvement. v1's breach ring was per-replica,
/// and a breach seen only by the pod nobody is looking at is a breach nobody
/// sees. These are the `brk:` records, which every replica writes and every
/// replica can read.
pub async fn recent_breaches(State(app): State<Shared>, Query(q): Query<LimitQuery>) -> ApiResult {
    ok(json!(
        crate::breaker::recent(&app.budgets, q.limit.unwrap_or(10) as u32).await
    ))
}

/// One row per `(application, sharedKey)`, read live.
///
/// Disagreement is REPORTED and not resolved: if two graphs declare the same key
/// with different numbers they are already spending against one counter and one
/// of the declarations is a lie. The console cannot tell which, so it names
/// both. Inside one document the same disagreement is a 422 (`shared-conflict`).
pub async fn shared_budgets(State(app): State<Shared>) -> ApiResult {
    let mut groups: HashMap<(String, String), Vec<(String, gate_core::CompiledBudget)>> =
        HashMap::new();
    for g in app.registry.all() {
        for np in g.plan.nodes.values() {
            for b in np.budgets.iter().filter(|b| b.shared_key.is_some()) {
                groups
                    .entry((
                        g.doc.application.clone(),
                        b.shared_key.clone().unwrap_or_default(),
                    ))
                    .or_default()
                    .push((format!("{}.{}", g.doc.graph, np.name), b.clone()));
            }
        }
    }

    let mut out = Vec::new();
    for ((application, id), members) in groups {
        let first = &members[0].1;
        let state = app
            .budgets
            .read(std::slice::from_ref(&first.key))
            .await
            .unwrap_or_default()
            .into_iter()
            .next();
        let conflicts: Vec<Value> = members
            .iter()
            .filter(|(_, b)| {
                b.count != first.count
                    || b.time_ms != first.time_ms
                    || b.sub_windows != first.sub_windows
            })
            .map(|(t, b)| {
                json!({ "target": t, "count": b.count, "timeMs": b.time_ms,
                        "subWindows": b.sub_windows })
            })
            .collect();

        out.push(json!({
            "id": id,
            "key": first.key,
            "application": application,
            "count": first.count,
            "timeMs": first.time_ms,
            "subWindows": first.sub_windows,
            "countSub": first.count_sub,
            "windowSubSeconds": first.window_sub_seconds,
            "confidence": first.confidence,
            "used": state.as_ref().map(|s| s.value).unwrap_or(0),
            "expiresAt": state.and_then(|s| s.expires_at_ms),
            "members": members.iter().map(|(t, _)| t.clone()).collect::<Vec<_>>(),
            // Empty is the normal case and the only one that means anything is
            // being enforced as declared.
            "conflicts": conflicts,
        }));
    }
    out.sort_by(|a, b| {
        (a["application"].as_str(), a["id"].as_str())
            .cmp(&(b["application"].as_str(), b["id"].as_str()))
    });
    ok(json!(out))
}

// ------------------------------------------------------------------ metrics

/// What an application's own product should tell ITS users about the limits.
///
/// Deliberately not the console's shape. The console answers "are we still in
/// control", which is an operator's question. This answers "when will my change
/// be live", which is what somebody waiting on a price update actually wants.
pub async fn app_metrics(
    State(app): State<Shared>,
    AxPath(application): AxPath<String>,
) -> ApiResult {
    let now = crate::now_ms();
    let mut targets = Vec::new();

    for g in app.registry.of_app(&application) {
        for (name, np) in &g.plan.nodes {
            let mut paths = Vec::new();
            let (mut waiting_budget, mut waiting_workers) = (0u64, 0u64);
            let mut rate: Option<f64> = None;

            for s in g.stages_of_node(name) {
                let budget_pending: u64 = app
                    .depths
                    .pending_of_group(&app.queen, &s.stage.source, &s.stage.group)
                    .await
                    .values()
                    .sum();
                waiting_budget += budget_pending;

                let per_sec = match (app.history.as_ref(), g.plan.counters_window_seconds) {
                    (Some(h), Some(_)) => Some(
                        h.rate_per_sec(
                            &g.doc.application,
                            &format!("{}.{}", g.doc.graph, name),
                            &s.stage.path,
                            now,
                        )
                        .await,
                    ),
                    // Null, not a lifetime average — the same fix as
                    // `/api/overview`, so the two fields of this name finally
                    // mean the same thing.
                    _ => None,
                };
                if let Some(r) = per_sec {
                    rate = Some(rate.unwrap_or(0.0) + r);
                }

                paths.push(json!({
                    "name": s.stage.path,
                    "share": s.stage.share,
                    "waiting_for_budget": budget_pending,
                    "admitted_per_sec": per_sec,
                    "drain_eta_seconds": eta(budget_pending, per_sec),
                }));
            }

            if let Some(q) = &np.egress_queue {
                waiting_workers = match &np.egress_group {
                    Some(gr) => app
                        .depths
                        .pending_of_group(&app.queen, q, gr)
                        .await
                        .values()
                        .sum(),
                    None => app.depths.pending(&app.queen, q).await.values().sum(),
                };
            }

            let keys: Vec<String> = np.unscoped().map(|b| b.key.clone()).collect();
            let states = app.budgets.read(&keys).await.unwrap_or_default();
            let binding = np
                .unscoped()
                .map(|b| {
                    let ceiling = b.max_for(np.widest_share()).max(1);
                    let v = states
                        .iter()
                        .find(|s| s.key == b.key)
                        .map(|s| s.value)
                        .unwrap_or(0);
                    (b, v as f64 / ceiling as f64, v, ceiling)
                })
                .fold(None::<(_, f64, i64, i64)>, |acc, x| match acc {
                    Some(a) if a.1 >= x.1 => Some(a),
                    _ => Some(x),
                });

            let breaker = crate::breaker::held(&app.budgets, np).await;
            let state = if breaker.is_some() {
                "breached"
            } else if waiting_budget > 0 {
                "pacing"
            } else {
                "flowing"
            };

            targets.push(json!({
                "name": format!("{}.{}", g.doc.graph, name),
                "graph": g.doc.graph,
                "node": name,
                "state": state,
                "binding_budget": binding.map(|(b, u, v, ceiling)| json!({
                    "id": b.id, "count": b.count, "time_ms": b.time_ms,
                    "count_sub": b.count_sub, "window_sub_seconds": b.window_sub_seconds,
                    "value": v, "ceiling": ceiling,
                    "utilisation": u, "confidence": b.confidence,
                    // v1's words for the two above, kept because this is the
                    // PRODUCT metrics endpoint and §12.1 does not move the
                    // shapes a live consumer already reads. A scraper that
                    // decodes into a struct silently gets 0 for a field that was
                    // renamed, and a dashboard then draws a budget with a cap of
                    // zero rather than failing.
                    "cap": b.count, "period_seconds": b.time_ms / 1000,
                })),
                "admitted_per_sec": rate,
                "waiting_for_budget": waiting_budget,
                "waiting_for_workers": waiting_workers,
                "drain_eta_seconds": eta(waiting_budget, rate),
                "last_breach_at": breaker.map(|b| b.at),
                "lanes": paths.clone(),
                "paths": paths,
            }));
        }
    }

    ok(json!({ "application": application, "at": now, "targets": targets }))
}

/// `null` rather than infinity when nothing is moving: "we cannot say" is an
/// honest answer and a product can render it, where a number that means for ever
/// would be rendered as a number.
pub fn eta(waiting: u64, per_sec: Option<f64>) -> Option<u64> {
    if waiting == 0 {
        return Some(0);
    }
    match per_sec {
        Some(r) if r > 0.0 => Some((waiting as f64 / r).ceil() as u64),
        _ => None,
    }
}

/// Who the console is talking to, and what it may do. The console reads `role`
/// to decide whether the editor is a button or a disabled one — a read-only
/// operator should be told before they type, not after they submit.
pub async fn me(
    State(app): State<Shared>,
    session: Option<axum::Extension<crate::auth::Session>>,
) -> ApiResult {
    // No session on the request means it did not come through the gate, which
    // means it arrived on the internal listener. The cluster boundary is the
    // authentication there.
    let Some(axum::Extension(s)) = session else {
        return ok(json!({ "actor": "internal", "role": "admin", "email": null }));
    };
    let _ = &app;
    ok(json!({
        "actor": if crate::auth::is_dev() { "dev" } else { "google" },
        "email": s.email,
        "role": if crate::auth::is_admin(&s.email) { "admin" } else { "viewer" },
        "expires_at": s.exp,
    }))
}
