//! The control plane: `PUT`, `GET`, `DELETE`, and the sync-with-reap.
//!
//! Both document generations arrive here. A body is tried as a v2 `GraphDoc`
//! first and as a v1 `GraphSpec`/`TargetSpec` second, and a v1 body is answered
//! **200 with warnings naming every field that was mapped or ignored** — never a
//! silent success, and never a 422 for having been written last year.

#![allow(deprecated)]

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::{json, Value};

use gate_core::{v1, GraphDoc};

use crate::api::{find, ok, resolve, ApiResult, Fail, Shared};
use crate::registry::GraphRuntime;

// ------------------------------------------------------------------- reading a body

/// A declaration, whichever generation wrote it.
struct Incoming {
    doc: GraphDoc,
    warnings: Vec<gate_core::Problem>,
}

/// Read a body as a graph. v2 first, v1 second.
///
/// The order matters: a v2 document refuses unknown fields, so it cannot
/// accidentally parse a v1 one, and a v1 document refuses unknown fields too, so
/// the fall-through is exact rather than lenient.
fn as_graph(application: &str, name: &str, body: Value) -> Result<Incoming, Fail> {
    match serde_json::from_value::<GraphDoc>(body.clone()) {
        Ok(mut doc) => {
            doc.application = application.to_string();
            doc.graph = name.to_string();
            Ok(Incoming {
                doc,
                warnings: Vec::new(),
            })
        }
        Err(v2_err) => match serde_json::from_value::<v1::GraphSpec>(body) {
            Ok(mut old) => {
                old.application = application.to_string();
                old.name = name.to_string();
                let m = gate_core::migrate::from_v1_graph(&old)
                    .map_err(|r| Fail(StatusCode::UNPROCESSABLE_ENTITY, r.0))?;
                Ok(Incoming {
                    doc: m.doc,
                    warnings: m.warnings,
                })
            }
            Err(v1_err) => Err(Fail(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!(
                    "this body is neither a v2 graph ({v2_err}) nor a v1 one ({v1_err}). Every \
                     document type refuses unknown fields on purpose: a field silently dropped on \
                     read is a configuration silently downgraded on the next reconcile pass."
                ),
            )),
        },
    }
}

/// The same, for the target sugar: a standalone target IS a one-node graph.
fn as_target(application: &str, name: &str, body: Value) -> Result<Incoming, Fail> {
    match serde_json::from_value::<GraphDoc>(body.clone()) {
        Ok(mut doc) => {
            doc.application = application.to_string();
            doc.graph = name.to_string();
            Ok(Incoming {
                doc,
                warnings: Vec::new(),
            })
        }
        Err(v2_err) => match serde_json::from_value::<v1::TargetSpec>(body) {
            Ok(mut old) => {
                old.application = application.to_string();
                old.name = name.to_string();
                let m = gate_core::migrate::from_v1_target(&old)
                    .map_err(|r| Fail(StatusCode::UNPROCESSABLE_ENTITY, r.0))?;
                Ok(Incoming {
                    doc: m.doc,
                    warnings: m.warnings,
                })
            }
            Err(v1_err) => Err(Fail(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("this body is neither a v2 graph ({v2_err}) nor a v1 target ({v1_err})."),
            )),
        },
    }
}

async fn apply(st: &Shared, incoming: Incoming) -> ApiResult {
    let migration: Vec<String> = incoming.warnings.iter().map(|p| p.to_string()).collect();
    let mut out = crate::graph::declare(st, incoming.doc).await?;
    if !migration.is_empty() {
        out["migrated"] = json!(true);
        out["migration"] = json!(migration);
    }
    ok(out)
}

// ------------------------------------------------------------------- graphs

pub async fn put_graph(
    State(st): State<Shared>,
    Path((application, name)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> ApiResult {
    apply(&st, as_graph(&application, &name, body)?).await
}

/// The flat form, and **the parity trap is fixed here**: `application` is pinned
/// from the resolved default rather than taken from the body, so a flat `PUT`
/// can no longer declare into another team's namespace.
pub async fn put_graph_default(
    State(st): State<Shared>,
    Path(name): Path<String>,
    Json(body): Json<Value>,
) -> ApiResult {
    apply(
        &st,
        as_graph(&gate_core::default_application(), &name, body)?,
    )
    .await
}

pub async fn get_graph(
    State(st): State<Shared>,
    Path((application, name)): Path<(String, String)>,
) -> ApiResult {
    let rt = find(&st, &application, &name)?;
    ok(view(&st, &rt).await)
}

pub async fn get_graph_default(State(st): State<Shared>, Path(name): Path<String>) -> ApiResult {
    let rt = resolve(&st, &name)?;
    ok(view(&st, &rt).await)
}

pub async fn del_graph(
    State(st): State<Shared>,
    Path((application, name)): Path<(String, String)>,
) -> ApiResult {
    ok(crate::graph::remove(&st, &application, &name).await?)
}

pub async fn del_graph_default(State(st): State<Shared>, Path(name): Path<String>) -> ApiResult {
    let app = match st.registry.resolve(&name) {
        crate::registry::Resolved::One(g) => g.doc.application.clone(),
        _ => gate_core::default_application(),
    };
    ok(crate::graph::remove(&st, &app, &name).await?)
}

pub async fn topology(
    State(st): State<Shared>,
    Path((application, name)): Path<(String, String)>,
) -> ApiResult {
    ok(crate::graph::topology(&find(&st, &application, &name)?))
}

pub async fn topology_default(State(st): State<Shared>, Path(name): Path<String>) -> ApiResult {
    ok(crate::graph::topology(&resolve(&st, &name)?))
}

// ------------------------------------------------------------------- targets

pub async fn put_target(
    State(st): State<Shared>,
    Path((application, name)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> ApiResult {
    apply(&st, as_target(&application, &name, body)?).await
}

pub async fn put_target_default(
    State(st): State<Shared>,
    Path(name): Path<String>,
    Json(body): Json<Value>,
) -> ApiResult {
    apply(
        &st,
        as_target(&gate_core::default_application(), &name, body)?,
    )
    .await
}

pub async fn get_target(
    State(st): State<Shared>,
    Path((application, name)): Path<(String, String)>,
) -> ApiResult {
    get_graph(State(st), Path((application, name))).await
}

pub async fn get_target_default(State(st): State<Shared>, Path(name): Path<String>) -> ApiResult {
    get_graph_default(State(st), Path(name)).await
}

pub async fn del_target(
    State(st): State<Shared>,
    Path((application, name)): Path<(String, String)>,
) -> ApiResult {
    del_graph(State(st), Path((application, name))).await
}

pub async fn del_target_default(State(st): State<Shared>, Path(name): Path<String>) -> ApiResult {
    del_graph_default(State(st), Path(name)).await
}

/// Declare the caller's WHOLE configuration, and reap what it no longer names.
///
/// This is what makes `forever` the right lifetime for a stored document. A TTL
/// would be the obvious way to stop the store growing, and it is the wrong one:
/// a configuration that expires is a configuration that vanishes at three in the
/// morning for a reason nobody can reconstruct, and renewing it on a heartbeat
/// only moves the failure to "the service was down slightly too long". The store
/// does not need an expiry, it needs an OWNER — and the owner is the caller, who
/// says here what the complete set is.
pub async fn sync_app(
    State(st): State<Shared>,
    Path(application): Path<String>,
    Json(bodies): Json<Vec<Value>>,
) -> ApiResult {
    do_sync(&st, &application, bodies).await
}

pub async fn sync_default(State(st): State<Shared>, Json(bodies): Json<Vec<Value>>) -> ApiResult {
    do_sync(&st, &gate_core::default_application(), bodies).await
}

async fn do_sync(st: &Shared, application: &str, bodies: Vec<Value>) -> ApiResult {
    let _guard = st.declare_lock.lock().await;

    let mut applied = Vec::new();
    let mut refused = Vec::new();
    let mut declared: Vec<String> = Vec::new();

    // A sync may land on a replica before its registry has reconciled. The
    // durable store is therefore part of the inventory to reap, not merely a
    // place each local runtime happens to be deleted from. Incomplete is not
    // empty: if even one page or row is unreadable, applying submitted targets
    // is safe but treating unseen targets as absent is not.
    let stored_targets = match crate::store::try_load_all(&st.queen).await {
        Ok(stored) if stored.complete => stored
            .items
            .into_iter()
            .filter(|doc| doc.application == application && doc.nodes.len() == 1)
            .map(|doc| doc.graph)
            .collect::<Vec<_>>(),
        Ok(_) => {
            refused.push(json!({
                "target": "",
                "error": "the stored target inventory is incomplete; submitted declarations may apply, but nothing omitted can be removed safely"
            }));
            Vec::new()
        }
        Err(e) => {
            refused.push(json!({
                "target": "",
                "error": format!("the stored target inventory could not be read ({e}); submitted declarations may apply, but nothing omitted can be removed safely")
            }));
            Vec::new()
        }
    };

    for body in bodies {
        // The name comes from the document here, because a sync body is a list
        // and there is no path segment to pin it from.
        let name = body
            .get("graph")
            .or_else(|| body.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if name.is_empty() {
            refused.push(json!({ "target": "", "error": "every document in a sync must name itself (`graph`, or v1's `name`)" }));
            continue;
        }
        declared.push(name.clone());
        let incoming = match as_target(application, &name, body) {
            Ok(i) => i,
            Err(Fail(_, msg)) => {
                refused.push(json!({ "target": name, "error": msg }));
                continue;
            }
        };
        match crate::graph::declare_locked(st, incoming.doc, true).await {
            Ok(_) => applied.push(name),
            Err(r) => refused.push(json!({ "target": name, "error": r.message() })),
        }
    }

    // Reap, and ONLY inside this application. The flat version of this reaped
    // everything the cell held, so two teams syncing against one deployment
    // would delete each other's graphs — including from the durable store.
    //
    // A partial declaration is not an authoritative inventory. One malformed
    // body must not turn `ok: false` into a successful deletion of every valid
    // target the caller omitted, so a sync that refused anything removes
    // nothing. Successfully applied documents stay applied and the caller can
    // repair/retry the list without recovering deleted configuration first.
    let mut removed = Vec::new();
    if refused.is_empty() {
        let mut candidates: BTreeSet<String> = stored_targets.into_iter().collect();
        candidates.extend(
            st.registry
                .of_app(application)
                .into_iter()
                .filter(|rt| rt.plan.nodes.len() == 1)
                .map(|rt| rt.doc.graph.clone()),
        );
        for name in candidates {
            if declared.contains(&name) {
                continue;
            }
            // The store's copy decided that a name was a target; the RUNTIME
            // decides whether this replica may reap it. A redeclare registers
            // before it saves, so a graph that grew nodes and then failed to
            // persist reads as a one-node document here and is a multi-node
            // graph in the registry — and a sync of targets does not delete a
            // graph. Asked before the store write, so a graph that is exempt
            // keeps its document too.
            let live = st.registry.get(application, &name);
            if live.as_ref().is_some_and(|rt| rt.plan.nodes.len() > 1) {
                continue;
            }
            if let Err(e) = crate::store::forget(&st.queen, application, &name).await {
                tracing::warn!(graph = %name, error = %e, "sync: not reaped, the stored document could not be removed");
                refused.push(json!({ "target": name, "error": format!("not reaped: {e}") }));
                continue;
            }
            if let Some(rt) = live {
                crate::supervisor::stop(&rt).await;
                st.registry.remove(application, &name);
            }
            removed.push(name);
        }
    }

    ok(json!({
        "ok": refused.is_empty(),
        "application": application,
        "applied": applied,
        "removed": removed,
        "refused": refused,
    }))
}

// ---------------------------------------------------------------------- view

/// One graph, live.
///
/// The budget bars read the counter itself — value AND `expiresAt` — so the
/// console can render the window's remaining time, which v1 could not: its
/// mirror was a copy of a state document with no expiry in it.
pub async fn view(st: &Shared, rt: &Arc<GraphRuntime>) -> Value {
    let mut nodes = Vec::new();
    for (name, np) in &rt.plan.nodes {
        let keys: Vec<String> = np.unscoped().map(|b| b.key.clone()).collect();
        let states = st.budgets.read(&keys).await.unwrap_or_default();
        let breaker = crate::breaker::held(&st.budgets, np).await;

        let budgets: Vec<Value> = np
            .budgets
            .iter()
            .map(|b| {
                let s = states.iter().find(|s| s.key == b.key);
                let ceiling = b.max_for(np.widest_share());
                let value = s.map(|s| s.value).unwrap_or(0);
                json!({
                    "id": b.id,
                    "key": b.key,
                    "scopeBy": b.scope_by,
                    "sharedKey": b.shared_key,
                    "count": b.count,
                    "timeMs": b.time_ms,
                    "subWindows": b.sub_windows,
                    "countSub": b.count_sub,
                    "windowSubSeconds": b.window_sub_seconds,
                    "confidence": b.confidence,
                    // A per-key budget has no single counter to report: the
                    // number that matters is the worst live key, and finding it
                    // means enumerating a namespace. `null` says so rather than
                    // reporting a zero that is a different question's answer.
                    "value": if b.is_scoped() { Value::Null } else { json!(value) },
                    "expiresAt": s.and_then(|s| s.expires_at_ms),
                    "utilisation": if b.is_scoped() || ceiling <= 0 {
                        Value::Null
                    } else {
                        json!(value as f64 / ceiling as f64)
                    },
                    "ceilings": np.shares.iter()
                        .map(|(p, sh)| (p.clone(), b.max_for(*sh)))
                        .collect::<std::collections::BTreeMap<_, _>>(),
                })
            })
            .collect();

        nodes.push(json!({
            "node": name,
            "ingressQueue": np.ingress_queue,
            "ingressOwnedByGate": np.ingress_owned,
            "httpPush": np.ingress_http,
            "egressQueue": np.egress_queue,
            "egressGroup": np.egress_group,
            "paths": gate_core::plan::paths_through(&rt.plan, name),
            "shares": np.shares,
            "budgets": budgets,
            "breaker": breaker.map(|b| json!({
                "at": b.at,
                "retryAfterSeconds": b.retry_after_seconds,
                "until": b.at + b.retry_after_seconds * 1000,
                "by": b.by,
            })),
        }));
    }

    // Per-stage `lag`: §13.10's successor to v1's `edges[].lag`, and one
    // group-scoped depth read per stage. The stage's OWN group, never the
    // queue: a queue-level number is the worst cursor across every reader, so
    // on a shared interior queue it would report another path's backlog as this
    // one's. A broker that will not answer gives `null` rather than zero — the
    // relay needs the truth or nothing, and so does a console.
    let mut stages: Vec<Value> = Vec::with_capacity(rt.stages.len());
    for s in &rt.stages {
        let last = s.last_refusal.read().clone();
        let lag: u64 = st
            .depths
            .pending_of_group(&st.queen, &s.stage.source, &s.stage.group)
            .await
            .values()
            .sum();
        stages.push(json!({
            "path": s.stage.path,
            "node": s.stage.node,
            "hop": s.stage.hop,
            "share": s.stage.share,
            "source": s.stage.source,
            "group": s.stage.group,
            "batch": s.stage.batch,
            "concurrency": s.stage.concurrency,
            "lag": lag,
            "destinations": s.stage.destinations.iter()
                .map(|d| d.queue.clone()).collect::<Vec<_>>(),
            "counters": s.counters.view(),
            "lastRefusal": last.map(|(id, at)| json!({ "budget": id, "at": at })),
        }));
    }

    json!({
        "application": rt.doc.application,
        "graph": rt.doc.graph,
        "name": rt.doc.graph,
        "version": rt.doc.version,
        "running": rt.is_running(),
        "persisted": rt.persisted.load(std::sync::atomic::Ordering::Relaxed),
        "namespace": rt.plan.namespace,
        "counters": rt.plan.counters_window_seconds,
        "nodes": nodes,
        "stages": stages,
        "paths": rt.doc.paths.iter().map(|p| json!({
            "name": p.name,
            "priority": p.priority,
            "share": p.share,
            "hops": gate_core::plan::hop_names(p),
        })).collect::<Vec<_>>(),
        "spec": rt.doc,
    })
}

pub async fn view_response(st: &Shared, rt: &Arc<GraphRuntime>) -> impl IntoResponse {
    Json(view(st, rt).await)
}
