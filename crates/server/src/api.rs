//! The HTTP surface.
//!
//! Two planes on one port, as `queen-http` does: `/v1` is what a caller speaks,
//! `/api` is what the console reads. The caller never learns there is a queue.
//!
//! Nothing here holds session state. A `next` hands back an opaque lease that
//! carries everything the ack needs, so any replica can settle a lease another
//! replica issued — sticky routing would turn a survivable failure into a
//! stuck one.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use queen_mq::{Message, Queen, SubscriptionMode, TxnPushItem};
use gate_core::{utilisation_max, GraphSpec, TargetSpec, GATE_META};

use serde::Deserialize;
use serde_json::{json, Value};

use crate::registry::{Registry, TargetRuntime};
use crate::supervisor;

pub struct App {
    pub auth: Option<Arc<crate::auth::Auth>>,
    pub queen: Queen,
    pub registry: Registry,
    pub meter: Arc<crate::meter::Meter>,
    pub depths: Arc<crate::depth::Depths>,
    pub history: Option<Arc<crate::history::History>>,
    pub queen_url: String,
    pub started_ms: i64,
    /// Held by every declare, every delete and every pass of the reconcile loop.
    ///
    /// Provisioning is stop-then-start, so two of them on one target interleave
    /// into a target that is registered under one spec and running under another.
    /// The reconcile loop makes that a certainty rather than a race, because it
    /// declares on a timer without anybody asking.
    pub declare_lock: tokio::sync::Mutex<()>,
}

impl App {
    /// Everything a declare needs, so a test can build one without the listeners.
    pub fn new(queen: Queen, queen_url: String) -> Self {
        Self {
            auth: None,
            queen,
            registry: Default::default(),
            meter: Arc::new(crate::meter::Meter::default()),
            depths: Arc::new(crate::depth::Depths::default()),
            history: None,
            queen_url,
            started_ms: now_ms(),
            declare_lock: tokio::sync::Mutex::new(()),
        }
    }
}


pub type Shared = Arc<App>;

/// The internal listener: no sign-in, because the cluster boundary is the
/// authentication and the applications inside it cannot do an interactive OAuth
/// round trip.
pub fn router(app: Shared) -> Router {
    routes().with_state(app)
}

/// The public listener: the same routes, plus sign-in and the console, with a
/// session required on every one of them.
pub fn public_router(app: Shared) -> Router {
    routes()
        .route("/api/auth/google/login", get(crate::auth::login))
        .route("/api/auth/google/callback", get(crate::auth::callback))
        .route("/api/auth/logout", get(crate::auth::logout))
        .merge(crate::webapp::router())
        .layer(axum::middleware::from_fn_with_state(
            app.clone(),
            crate::auth::require_session,
        ))
        .with_state(app)
}

fn routes() -> Router<Shared> {
    Router::new()
        // Application-scoped: the identity of a target is the pair, and a sync
        // may only reap inside its own envelope.
        .route("/v1/apps/:app/targets", put(sync_app))
        .route("/v1/apps/:app/targets/:name", put(put_scoped).get(get_scoped).delete(del_scoped))
        .route("/v1/apps/:app/targets/:name/lanes/:lane/push", post(push_scoped))
        .route("/v1/apps/:app/targets/:name/lanes/:lane/next", get(next_scoped))
        // The flat forms resolve inside `default`, so a caller with one
        // application never has to learn the concept exists.
        .route("/v1/targets", put(sync_default))
        .route("/v1/targets/:name", put(put_target).get(get_target).delete(delete_target))
        .route("/v1/targets/:name/lanes/:lane/push", post(push))
        .route("/v1/targets/:name/lanes/:lane/next", get(next))
        // Graphs. A node is a target and a caller never names one: it pushes into
        // an entry and pops a terminal, exactly as it pushed and popped a target.
        .route(
            "/v1/apps/:app/graphs/:name",
            put(put_graph).get(get_graph).delete(del_graph),
        )
        .route("/v1/apps/:app/graphs/:graph/nodes/:node/push", post(graph_push))
        .route("/v1/apps/:app/graphs/:graph/nodes/:node/next", get(graph_next))
        .route("/v1/graphs/:name", put(put_graph_default).get(get_graph_default).delete(del_graph_default))
        .route("/v1/graphs/:graph/nodes/:node/push", post(graph_push_default))
        .route("/v1/graphs/:graph/nodes/:node/next", get(graph_next_default))
        .route("/v1/leases/ack", post(ack))
        .route("/v1/leases/nack", post(nack))
        .route("/v1/leases/renew", post(renew))
        .route("/api/graphs", get(list_graphs))
        .route("/api/graphs/:name", get(get_graph_default))
        .route("/api/graphs/:name/topology", get(graph_topology_default))
        .route("/api/apps/:app/graphs/:name", get(get_graph))
        .route("/api/apps/:app/graphs/:name/topology", get(graph_topology))

        .route("/api/overview", get(overview))
        .route("/api/targets", get(list_targets))
        .route("/api/targets/:name", get(get_target))
        .route("/v1/apps/:app/metrics", get(app_metrics))
        .route("/api/apps", get(list_apps))
        .route("/api/flow", get(flow))
        .route("/api/apps/:app/targets/:name", get(get_scoped))
        .route("/api/budgets", get(shared_budgets))
        .route("/api/breaches/recent", get(recent_breaches))
        .route("/api/rollups", get(rollups))
        .route("/api/traces", get(traces))
        .route("/api/me", get(me))
        .route("/health", get(|| async { Json(json!({"status":"healthy"})) }))
}

use crate::now_ms;

struct Fail(StatusCode, String);

impl IntoResponse for Fail {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

type ApiResult = std::result::Result<Response, Fail>;

// ------------------------------------------------------------- control plane

async fn put_scoped(
    State(st): State<Shared>,
    Path((application, name)): Path<(String, String)>,
    Json(mut spec): Json<TargetSpec>,
) -> ApiResult {
    spec.application = application;
    spec.name = name;
    declare(&st, spec).await
}

async fn put_target(
    State(st): State<Shared>,
    Path(name): Path<String>,
    Json(mut spec): Json<TargetSpec>,
) -> ApiResult {
    spec.name = name;
    declare(&st, spec).await
}

async fn get_scoped(
    State(st): State<Shared>,
    Path((application, name)): Path<(String, String)>,
) -> ApiResult {
    target_view(&st, &application, &name).await
}

async fn del_scoped(
    State(st): State<Shared>,
    Path((application, name)): Path<(String, String)>,
) -> ApiResult {
    remove_target(&st, &application, &name).await
}

async fn push_scoped(
    State(st): State<Shared>,
    Path((application, name, lane)): Path<(String, String, String)>,
    Json(body): Json<PushBody>,
) -> ApiResult {
    do_push(&st, &application, &name, &lane, body).await
}

async fn next_scoped(
    State(st): State<Shared>,
    Path((application, name, lane)): Path<(String, String, String)>,
    Query(q): Query<NextQuery>,
) -> ApiResult {
    do_next(&st, &application, &name, &lane, q).await
}

async fn sync_app(
    State(st): State<Shared>,
    Path(application): Path<String>,
    Json(specs): Json<Vec<TargetSpec>>,
) -> ApiResult {
    do_sync(&st, &application, specs).await
}

async fn sync_default(State(st): State<Shared>, Json(specs): Json<Vec<TargetSpec>>) -> ApiResult {
    do_sync(&st, &gate_core::default_application(), specs).await
}

async fn list_apps(State(st): State<Shared>) -> ApiResult {
    let out: Vec<Value> = st
        .registry
        .applications()
        .into_iter()
        .map(|a| {
            let ts = st.registry.of_app(&a);
            let (mut adm, mut den) = (0u64, 0u64);
            for rt in &ts {
                for l in rt.lanes.values() {
                    let s = l.stats.read();
                    adm += s.admitted;
                    den += s.denied;
                }
            }
            json!({ "application": a, "targets": ts.len(), "admitted": adm, "denied": den })
        })
        .collect();
    Ok(Json(json!(out)).into_response())
}

async fn declare(app: &Shared, spec: TargetSpec) -> ApiResult {
    let _guard = app.declare_lock.lock().await;
    declare_locked(app, spec).await
}

/// The declare itself. Separate because `do_sync` declares a whole set and must
/// hold the lock across all of them — a reconcile pass landing between two of them
/// would see half a configuration and reap the other half.
async fn declare_locked(app: &Shared, spec: TargetSpec) -> ApiResult {
    let name = spec.name.clone();

    let problems = gate_core::validate(&spec);
    if !problems.is_empty() {
        return Err(Fail(
            StatusCode::UNPROCESSABLE_ENTITY,
            problems.iter().map(|p| p.to_string()).collect::<Vec<_>>().join("; "),
        ));
    }

    // G10, from the target side: a graph node IS the target `{graph}.{node}`, so
    // declaring that name standalone would put two owners on one queue family and
    // whichever declared last would be the one enforcing.
    if let Some(g) = app.registry.graph_owning_target(&spec.application, &name) {
        return Err(Fail(
            StatusCode::CONFLICT,
            format!(
                "`{name}` is a node of graph `{}`: it is declared there, not as a standalone target",
                g.spec.name
            ),
        ));
    }
    // And the same question of the STORE, because a declare lands on one replica: a
    // graph declared a second ago on another pod is not in this registry yet, and
    // without this the two would each accept an owner for one queue family and
    // disagree until the reconcile loop noticed. A store that will not answer is not
    // a reason to refuse — the local check still stands.
    if let Ok(stored) = crate::store::try_load_graphs(&app.queen).await {
        if let Some(g) = stored.items.iter().find(|g| {
            g.application == spec.application
                && g.nodes.keys().any(|n| g.node_target_name(n) == name)
        }) {
            return Err(Fail(
                StatusCode::CONFLICT,
                format!(
                    "`{name}` is a node of graph `{}` (declared on another replica): it is declared \
                     there, not as a standalone target",
                    g.name
                ),
            ));
        }
    }


    let old = app.registry.get(&spec.application, &name);
    if let Some(old) = &old {
        if gate_core::needs_version_bump(&old.spec, &spec) && spec.version <= old.spec.version {
            // Sharding is named specially because it moves the QUEUES as well as the
            // counters: work already waiting in the old partitions has no runner in
            // the new layout, and nothing migrates it.
            let resharding = old.spec.shard_by != spec.shard_by
                || old.spec.shard_count() != spec.shard_count();
            return Err(Fail(
                StatusCode::CONFLICT,
                format!(
                    "this change re-founds the counters (a new partition starts at zero): bump version \
                     above {}{}",
                    old.spec.version,
                    if resharding {
                        ". It also re-partitions the push queue: drain this target before applying it, \
                         because work already waiting in the old partitions has no runner in the new \
                         layout"
                    } else {
                        ""
                    }
                ),
            ));
        }

    }

    // Stop-then-start, with the old spec put back if the new one cannot be
    // provisioned. Without that restore the target is left stopped and still
    // registered: it accepts pushes and admits nothing, for ever, which is the one
    // failure an operator cannot recover from without knowing this code.
    let rt = match supervisor::swap(
        &app.queen,
        app.meter.clone(),
        app.history.clone(),
        old.as_ref(),
        spec.clone(),
        None,
    )
    .await
    {
        Ok(rt) => rt,
        Err(failed) => {
            let detail = match failed.restored {
                Some(rt) => {
                    let version = rt.spec.version;
                    app.registry.put(rt);
                    format!("provisioning failed: {}; still serving version {version}", failed.error)
                }
                None => {
                    // Nothing is serving it. Unregister, so a push is refused —
                    // recoverable — rather than accepted into a queue nobody drains.
                    app.registry.remove(&spec.application, &name);
                    format!(
                        "provisioning failed: {}; the old runtime could not be restarted either, so \
                         `{name}` is unregistered and will refuse pushes until a declare succeeds",
                        failed.error
                    )
                }
            };
            return Err(Fail(StatusCode::BAD_GATEWAY, detail));
        }
    };
    app.registry.put(rt.clone());
    // Persisted only after the gates are actually up: a spec saved for a target
    // that failed to provision would come back on the next boot and fail again,
    // for ever.
    match crate::store::save(&app.queen, &spec).await {
        Ok(()) => rt.persisted.store(true, std::sync::atomic::Ordering::Relaxed),
        // NOT persisted, and marked as such: the reconcile loop removes a target
        // the store no longer holds, and it must not read this as a delete.
        Err(e) => tracing::warn!(target_name = %spec.name, error = %e, "declared but not persisted"),
    }

    Ok(Json(json!({

        "ok": true,
        "resolved": {
            "pushQueue": spec.push_queue(),
            "lanes": spec.lanes.iter().map(|l| json!({
                "name": l.name,
                "partition": l.name,
                "admittedQueue": spec.admitted_queue(&l.name),
                "queryId": spec.query_id(&l.name),
            })).collect::<Vec<_>>(),
            "callsQueue": spec.calls_queue(),
        },
        "warnings": gate_core::warnings(&spec).iter().map(|p| p.to_string()).collect::<Vec<_>>(),
    }))
    .into_response())
}

/// Declare the caller's WHOLE configuration, and reap what it no longer names.
///
/// This is what makes `forever` the right lifetime for a stored spec. A TTL
/// would be the obvious way to stop the store growing, and it is the wrong one:
/// a configuration that expires is a configuration that vanishes at three in
/// the morning for a reason nobody can reconstruct, and renewing it on a
/// heartbeat only moves the failure to "the service was down slightly too
/// long". The store does not need an expiry, it needs an OWNER — and the owner
/// is the caller, who says here what the complete set is. Anything not in it is
/// deleted, so a target the caller has stopped declaring stops coming back on
/// every boot.
async fn do_sync(st: &Shared, application: &str, specs: Vec<TargetSpec>) -> ApiResult {
    let _guard = st.declare_lock.lock().await;
    let declared: Vec<String> = specs.iter().map(|s| s.name.clone()).collect();

    let mut applied = Vec::new();
    let mut refused = Vec::new();
    for mut spec in specs {
        spec.application = application.to_string();
        let name = spec.name.clone();
        match declare_locked(st, spec).await {
            Ok(_) => applied.push(name),
            Err(Fail(_, msg)) => refused.push(json!({ "target": name, "error": msg })),
        }
    }


    // Reap, and ONLY inside this application. The flat version of this reaped
    // everything the cell held, so two teams syncing against one deployment
    // would delete each other's targets — including from the durable store.
    // Done after the declares, so a sync that fails half way removes nothing.
    // Graph nodes are exempt: the caller's target list does not name them, they are
    // owned by a graph document, and reaping one would tear down half a topology
    // while its entry node kept accepting pushes.
    let mut removed = Vec::new();
    for rt in st.registry.standalone_of_app(application) {
        if !declared.contains(&rt.spec.name) {

            let name = rt.spec.name.clone();
            // The stored declaration goes first here too: a reap that stops the
            // runners and leaves the spec behind is undone by the next reconcile, on
            // this replica and on every other one.
            if let Err(e) = crate::store::forget(&st.queen, application, &name).await {
                tracing::warn!(target_name = %name, error = %e,
                               "sync: not reaped, the stored spec could not be removed");
                refused.push(json!({ "target": name, "error": format!("not reaped: {e}") }));
                continue;
            }
            supervisor::stop_with(&st.queen, &rt).await;
            st.registry.remove(application, &name);
            removed.push(name);
        }
    }

    Ok(Json(json!({
        "ok": refused.is_empty(), "application": application,
        "applied": applied, "removed": removed, "refused": refused
    }))
    .into_response())
}

async fn get_target(State(st): State<Shared>, Path(name): Path<String>) -> ApiResult {
    target_view(&st, &gate_core::default_application(), &name).await
}

async fn target_view(app: &Shared, application: &str, name: &str) -> ApiResult {
    let rt = app
        .registry
        .get(application, name)
        .ok_or_else(|| Fail(StatusCode::NOT_FOUND, format!("no target `{application}/{name}`")))?;
    let now = now_ms();
    let states = rt.last_state.read().clone();

    let budgets: Vec<Value> = rt
        .spec
        .budgets
        .iter()
        .map(|b| {
            // The worst lane, shard and scope key is the honest one: a budget is as
            // spent as the counter closest to refusing. Asking for the unscoped `-`
            // cell reported a per-listing limit at 0% while it was refusing work.
            let used = states
                .values()
                .map(|s| utilisation_max(b, s, now))
                .fold(0.0f64, f64::max);
            let keys: usize = states.values().map(|s| gate_core::key_count(b, s)).sum();

            json!({
                "id": b.id,
                "cap": b.cap,
                "periodSeconds": b.period_seconds,
                "alignment": b.alignment,
                "scope": b.scope,
                "store": b.store,
                "confidence": b.confidence,
                "source": b.source,
                "as_of": b.as_of,
                "match": b.matcher,
                "maxKeys": b.max_keys,
                "keys": keys,
                "used": used * b.cap,
                "utilisation": used,
            })
        })
        .collect();


    let lanes: Vec<Value> = rt
        .spec
        .lanes
        .iter()
        .map(|l| {
            let rtl = rt.lanes.get(&l.name);
            let stats = rtl.map(|r| r.stats.read().clone()).unwrap_or_default();
            json!({
                "name": l.name,
                "default": l.default,
                "cap_policy": l.cap,
                "concurrency": l.concurrency,
                "lease_seconds": rt.spec.pacing.lease_seconds,
                "effective_cap": rtl.and_then(|r| *r.effective_cap.read()),
                // For a derived lane this, not `effective_cap`, is the number
                // that decides how much of the ceiling it may take: the share
                // of the ceiling the OTHER lanes were measured spending.
                "measured_share": rtl.and_then(|r| *r.measured_share.read()),
                "admitted": stats.admitted,
                "denied": stats.denied,
                "calls": stats.calls,
                "throttled": stats.throttled,
                "last_denial_budget": stats.last_denial_budget,
                "state": if stats.denied > 0 { "pacing" } else { "flowing" },
            })
        })
        .collect();

    let breach = rt.last_breach.read().clone();
    Ok(Json(json!({
        "application": rt.spec.application,
        "name": rt.spec.name,
        "version": rt.spec.version,
        "egress": rt.spec.egress,
        // Named, so the console does not offer an edit button for a target whose
        // owner is a graph document.
        "graph": rt.graph,
        "shardBy": rt.spec.shard_by,
        "shards": rt.spec.shard_count(),
        "spec": rt.spec,

        "budgets": budgets,
        "lanes": lanes,
        "last_breach_budget": breach.as_ref().map(|(b, _)| b.clone()),
        "last_breach_at": breach.as_ref().map(|(_, t)| *t),
    }))
    .into_response())
}

async fn delete_target(State(st): State<Shared>, Path(name): Path<String>) -> ApiResult {
    remove_target(&st, &gate_core::default_application(), &name).await
}

async fn remove_target(app: &Shared, application: &str, name: &str) -> ApiResult {
    let _guard = app.declare_lock.lock().await;
    // A node is deleted by declaring the graph without it, never on its own: half a
    // graph accepts work at its entry and drops it at the hole.
    if let Some(rt) = app.registry.get(application, name) {
        if let Some(g) = &rt.graph {
            return Err(Fail(
                StatusCode::CONFLICT,
                format!(
                    "`{name}` is a node of graph `{g}`: delete the graph, or declare it without this \
                     node"
                ),
            ));
        }
    }
    if app.registry.get(application, name).is_none() {
        return Err(Fail(
            StatusCode::NOT_FOUND,
            format!("no target `{application}/{name}`"),
        ));
    }
    // The store FIRST, and a failure there refuses the delete.
    //
    // The store is what the fleet reconciles against: a target dropped from this
    // registry but still in the store comes back here within one interval and never
    // left the other replicas at all — so a 200 would be a lie that takes fifteen
    // seconds to expose itself.
    crate::store::forget(&app.queen, application, name)
        .await
        .map_err(|e| {
            Fail(
                StatusCode::BAD_GATEWAY,
                format!(
                    "`{application}/{name}` was not deleted: the stored declaration could not be \
                     removed ({e}), and it would come back on the next reconcile"
                ),
            )
        })?;
    match app.registry.remove(application, name) {
        Some(rt) => {
            supervisor::stop_with(&app.queen, &rt).await;
            Ok(Json(json!({ "ok": true })).into_response())
        }
        None => Ok(Json(json!({ "ok": true })).into_response()),
    }
}



// ---------------------------------------------------------------- data plane

#[derive(Deserialize)]
struct PushBody {
    op: String,
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    cost: Option<f64>,
    /// Deterministic per (entity, capability): this is the coalescing lever, not
    /// a safety accessory. Two pushes with the same txn inside the dedup window
    /// collapse to one, so lag compresses the backlog instead of growing it.
    #[serde(default)]
    txn: Option<String>,
    #[serde(default)]
    payload: Value,
    /// Which lane, when the route does not carry one (the graph push routes take a
    /// node, not a lane). Defaults to the node's default lane.
    #[serde(default)]
    lane: Option<String>,
}


async fn push(
    State(st): State<Shared>,
    Path((name, lane)): Path<(String, String)>,
    Json(body): Json<PushBody>,
) -> ApiResult {
    do_push(&st, &gate_core::default_application(), &name, &lane, body).await
}

async fn do_push(
    app: &Shared,
    application: &str,
    name: &str,
    lane: &str,
    body: PushBody,
) -> ApiResult {
    let rt = app
        .registry
        .get(application, name)
        .ok_or_else(|| Fail(StatusCode::NOT_FOUND, format!("no target `{application}/{name}`")))?;
    refuse_graph_node(&rt, "pushed into")?;
    push_into(app, &rt, lane, body, None).await
}

/// A graph node is reachable by its target name — `{graph}.{node}` — and the
/// target routes would serve it like any other target. They must not.
///
/// The graph routes exist to enforce two rules the target routes know nothing
/// about: only an ENTRY may be pushed into (anywhere else skips every budget
/// upstream of it) and only a TERMINAL may be popped (anywhere else steals from
/// the relay, and the item is then executed by the caller AND forwarded by the
/// graph). Reaching a node through the target path would bypass both, and the
/// entry stamp a breach rule reads.
fn refuse_graph_node(rt: &Arc<TargetRuntime>, verb: &str) -> Result<(), Fail> {
    match &rt.graph {
        None => Ok(()),
        Some(key) => {
            let graph = key.split_once('/').map(|(_, g)| g).unwrap_or(key);
            Err(Fail(
                StatusCode::CONFLICT,
                format!(
                    "`{}` is a node of graph `{graph}` and cannot be {verb} as a target: the graph \
                     routes are what enforce which node may be pushed to and which may be popped. \
                     Use /v1/apps/{}/graphs/{graph}/nodes/…",
                    rt.spec.name, rt.spec.application
                ),
            ))
        }
    }
}


/// The one push. Everything above it decides which target, which lane and what
/// envelope; this decides the partition and hands the item to the broker.
async fn push_into(
    app: &Shared,
    rt: &Arc<TargetRuntime>,
    lane: &str,
    body: PushBody,
    meta: Option<Value>,
) -> ApiResult {
    let name = &rt.spec.name;
    if rt.spec.lane(lane).is_none() {
        return Err(Fail(StatusCode::NOT_FOUND, format!("no lane `{lane}` on `{name}`")));
    }

    let cost = body.cost.unwrap_or(rt.spec.cost.default);
    if cost > rt.spec.cost.max {
        return Err(Fail(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("cost {cost} exceeds the declared cost.max {}", rt.spec.cost.max),
        ));
    }

    let mut item = body.payload.clone();
    if !item.is_object() {
        item = json!({});
    }
    let obj = item.as_object_mut().expect("object");
    obj.insert("op".into(), json!(body.op));
    obj.insert(rt.spec.cost.field.clone(), json!(cost));
    if let Some(k) = &body.key {
        obj.insert("key".into(), json!(k));
    }
    // Where this item entered the graph, and how many times it has been retried.
    // Stamped at the first push and carried by every relay, because a breach has to
    // send it back to the entry it came in at — a re-entry anywhere else spends a
    // path it never paid for.
    if let Some(m) = meta {
        obj.insert(GATE_META.to_string(), m);
    }

    // A shard is a partition, and the partition IS the lane when the target is not
    // sharded. A missing dimension is refused rather than defaulted: a counter keyed
    // on an absent value measures the wrong thing, and the gate would deny the item
    // for ever at the head of its lane.
    let partition = match rt.spec.shard_by {
        None => lane.to_string(),
        Some(dim) => match item.get(dim.as_str()).and_then(|v| v.as_str()) {
            Some(v) => rt.spec.push_partition(lane, rt.spec.shard_of(v)),
            None => {
                return Err(Fail(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    format!(
                        "`{name}` is sharded by `{}` and this push carries no `{}`: there is no shard \
                         it could belong to",
                        dim.as_str(),
                        dim.as_str()
                    ),
                ))
            }
        },
    };

    let q = app
        .queen
        .queue(rt.spec.push_queue())
        .partition(partition.clone());
    let res = match body.txn {
        Some(t) => {
            q.push_items(vec![queen_mq::PushItem {
                queue: rt.spec.push_queue(),
                partition: Some(partition),
                payload: item,
                transaction_id: Some(t),
            }])
            .await
        }
        None => q.push(item).await,
    }
    .map_err(|e| Fail(StatusCode::BAD_GATEWAY, e.to_string()))?;

    Ok(Json(json!({ "ok": true, "pushed": res.len() })).into_response())
}


#[derive(Deserialize)]
struct NextQuery {
    #[serde(default)]
    batch: Option<i32>,
    #[serde(default)]
    wait_ms: Option<u64>,
    /// Which lane to pop, for the graph routes, which name a node instead.
    #[serde(default)]
    lane: Option<String>,
}


async fn next(
    State(st): State<Shared>,
    Path((name, lane)): Path<(String, String)>,
    Query(q): Query<NextQuery>,
) -> ApiResult {
    do_next(&st, &gate_core::default_application(), &name, &lane, q).await
}

async fn do_next(
    app: &Shared,
    application: &str,
    name: &str,
    lane: &str,
    q: NextQuery,
) -> ApiResult {
    let rt = app
        .registry
        .get(application, name)
        .ok_or_else(|| Fail(StatusCode::NOT_FOUND, format!("no target `{application}/{name}`")))?;
    refuse_graph_node(&rt, "popped")?;
    next_from(app, &rt, lane, q).await
}

/// The pop itself, with no ownership rule attached: a graph route has already
/// decided that this node is one a caller may consume.
async fn next_from(app: &Shared, rt: &Arc<TargetRuntime>, lane: &str, q: NextQuery) -> ApiResult {
    let l = rt
        .spec
        .lane(lane)
        .ok_or_else(|| Fail(StatusCode::NOT_FOUND, format!("no lane `{lane}`")))?;

    // Long poll. If the gate is not admitting, this simply does not return, and
    // that silence IS the pacing signal — there is no "you are throttled"
    // response for a caller to interpret or back off from.
    let wait = q.wait_ms.unwrap_or(5_000);
    let msgs = app
        .queen
        .queue(rt.spec.admitted_queue(lane))
        .group(format!("gate.exec.{}", lane))
        // `all`, always. A group created at the tail skips every message already
        // admitted and waiting — a consumer that starts after the gate has been
        // running would silently abandon the backlog, and the broker's own default
        // is `new` unless the deployment says otherwise.
        .subscription_mode(SubscriptionMode::All)
        .batch(q.batch.unwrap_or(l.concurrency as i32).max(1))
        .partitions(rt.spec.admitted.partitions.max(1) as i32)

        .wait(true)
        .poll_timeout(std::time::Duration::from_millis(wait))
        .pop()
        .await
        .map_err(|e| Fail(StatusCode::BAD_GATEWAY, e.to_string()))?;

    let items: Vec<Value> = msgs
        .iter()
        .map(|m| json!({ "id": m.id, "payload": m.data }))
        .collect();

    Ok(Json(json!({
        "items": items,
        // Opaque, and deliberately so: it carries what the ack needs, so any
        // replica can settle it. The caller never parses it.
        "lease": serde_json::to_value(&msgs).unwrap_or(Value::Null),
    }))
    .into_response())
}

#[derive(Deserialize)]
struct AckBody {
    lease: Vec<Message>,
    /// Prefix semantics, because queen's ack is positional: you settle the
    /// first N, never an arbitrary subset. Expressed as a count so the shape of
    /// the API makes the mistake inexpressible.
    #[serde(default)]
    up_to: Option<usize>,
    /// The REAL number of HTTP calls the work produced. At push time it was an
    /// estimate; this is what makes the meter able to correct the model.
    #[serde(default)]
    calls: Option<u64>,
    #[serde(default = "default_outcome")]
    outcome: String,
    #[serde(default)]
    target: Option<String>,
    #[serde(default)]
    application: Option<String>,
    /// Told, never inferred: an admitted message's partition is a hash bucket,
    /// not a lane.
    #[serde(default)]
    lane: Option<String>,
    #[serde(default)]
    op: Option<String>,
    #[serde(default)]
    cost_estimated: Option<f64>,
    /// Which budget the vendor's throttle should be attributed to, when the
    /// caller's classifier could tell.
    #[serde(default)]
    budget_id: Option<String>,
    /// The vendor's HTTP status, when the caller kept it. A `breach` rule may match
    /// on it; `outcome: throttled` is what a 429 looks like when it did not.
    #[serde(default)]
    status: Option<i64>,
    /// Override the entry a breached item re-enters at. Validated against the
    /// graph — an entry node or `origin-entry`, never an interior node, because a
    /// re-entry that skips upstream budgets spends a path it never paid for.
    #[serde(default, rename = "retryTo")]
    retry_to: Option<String>,
    /// WHICH items of this lease the outcome is about, by message id.
    ///
    /// `outcome` is one field for a whole lease, so a consumer that runs a batch and
    /// acks once cannot otherwise say "one of these fifty was throttled" — and a
    /// breach rule applied to all fifty would re-enter forty-nine calls the vendor
    /// accepted. Absent, the outcome is taken to be about every item settled by this
    /// ack, which is what a one-item ack means.
    #[serde(default)]
    breached: Option<Vec<String>>,

}


/// The broker's own classification for "this transaction id is already in that
/// queue's dedup window". Matched on the message because the SDK surfaces the
/// broker's `RAISE` text; the code is stable and documented (`QDUP`).
fn is_duplicate(e: &queen_mq::Error) -> bool {
    e.to_string().contains("QDUP")
}

fn default_outcome() -> String {
    "ok".to_string()

}

async fn ack(State(app): State<Shared>, Json(body): Json<AckBody>) -> ApiResult {
    if body.lease.is_empty() {
        return Ok(Json(json!({ "ok": true, "acked": 0 })).into_response());
    }
    let n = body.up_to.unwrap_or(body.lease.len()).min(body.lease.len());
    let slice = &body.lease[..n];
    let application = body
        .application
        .clone()
        .unwrap_or_else(gate_core::default_application);
    let rt = body
        .target
        .as_ref()
        .and_then(|t| app.registry.get(&application, t));

    // A vendor throttle routed back into the graph, paced instead of amplified.
    //
    // The item re-enters at the entry it came in at and WAITS FOR BUDGET — the
    // pacing is the backoff, and no timer is involved. It re-pays every budget on
    // its path, which is correct rather than harsh: the vendor counted the call that
    // failed, so the retry is a new call and owes the whole path.
    let retro = plan_retro(&app, &rt, &body, slice);




    // The cursor advance and the measurement event go in ONE transaction.
    // Split across two calls the failure is asymmetric and nasty: if the ack
    // lands and the event does not, the work is settled but the spend is never
    // counted, the meter under-reads, the derived cap rises, and the limiter
    // believes it has budget it does not — overshooting in silence exactly when
    // it is already in trouble.
    //
    // The retro pushes ride in the same transaction, for the same reason and one
    // more: a retry that lands without its ack is a duplicate, and an ack that
    // lands without its retry is a call the vendor refused and nobody will make
    // again.
    let lane = body.lane.clone().unwrap_or_default();
    let event = body.target.as_ref().map(|t| {

        json!({
            "target": t,
            "lane": lane,
            "op": body.op.clone().unwrap_or_default(),
            "calls": body.calls.unwrap_or(n as u64),
            "cost_estimated": body.cost_estimated.unwrap_or(n as f64),
            "outcome": body.outcome,
            "at": now_ms(),
        })
    });

    let mut already_re_entered = false;
    match (&event, &rt) {
        (Some(ev), Some(target)) => {
            let mut tx = app.queen.transaction();
            for m in slice {
                tx = tx.ack(m);
            }
            tx = tx
                .push(target.spec.calls_queue(), ev.clone())
                .map_err(|e| Fail(StatusCode::BAD_GATEWAY, e.to_string()))?;
            for item in &retro.pushes {
                tx = tx
                    .push_item(item.clone())
                    .map_err(|e| Fail(StatusCode::BAD_GATEWAY, e.to_string()))?;
            }
            match tx.commit().await {
                Ok(_) => {}
                // A re-entry this ack has already made.
                //
                // A duplicate transaction id is a soft verdict for a plain push and a
                // HARD one inside a transaction — the broker raises `QDUP` and rolls
                // the whole bundle back (queen `005_log_ack.sql`: "a duplicate is a
                // SOFT verdict for plain pushes but a HARD error inside a
                // transaction"). The retro push is keyed `{txn}:r{attempt}`, so this
                // means exactly one thing: this ack has been sent before, the retry is
                // already waiting for budget, and all that is left to do is settle.
                // Failing here instead would leave a caller retrying an ack that can
                // never succeed until its lease lapses — and then doing the work again.
                Err(e) if is_duplicate(&e) => {
                    already_re_entered = true;
                    app.queen
                        .ack_all(slice)
                        .await
                        .map_err(|e| Fail(StatusCode::BAD_GATEWAY, e.to_string()))?;
                }
                Err(e) => return Err(Fail(StatusCode::BAD_GATEWAY, e.to_string())),
            }
        }
        _ => {
            app.queen
                .ack_all(slice)
                .await
                .map_err(|e| Fail(StatusCode::BAD_GATEWAY, e.to_string()))?;
        }
    }

    if let Some(t) = &rt {

        // The lane is told to us, not guessed. It used to be inferred from the
        // message's partition, which is a hash bucket (`p0`..`pN`) and never
        // matched a lane name, so every ack landed on whichever lane happened to
        // be first in the map.
        if let Some(l) = t.lanes.get(&lane) {
            let mut s = l.stats.write();
            s.calls += body.calls.unwrap_or(n as u64);
            if body.outcome == "throttled" {
                s.throttled += 1;
            }
            s.retried += if already_re_entered { 0 } else { retro.pushes.len() as u64 };

            s.exhausted += retro.exhausted;
        }
        if body.outcome == "throttled" {
            *t.last_breach.write() =
                Some((body.budget_id.clone().unwrap_or_else(|| "unattributed".into()), now_ms()));
        }
        // Traces, because a retry that is paced looks like nothing at all from the
        // outside: the queue simply takes longer to drain. These are the only record
        // that a vendor refused work this gate had admitted and what became of it.
        for outcome in retro.traces {
            app.meter.record_outcome(
                &t.spec.key(),
                &lane,
                &body.op.clone().unwrap_or_default(),
                &outcome,
                body.budget_id.as_deref(),
                now_ms(),
            );
        }
    }

    Ok(Json(json!({
        "ok": true,
        "acked": n,
        // Named, because a caller that acks `throttled` and sees `retried: 0` is
        // looking at a graph with no breach rule, and that is worth knowing.
        // Zero when the re-entry was already made by an earlier copy of this same
        // ack: the caller is not being told a retry happened twice.
        "retried": if already_re_entered { 0 } else { retro.pushes.len() },
        "exhausted": retro.exhausted,
        "unroutable": retro.unroutable,
        "refused": if already_re_entered {
            Some("this ack had already been settled; its re-entry is already waiting for budget"
                .to_string())
        } else {
            retro.refused.clone()
        },

    }))

    .into_response())
}

/// What a breach rule does to one settled batch.
#[derive(Default)]
struct Retro {
    /// One push per item that re-enters, already carrying its bumped attempt count.
    pushes: Vec<TxnPushItem>,
    /// Items that had used up `maxAttempts` and are settled instead.
    exhausted: u64,
    /// Items a rule matched but that had nowhere to go — no entry could be resolved,
    /// or the entry could never admit them. Settled, counted and traced, never
    /// silently dropped.
    unroutable: u64,
    /// Why nothing was retried, when the caller asked for something this graph
    /// cannot do. Reported in the ack's answer rather than as a failed ack: the work
    /// has already been done, and refusing to settle it would have it done again.
    refused: Option<String>,
    traces: Vec<String>,
}

/// Work out the re-entries for an ack, without touching the broker.
///
/// Everything it needs is already in hand: the lease carries the payloads (a `next`
/// hands them back verbatim), the payload carries the entry it came in at and its
/// attempt count, and the graph document carries the rules. So a retry costs no
/// round trip and no state — it is the ack's own transaction with one more push in
/// it.
fn plan_retro(
    app: &Shared,
    rt: &Option<Arc<TargetRuntime>>,
    body: &AckBody,
    slice: &[Message],
) -> Retro {
    let mut out = Retro::default();
    let Some(rt) = rt else { return out };
    let Some(graph_key) = rt.graph.clone() else {
        // A standalone target has no graph to re-enter, and inventing one would be a
        // retry loop on the same queue with no budget between the attempts.
        if body.retry_to.is_some() {
            out.refused = Some(format!(
                "`{}` is not a graph node: there is no entry to retry to",
                rt.spec.name
            ));
        }
        return out;
    };
    let Some(g) = app.registry.graph_by_key(&graph_key) else {
        return out;
    };

    // A rule has to MATCH this ack, whether or not the caller also named a
    // `retryTo`. An override chooses the door; it does not decide that a breach
    // happened — otherwise an `outcome: ok` with a stray `retryTo` would re-enter
    // work that succeeded.
    let Some(declared) = g.spec.breach_for(&body.outcome, body.status) else {
        if body.retry_to.is_some() {
            out.refused = Some(format!(
                "no breach rule of graph `{}` matches outcome `{}`{}: a retryTo names the door, it \
                 does not declare the breach",
                g.spec.name,
                body.outcome,
                body.status.map(|s| format!(" / status {s}")).unwrap_or_default(),
            ));
        }
        return out;
    };
    let rule = match &body.retry_to {
        None => declared.clone(),
        Some(to) => {
            let mut r = declared.clone();
            r.retry_to = to.clone();
            if r.retry_to != gate_core::ORIGIN_ENTRY && !g.spec.is_entry(&r.retry_to) {
                out.refused = Some(format!(
                    "`retryTo: {to}` is not an entry of `{}`: a re-entry that skips the upstream \
                     budgets spends a path it never paid for. Entries: {}",
                    g.spec.name,
                    g.spec.entries().join(", ")
                ));
                return out;
            }
            r
        }
    };

    // Which items the breach is about.
    //
    // `outcome` is ONE field for a whole lease, and a consumer that runs a batch of
    // fifty and acks once would otherwise re-enter all fifty because one of them was
    // refused — forty-nine duplicate calls, in the name of not amplifying retries.
    // So a caller that batches names the ids it is talking about; without them the
    // rule applies to the settled slice, which is exactly right for the one-item ack
    // and is what a caller asked for by not narrowing it.
    let breached: Option<std::collections::HashSet<&str>> = body
        .breached
        .as_ref()
        .map(|ids| ids.iter().map(|s| s.as_str()).collect());

    for m in slice {
        if let Some(only) = &breached {
            if !only.contains(m.id.as_str()) {
                continue;
            }
        }
        let meta = m.data.get(GATE_META);
        let attempt = meta
            .and_then(|v| v.get("attempt"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let origin = meta.and_then(|v| v.get("entry")).and_then(|v| v.as_str());
        let next = attempt + 1;

        if next > rule.max_attempts as u64 {
            out.exhausted += 1;
            out.traces.push("exhausted".to_string());
            continue;
        }
        let Some(entry) = g.spec.retry_entry(&rule, origin) else {
            // No stamp and more than one entry: we do not guess which door it came
            // in at, because the wrong one skips a budget.
            out.unroutable += 1;
            out.traces.push("unroutable".to_string());
            continue;
        };
        let Some(ns) = g.spec.node_spec(&entry) else {
            out.unroutable += 1;
            out.traces.push("unroutable".to_string());
            continue;
        };
        let Some(lane) = ns.default_lane().map(|l| l.name.clone()) else {
            out.unroutable += 1;
            out.traces.push("unroutable".to_string());
            continue;
        };
        // The same check the push route makes, for the same reason: an item costing
        // more than the entry's `cost.max` can never be admitted there, and it would
        // sit at the head of that lane for ever without ever reaching a DLQ.
        // Validation (`retry-cost`) refuses the topologies where this is possible, so
        // reaching it means a document changed under work already in flight.
        let cost = m
            .data
            .get(&ns.cost.field)
            .and_then(|v| v.as_f64())
            .unwrap_or(ns.cost.default);
        if cost > ns.cost.max {
            out.unroutable += 1;
            out.traces.push("unroutable".to_string());
            continue;
        }
        let partition = match ns.shard_by {
            None => lane,
            Some(dim) => match m.data.get(dim.as_str()).and_then(|v| v.as_str()) {
                Some(v) => ns.push_partition(&lane, ns.shard_of(v)),
                None => {
                    out.unroutable += 1;
                    out.traces.push("unroutable".to_string());
                    continue;
                }
            },
        };

        let mut payload = m.data.clone();
        if let Some(obj) = payload.as_object_mut() {
            let mut meta = obj
                .get(GATE_META)
                .cloned()
                .unwrap_or_else(|| json!({ "graph": g.spec.name }));
            if let Some(mo) = meta.as_object_mut() {
                mo.insert("attempt".into(), json!(next));
                mo.insert("entry".into(), json!(entry));
            }
            obj.insert(GATE_META.to_string(), meta);
        }

        out.pushes.push(TxnPushItem {
            queue: ns.push_queue(),
            partition: Some(partition),
            payload,
            // The attempt is IN the id, so a replayed ack re-pushes the same
            // re-entry and the broker's dedup collapses it — while a genuine second
            // attempt is a different id and is not mistaken for one.
            transaction_id: Some(format!("{}:r{next}", m.transaction_id)),
            trace_id: None,
        });
        out.traces.push("retried".to_string());
    }
    out
}



#[derive(Deserialize)]
struct NackBody {
    lease: Vec<Message>,
    #[serde(default)]
    reason: Option<String>,
}

/// Not attempted. The work goes back to its lane AND the budget is refunded,
/// because the vendor never saw the call — which is the whole distinction
/// against an `ack` with `outcome: throttled`, where the request did leave, the
/// vendor did count it, and refunding would be a lie told to our own meter.
async fn nack(State(app): State<Shared>, Json(body): Json<NackBody>) -> ApiResult {
    if body.lease.is_empty() {
        return Ok(Json(json!({ "ok": true, "nacked": 0 })).into_response());
    }
    let reason = body.reason.unwrap_or_else(|| "not attempted".into());
    app.queen
        .nack_all(&body.lease, reason)
        .await
        .map_err(|e| Fail(StatusCode::BAD_GATEWAY, e.to_string()))?;
    Ok(Json(json!({ "ok": true, "nacked": body.lease.len() })).into_response())
}

#[derive(Deserialize)]
struct RenewBody {
    lease: Vec<Message>,
    #[serde(default = "default_renew")]
    seconds: i32,
}

fn default_renew() -> i32 {
    30
}

/// For a slow call. Without it the lease can expire mid-work and the item is
/// redelivered while the first attempt is still in flight — which is why the
/// design forbids doing the HTTP call inside the gate cycle, where no renewal
/// exists at all.
async fn renew(State(app): State<Shared>, Json(body): Json<RenewBody>) -> ApiResult {
    let mut renewed = 0usize;
    for m in &body.lease {
        if app.queen.renew(m, Some(body.seconds)).await.is_ok() {
            renewed += 1;
        }
    }
    Ok(Json(json!({ "ok": true, "renewed": renewed })).into_response())
}

// --------------------------------------------------------------------- graphs

async fn put_graph(
    State(st): State<Shared>,
    Path((application, name)): Path<(String, String)>,
    Json(mut spec): Json<GraphSpec>,
) -> ApiResult {
    spec.application = application;
    spec.name = name;
    declare_graph(&st, spec).await
}

async fn put_graph_default(
    State(st): State<Shared>,
    Path(name): Path<String>,
    Json(mut spec): Json<GraphSpec>,
) -> ApiResult {
    spec.name = name;
    declare_graph(&st, spec).await
}

async fn declare_graph(st: &Shared, spec: GraphSpec) -> ApiResult {
    let _guard = st.declare_lock.lock().await;
    let rt = crate::graph::declare(st, spec).await.map_err(|r| match r {
        crate::graph::Refusal::Invalid(m) => Fail(StatusCode::UNPROCESSABLE_ENTITY, m),
        crate::graph::Refusal::Conflict(m) => Fail(StatusCode::CONFLICT, m),
        crate::graph::Refusal::Gateway(m) => Fail(StatusCode::BAD_GATEWAY, m),
    })?;

    // The resolved topology, so a caller can see what its document became without
    // learning the naming rules: which node is pushable, which is poppable, and
    // which queues the relays own.
    let nodes: Vec<Value> = rt
        .spec
        .nodes
        .keys()
        .map(|n| {
            let ns = rt.spec.node_spec(n).expect("declared");
            json!({
                "name": n,
                "target": ns.name,
                "entry": rt.spec.is_entry(n),
                "consume": rt.spec.is_consume(n),
                "pushQueue": ns.push_queue(),
                "shards": ns.shard_count(),
                "admittedQueues": ns.lanes.iter().map(|l| ns.admitted_queue(&l.name))
                    .collect::<Vec<_>>(),
            })
        })
        .collect();

    Ok(Json(json!({
        "ok": true,
        "resolved": {
            "nodes": nodes,
            "edges": rt.spec.edges,
            "relays": rt.relays.iter().map(|r| json!({
                "dest": r.dest,
                "sources": r.sources.iter().map(|(n, p)| json!({ "node": n, "priority": p }))
                    .collect::<Vec<_>>(),
                // How deep the destination's push queue is allowed to get. Shallow
                // is what makes priority at the entrance priority in fact.
                "window": r.window,
            })).collect::<Vec<_>>(),
            "consume": rt.spec.consume,
        },
        "warnings": gate_core::graph_warnings(&rt.spec).iter().map(|p| p.to_string())
            .collect::<Vec<_>>(),
    }))
    .into_response())
}

/// The flat form resolves a graph by name: the default application first, then a
/// unique match anywhere. Several owners of one name is the one case we refuse to
/// guess at.
fn resolve_graph(
    st: &Shared,
    application: Option<&str>,
    name: &str,
) -> Result<Arc<crate::registry::GraphRuntime>, Fail> {
    if let Some(app) = application {
        return st
            .registry
            .graph(app, name)
            .ok_or_else(|| Fail(StatusCode::NOT_FOUND, format!("no graph `{app}/{name}`")));
    }
    if let Some(g) = st.registry.graph(&gate_core::default_application(), name) {
        return Ok(g);
    }
    let matches: Vec<_> = st
        .registry
        .graphs()
        .into_iter()
        .filter(|g| g.spec.name == name)
        .collect();
    match matches.len() {
        0 => Err(Fail(StatusCode::NOT_FOUND, format!("no graph `{name}`"))),
        1 => Ok(matches.into_iter().next().expect("one")),
        _ => Err(Fail(
            StatusCode::CONFLICT,
            format!(
                "`{name}` is declared by {} applications; name one: /v1/apps/:app/graphs/{name}",
                matches.len()
            ),
        )),
    }
}

async fn get_graph(
    State(st): State<Shared>,
    Path((application, name)): Path<(String, String)>,
) -> ApiResult {
    let g = resolve_graph(&st, Some(&application), &name)?;
    Ok(Json(crate::graph::view(&st, &g).await).into_response())
}

async fn get_graph_default(State(st): State<Shared>, Path(name): Path<String>) -> ApiResult {
    let g = resolve_graph(&st, None, &name)?;
    Ok(Json(crate::graph::view(&st, &g).await).into_response())
}

async fn del_graph(
    State(st): State<Shared>,
    Path((application, name)): Path<(String, String)>,
) -> ApiResult {
    remove_graph(&st, &application, &name).await
}

async fn del_graph_default(State(st): State<Shared>, Path(name): Path<String>) -> ApiResult {
    let g = resolve_graph(&st, None, &name)?;
    let application = g.spec.application.clone();
    remove_graph(&st, &application, &name).await
}

async fn remove_graph(st: &Shared, application: &str, name: &str) -> ApiResult {
    let _guard = st.declare_lock.lock().await;
    match crate::graph::remove(st, application, name).await {
        Ok(true) => Ok(Json(json!({ "ok": true })).into_response()),
        Ok(false) => Err(Fail(
            StatusCode::NOT_FOUND,
            format!("no graph `{application}/{name}`"),
        )),
        Err(crate::graph::Refusal::Invalid(m)) => Err(Fail(StatusCode::UNPROCESSABLE_ENTITY, m)),
        Err(crate::graph::Refusal::Conflict(m)) => Err(Fail(StatusCode::CONFLICT, m)),
        Err(crate::graph::Refusal::Gateway(m)) => Err(Fail(StatusCode::BAD_GATEWAY, m)),
    }
}


async fn graph_push(
    State(st): State<Shared>,
    Path((application, graph, node)): Path<(String, String, String)>,
    Json(body): Json<PushBody>,
) -> ApiResult {
    do_graph_push(&st, Some(&application), &graph, &node, body).await
}

async fn graph_push_default(
    State(st): State<Shared>,
    Path((graph, node)): Path<(String, String)>,
    Json(body): Json<PushBody>,
) -> ApiResult {
    do_graph_push(&st, None, &graph, &node, body).await
}

async fn do_graph_push(
    st: &Shared,
    application: Option<&str>,
    graph: &str,
    node: &str,
    body: PushBody,
) -> ApiResult {
    let g = resolve_graph(st, application, graph)?;
    if !g.spec.nodes.contains_key(node) {
        return Err(Fail(
            StatusCode::NOT_FOUND,
            format!("graph `{graph}` has no node `{node}`"),
        ));
    }
    // An interior node's push queue belongs to its in-edges. A caller pushing there
    // would inject work that has paid none of the upstream budgets — which is the
    // whole point of the path it skipped.
    if !g.spec.is_entry(node) {
        return Err(Fail(
            StatusCode::CONFLICT,
            format!(
                "`{node}` is not an entry of `{graph}`: its push queue is fed by {} and pushing there \
                 would skip every budget upstream of it. Entries: {}",
                g.spec
                    .in_edges(node)
                    .iter()
                    .map(|e| e.from.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                g.spec.entries().join(", ")
            ),
        ));
    }

    let ns = g.spec.node_spec(node).expect("declared");
    let rt = st
        .registry
        .get(&ns.application, &ns.name)
        .ok_or_else(|| Fail(StatusCode::SERVICE_UNAVAILABLE, format!("node `{node}` is not running")))?;
    let lane = match &body.lane {
        Some(l) => l.clone(),
        None => ns
            .default_lane()
            .map(|l| l.name.clone())
            .ok_or_else(|| Fail(StatusCode::CONFLICT, format!("node `{node}` has no default lane")))?,
    };

    let meta = json!({ "graph": g.spec.name, "entry": node, "attempt": 0 });
    push_into(st, &rt, &lane, body, Some(meta)).await
}

async fn graph_next(
    State(st): State<Shared>,
    Path((application, graph, node)): Path<(String, String, String)>,
    Query(q): Query<NextQuery>,
) -> ApiResult {
    do_graph_next(&st, Some(&application), &graph, &node, q).await
}

async fn graph_next_default(
    State(st): State<Shared>,
    Path((graph, node)): Path<(String, String)>,
    Query(q): Query<NextQuery>,
) -> ApiResult {
    do_graph_next(&st, None, &graph, &node, q).await
}

async fn do_graph_next(
    st: &Shared,
    application: Option<&str>,
    graph: &str,
    node: &str,
    q: NextQuery,
) -> ApiResult {
    let g = resolve_graph(st, application, graph)?;
    if !g.spec.nodes.contains_key(node) {
        return Err(Fail(
            StatusCode::NOT_FOUND,
            format!("graph `{graph}` has no node `{node}`"),
        ));
    }
    // An interior node's admitted queue is the relay's source. A caller popping it
    // steals from the graph: two consumers on one queue split the work at random and
    // the item never reaches the node that was supposed to pace it.
    if !g.spec.is_consume(node) {
        return Err(Fail(
            StatusCode::CONFLICT,
            format!(
                "`{node}` is not consumable: its admitted queue is drained by the relay into {}. Pop \
                 one of: {}",
                g.spec
                    .out_edges(node)
                    .iter()
                    .map(|e| e.to.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                g.spec.consume.join(", ")
            ),
        ));
    }

    let ns = g.spec.node_spec(node).expect("declared");
    let lane = match &q.lane {
        Some(l) => l.clone(),
        None => ns
            .default_lane()
            .map(|l| l.name.clone())
            .ok_or_else(|| Fail(StatusCode::CONFLICT, format!("node `{node}` has no default lane")))?,
    };
    let rt = st
        .registry
        .get(&ns.application, &ns.name)
        .ok_or_else(|| Fail(StatusCode::SERVICE_UNAVAILABLE, format!("node `{node}` is not running")))?;
    next_from(st, &rt, &lane, q).await
}


async fn list_graphs(State(st): State<Shared>) -> ApiResult {
    let mut out = Vec::new();
    for g in st.registry.graphs() {
        let mut nodes = Vec::new();
        for (name, ns) in g.spec.node_specs() {
            let waiting: u64 = st
                .depths
                .pending(&st.queen, &ns.push_queue())
                .await
                .values()
                .sum();
            let rt = st.registry.get(&ns.application, &ns.name);
            let (mut admitted, mut denied) = (0u64, 0u64);
            if let Some(rt) = &rt {
                for l in rt.lanes.values() {
                    let s = l.stats.read();
                    admitted += s.admitted;
                    denied += s.denied;
                }
            }
            nodes.push(json!({
                "name": name,
                "target": ns.name,
                "entry": g.spec.is_entry(&name),
                "consume": g.spec.is_consume(&name),
                "running": rt.is_some(),
                "budgets": ns.budgets.len(),
                "shards": ns.shard_count(),
                "waiting_for_budget": waiting,
                "admitted": admitted,
                "denied": denied,
            }));
        }
        out.push(json!({
            "application": g.spec.application,
            "name": g.spec.name,
            "version": g.spec.version,
            "nodes": nodes,
            "edges": g.spec.edges,
            "consume": g.spec.consume,
            "breach": g.spec.breach,
            "forwarded": g.relays.iter().map(|r| r.forwarded()).sum::<u64>(),
        }));
    }
    Ok(Json(json!(out)).into_response())
}

async fn graph_topology(
    State(st): State<Shared>,
    Path((application, name)): Path<(String, String)>,
) -> ApiResult {
    let g = resolve_graph(&st, Some(&application), &name)?;
    Ok(Json(crate::graph::topology(&g)).into_response())
}

async fn graph_topology_default(State(st): State<Shared>, Path(name): Path<String>) -> ApiResult {
    let g = resolve_graph(&st, None, &name)?;
    Ok(Json(crate::graph::topology(&g)).into_response())
}

// -------------------------------------------------------------- console read


#[derive(Deserialize)]
struct RollupQuery {
    target: String,
    #[serde(default)]
    application: Option<String>,
    #[serde(default)]
    minutes: Option<usize>,
}

/// Every application on one axis: how close each one came to its own ceiling,
/// minute by minute.
///
/// Applications have different ceilings — 150/s here, 20/s there — so raw
/// admissions cannot share a y-axis without the big one burying the small one.
/// Utilisation can, and it is also the question a dashboard is asked: not "how
/// much did we send" but "how close are we to being refused".
///
/// One line per application, and its value is the utilisation of its BUSIEST
/// target that minute, not the average across its targets. A team with four
/// quiet targets and one pinned at its cap is a team being throttled, and an
/// average would draw that as a comfortable 20%. The trade is that volume is
/// invisible here — the target detail pages hold that — so the point carries
/// which target it was, and the tooltip names it.
async fn flow(State(app): State<Shared>, Query(q): Query<FlowQuery>) -> ApiResult {
    let now = now_ms();
    let minutes = q.minutes.unwrap_or(120).clamp(1, 1440) as i64;

    // Ceiling per target, from the declaration rather than from the data: it is
    // what the gate enforces, and a target that admitted nothing this minute
    // still has one.
    let mut ceiling: HashMap<(String, String), f64> = HashMap::new();
    for rt in app.registry.all() {
        let per_min = rt
            .spec
            .budgets
            .iter()
            .map(|b| b.rate_per_sec() * 60.0)
            .fold(f64::INFINITY, f64::min);
        if per_min.is_finite() && per_min > 0.0 {
            ceiling.insert(
                (rt.spec.application.clone(), rt.spec.name.clone()),
                per_min,
            );
        }
    }

    let rows = match app.history.as_ref() {
        Some(h) => h.flow(minutes, now).await,
        // Without a database this is one replica's ring, same as every other
        // history surface, and it says so by being short rather than by lying.
        None => app.meter.flow(minutes as usize, now),
    };

    // (application, minute) -> the busiest target in it
    struct Cell {
        utilisation: f64,
        target: String,
        admitted: i64,
        ceiling: f64,
        total: i64,
    }
    let mut cells: HashMap<(String, i64), Cell> = HashMap::new();
    let mut minute_set: BTreeSet<i64> = BTreeSet::new();

    for (application, target, minute, admitted) in rows {
        minute_set.insert(minute);
        // A row whose target is no longer declared still counts toward the
        // application's volume; it just has no ceiling to be measured against.
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
    let mut apps: BTreeSet<String> = BTreeSet::new();
    for (a, _) in cells.keys() {
        apps.insert(a.clone());
    }

    let series: Vec<Value> = apps
        .iter()
        .map(|a| {
            let points: Vec<Value> = minutes_axis
                .iter()
                .map(|t| match cells.get(&(a.clone(), *t)) {
                    Some(c) => json!({
                        "t": t,
                        "utilisation": c.utilisation,
                        "target": c.target,
                        "admitted": c.admitted,
                        "ceiling": c.ceiling,
                        "total_admitted": c.total,
                    }),
                    // A minute this application did not appear in is a minute it
                    // admitted nothing, which is a real zero and not a gap.
                    None => json!({ "t": t, "utilisation": 0.0, "admitted": 0, "total_admitted": 0 }),
                })
                .collect();
            json!({ "application": a, "points": points })
        })
        .collect();

    Ok(Json(json!({
        "minutes": minutes_axis,
        "applications": series,
        "durable": app.history.is_some(),
    }))
    .into_response())
}

#[derive(Deserialize)]
struct FlowQuery {
    #[serde(default)]
    minutes: Option<usize>,
}

async fn rollups(State(app): State<Shared>, Query(q): Query<RollupQuery>) -> ApiResult {
    // Without a database this replica's own ring is all there is. It is honest
    // for a single-replica deployment and quietly partial for several, which is
    // the reason the database exists.
    let Some(h) = app.history.as_ref() else {
        return Ok(Json(json!(app.meter.rollups(&q.target, q.minutes.unwrap_or(120)))).into_response());
    };
    let (a, t) = match q.target.split_once('/') {
        Some((a, t)) => (a.to_string(), t.to_string()),
        None => (
            q.application.clone().unwrap_or_else(gate_core::default_application),
            q.target.clone(),
        ),
    };
    Ok(Json(json!(h.rollups(&a, &t, q.minutes.unwrap_or(120) as i64).await)).into_response())
}

#[derive(Deserialize)]
struct TraceQuery {
    #[serde(default)]
    outcome: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

async fn traces(State(app): State<Shared>, Query(q): Query<TraceQuery>) -> ApiResult {
    match app.history.as_ref() {
        Some(h) => Ok(Json(json!(
            h.traces(q.outcome.as_deref(), q.limit.unwrap_or(100) as i64).await
        ))
        .into_response()),
        // Without a database the recent tail still lives in memory, which is
        // enough to debug a running cell even though it answers nothing about
        // yesterday.
        None => Ok(Json(json!(app
            .meter
            .traces(q.outcome.as_deref(), q.limit.unwrap_or(100))))
        .into_response()),
    }
}

#[derive(Deserialize)]
struct LimitQuery {
    #[serde(default)]
    limit: Option<usize>,
}

async fn recent_breaches(State(app): State<Shared>, Query(q): Query<LimitQuery>) -> ApiResult {
    let limit = q.limit.unwrap_or(10);
    // A breach is a trace with `throttled` on it — the vendor refused something
    // this gate had admitted — so it is one query against the same table, and
    // it is the one number that must not be per-replica: a breach seen by the
    // pod nobody is looking at is a breach nobody sees.
    match app.history.as_ref() {
        Some(h) => Ok(Json(json!(h.traces(Some("throttled"), limit as i64).await)).into_response()),
        None => Ok(Json(json!(app.meter.breaches(limit))).into_response()),
    }
}

/// Budgets declared with `store: kv` are the ones that cross targets. They are
/// listed on their own because they are the only limits a single gate cannot
/// see, and therefore the only ones settled by a round trip instead of by
/// arithmetic in memory.
/// The budgets that cross targets, one row per budget rather than one per
/// declaration.
///
/// A shared budget is not an object anybody creates: it is what two targets
/// have when they declare a `store: kv` budget with the same id inside the same
/// application, because the kv key IS `{application}:{id}:{window}` and that is
/// what makes the spend shared. Listing one row per declaring target would show
/// the same ceiling twice, each with the same `used`, and read as two budgets
/// that happen to be equally busy.
///
/// Which is also why disagreement has to be reported and not resolved: if two
/// targets declare the same id with different caps, they are already spending
/// against one key and one of the two declarations is a lie. The console cannot
/// tell which, so it names both.
async fn shared_budgets(State(app): State<Shared>) -> ApiResult {
    let now = now_ms();

    // (application, id) -> the declarations that claim it
    let mut groups: HashMap<(String, String), Vec<(String, gate_core::Budget)>> = HashMap::new();
    for rt in app.registry.all() {
        for b in rt.spec.budgets.iter().filter(|b| b.store == gate_core::Store::Kv) {
            groups
                .entry((rt.spec.application.clone(), b.id.clone()))
                .or_default()
                .push((rt.spec.key(), b.clone()));
        }
    }

    let mut out = Vec::new();
    for ((application, id), members) in groups {
        let (_, first) = &members[0];
        let sb = crate::shared::SharedBudget {
            scope: application.clone(),
            id: id.clone(),
            cap: first.cap as i64,
            period_seconds: first.period_seconds,
        };
        let used = crate::shared::used(&app.queen, &sb, now).await;

        // The lease this deployment is holding out of the shared pool, summed
        // over every target that took one: it is spent capacity that has not
        // reached the vendor yet, and the difference between `used` and it is
        // the only reason the two numbers ever disagree.
        let local: i64 = app
            .registry
            .all()
            .iter()
            .flat_map(|rt| rt.pools.iter())
            .filter(|p| p.budget.scope == application && p.budget.id == id)
            .map(|p| p.remaining())
            .sum();

        let conflicts: Vec<Value> = members
            .iter()
            .filter(|(_, b)| {
                b.cap != first.cap
                    || b.period_seconds != first.period_seconds
                    || b.alignment != first.alignment
            })
            .map(|(t, b)| {
                json!({ "target": t, "cap": b.cap, "periodSeconds": b.period_seconds,
                        "alignment": b.alignment })
            })
            .collect();

        out.push(json!({
            "id": id,
            "application": application,
            "cap": first.cap,
            "periodSeconds": first.period_seconds,
            "alignment": first.alignment,
            "enforcement": "reserve",
            "confidence": first.confidence,
            "used": used,
            "local_lease": local,
            "members": members.iter().map(|(t, _)| t.clone()).collect::<Vec<_>>(),
            // Empty is the normal case and the only one that means anything is
            // being enforced as declared.
            "conflicts": conflicts,
        }));
    }
    out.sort_by(|a, b| {
        (a["application"].as_str(), a["id"].as_str()).cmp(&(b["application"].as_str(), b["id"].as_str()))
    });
    Ok(Json(json!(out)).into_response())
}

/// What an application's own product should tell ITS users about the limits.
///
/// Deliberately not the console's shape. The console answers "are we still in
/// control", which is an operator's question. This answers "when will my change
/// be live", which is what somebody waiting on a price update actually wants —
/// so the number that carries the page is `drain_eta_seconds`, and the two
/// backlogs are kept apart because one of them is gate holding work back on
/// purpose and the other is the caller's own workers falling behind.
async fn app_metrics(State(app): State<Shared>, Path(application): Path<String>) -> ApiResult {
    let now = now_ms();
    let mut targets = Vec::new();

    for rt in app.registry.of_app(&application) {
        let push = app.depths.pending(&app.queen, &rt.spec.push_queue()).await;

        let mut lanes = Vec::new();
        let (mut waiting_budget, mut waiting_workers, mut rate) = (0u64, 0u64, 0.0f64);

        for lane in &rt.spec.lanes {
            let admitted_q = rt.spec.admitted_queue(&lane.name);
            let admitted_pending: u64 = app
                .depths
                .pending(&app.queen, &admitted_q)
                .await
                .values()
                .sum();
            // Every partition this lane owns. A sharded target's push partitions are
            // `{lane}:{shard}`, and looking only for the bare lane name reported a
            // sharded target as having nothing waiting — which is the one number this
            // endpoint exists to answer.
            let shard_prefix = format!("{}:", lane.name);
            let budget_pending: u64 = push
                .iter()
                .filter(|(p, _)| *p == &lane.name || p.starts_with(&shard_prefix))
                .map(|(_, n)| *n)
                .sum();


            // A rate, not a lifetime total: dividing a running counter by an
            // uptime the caller does not know would be a number that is wrong in
            // a way nobody could see.
            let per_sec = match app.history.as_ref() {
                Some(h) => {
                    h.rate_per_sec(&rt.spec.application, &rt.spec.name, &lane.name, now)
                        .await
                }
                None => app.meter.rate_per_sec(&rt.spec.key(), &lane.name, now),
            };

            waiting_budget += budget_pending;
            waiting_workers += admitted_pending;
            rate += per_sec;

            lanes.push(json!({
                "name": lane.name,
                "waiting_for_budget": budget_pending,
                "waiting_for_workers": admitted_pending,
                "admitted_per_sec": per_sec,
                "drain_eta_seconds": eta(budget_pending, per_sec),
            }));
        }

        let states = rt.last_state.read().clone();
        let binding = rt
            .spec
            .budgets
            .iter()
            .map(|b| {
                let u = states
                    .values()
                    .map(|s| utilisation_max(b, s, now))
                    .fold(0.0f64, f64::max);
                (b, u)
            })

            .fold(None::<(&gate_core::Budget, f64)>, |acc, x| match acc {
                Some(a) if a.1 >= x.1 => Some(a),
                _ => Some(x),
            });

        let breach = rt.last_breach.read().clone();
        let state = if breach.is_some() {
            "breached"
        } else if waiting_budget > 0 {
            "pacing"
        } else {
            "flowing"
        };

        targets.push(json!({
            "name": rt.spec.name,
            "state": state,
            "binding_budget": binding.map(|(b, u)| json!({
                "id": b.id, "cap": b.cap, "period_seconds": b.period_seconds,
                "utilisation": u, "confidence": b.confidence,
            })),
            "admitted_per_sec": rate,
            "waiting_for_budget": waiting_budget,
            "waiting_for_workers": waiting_workers,
            "drain_eta_seconds": eta(waiting_budget, rate),
            "last_breach_at": breach.map(|(_, t)| t),
            "lanes": lanes,
        }));
    }

    Ok(Json(json!({ "application": application, "at": now, "targets": targets })).into_response())
}

/// `null` rather than infinity when nothing is moving: "we cannot say" is an
/// honest answer and a product can render it, where a number that means forever
/// would be rendered as a number.
fn eta(waiting: u64, per_sec: f64) -> Option<u64> {
    if waiting == 0 {
        return Some(0);
    }
    if per_sec <= 0.0 {
        return None;
    }
    Some((waiting as f64 / per_sec).ceil() as u64)
}

/// Who the console is talking to, and what it may do. The console reads `role`
/// to decide whether the spec editor is a button or a disabled one — a
/// read-only operator should be told before they type, not after they submit.
async fn me(
    State(app): State<Shared>,
    session: Option<axum::Extension<crate::auth::Session>>,
) -> ApiResult {
    // No session on the request means it did not come through the gate, which
    // means it arrived on the internal listener. The cluster boundary is the
    // authentication there, and nothing on that port is reachable from outside
    // it.
    let Some(axum::Extension(s)) = session else {
        return Ok(
            Json(json!({ "actor": "internal", "role": "admin", "email": null })).into_response(),
        );
    };
    let _ = &app;
    Ok(Json(json!({
        "actor": if crate::auth::is_dev() { "dev" } else { "google" },
        "email": s.email,
        "role": if crate::auth::is_admin(&s.email) { "admin" } else { "viewer" },
        "expires_at": s.exp,
    }))
    .into_response())
}

async fn overview(State(app): State<Shared>) -> ApiResult {
    let targets = app.registry.all();
    let (mut admitted, mut denied) = (0u64, 0u64);
    let mut assumed = 0usize;
    for t in &targets {
        for l in t.lanes.values() {
            let s = l.stats.read();
            admitted += s.admitted;
            denied += s.denied;
        }
        assumed += t
            .spec
            .budgets
            .iter()
            .filter(|b| b.confidence == gate_core::Confidence::Assumed)
            .count();
    }
    let up_s = ((now_ms() - app.started_ms).max(1) as f64) / 1000.0;
    Ok(Json(json!({
        "queen": { "reachable": true, "url": app.queen_url, "version": "1.0.3" },
        "targets": targets.len(),
        "admitted_total": admitted,
        "denied_total": denied,
        "admitted_per_sec": admitted as f64 / up_s,
        "budgets_assumed": assumed,
        "budgets_stale": 0,
    }))
    .into_response())
}

async fn list_targets(State(app): State<Shared>) -> ApiResult {
    let now = now_ms();
    let mut backlogs: std::collections::HashMap<String, u64> = Default::default();
    for rt in app.registry.all() {
        let pending: u64 = app
            .depths
            .pending(&app.queen, &rt.spec.push_queue())
            .await
            .values()
            .sum();
        backlogs.insert(rt.spec.key(), pending);
    }
    let out: Vec<Value> = app
        .registry
        .all()
        .iter()
        .map(|rt| {
            let states = rt.last_state.read().clone();
            let (mut worst_id, mut worst_u, mut worst_cap, mut worst_period) =
                (String::new(), 0.0f64, 0.0f64, 0i64);
            for b in &rt.spec.budgets {
                let u = states
                    .values()
                    .map(|s| utilisation_max(b, s, now))
                    .fold(0.0f64, f64::max);

                if u >= worst_u {
                    worst_id = b.id.clone();
                    worst_u = u;
                    worst_cap = b.cap;
                    worst_period = b.period_seconds;
                }
            }
            let (mut adm, mut den) = (0u64, 0u64);
            for l in rt.lanes.values() {
                let s = l.stats.read();
                adm += s.admitted;
                den += s.denied;
            }
            json!({
                "application": rt.spec.application,
                "name": rt.spec.name,
                "version": rt.spec.version,
                "graph": rt.graph,
                "lanes": rt.spec.lanes.iter().map(|l| json!({"name": l.name})).collect::<Vec<_>>(),

                "budgets_total": rt.spec.budgets.len(),
                "assumed_budgets": rt.spec.budgets.iter()
                    .filter(|b| b.confidence == gate_core::Confidence::Assumed).count(),
                "worst_budget_id": worst_id,
                "worst_used": worst_u * worst_cap,
                "worst_cap": worst_cap,
                "worst_period_seconds": worst_period,
                "worst_assumed": false,
                "admitted": adm,
                "denied": den,
                "state": if den > 0 { "pacing" } else { "flowing" },
                "backlog": backlogs.get(&rt.spec.key()).copied().unwrap_or(0),
            })
        })
        .collect();
    Ok(Json(json!(out)).into_response())
}
