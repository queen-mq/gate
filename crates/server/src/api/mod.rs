//! The HTTP surface.
//!
//! Two planes on one port, as `queen-http` does: `/v1` is what a caller speaks,
//! `/api` is what the console reads.
//!
//! Nothing here holds session state, and there is no lease protocol left to hold
//! any: the application consumes its egress queue with its own SDK. The routes
//! that used to hand out opaque leases — `GET .../next`, `POST /v1/leases/*` —
//! answer **410 Gone** with the queue name and a two-line snippet, for one
//! release.
//!
//! The 2469-line original split here into `declare`, `data`, `console`, `eta`
//! and `breaker`; the route table stayed recognisable and the handlers lost
//! `next_from`, `ack`, `plan_retro`, `settle_item_by_item`, `nack` and `renew`.

pub mod breaker;
pub mod console;
pub mod data;
pub mod declare;
pub mod eta;

use std::sync::Arc;
use std::time::Instant;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use parking_lot::RwLock;
use queen_mq::Queen;
use serde_json::json;

use crate::budget::Budgets;
use crate::obs::Traces;
use crate::registry::Registry;

pub struct App {
    pub auth: Option<Arc<crate::auth::Auth>>,
    pub queen: Queen,
    pub budgets: Budgets,
    pub registry: Registry,
    pub depths: Arc<crate::depth::Depths>,
    pub traces: Arc<Traces>,
    pub history: Option<Arc<crate::history::History>>,
    pub queen_url: String,
    pub started_ms: i64,
    /// `/api/overview` used to hardcode `reachable: true` and a version string.
    /// This is the probe that replaces them, cached for five seconds so a
    /// console poll does not become a health-check flood.
    pub broker: RwLock<Option<(BrokerHealth, Instant)>>,
    /// Held by every declare, every delete and every pass of the reconcile loop.
    ///
    /// Provisioning is stop-then-start, so two of them on one graph interleave
    /// into a graph that is registered under one document and running under
    /// another. The reconcile loop makes that a certainty rather than a race,
    /// because it declares on a timer without anybody asking.
    pub declare_lock: tokio::sync::Mutex<()>,
}

#[derive(Debug, Clone, Default)]
pub struct BrokerHealth {
    pub reachable: bool,
    pub version: Option<String>,
}

impl App {
    /// Everything a declare needs, so a test can build one without the
    /// listeners.
    pub fn new(queen: Queen, queen_url: String) -> Self {
        Self {
            auth: None,
            budgets: Budgets::new(queen.clone()),
            queen,
            registry: Default::default(),
            depths: Arc::new(crate::depth::Depths::default()),
            traces: Arc::new(Traces::default()),
            history: None,
            queen_url,
            started_ms: crate::now_ms(),
            broker: RwLock::new(None),
            declare_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Is the broker there, and which version. Probed, not assumed.
    pub async fn broker_health(&self) -> BrokerHealth {
        if let Some((h, at)) = self.broker.read().as_ref() {
            if at.elapsed() < std::time::Duration::from_secs(5) {
                return h.clone();
            }
        }
        let probe = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            self.queen.admin().health(),
        )
        .await;
        let health = match probe {
            Ok(Ok(v)) => BrokerHealth {
                reachable: true,
                version: v.get("version").and_then(|s| s.as_str()).map(String::from),
            },
            _ => BrokerHealth {
                reachable: false,
                version: None,
            },
        };
        *self.broker.write() = Some((health.clone(), Instant::now()));
        health
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
/// session required on every one of them. Not "every route except the control
/// plane" — every route, so there is no path table for an ingress rule to get
/// wrong.
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
        // ---- graphs: the one document type.
        .route(
            "/v1/apps/:app/graphs/:name",
            put(declare::put_graph)
                .get(declare::get_graph)
                .delete(declare::del_graph),
        )
        .route(
            "/v1/graphs/:name",
            put(declare::put_graph_default)
                .get(declare::get_graph_default)
                .delete(declare::del_graph_default),
        )
        .route(
            "/v1/apps/:app/graphs/:graph/nodes/:node/push",
            post(data::graph_push),
        )
        .route(
            "/v1/graphs/:graph/nodes/:node/push",
            post(data::graph_push_default),
        )
        .route(
            "/v1/apps/:app/graphs/:graph/nodes/:node/eta",
            get(eta::graph_eta),
        )
        .route(
            "/v1/graphs/:graph/nodes/:node/eta",
            get(eta::graph_eta_default),
        )
        // ---- the breaker, which is what a vendor's 429 does now.
        .route(
            "/v1/apps/:app/graphs/:graph/nodes/:node/backoff",
            post(breaker::backoff).delete(breaker::reset),
        )
        .route(
            "/v1/graphs/:graph/nodes/:node/backoff",
            post(breaker::backoff_default).delete(breaker::reset_default),
        )
        // ---- the target sugar: a one-node graph, on the routes that already
        // existed. The shapes do not move; a caller with one limit never has to
        // learn the word "graph".
        .route("/v1/apps/:app/targets", put(declare::sync_app))
        .route("/v1/targets", put(declare::sync_default))
        .route(
            "/v1/apps/:app/targets/:name",
            put(declare::put_target)
                .get(declare::get_target)
                .delete(declare::del_target),
        )
        .route(
            "/v1/targets/:name",
            put(declare::put_target_default)
                .get(declare::get_target_default)
                .delete(declare::del_target_default),
        )
        .route(
            "/v1/apps/:app/targets/:name/lanes/:lane/push",
            post(data::target_push),
        )
        .route(
            "/v1/targets/:name/lanes/:lane/push",
            post(data::target_push_default),
        )
        .route("/v1/apps/:app/targets/:name/eta", get(eta::target_eta))
        .route("/v1/targets/:name/eta", get(eta::target_eta_default))
        // ---- gone, and saying where to go instead.
        .route(
            "/v1/apps/:app/graphs/:graph/nodes/:node/next",
            get(data::gone_node),
        )
        .route(
            "/v1/graphs/:graph/nodes/:node/next",
            get(data::gone_node_default),
        )
        .route(
            "/v1/apps/:app/targets/:name/lanes/:lane/next",
            get(data::gone_target),
        )
        .route(
            "/v1/targets/:name/lanes/:lane/next",
            get(data::gone_target_default),
        )
        .route("/v1/leases/ack", post(data::gone_lease))
        .route("/v1/leases/nack", post(data::gone_lease))
        .route("/v1/leases/renew", post(data::gone_lease))
        // ---- console reads.
        .route("/v1/apps/:app/metrics", get(console::app_metrics))
        .route("/api/overview", get(console::overview))
        .route("/api/apps", get(console::list_apps))
        .route("/api/targets", get(console::list_targets))
        .route("/api/targets/:name", get(declare::get_target_default))
        .route("/api/apps/:app/targets/:name", get(declare::get_target))
        .route("/api/graphs", get(console::list_graphs))
        .route("/api/graphs/:name", get(declare::get_graph_default))
        .route("/api/graphs/:name/topology", get(declare::topology_default))
        .route("/api/apps/:app/graphs/:name", get(declare::get_graph))
        .route(
            "/api/apps/:app/graphs/:name/topology",
            get(declare::topology),
        )
        .route("/api/flow", get(console::flow))
        .route("/api/rollups", get(console::rollups))
        .route("/api/budgets", get(console::shared_budgets))
        .route("/api/breaches/recent", get(console::recent_breaches))
        .route("/api/traces", get(console::traces))
        .route("/api/me", get(console::me))
        .route(
            "/health",
            get(|| async { Json(json!({"status":"healthy"})) }),
        )
        // A route that exists only so its absence is not mistaken for a 404 of
        // the graph itself.
        .route("/api/version", get(version))
}

async fn version(State(st): State<Shared>) -> Response {
    let h = st.broker_health().await;
    Json(json!({
        "gate": env!("CARGO_PKG_VERSION"),
        "queen": { "url": st.queen_url, "reachable": h.reachable, "version": h.version },
    }))
    .into_response()
}

// ------------------------------------------------------------------- failures

pub struct Fail(pub StatusCode, pub String);

impl IntoResponse for Fail {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

pub type ApiResult = std::result::Result<Response, Fail>;

pub fn ok(v: serde_json::Value) -> ApiResult {
    Ok(Json(v).into_response())
}

impl From<crate::graph::Refusal> for Fail {
    fn from(r: crate::graph::Refusal) -> Self {
        match r {
            crate::graph::Refusal::Invalid(m) => Fail(StatusCode::UNPROCESSABLE_ENTITY, m),
            crate::graph::Refusal::Conflict(m) => Fail(StatusCode::CONFLICT, m),
            crate::graph::Refusal::Gateway(m) => Fail(StatusCode::BAD_GATEWAY, m),
        }
    }
}

/// Find a graph, by application and name.
pub fn find(
    st: &Shared,
    app: &str,
    name: &str,
) -> Result<Arc<crate::registry::GraphRuntime>, Fail> {
    st.registry
        .get(app, name)
        .ok_or_else(|| Fail(StatusCode::NOT_FOUND, format!("no graph `{app}/{name}`")))
}

/// Find a graph by bare name, across applications.
///
/// The one case the server refuses to guess: two applications with a graph of
/// one name. Picking either would run somebody else's declaration.
pub fn resolve(st: &Shared, name: &str) -> Result<Arc<crate::registry::GraphRuntime>, Fail> {
    match st.registry.resolve(name) {
        crate::registry::Resolved::One(g) => Ok(g),
        crate::registry::Resolved::None => {
            Err(Fail(StatusCode::NOT_FOUND, format!("no graph `{name}`")))
        }
        crate::registry::Resolved::Ambiguous(apps) => Err(Fail(
            StatusCode::CONFLICT,
            format!(
                "`{name}` is declared in {} applications ({}). Name one: \
                 /v1/apps/{{app}}/graphs/{name}",
                apps.len(),
                apps.join(", ")
            ),
        )),
    }
}

/// A runtime whose stages have been cancelled must not be handed a caller.
///
/// Provisioning is stop-then-start, so this is the window a swap opens; it
/// outlives the swap when a restore fails. Answering "not available" is
/// recoverable — the reconcile loop repairs it on its next pass — where
/// accepting a push into a queue nothing drains is not.
pub fn refuse_if_stopped(rt: &Arc<crate::registry::GraphRuntime>) -> Result<(), Fail> {
    if rt.is_running() {
        return Ok(());
    }
    Err(Fail(
        StatusCode::SERVICE_UNAVAILABLE,
        format!(
            "`{}` is not running: its stages were stopped and not replaced. It will be brought \
             back by the next reconcile pass",
            rt.key()
        ),
    ))
}
