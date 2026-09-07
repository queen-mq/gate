//! `POST .../nodes/:node/backoff` — the route a vendor's 429 becomes.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;

use crate::api::{find, ok, resolve, ApiResult, Fail, Shared};
use crate::breaker::BackoffBody;
use crate::registry::GraphRuntime;

async fn trip(
    st: &Shared,
    rt: &std::sync::Arc<GraphRuntime>,
    node: &str,
    body: BackoffBody,
) -> ApiResult {
    let np = rt.plan.node(node).ok_or_else(|| {
        Fail(
            StatusCode::NOT_FOUND,
            format!("no node `{node}` in graph `{}`", rt.key()),
        )
    })?;
    let out = crate::breaker::trip(&st.budgets, &rt.doc.application, &rt.doc.graph, np, &body)
        .await
        .map_err(|e| Fail(StatusCode::BAD_GATEWAY, e.to_string()))?;
    if out.get("ok").and_then(|v| v.as_bool()) == Some(false) {
        return Err(Fail(
            StatusCode::UNPROCESSABLE_ENTITY,
            out.get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("the breaker could not be tripped")
                .to_string(),
        ));
    }
    ok(out)
}

pub async fn backoff(
    State(st): State<Shared>,
    Path((application, graph, node)): Path<(String, String, String)>,
    Json(body): Json<BackoffBody>,
) -> ApiResult {
    let rt = find(&st, &application, &graph)?;
    trip(&st, &rt, &node, body).await
}

pub async fn backoff_default(
    State(st): State<Shared>,
    Path((graph, node)): Path<(String, String)>,
    Json(body): Json<BackoffBody>,
) -> ApiResult {
    let rt = resolve(&st, &graph)?;
    trip(&st, &rt, &node, body).await
}

/// Un-break early.
pub async fn reset(
    State(st): State<Shared>,
    Path((application, graph, node)): Path<(String, String, String)>,
) -> ApiResult {
    let rt = find(&st, &application, &graph)?;
    do_reset(&st, &rt, &node).await
}

pub async fn reset_default(
    State(st): State<Shared>,
    Path((graph, node)): Path<(String, String)>,
) -> ApiResult {
    let rt = resolve(&st, &graph)?;
    do_reset(&st, &rt, &node).await
}

async fn do_reset(st: &Shared, rt: &std::sync::Arc<GraphRuntime>, node: &str) -> ApiResult {
    let np = rt.plan.node(node).ok_or_else(|| {
        Fail(
            StatusCode::NOT_FOUND,
            format!("no node `{node}` in graph `{}`", rt.key()),
        )
    })?;
    let out = crate::breaker::reset(&st.budgets, np)
        .await
        .map_err(|e| Fail(StatusCode::BAD_GATEWAY, e.to_string()))?;
    if out.get("ok").and_then(|v| v.as_bool()) == Some(false) {
        return Err(Fail(
            StatusCode::UNPROCESSABLE_ENTITY,
            out.get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("the breaker could not be reset")
                .to_string(),
        ));
    }
    ok(out)
}
