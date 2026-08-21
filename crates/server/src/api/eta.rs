//! The ETA routes. The paths do not move; `?lane=` is accepted as a deprecated
//! alias for `?path=`.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::Deserialize;

use crate::api::{find, ok, resolve, ApiResult, Fail, Shared};
use crate::registry::GraphRuntime;

#[derive(Debug, Deserialize)]
pub struct PathQuery {
    #[serde(default)]
    pub path: Option<String>,
    /// v1's word for it.
    #[serde(default)]
    pub lane: Option<String>,
}

/// Which path to answer about.
///
/// Defaulting to the HIGHEST priority is deliberate: it is the path with the
/// largest ceiling, so it gives the earliest of the honest answers — and this
/// number is a bound of the form "no earlier than", which a default must not
/// quietly make later than it needs to be.
fn pick(rt: &GraphRuntime, node: &str, q: &PathQuery) -> Result<String, Fail> {
    if let Some(p) = q.path.clone().or_else(|| q.lane.clone()) {
        if rt.plan.stage(&p, node).is_some() {
            return Ok(p);
        }
        return Err(Fail(
            StatusCode::NOT_FOUND,
            format!(
                "no path `{p}` through `{node}`. It is crossed by: {}",
                gate_core::plan::paths_through(&rt.plan, node).join(", ")
            ),
        ));
    }
    rt.plan
        .stages_of_node(node)
        .min_by_key(|s| s.priority)
        .map(|s| s.path.clone())
        .ok_or_else(|| Fail(StatusCode::NOT_FOUND, format!("no path visits `{node}`")))
}

async fn answer(
    st: &Shared,
    rt: &std::sync::Arc<GraphRuntime>,
    node: &str,
    q: PathQuery,
) -> ApiResult {
    // A stopped runtime is REFUSED rather than answered. An ETA is the one read
    // that would turn "registered but stopped" into a confident number: a graph
    // whose swap failed and whose old plan could not be restarted would answer
    // `state: "waiting-budget"`, an `etaSeconds` and a `boundBy`, none of which
    // anything is going to act on, because no stage is running to act. The 503
    // is recoverable — the reconcile loop repairs it on its next pass — and it
    // is the same guard `push` makes.
    crate::api::refuse_if_stopped(rt)?;
    if rt.plan.node(node).is_none() {
        return Err(Fail(
            StatusCode::NOT_FOUND,
            format!("no node `{node}` in graph `{}`", rt.key()),
        ));
    }
    let path = pick(rt, node, &q)?;
    match crate::eta::view(st, rt, node, &path).await {
        Some(v) => ok(v),
        None => Err(Fail(
            StatusCode::NOT_FOUND,
            format!("no stage for `{path}` at `{node}`"),
        )),
    }
}

pub async fn graph_eta(
    State(st): State<Shared>,
    Path((application, graph, node)): Path<(String, String, String)>,
    Query(q): Query<PathQuery>,
) -> ApiResult {
    let rt = find(&st, &application, &graph)?;
    answer(&st, &rt, &node, q).await
}

pub async fn graph_eta_default(
    State(st): State<Shared>,
    Path((graph, node)): Path<(String, String)>,
    Query(q): Query<PathQuery>,
) -> ApiResult {
    let rt = resolve(&st, &graph)?;
    answer(&st, &rt, &node, q).await
}

/// The target sugar. A one-node graph has one node to answer about; a migrated
/// multi-node one answers about its terminal, which is the node the caller's
/// work leaves by and therefore the one the question is really asking.
pub async fn target_eta(
    State(st): State<Shared>,
    Path((application, name)): Path<(String, String)>,
    Query(q): Query<PathQuery>,
) -> ApiResult {
    let rt = find(&st, &application, &name)?;
    let node = terminal(&rt)?;
    answer(&st, &rt, &node, q).await
}

pub async fn target_eta_default(
    State(st): State<Shared>,
    Path(name): Path<String>,
    Query(q): Query<PathQuery>,
) -> ApiResult {
    let rt = resolve(&st, &name)?;
    let node = terminal(&rt)?;
    answer(&st, &rt, &node, q).await
}

fn terminal(rt: &GraphRuntime) -> Result<String, Fail> {
    rt.plan
        .nodes
        .iter()
        .find(|(_, np)| np.egress_queue.is_some())
        .map(|(n, _)| n.clone())
        .ok_or_else(|| {
            Fail(
                StatusCode::NOT_FOUND,
                format!("`{}` has no terminal node", rt.key()),
            )
        })
}
