//! What is left of the caller's data plane: one optional push, and four
//! headstones.
//!
//! The HTTP push **survives as an optional front door**. It is optional because
//! a node may name a queue the application already owns, and then producers push
//! with their normal SDK — which is the change that lets Gate be down without
//! blocking ingest. Where Gate owns the ingress queue this is how work gets in.
//!
//! `GET .../next` and `POST /v1/leases/*` are **410 Gone**. They existed to hand
//! out an opaque lease and take an ack back, and with the egress queue named in
//! the declaration the application pops it with its own SDK: an ordinary queue
//! that is sometimes empty, instead of a protocol where silence was the pacing
//! signal. The 410 names the queue and shows the two lines that replace it,
//! because a 404 would read as "wrong URL" and send somebody hunting.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use gate_core::plan::NodePlan;
use gate_core::GATE_META;

use crate::api::{find, ok, refuse_if_stopped, resolve, ApiResult, Fail, Shared};
use crate::registry::GraphRuntime;

#[derive(Debug, Deserialize)]
pub struct PushBody {
    #[serde(default)]
    pub op: String,
    /// The partition. **The producer's choice, not a hash Gate owns** — it is
    /// passed through unchanged at every hop, which is what preserves
    /// per-connection ordering end to end and keeps the relay's transactions
    /// lane-disjoint.
    #[serde(default)]
    pub partition: Option<String>,
    /// An alias for `partition`, kept because v1's push took a `key`.
    #[serde(default)]
    pub key: Option<String>,
    /// Deterministic per (entity, capability): this is the coalescing lever, not
    /// a safety accessory. Two pushes with the same txn inside the dedup window
    /// collapse to one, so lag compresses the backlog instead of growing it.
    #[serde(default)]
    pub txn: Option<String>,
    #[serde(default)]
    pub payload: Value,
}

pub async fn graph_push(
    State(st): State<Shared>,
    Path((application, graph, node)): Path<(String, String, String)>,
    Json(body): Json<PushBody>,
) -> ApiResult {
    let rt = find(&st, &application, &graph)?;
    push_into(&st, &rt, &node, body).await
}

pub async fn graph_push_default(
    State(st): State<Shared>,
    Path((graph, node)): Path<(String, String)>,
    Json(body): Json<PushBody>,
) -> ApiResult {
    let rt = resolve(&st, &graph)?;
    push_into(&st, &rt, &node, body).await
}

/// The target sugar: a one-node graph, and the lane segment in the path is a
/// vestige. It is accepted and ignored rather than 404'd, because every existing
/// caller has it in their URL.
pub async fn target_push(
    State(st): State<Shared>,
    Path((application, name, _lane)): Path<(String, String, String)>,
    Json(body): Json<PushBody>,
) -> ApiResult {
    let rt = find(&st, &application, &name)?;
    let node = sole_ingress(&rt)?;
    push_into(&st, &rt, &node, body).await
}

pub async fn target_push_default(
    State(st): State<Shared>,
    Path((name, _lane)): Path<(String, String)>,
    Json(body): Json<PushBody>,
) -> ApiResult {
    let rt = resolve(&st, &name)?;
    let node = sole_ingress(&rt)?;
    push_into(&st, &rt, &node, body).await
}

fn sole_ingress(rt: &GraphRuntime) -> Result<String, Fail> {
    let mut entries: Vec<&String> = rt
        .plan
        .nodes
        .iter()
        .filter(|(_, np)| np.ingress_queue.is_some())
        .map(|(n, _)| n)
        .collect();
    match entries.len() {
        1 => Ok(entries.remove(0).clone()),
        0 => Err(Fail(
            StatusCode::CONFLICT,
            format!("`{}` has no node work can enter by", rt.key()),
        )),
        _ => Err(Fail(
            StatusCode::CONFLICT,
            format!(
                "`{}` has {} nodes work can enter by ({}). The target routes cannot choose: use \
                 /v1/apps/{}/graphs/{}/nodes/{{node}}/push",
                rt.key(),
                entries.len(),
                entries
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                rt.doc.application,
                rt.doc.graph
            ),
        )),
    }
}

/// The one push.
async fn push_into(
    st: &Shared,
    rt: &std::sync::Arc<GraphRuntime>,
    node: &str,
    body: PushBody,
) -> ApiResult {
    refuse_if_stopped(rt)?;
    let np = rt.plan.node(node).ok_or_else(|| {
        Fail(
            StatusCode::NOT_FOUND,
            format!("no node `{node}` in graph `{}`", rt.key()),
        )
    })?;

    let Some(queue) = np.ingress_queue.clone() else {
        let entries: Vec<&str> = rt
            .plan
            .nodes
            .iter()
            .filter(|(_, n)| n.ingress_queue.is_some())
            .map(|(n, _)| n.as_str())
            .collect();
        // Pushing into an interior queue would skip every budget upstream of it,
        // which is the one thing a limiter must not let a caller do by accident.
        return Err(Fail(
            StatusCode::CONFLICT,
            format!(
                "`{node}` declares no ingress: it is fed by the paths that relay into it, and \
                 pushing straight in would skip every budget upstream of it. The nodes work enters \
                 by are: {}",
                if entries.is_empty() {
                    "none".to_string()
                } else {
                    entries.join(", ")
                }
            ),
        ));
    };
    if !np.ingress_http {
        return Err(Fail(
            StatusCode::CONFLICT,
            format!(
                "`{node}` takes its work from `{queue}`, which your application owns. Push to it \
                 with your own SDK — that is what lets Gate be down without blocking your ingest. \
                 Declare `\"ingress\": {{ \"queue\": \"{queue}\", \"http\": true }}` if you want \
                 this door as well."
            ),
        ));
    }

    // ---- the envelope.
    let mut item = body.payload.clone();
    if !item.is_object() {
        item = json!({});
    }
    {
        let obj = item.as_object_mut().expect("object");
        if !body.op.is_empty() {
            obj.insert("op".into(), json!(body.op));
        }
        obj.insert(
            GATE_META.to_string(),
            json!({
                "graph": rt.doc.graph,
                "hop": 0,
                "at": crate::now_ms(),
                "node": node,
            }),
        );
    }

    // ---- the two refusals this door exists to make early.
    //
    // An item costing more than the node's ceiling can never be admitted: it
    // would park the head of its partition for ever and never reach a DLQ,
    // because a lease that expires charges no retry budget.
    let cost = gate_core::cost_of(&np.cost, &item)
        .map_err(|e| Fail(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()))?;
    // And a counter keyed on an absent value measures the wrong thing.
    for b in np.budgets.iter().filter(|b| b.is_scoped()) {
        let path = b.scope_by.as_deref().unwrap_or_default();
        if gate_core::scope_value(&item, path).is_none() {
            return Err(Fail(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!(
                    "budget `{}` of node `{node}` counts per `{path}` and this push carries none: \
                     a counter keyed on an absent value measures the wrong thing.",
                    b.id
                ),
            ));
        }
    }

    // ---- shed load, optionally.
    //
    // A pre-check, never a charge: the decision is made by the relay when it
    // pops, and charging here would spend budget for work that has not moved.
    // What this buys is a caller who can back off instead of filling a queue —
    // 429 with the vendor's own deadline, read off the counter's TTL.
    if let Some(retry_after) = shed(st, np, cost).await {
        return Err(Fail(
            StatusCode::TOO_MANY_REQUESTS,
            format!(
                "`{node}` is at its ceiling; retry after {retry_after} second(s). Nothing was \
                 charged and nothing was queued."
            ),
        ));
    }

    let partition = body.partition.or(body.key);
    let res = st
        .queen
        .queue(&queue)
        .push_items(vec![queen_mq::PushItem {
            queue: queue.clone(),
            partition: partition.clone(),
            payload: item,
            transaction_id: body.txn,
        }])
        .await
        .map_err(|e| Fail(StatusCode::BAD_GATEWAY, e.to_string()))?;

    ok(json!({
        "ok": true,
        "pushed": res.len(),
        "queue": queue,
        "partition": partition,
        "cost": cost,
    }))
}

/// `Some(seconds)` when every unscoped counter of this node is already full.
///
/// Read-only, and best-effort: a broker that will not answer means the push goes
/// through and the relay decides, which is the right way round — the door must
/// never be the thing that stops work when the limiter itself is fine.
async fn shed(st: &Shared, np: &NodePlan, cost: i64) -> Option<i64> {
    let keys: Vec<String> = np.unscoped().map(|b| b.key.clone()).collect();
    if keys.is_empty() {
        return None;
    }
    let states = st.budgets.read(&keys).await.ok()?;
    let now = crate::now_ms();
    let mut worst: Option<i64> = None;
    for b in np.unscoped() {
        let ceiling = b.max_for(np.widest_share());
        let s = states.iter().find(|s| s.key == b.key)?;
        if s.value + cost <= ceiling {
            return None;
        }
        let wait = s
            .expires_at_ms
            .map(|e| ((e - now) as f64 / 1000.0).ceil() as i64)
            .unwrap_or(1)
            .max(1);
        worst = Some(worst.map_or(wait, |w: i64| w.max(wait)));
    }
    worst
}

// ------------------------------------------------------------------- gone

fn gone(what: &str, instead: String) -> Fail {
    Fail(
        StatusCode::GONE,
        format!(
            "{what} is gone. {instead} Gate no longer mediates the pop: it paces what reaches the \
             egress queue, and your consumers own the lease, the ack and the retry from there."
        ),
    )
}

pub async fn gone_node(
    State(st): State<Shared>,
    Path((application, graph, node)): Path<(String, String, String)>,
) -> ApiResult {
    Err(gone(
        "`GET .../next`",
        egress_hint(&st, &application, &graph, &node),
    ))
}

pub async fn gone_node_default(
    State(st): State<Shared>,
    Path((graph, node)): Path<(String, String)>,
) -> ApiResult {
    let app = match st.registry.resolve(&graph) {
        crate::registry::Resolved::One(g) => g.doc.application.clone(),
        _ => gate_core::default_application(),
    };
    Err(gone(
        "`GET .../next`",
        egress_hint(&st, &app, &graph, &node),
    ))
}

pub async fn gone_target(
    State(st): State<Shared>,
    Path((application, name, _lane)): Path<(String, String, String)>,
) -> ApiResult {
    let node = st
        .registry
        .get(&application, &name)
        .and_then(|rt| sole_ingress(&rt).ok())
        .unwrap_or_else(|| name.clone());
    Err(gone(
        "`GET .../next`",
        egress_hint(&st, &application, &name, &node),
    ))
}

pub async fn gone_target_default(
    State(st): State<Shared>,
    Path((name, _lane)): Path<(String, String)>,
) -> ApiResult {
    let app = match st.registry.resolve(&name) {
        crate::registry::Resolved::One(g) => g.doc.application.clone(),
        _ => gate_core::default_application(),
    };
    Err(gone("`GET .../next`", egress_hint(&st, &app, &name, &name)))
}

pub async fn gone_lease() -> impl IntoResponse {
    (
        StatusCode::GONE,
        Json(json!({
            "error":
                "`/v1/leases/*` is gone: there is no Gate-mediated lease any more. Ack, nack and \
                 renew against the egress queue with your own SDK. A vendor throttle is reported \
                 to `POST /v1/apps/{app}/graphs/{graph}/nodes/{node}/backoff \
                 {\"retryAfterSeconds\": 30}`, which spends the node's window so every path stops \
                 and every parked consumer's wait IS your Retry-After."
        })),
    )
}

fn egress_hint(st: &Shared, application: &str, graph: &str, node: &str) -> String {
    let queue = st
        .registry
        .get(application, graph)
        .and_then(|rt| rt.plan.node(node).and_then(|n| n.egress_queue.clone()));
    match queue {
        Some(q) => format!(
            "Consume `{q}` directly:  queen.queue(\"{q}\").group(\"your-workers\").consume(|m| \
             ...).await"
        ),
        None => format!(
            "`{node}` declares no egress queue; name one in the declaration and consume it \
             directly."
        ),
    }
}
