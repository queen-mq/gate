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
    /// v1's push body carried the cost here, beside the payload; v2 reads it
    /// from the payload at the node's declared `cost.path`, because a
    /// user-owned ingress queue has no envelope to put it in.
    ///
    /// Kept, and NOT as a no-op. §12.1 says the endpoint shapes do not move, and
    /// this is the field that decides how much budget an item spends: ignoring
    /// it would charge every v1 caller's item the declared default instead of
    /// what it asked for — a limiter quietly enforcing the wrong limit, which is
    /// the failure this whole service exists to avoid. It is written into the
    /// payload at the node's own cost path, so the relay reads it exactly as it
    /// would read a producer's own.
    #[serde(default)]
    pub cost: Option<i64>,
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
    // v1's `cost`, written where v2 reads one. A zero or a negative is v1's
    // "use the declared default" and is left alone; anything else lands at the
    // node's own `cost.path` unless the payload already carries a value there,
    // which the producer meant and this must not overwrite.
    if let (Some(c), gate_core::Cost::Path(p)) = (body.cost.filter(|c| *c > 0), &np.cost) {
        if gate_core::resolve(&item, &p.path).is_none() {
            write_path(&mut item, &p.path, json!(c));
        }
    }

    // ---- the two refusals this door exists to make early.
    //
    // An item costing more than the node's ceiling can never be admitted: it
    // would park the head of its partition for ever and never reach a DLQ,
    // because a lease that expires charges no retry budget.
    let cost = gate_core::cost_of(&np.cost, &item)
        .map_err(|e| Fail(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()))?;
    // And a counter keyed on an absent value measures the wrong thing. Only an
    // applicable budget requires its scope: a `whenOp` that does not match this
    // item charges nothing and needs no key.
    if let Some((budget, path)) = gate_core::missing_scope(&np.budgets, &item) {
        return Err(Fail(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "budget `{budget}` of node `{node}` counts per `{path}` and this push carries none: \
                 a counter keyed on an absent value measures the wrong thing."
            ),
        ));
    }

    // ---- shed load, if and only if the declaration asked for it.
    //
    // OFF by default, and the default is the important half: holding work that
    // does not fit until it does is the entire point of this service, and a door
    // that refuses the moment the window is full turns a limiter into a load
    // shedder. A tight-looping producer would lose most of what it sent.
    //
    // Where it IS asked for, it is a pre-check and never a charge: the decision
    // is made by the relay when it pops, and charging here would spend budget
    // for work that has not moved. What it buys is a caller who can back off
    // instead of filling a queue — 429 with the deadline read off the counter's
    // own TTL.
    if let Some(retry_after) = shed(st, np, &item, cost).await {
        return Err(Fail(
            StatusCode::TOO_MANY_REQUESTS,
            format!(
                "`{node}` is at its ceiling; retry after {retry_after} second(s). Nothing was \
                 charged and nothing was queued."
            ),
        ));
    }

    let partition = body
        .partition
        .or(body.key)
        .or_else(|| spread(rt.plan.queue(&queue).and_then(|q| q.partitions)));
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

/// Write a value at a dotted payload path, creating the objects on the way.
///
/// The mirror of `gate_core::resolve`, and it exists for exactly one caller: the
/// v1 push body's `cost`, which has to land where the compiled cost model reads
/// one. A segment that is occupied by a non-object is left alone rather than
/// replaced — this is a compatibility shim and it may not destroy a payload.
fn write_path(data: &mut Value, path: &str, value: Value) {
    let mut segs: Vec<&str> = path.split('.').collect();
    if segs.first() != Some(&gate_core::PAYLOAD_ROOT) || segs.len() < 2 {
        return;
    }
    segs.remove(0);
    let last = segs.pop().expect("at least one segment");
    let mut cur = data;
    for s in segs {
        if !cur.is_object() {
            return;
        }
        cur = cur
            .as_object_mut()
            .expect("object")
            .entry(s.to_string())
            .or_insert_with(|| json!({}));
    }
    if let Some(map) = cur.as_object_mut() {
        map.insert(last.to_string(), value);
    }
}

/// A partition for a push that named none, so a queue Gate owns is as wide as
/// it was declared to be.
///
/// **This is what makes `ingress.partitions` real.** Partitions in queen are
/// created by producers naming them — there is no partition field on
/// `QueueOptions` and nothing to provision — so a Gate-owned ingress queue
/// driven through this door with no `partition`/`key` had exactly ONE partition,
/// the literal `Default`, while its stage ran `max(4, 16)` workers claiming one
/// at a time. The declare response said sixteen, §15 B's throughput arithmetic
/// assumed sixteen, and the `single-partition` guard-rail could not fire for the
/// queues Gate owns.
///
/// Round-robin and not a hash: a push that names nothing has no ordering to
/// preserve, and hashing something arbitrary would invent one. A push that DOES
/// name a partition or a key still passes it through untouched, which is the
/// property the whole passthrough design rests on — a producer's choice, never
/// a hash Gate owns.
///
/// `None` for a user-owned queue: its width belongs to the application, and the
/// producer that pushes to it is the application anyway.
fn spread(partitions: Option<u32>) -> Option<String> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = partitions.filter(|n| *n > 1)?;
    let i = NEXT.fetch_add(1, Ordering::Relaxed) % n as u64;
    Some(format!("p{i}"))
}

/// `Some(seconds)` when any counter applicable to this item is already full.
///
/// Read-only, and best-effort: a broker that will not answer means the push goes
/// through and the relay decides, which is the right way round — the door must
/// never be the thing that stops work when the limiter itself is fine.
async fn shed(st: &Shared, np: &NodePlan, item: &Value, cost: i64) -> Option<i64> {
    if !np.ingress_shed {
        return None;
    }
    let applicable = shed_budgets(np, item);
    if applicable.is_empty() {
        return None;
    }
    let keys: Vec<String> = applicable.iter().map(|b| b.key.clone()).collect();
    let states = st.budgets.read(&keys).await.ok()?;
    shed_wait(&applicable, &states, cost, crate::now_ms())
}

#[derive(Debug, PartialEq, Eq)]
struct ShedBudget {
    key: String,
    ceiling: i64,
}

/// Resolve the exact counters the relay would charge for this payload.
fn shed_budgets(np: &NodePlan, item: &Value) -> Vec<ShedBudget> {
    let op = gate_core::op_of(item);
    let mut out = Vec::new();
    for b in &np.budgets {
        if b.when_op
            .as_ref()
            .is_some_and(|patterns| !gate_core::op_matches(patterns, op))
        {
            continue;
        }
        let key = match &b.scope_by {
            Some(path) => match gate_core::scope_value(item, path) {
                Some(value) => b.key_for(Some(&value)),
                None => continue,
            },
            None => b.key.clone(),
        };
        if out.iter().any(|seen: &ShedBudget| seen.key == key) {
            continue;
        }
        out.push(ShedBudget {
            key,
            ceiling: b.max_for(np.widest_share()),
        });
    }
    out
}

fn shed_wait(
    budgets: &[ShedBudget],
    states: &[crate::budget::State],
    cost: i64,
    now: i64,
) -> Option<i64> {
    budgets
        .iter()
        .filter_map(|b| {
            let s = states.iter().find(|s| s.key == b.key)?;
            if s.value <= b.ceiling.saturating_sub(cost) {
                return None;
            }
            let wait = s
                .expires_at_ms
                .map(|e| ((e - now) as f64 / 1000.0).ceil() as i64)
                .unwrap_or(1)
                .max(1);
            Some(wait)
        })
        .max()
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

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(id: &str) -> gate_core::CompiledBudget {
        gate_core::CompiledBudget {
            id: id.into(),
            key: format!("key:{id}"),
            scope_by: None,
            shared_key: None,
            when_op: None,
            count: 10,
            time_ms: 1000,
            sub_windows: 1,
            count_sub: 10,
            window_sub_seconds: 1,
            confidence: gate_core::Confidence::Inferred,
        }
    }

    fn node(budgets: Vec<gate_core::CompiledBudget>) -> NodePlan {
        NodePlan {
            name: "n".into(),
            budgets,
            cost: gate_core::Cost::Fixed(1),
            ingress_queue: Some("in".into()),
            ingress_owned: true,
            ingress_http: true,
            ingress_shed: true,
            interior_queue: "interior".into(),
            egress_queue: Some("out".into()),
            egress_group: None,
            breaker_key: "breaker".into(),
            shares: Default::default(),
        }
    }

    #[test]
    fn one_full_budget_is_enough_to_shed() {
        let budgets = vec![
            ShedBudget {
                key: "full".into(),
                ceiling: 10,
            },
            ShedBudget {
                key: "room".into(),
                ceiling: 10,
            },
        ];
        let states = vec![
            crate::budget::State {
                key: "full".into(),
                value: 10,
                expires_at_ms: Some(12_000),
            },
            crate::budget::State {
                key: "room".into(),
                value: 0,
                expires_at_ms: Some(20_000),
            },
        ];

        assert_eq!(shed_wait(&budgets, &states, 1, 10_000), Some(2));
    }

    #[test]
    fn shed_resolves_when_op_and_scoped_keys_for_this_item() {
        let global = budget("global");
        let mut writes = budget("writes");
        writes.when_op = Some(vec!["listing.write".into()]);
        let mut customer = budget("customer");
        customer.scope_by = Some("payload.customerId".into());
        let np = node(vec![global, writes, customer]);

        let got = shed_budgets(&np, &json!({ "op": "listing.read", "customerId": "c-7" }));
        assert_eq!(
            got,
            vec![
                ShedBudget {
                    key: "key:global".into(),
                    ceiling: 10,
                },
                ShedBudget {
                    key: "key:customer:c-7".into(),
                    ceiling: 10,
                },
            ]
        );
    }
}
