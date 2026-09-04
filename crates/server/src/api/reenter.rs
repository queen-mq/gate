//! Re-entry: design §16.6, option (2).
//!
//! *"The vendor said 429 — put this item back at the door it came in at and make
//! it re-pay every budget on its path."*
//!
//! v1 did that by itself, in `plan_retro`, because it saw every outcome through
//! `POST /v1/leases/ack`. The settled v2 architecture removes that ack — the
//! application consumes the egress queue with its own SDK and Gate never learns
//! what the vendor said — so the two halves of v1's breach machinery separate:
//!
//! * the **aggregate** half is [`crate::breaker`]: one 429 spends the node's
//!   window and every path stops through the ordinary refusal path;
//! * the **per-item** half is here. The application reports the item, and Gate
//!   puts it back at its origin entry.
//!
//! The three properties v1's version had are the three this keeps, and they are
//! the reason this is not just "re-push it yourself with your own SDK":
//!
//! 1. **origin-entry.** The item goes back to the ingress queue of the FIRST
//!    node of its own path, not to whatever queue the caller happens to know
//!    about — so it re-pays every budget on that path rather than skipping the
//!    ones upstream of where it failed. A caller pushing to an interior queue
//!    would skip them, which is the one thing a limiter must not let anybody do
//!    by accident (and `push` refuses it for exactly that reason).
//! 2. **the attempt is in the transaction id.** `derive(txn, "r{n}")`, so a
//!    caller that reports the same item twice — a redelivery on its own egress
//!    queue, a retry of this very call — collapses on the broker's dedup instead
//!    of re-entering twice. Nothing here keeps a table of what has re-entered.
//! 3. **bounded.** `maxAttempts` on the document, counted in `_gate.attempt`
//!    which every relay hop now carries forward. An unbounded re-entry is a
//!    livelock the limiter would be paying for.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use gate_core::GATE_META;

use crate::api::{find, object_payload, ok, refuse_if_stopped, resolve, ApiResult, Fail, Shared};
use crate::registry::GraphRuntime;

#[derive(Debug, Deserialize)]
pub struct ReenterBody {
    /// The payload as it was popped off the egress queue, `_gate` and all.
    #[serde(default)]
    pub payload: Value,
    /// The transaction id it arrived with. Required: it is what the re-entry id
    /// is derived from, and without it two reports of one item would re-enter
    /// twice.
    pub txn: String,
    /// Which path to re-enter. Defaults to the one the payload's own `_gate`
    /// stamp names, which is the item's actual origin.
    #[serde(default)]
    pub path: Option<String>,
    /// Which attempt this is. Defaults to one past whatever `_gate.attempt`
    /// says, which is what makes a caller that simply reports every failure
    /// bounded without having to count.
    #[serde(default)]
    pub attempt: Option<u32>,
    /// The partition, passed through exactly as `push` does.
    #[serde(default)]
    pub partition: Option<String>,
}

pub async fn graph_reenter(
    State(st): State<Shared>,
    Path((application, graph)): Path<(String, String)>,
    Json(body): Json<ReenterBody>,
) -> ApiResult {
    let rt = find(&st, &application, &graph)?;
    reenter(&st, &rt, body).await
}

pub async fn graph_reenter_default(
    State(st): State<Shared>,
    Path(graph): Path<String>,
    Json(body): Json<ReenterBody>,
) -> ApiResult {
    let rt = resolve(&st, &graph)?;
    reenter(&st, &rt, body).await
}

async fn reenter(st: &Shared, rt: &std::sync::Arc<GraphRuntime>, body: ReenterBody) -> ApiResult {
    refuse_if_stopped(rt)?;

    validate_parent_txn(&body.txn).map_err(|why| {
        Fail(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("cannot re-enter an item: {why}"),
        )
    })?;

    let mut item = object_payload(body.payload.clone())?;
    let stamp = item.get(GATE_META);
    let path = body
        .path
        .clone()
        .or_else(|| {
            stamp
                .and_then(|g| g.get("path"))
                .and_then(|p| p.as_str())
                .map(String::from)
        })
        .ok_or_else(|| {
            Fail(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!(
                    "this payload carries no `{GATE_META}.path`, so there is no origin to put it \
                     back at. Name one: {{\"path\": \"...\"}}. The paths of `{}` are: {}",
                    rt.key(),
                    rt.doc
                        .paths
                        .iter()
                        .map(|p| p.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            )
        })?;

    // The FIRST hop of that path: the door the item came in at. Not the stage
    // that failed, and not a queue the caller names — re-entering anywhere else
    // skips every budget upstream of it.
    let entry = rt
        .plan
        .stages
        .iter()
        .find(|s| s.path == path && s.first_hop)
        .ok_or_else(|| {
            Fail(
                StatusCode::NOT_FOUND,
                format!("no path `{path}` in `{}`", rt.key()),
            )
        })?;
    let np = rt.plan.node(&entry.node).ok_or_else(|| {
        Fail(
            StatusCode::NOT_FOUND,
            format!("no node `{}` in `{}`", entry.node, rt.key()),
        )
    })?;
    let Some(queue) = np.ingress_queue.clone() else {
        return Err(Fail(
            StatusCode::CONFLICT,
            format!(
                "path `{path}` starts at `{}`, which declares no ingress: there is no door to put \
                 this item back at.",
                entry.node
            ),
        ));
    };

    let attempt = reentry_attempt(stamp, body.attempt).map_err(|why| {
        Fail(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("cannot re-enter an item in `{}`: {why}", rt.key()),
        )
    })?;
    let max = rt.plan.max_attempts;
    if attempt > max {
        return Err(Fail(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "attempt {attempt} of `{}` is past its `maxAttempts` of {max}: this item has had \
                 every re-entry the declaration allows, and it is now the application's to \
                 dead-letter. Raise `maxAttempts` if it should keep trying.",
                rt.key()
            ),
        ));
    }

    // Restamped at hop 0 of its own path, with the attempt on it. The relay
    // carries `attempt` forward across every hop, so the next report of this
    // item counts from here rather than starting again at one.
    {
        let obj = item.as_object_mut().expect("object");
        obj.insert(
            GATE_META.to_string(),
            json!({
                "graph": rt.doc.graph,
                "path": path,
                "hop": 0,
                "node": entry.node,
                "at": crate::now_ms(),
                "attempt": attempt,
            }),
        );
    }

    // The attempt rides in the id, which is what makes a caller that reports one
    // item twice idempotent: the second push is a dedup hit at the broker and
    // nothing re-enters. `r{n}` is v1's own label.
    let txn = gate_core::derive(&body.txn, &format!("r{attempt}"));

    let pushed = st
        .queen
        .queue(&queue)
        .push_items(vec![queen_mq::PushItem {
            queue: queue.clone(),
            partition: body.partition.clone(),
            payload: item,
            transaction_id: Some(txn.clone()),
        }])
        .await
        .map_err(|e| Fail(StatusCode::BAD_GATEWAY, e.to_string()))?;

    tracing::info!(
        graph = %rt.key(), path = %path, node = %entry.node, attempt,
        "an item re-entered at its origin"
    );

    ok(json!({
        "ok": true,
        "queue": queue,
        "path": path,
        "node": entry.node,
        "attempt": attempt,
        "maxAttempts": max,
        "transactionId": txn,
        "pushed": pushed.len(),
    }))
}

fn validate_parent_txn(txn: &str) -> Result<(), &'static str> {
    if txn.trim().is_empty() {
        return Err("`txn` must not be empty; it is the identity used to deduplicate reports");
    }
    Ok(())
}

/// Pick a strictly later attempt without truncating an untrusted JSON number.
/// A caller may skip forward, but it may never reset the attempt carried by the
/// item: doing so would turn a bounded re-entry into a livelock.
fn reentry_attempt(stamp: Option<&Value>, requested: Option<u32>) -> Result<u32, String> {
    let was = match stamp.and_then(|g| g.get("attempt")) {
        None => 0,
        Some(raw) => {
            let n = raw.as_u64().ok_or_else(|| {
                "`_gate.attempt` must be a non-negative integer when it is present".to_string()
            })?;
            u32::try_from(n)
                .map_err(|_| format!("`_gate.attempt` of {n} is too large to be a valid attempt"))?
        }
    };

    let attempt = match requested {
        Some(n) => n,
        None => was
            .checked_add(1)
            .ok_or_else(|| format!("`_gate.attempt` of {was} cannot be incremented any further"))?,
    };
    if attempt <= was {
        return Err(format!(
            "attempt {attempt} does not advance the payload's existing attempt {was}"
        ));
    }
    Ok(attempt)
}

#[cfg(test)]
mod tests {
    use super::{reentry_attempt, validate_parent_txn};
    use serde_json::json;

    #[test]
    fn a_reentry_attempt_must_move_forward() {
        let stamp = json!({ "attempt": 2 });
        assert_eq!(reentry_attempt(Some(&stamp), None), Ok(3));
        assert_eq!(reentry_attempt(Some(&stamp), Some(4)), Ok(4));
        assert!(reentry_attempt(Some(&stamp), Some(2)).is_err());
        assert!(reentry_attempt(Some(&stamp), Some(0)).is_err());
    }

    #[test]
    fn an_untrusted_attempt_neither_truncates_nor_overflows() {
        let too_large = json!({ "attempt": u64::from(u32::MAX) + 1 });
        let at_limit = json!({ "attempt": u32::MAX });
        let malformed = json!({ "attempt": "many" });
        assert!(reentry_attempt(Some(&too_large), None).is_err());
        assert!(reentry_attempt(Some(&at_limit), None).is_err());
        assert!(reentry_attempt(Some(&malformed), None).is_err());
    }

    #[test]
    fn a_reentry_needs_a_real_parent_transaction() {
        assert!(validate_parent_txn("").is_err());
        assert!(validate_parent_txn("  \n").is_err());
        assert!(validate_parent_txn("parent-42").is_ok());
    }
}
