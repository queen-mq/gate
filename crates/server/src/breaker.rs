//! The breaker: what a vendor's 429 does now.
//!
//! v1 answered a throttle with `plan_retro` — a per-item re-entry at the door
//! the item came in at, with the attempt number in the transaction id so a
//! replayed ack could not double-retry. It was good, and it hung entirely off
//! `POST /v1/leases/ack`, which this architecture removes: the application
//! consumes the egress queue with its own SDK and Gate never sees the outcome.
//!
//! What replaces it is smaller and, for the aggregate case, better. Gate
//! **spends the node's window**:
//!
//! ```text
//! kv.put(key, value = the widest path's ceiling, { ttl: retryAfterSeconds })
//! ```
//!
//! and every path stops through the ordinary refusal path. No new code path, no
//! flag for the hot loop to check, nothing to forget to clear. And because
//! `put`'s TTL is **not** create-only (only `incr`'s is), the rewrite moves both
//! the value and the expiry — so every parked consumer's `expiresAt` **is** the
//! vendor's own `Retry-After` deadline, and `relay::park_or_release` computes it
//! without being told.
//!
//! This is the AGGREGATE half of what v1's breach rules did. The per-item half
//! — put this one item back at the door it came in at and make it re-pay every
//! budget on its path — is [`crate::api::reenter`], design §16.6 option (2).
//! Together they cover what `plan_retro` covered, with the trigger moved: v1
//! watched the ack, and v2 is told.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use gate_core::plan::NodePlan;

use crate::budget::Budgets;

/// The bounds. An hour because a limiter that can be told to stop for a day is a
/// limiter one bad automation turns into an outage; one second because zero
/// would be a breaker that does nothing and reads like one that did something.
pub const MIN_SECONDS: i64 = 1;
pub const MAX_SECONDS: i64 = 3600;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackoffBody {
    #[serde(rename = "retryAfterSeconds")]
    pub retry_after_seconds: i64,
    /// Optionally give back the token the reporter spent on the call the vendor
    /// refused. The vendor did not serve it, so charging for it is charging
    /// twice for one unit of work.
    #[serde(
        default,
        rename = "refundCost",
        skip_serializing_if = "Option::is_none"
    )]
    pub refund_cost: Option<i64>,
    /// Who is reporting. Free text, kept on the record so `/api/breaches/recent`
    /// can say where a backoff came from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub at: i64,
    #[serde(rename = "retryAfterSeconds")]
    pub retry_after_seconds: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<String>,
    pub application: String,
    pub graph: String,
    pub node: String,
}

/// Trip the breaker on one node.
///
/// The counter writes and the record are one KV batch, hence one PostgreSQL
/// transaction. A caller must never be told the trip failed while an invisible
/// breaker was nevertheless left holding the node.
pub async fn trip(
    budgets: &Budgets,
    application: &str,
    graph: &str,
    node: &NodePlan,
    body: &BackoffBody,
) -> queen_mq::Result<Value> {
    let seconds = body.retry_after_seconds.clamp(MIN_SECONDS, MAX_SECONDS);

    // DEDUPLICATED, and it is not tidiness. Two budgets on one node may legally
    // compile to the same key — `shared-conflict` only fires when their `count`,
    // `time_ms` or `sub_windows` differ, so two budgets carrying the same
    // `sharedKey` with identical parameters validate clean and map to one row.
    // The broker refuses a call that writes one key twice
    // (`024_kv.sql`, `kv_duplicate_key_in_call`, described there as load-bearing
    // for the intra-space lock order), so without this the whole `trip` returns
    // an error and the node KEEPS ADMITTING after a vendor has said 429.
    let keys: Vec<String> = unique_keys(node);
    if keys.is_empty() {
        // `node-unscoped-budget` refuses this at declare time; a document stored
        // by an older build could still carry it.
        return Ok(json!({
            "ok": false,
            "error": format!(
                "node `{}` has no budget on the node itself, so there is nothing to spend. A \
                 per-key budget cannot be a breaker: there is no one counter every path meets.",
                node.name
            ),
        }));
    }

    // The WIDEST path's ceiling, so no path can slip under it: a path at
    // share 0.5 refuses itself at half the counter, and writing half would leave
    // it admitting. Replacing the old counter also gives back `refundCost`:
    // none of the old window survives this write, and after the breaker expires
    // the next charge starts a fresh window at zero. A separate decrement before
    // this replacement was a no-op on success and leaked capacity if the trip
    // subsequently failed.
    let spend: Vec<(String, i64)> = first_by_key(node)
        .into_iter()
        .map(|b| (b.key.clone(), b.max_for(node.widest_share())))
        .collect();

    let rec = Record {
        at: crate::now_ms(),
        retry_after_seconds: seconds,
        by: body.by.clone(),
        application: application.to_string(),
        graph: graph.to_string(),
        node: node.name.clone(),
    };
    // Fleet-wide and atomic with the counters. v1's breach ring was per-replica,
    // and a breach seen only by the pod nobody is looking at is a breach nobody
    // sees; a counter spent without this record is the same operationally.
    budgets
        .spend_with_record(
            &spend,
            &node.breaker_key,
            serde_json::to_value(&rec).unwrap_or(json!({})),
            seconds,
        )
        .await?;

    tracing::warn!(
        application = %application, graph = %graph, node = %node.name,
        retry_after_seconds = seconds, by = ?body.by,
        "breaker tripped: the node's window is spent, every path will refuse until it expires"
    );

    Ok(json!({
        "ok": true,
        "node": node.name,
        "retryAfterSeconds": seconds,
        "until": rec.at + seconds * 1000,
        "keys": keys,
        "refunded": body.refund_cost.filter(|c| *c > 0).unwrap_or(0),
    }))
}

/// Un-break early.
///
/// Deleting the counters is not the same as writing zero: the next `incr`
/// recreates the row with a FRESH window, where a zero would keep whatever
/// expiry the breaker wrote and rotate at the wrong moment.
pub async fn reset(budgets: &Budgets, node: &NodePlan) -> queen_mq::Result<Value> {
    // Deduplicated for the same reason as `trip`: one key twice in one call is
    // `kv_duplicate_key_in_call`, and a reset that errors leaves the node held.
    let mut keys: Vec<String> = unique_keys(node);
    keys.push(node.breaker_key.clone());
    budgets.clear(&keys).await?;
    tracing::info!(node = %node.name, "breaker reset");
    Ok(json!({ "ok": true, "node": node.name, "cleared": keys }))
}

/// The unscoped budgets of a node, one per distinct kv key.
///
/// Two budgets can share a key (the same `sharedKey`, same parameters), and one
/// key is one counter: it is spent once, credited once and cleared once.
fn first_by_key(node: &NodePlan) -> Vec<&gate_core::CompiledBudget> {
    let mut seen: Vec<&str> = Vec::new();
    let mut out: Vec<&gate_core::CompiledBudget> = Vec::new();
    for b in node.unscoped() {
        if seen.contains(&b.key.as_str()) {
            continue;
        }
        seen.push(b.key.as_str());
        out.push(b);
    }
    out
}

fn unique_keys(node: &NodePlan) -> Vec<String> {
    first_by_key(node)
        .into_iter()
        .map(|b| b.key.clone())
        .collect()
}

/// Every breaker currently holding a node, fleet-wide.
pub async fn recent(budgets: &Budgets, limit: u32) -> Vec<Value> {
    match budgets.get_prefix("brk:", limit).await {
        Ok(rows) => {
            let mut out: Vec<Record> = rows
                .into_iter()
                .filter_map(|r| r.value.and_then(|v| serde_json::from_value(v).ok()))
                .collect();
            out.sort_by_key(|r| std::cmp::Reverse(r.at));
            out.iter()
                .map(|r| {
                    json!({
                        "at": r.at,
                        "application": r.application,
                        "target": format!("{}.{}", r.graph, r.node),
                        "graph": r.graph,
                        "node": r.node,
                        "retryAfterSeconds": r.retry_after_seconds,
                        "until": r.at + r.retry_after_seconds * 1000,
                        "by": r.by,
                    })
                })
                .collect()
        }
        Err(e) => {
            tracing::warn!(error = %e, "could not read the breaker records");
            Vec::new()
        }
    }
}

/// One node's breaker record, if it is currently held.
///
/// The record's own TTL is the answer: a key that has expired is a breaker that
/// has lifted, and there is nothing to sweep and nothing to clear.
pub async fn held(budgets: &Budgets, node: &NodePlan) -> Option<Record> {
    let rows = budgets
        .get_raw(std::slice::from_ref(&node.breaker_key))
        .await
        .ok()?;
    rows.into_iter()
        .next()
        .and_then(|r| r.value)
        .and_then(|v| serde_json::from_value(v).ok())
}
