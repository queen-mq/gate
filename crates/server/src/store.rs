//! Where the declared targets live between restarts.
//!
//! `gate` has no database of its own, and that is a deliberate position:
//! the work is in queen's log, the budget counters are in `queen_streams.state`
//! keyed by partition, the cross-target ceilings are in `queen.kv`. Everything
//! that matters is already durable in the Postgres queen owns, and adding a
//! second data system to hold one more thing would be a second data system to
//! run, size, back up and lose.
//!
//! But the target SPECS were the exception, and not on purpose: they lived in a
//! HashMap in the process. A restart dropped every one of them while their
//! queues stayed in the broker with nobody draining — 112 orphans after an
//! afternoon of testing. So they go where they always belonged, which is the
//! store queen already gives us for exactly this: a small durable value under a
//! name we choose.
//!
//! `forever` rather than a TTL, and it is the one place in this codebase that
//! asks for it: a configuration that expires is a configuration that vanishes
//! at 3am for no reason anybody can reconstruct.

use queen_mq::{Expiry, Queen, Result};
use gate_core::{GraphSpec, TargetSpec};

const NS: &str = "gate";
const PREFIX: &str = "spec:";
/// Graphs live under their own prefix and are the authority for their nodes: a
/// node's projected target spec is deliberately NOT saved, so there is exactly one
/// document to reconcile and no way for the two to disagree.
const GRAPH_PREFIX: &str = "graph:";

fn key(app: &str, name: &str) -> String {
    format!("{PREFIX}{app}:{name}")
}

fn graph_key(app: &str, name: &str) -> String {
    format!("{GRAPH_PREFIX}{app}:{name}")
}


pub async fn save(queen: &Queen, spec: &TargetSpec) -> Result<()> {
    let value = serde_json::to_value(spec).map_err(|e| queen_mq::Error::Decode(e.to_string()))?;
    queen
        .kv()
        .put(NS, &key(&spec.application, &spec.name), value, Expiry::forever())
        .send()
        .await?;
    Ok(())
}

pub async fn forget(queen: &Queen, app: &str, name: &str) -> Result<()> {
    queen.kv().delete(NS, &key(app, name)).send().await?;
    Ok(())
}

/// Every spec this cell has been told about.
///
/// A prefix is mandatory — a namespace is not a table to enumerate — which is
/// why the specs are keyed under one rather than at the root.
pub async fn load_all(queen: &Queen) -> Vec<TargetSpec> {
    try_load_all(queen)
        .await
        .map(|s| s.items)
        .unwrap_or_default()
}


/// The same read, with the failure kept.
///
/// The reconcile loop removes what the store no longer holds, so it must be able
/// to tell "nobody declared anything" from "the broker did not answer". Reading
/// an error as an empty set would reap the entire fleet's configuration on a
/// transient failure — which is the loudest possible way for a background task to
/// be wrong.
pub async fn try_load_all(queen: &Queen) -> Result<Stored<TargetSpec>> {
    let res = queen.kv().get_prefix(NS, PREFIX).limit(1000).send().await?;
    Ok(collect(res))
}

/// What a prefix read found, and whether it found everything.
///
/// The distinction is the difference between "nobody declared that" and "we did not
/// see it": the reconcile loop removes what the store no longer holds, and a page
/// the broker clamped, or a row this build cannot parse, would otherwise read as a
/// delete and tear down a running target. So an incomplete read is allowed to ADD
/// and CHANGE, and never to remove.
pub struct Stored<T> {
    pub items: Vec<T>,
    pub complete: bool,
}

fn collect<T: serde::de::DeserializeOwned>(res: queen_mq::KvResult) -> Stored<T> {

    let truncated = res.truncated();
    let rows = res.rows.unwrap_or_default();
    let mut items = Vec::with_capacity(rows.len());
    let mut unreadable = 0usize;
    for row in &rows {
        match row.value.clone().map(serde_json::from_value::<T>) {
            Some(Ok(v)) => items.push(v),
            // A document written by a NEWER build (an unknown field, and every spec
            // type refuses those on purpose) or a corrupt row. Counted, not ignored.
            Some(Err(_)) | None => unreadable += 1,
        }
    }
    if unreadable > 0 {
        tracing::warn!(
            unreadable,
            "the store holds documents this build cannot read; nothing will be removed on this pass"
        );
    }
    Stored { items, complete: !truncated && unreadable == 0 }
}


// --------------------------------------------------------------------- graphs

pub async fn save_graph(queen: &Queen, g: &GraphSpec) -> Result<()> {
    let value = serde_json::to_value(g).map_err(|e| queen_mq::Error::Decode(e.to_string()))?;
    queen
        .kv()
        .put(NS, &graph_key(&g.application, &g.name), value, Expiry::forever())
        .send()
        .await?;
    Ok(())
}

pub async fn forget_graph(queen: &Queen, app: &str, name: &str) -> Result<()> {
    queen.kv().delete(NS, &graph_key(app, name)).send().await?;
    Ok(())
}

pub async fn try_load_graphs(queen: &Queen) -> Result<Stored<GraphSpec>> {
    let res = queen
        .kv()
        .get_prefix(NS, GRAPH_PREFIX)
        .limit(1000)
        .send()
        .await?;
    Ok(collect(res))
}

pub async fn load_graphs(queen: &Queen) -> Vec<GraphSpec> {
    try_load_graphs(queen)
        .await
        .map(|s| s.items)
        .unwrap_or_default()
}


