//! Where the declared graphs live between restarts.
//!
//! `gate` has no database of its own, and that is a deliberate position: the
//! work is in queen's log and the budget counters are rows in `queen.kv`.
//! Everything that matters is already durable in the Postgres queen owns, and
//! adding a second data system to hold one more thing would be a second data
//! system to run, size, back up and lose.
//!
//! The declarations were the exception, and not on purpose: they lived in a
//! HashMap in the process. A restart dropped every one of them while their
//! queues stayed in the broker with nobody draining — 112 orphans after an
//! afternoon of testing. So they go where they always belonged.
//!
//! `forever` rather than a TTL, and it is the one place in this codebase that
//! asks for it: a configuration that expires is a configuration that vanishes at
//! 3am for no reason anybody can reconstruct.

#![allow(deprecated)]

use queen_mq::{Expiry, Queen, Result};

use gate_core::{v1, GraphDoc};

/// The same namespace the counters live in, and read from the same place: one
/// namespace per deployment is one thing to look at in the console, and two
/// spellings of it is a store nobody can find.
fn ns() -> String {
    crate::budget::namespace()
}

/// v2 documents. Also where v1 `GraphSpec`s were kept, which is why the read
/// below tries both.
const GRAPH_PREFIX: &str = "graph:";

/// v1 standalone `TargetSpec`s. Nothing writes here any more; the read exists so
/// an upgrade brings them across instead of leaving their queues in the broker
/// with nobody draining.
const V1_TARGET_PREFIX: &str = "spec:";

fn graph_key(app: &str, name: &str) -> String {
    format!("{GRAPH_PREFIX}{app}:{name}")
}

fn v1_target_key(app: &str, name: &str) -> String {
    format!("{V1_TARGET_PREFIX}{app}:{name}")
}

pub async fn save(queen: &Queen, doc: &GraphDoc) -> Result<()> {
    let value = serde_json::to_value(doc).map_err(|e| queen_mq::Error::Decode(e.to_string()))?;
    queen
        .kv()
        .put(
            &ns(),
            &graph_key(&doc.application, &doc.graph),
            value,
            Expiry::forever(),
        )
        .send()
        .await?;
    Ok(())
}

pub async fn forget(queen: &Queen, app: &str, name: &str) -> Result<()> {
    // Remove legacy target rows first. A v1 graph-node target named
    // `airbnb.ip` migrates to the one-node graph `ip`; deleting only
    // `spec:{app}:ip` leaves `spec:{app}:airbnb.ip` behind and the next boot
    // restores the graph that the caller just deleted.
    queen
        .kv()
        .delete(&ns(), &v1_target_key(app, name))
        .send()
        .await?;
    forget_dotted_v1_targets(queen, app, name).await?;

    queen
        .kv()
        .delete(&ns(), &graph_key(app, name))
        .send()
        .await?;
    Ok(())
}

async fn forget_dotted_v1_targets(queen: &Queen, app: &str, graph: &str) -> Result<()> {
    let prefix = format!("{V1_TARGET_PREFIX}{app}:");
    let found = queen
        .kv()
        .get_prefix(&ns(), &prefix)
        .limit(1000)
        .send()
        .await?;
    if found.truncated() {
        return Err(queen_mq::Error::Invalid(format!(
            "cannot safely delete `{app}/{graph}`: more than 1000 legacy target rows share this application"
        )));
    }

    for row in found.rows.unwrap_or_default() {
        let belongs = row
            .value
            .and_then(|value| serde_json::from_value::<v1::TargetSpec>(value).ok())
            .is_some_and(|old| legacy_graph_name(&old.name) == graph);
        if belongs {
            queen.kv().delete(&ns(), &row.key).send().await?;
        }
    }
    Ok(())
}

fn legacy_graph_name(target: &str) -> &str {
    target.rsplit('.').next().unwrap_or(target)
}

/// What a prefix read found, and whether it found everything.
///
/// The distinction is the difference between "nobody declared that" and "we did
/// not see it": the reconcile loop removes what the store no longer holds, and a
/// page the broker clamped, or a row this build cannot parse, would otherwise
/// read as a delete and tear down a running graph. So an incomplete read is
/// allowed to ADD and CHANGE, and never to remove.
pub struct Stored {
    pub items: Vec<GraphDoc>,
    pub complete: bool,
    /// Documents that were read as v1 and mapped on the way in. The caller
    /// re-saves them in the new shape, so this happens once per upgrade rather
    /// than on every boot.
    pub migrated: Vec<String>,
}

/// Every declaration this cell has been told about.
///
/// A prefix is mandatory — a namespace is not a table to enumerate — which is
/// why the documents are keyed under one rather than at the root.
pub async fn load_all(queen: &Queen) -> Stored {
    try_load_all(queen).await.unwrap_or_else(|_| Stored {
        items: Vec::new(),
        complete: false,
        migrated: Vec::new(),
    })
}

/// The same read, with the failure kept.
///
/// The reconcile loop removes what the store no longer holds, so it must be able
/// to tell "nobody declared anything" from "the broker did not answer". Reading
/// an error as an empty set would reap the entire fleet's configuration on a
/// transient failure — which is the loudest possible way for a background task
/// to be wrong.
pub async fn try_load_all(queen: &Queen) -> Result<Stored> {
    let mut items = Vec::new();
    let mut migrated = Vec::new();
    let mut unreadable = 0usize;
    let mut truncated = false;

    let res = queen
        .kv()
        .get_prefix(&ns(), GRAPH_PREFIX)
        .limit(1000)
        .send()
        .await?;
    truncated |= res.truncated();
    for row in res.rows.unwrap_or_default() {
        let Some(value) = row.value else {
            unreadable += 1;
            continue;
        };
        match serde_json::from_value::<GraphDoc>(value.clone()) {
            Ok(doc) => items.push(doc),
            Err(_) => match serde_json::from_value::<v1::GraphSpec>(value) {
                Ok(old) => match gate_core::migrate::from_v1_graph(&old) {
                    Ok(m) => {
                        migrated.push(m.doc.key());
                        items.push(m.doc);
                    }
                    Err(refused) => {
                        // A v1 graph carrying breach rules. It is NOT silently
                        // dropped and it is NOT silently accepted with the
                        // policy gone: the row stays where it is and the read is
                        // incomplete, so nothing reaps anything on this pass.
                        tracing::error!(
                            key = %row.key, detail = %refused.0,
                            "a stored v1 graph cannot be migrated; it is not running and nothing \
                             will be removed on this pass"
                        );
                        unreadable += 1;
                    }
                },
                // A document written by a NEWER build (an unknown field, and
                // every document type refuses those on purpose) or a corrupt
                // row. Counted, not ignored.
                Err(_) => unreadable += 1,
            },
        }
    }

    // v1 standalone targets. Each becomes a one-node graph named for itself.
    let res = queen
        .kv()
        .get_prefix(&ns(), V1_TARGET_PREFIX)
        .limit(1000)
        .send()
        .await?;
    truncated |= res.truncated();
    for row in res.rows.unwrap_or_default() {
        let Some(value) = row.value else {
            unreadable += 1;
            continue;
        };
        match serde_json::from_value::<v1::TargetSpec>(value) {
            Ok(old) => match gate_core::migrate::from_v1_target(&old) {
                Ok(m) => {
                    if items.iter().any(|d| d.key() == m.doc.key()) {
                        // Already declared as a graph. The graph wins, exactly
                        // as v1's restore let a graph win over a stray target of
                        // the same name.
                        continue;
                    }
                    migrated.push(m.doc.key());
                    items.push(m.doc);
                }
                Err(refused) => {
                    tracing::error!(key = %row.key, detail = %refused.0, "a stored v1 target cannot be migrated");
                    unreadable += 1;
                }
            },
            Err(_) => unreadable += 1,
        }
    }

    if unreadable > 0 {
        tracing::warn!(
            unreadable,
            "the store holds documents this build cannot read; nothing will be removed on this pass"
        );
    }
    Ok(Stored {
        items,
        complete: !truncated && unreadable == 0,
        migrated,
    })
}

#[cfg(test)]
mod tests {
    use super::legacy_graph_name;

    #[test]
    fn a_dotted_v1_target_maps_to_its_leaf_graph() {
        assert_eq!(legacy_graph_name("airbnb.ip"), "ip");
        assert_eq!(legacy_graph_name("standalone"), "standalone");
    }
}
