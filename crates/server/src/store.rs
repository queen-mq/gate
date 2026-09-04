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
    queen
        .kv()
        .delete(&ns(), &graph_key(app, name))
        .send()
        .await?;
    // A graph that came across from a v1 standalone target keeps its old row
    // until it is deleted too, or the next boot restores it and the delete looks
    // like it did not take.
    queen
        .kv()
        .delete(&ns(), &v1_target_key(app, name))
        .send()
        .await
        .ok();
    Ok(())
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

    let (rows, graphs_complete) = scan_prefix(queen, GRAPH_PREFIX).await?;
    for row in rows {
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
    let (rows, targets_complete) = scan_prefix(queen, V1_TARGET_PREFIX).await?;
    for row in rows {
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
        complete: graphs_complete && targets_complete && unreadable == 0,
        migrated,
    })
}

/// Read every page under one store prefix.
///
/// The broker caps a page by both rows and bytes. `truncated` therefore does
/// not mean merely "there may be more than 1,000 documents": one large value
/// can make even a short page incomplete. The exclusive `nextAfter` cursor is
/// the only correct way to resume it.
///
/// A malformed truncated response is returned as incomplete rather than spun
/// on forever. Reconcile may still add/change the documents it did see, but its
/// `complete` guard will not interpret an unseen one as deleted.
async fn scan_prefix(queen: &Queen, prefix: &str) -> Result<(Vec<queen_mq::KvRow>, bool)> {
    let namespace = ns();
    let mut rows = Vec::new();
    let mut after: Option<String> = None;

    loop {
        let mut query = queen.kv().get_prefix(&namespace, prefix).limit(1000);
        if let Some(cursor) = &after {
            query = query.after(cursor);
        }
        let page = query.send().await?;
        let truncated = page.truncated();
        let next = page.next_after.clone();
        rows.extend(page.rows.unwrap_or_default());

        if !truncated {
            return Ok((rows, true));
        }
        match next {
            Some(cursor) if after.as_ref().is_none_or(|previous| cursor > *previous) => {
                after = Some(cursor);
            }
            _ => return Ok((rows, false)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::extract::State;
    use axum::routing::post;
    use axum::{Json, Router};
    use parking_lot::Mutex;
    use queen_mq::{Config, Queen};
    use serde_json::{json, Value};

    use super::*;

    fn document(name: &str) -> Value {
        json!({
            "application": "paging",
            "graph": name,
            "version": 1,
            "nodes": {
                "n": {
                    "budgets": [{ "id": "b", "count": 10, "timeMs": 1000 }],
                    "ingress": true,
                    "egress": "paging.out"
                }
            },
            "paths": [{ "name": "main", "nodes": ["n"] }]
        })
    }

    async fn kv_page(
        State(seen): State<Arc<Mutex<Vec<Value>>>>,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        seen.lock().push(body.clone());
        let op = &body["operations"][0];
        let prefix = op["prefix"].as_str().unwrap_or_default();
        let after = op.get("after").and_then(Value::as_str);
        let result = match (prefix, after) {
            (GRAPH_PREFIX, None) => json!({
                "index": 0,
                "op": "getPrefix",
                "rows": [{ "key": "graph:paging:a", "value": document("a"), "version": 1 }],
                "truncated": true,
                "nextAfter": "graph:paging:a"
            }),
            (GRAPH_PREFIX, Some("graph:paging:a")) => json!({
                "index": 0,
                "op": "getPrefix",
                "rows": [{ "key": "graph:paging:b", "value": document("b"), "version": 1 }],
                "truncated": false
            }),
            (V1_TARGET_PREFIX, None) => json!({
                "index": 0,
                "op": "getPrefix",
                "rows": [],
                "truncated": false
            }),
            _ => panic!("unexpected prefix page: {op}"),
        };
        Json(json!({ "results": [result] }))
    }

    #[tokio::test]
    async fn a_truncated_store_scan_resumes_from_the_brokers_cursor() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake broker");
        let url = format!("http://{}", listener.local_addr().expect("address"));
        let router = Router::new()
            .route("/api/v1/kv", post(kv_page))
            .with_state(seen.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .await
                .expect("serve fake broker")
        });
        let queen = Queen::connect(Config::new(url)).expect("client");

        let stored = try_load_all(&queen).await.expect("scan store");
        server.abort();

        assert!(stored.complete, "both prefixes reached their final page");
        assert_eq!(
            stored.items.iter().map(GraphDoc::key).collect::<Vec<_>>(),
            ["paging/a", "paging/b"]
        );
        let requests = seen.lock();
        assert_eq!(requests.len(), 3, "two graph pages and one target page");
        assert_eq!(
            requests[1]["operations"][0]["after"],
            json!("graph:paging:a"),
            "the second page must use the broker's exclusive cursor"
        );
    }
}
