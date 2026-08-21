//! Declaring a graph: validate, compile, provision, run, store.
//!
//! Atomic in the only sense that matters to a caller: either the whole document
//! is running and stored, or nothing changed and the previous one is still
//! serving. There are no per-node targets to swap one at a time any more, which
//! removes most of what made v1's rollback delicate.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use serde_json::{json, Value};

use gate_core::plan::{Plan, PlanOpts, QueueKind};
use gate_core::{GraphDoc, Problem};

use crate::api::Shared;
use crate::knobs::knobs;
use crate::registry::GraphRuntime;
use crate::supervisor;

pub enum Refusal {
    Invalid(String),
    Conflict(String),
    Gateway(String),
}

impl Refusal {
    pub fn message(&self) -> &str {
        match self {
            Refusal::Invalid(m) | Refusal::Conflict(m) | Refusal::Gateway(m) => m,
        }
    }
}

/// Compile a document against what the broker and the registry currently say.
///
/// Two passes on purpose: the first plan exists only to learn which queues the
/// document names, so the broker can be asked about the ones Gate does not own.
/// Their partition count is a fact Gate READS rather than chooses, and it is
/// what the per-stage worker count defaults from.
pub async fn compile(app: &Shared, doc: &GraphDoc) -> (Plan, gate_core::ExternalFacts) {
    let provisional = gate_core::compile(doc);
    let queues = supervisor::probe(&app.queen, &provisional).await;

    let mut partitions = std::collections::BTreeMap::new();
    for (name, facts) in &queues {
        if facts.partitions > 0 {
            partitions.insert(name.clone(), facts.partitions);
        }
    }
    let opts = PlanOpts {
        batch: knobs().batch,
        concurrency: knobs().concurrency,
        lane_capacity: knobs().lane_capacity,
        partitions,
        // §16.3 — the `assumed` discount is wired and NOT switched on. v1
        // defined it, unit-tested it, documented it in the README and never
        // applied it; turning it on changes what every existing `assumed` budget
        // admits, which is a product decision and not one to make on the way
        // past.
        assumed_factor: 1.0,
    };
    let plan = gate_core::compile_with(doc, &opts);

    let key = doc.key();
    let facts = gate_core::ExternalFacts {
        queues,
        ingress_owners: app.registry.ingress_owners(&key),
        egress_owners: app.registry.egress_owners(&key),
    };
    (plan, facts)
}

/// A caller's declare: everything below, plus the checks that only apply when a
/// human (or their deploy) asked for the change.
pub async fn declare(app: &Shared, doc: GraphDoc) -> Result<Value, Refusal> {
    let _guard = app.declare_lock.lock().await;
    declare_locked(app, doc, true).await
}

/// The declare itself. Separate because a sync declares a whole set and must
/// hold the lock across all of them — a reconcile pass landing between two would
/// see half a configuration and reap the other half.
pub async fn declare_locked(
    app: &Shared,
    doc: GraphDoc,
    from_caller: bool,
) -> Result<Value, Refusal> {
    let key = doc.key();
    let (plan, facts) = compile(app, &doc).await;

    let problems = gate_core::validate_with(&doc, &facts);
    if !problems.is_empty() {
        return Err(Refusal::Invalid(join(&problems)));
    }

    let old = app.registry.get(&doc.application, &doc.graph);
    if from_caller {
        if let Some(old) = &old {
            if gate_core::needs_version_bump(&old.doc, &doc) && doc.version <= old.doc.version {
                return Err(Refusal::Conflict(format!(
                    "this change re-founds a counter or strands a queue (a new key starts at zero \
                     while the old one counts down its TTL, and work already in an interior queue \
                     has no consumer in the new plan): bump version above {}. Drain first — stop \
                     pushing, wait for `waitingForBudget` to reach zero on every node, then \
                     declare.",
                    old.doc.version
                )));
            }
        }
        // The same question of the STORE, because a declare lands on ONE
        // replica: a graph declared a second ago on another pod is not in this
        // registry yet. A store that will not answer is not a reason to refuse —
        // the local check still stands.
        if let Ok(stored) = crate::store::try_load_all(&app.queen).await {
            for other in stored.items.iter().filter(|d| d.key() != key) {
                let mine = gate_core::compile(other);
                for (node, np) in &mine.nodes {
                    let Some(q) = &np.ingress_queue else { continue };
                    if plan
                        .nodes
                        .values()
                        .any(|n| n.ingress_queue.as_deref() == Some(q.as_str()))
                    {
                        return Err(Refusal::Conflict(format!(
                            "`{q}` is already the ingress of node `{node}` in graph `{}` (declared \
                             on another replica). Two consumers of one queue in different groups \
                             each get every message, which doubles what leaves.",
                            other.key()
                        )));
                    }
                }
            }
        }
    }

    // Stop-then-start, with the old document put back if the new one cannot be
    // provisioned. Without that restore the graph is left stopped and still
    // registered: it accepts pushes and admits nothing, for ever, which is the
    // one failure an operator cannot recover from without knowing this code.
    let rt = match supervisor::swap(
        &app.queen,
        &app.budgets,
        app.traces.clone(),
        old.as_ref(),
        doc.clone(),
        plan.clone(),
    )
    .await
    {
        Ok(rt) => rt,
        Err(failed) => {
            let detail = match failed.restored {
                Some(rt) => {
                    let version = rt.doc.version;
                    app.registry.put(rt);
                    format!(
                        "provisioning failed: {}; still serving version {version}",
                        failed.error
                    )
                }
                None => {
                    // Nothing is serving it. Unregister, so a push is refused —
                    // recoverable — rather than accepted into a queue nobody
                    // drains.
                    app.registry.remove(&doc.application, &doc.graph);
                    format!(
                        "provisioning failed: {}; the old plan could not be restarted either, so \
                         `{key}` is unregistered and will refuse pushes until a declare succeeds",
                        failed.error
                    )
                }
            };
            return Err(Refusal::Gateway(detail));
        }
    };
    app.registry.put(rt.clone());

    if from_caller {
        // Persisted only after the stages are actually up: a document saved for
        // a graph that failed to provision would come back on the next boot and
        // fail again, for ever.
        match crate::store::save(&app.queen, &doc).await {
            Ok(()) => rt.persisted.store(true, Ordering::Relaxed),
            // A declare that did not persist is not a declare that happened.
            //
            // It used to warn and answer 200. With a reconcile loop that is a
            // lie with a fifteen-second fuse: the store still holds the previous
            // document, so the very next pass restarts this graph on it and the
            // change the caller was told had landed is gone. The runtime keeps
            // serving the new document until then — tearing it down would add an
            // outage to a failed write — but the caller is told, so it can
            // retry.
            Err(e) => {
                tracing::warn!(graph = %key, error = %e, "declared but not persisted");
                return Err(Refusal::Gateway(format!(
                    "`{key}` is running this document but it could not be stored ({e}), so it is \
                     NOT durable: the next reconcile pass will restart it on the stored one. Retry \
                     the declare"
                )));
            }
        }
    } else {
        rt.persisted.store(true, Ordering::Relaxed);
    }

    Ok(resolved(&rt, &gate_core::warnings_with(&doc, &facts)))
}

/// Apply a document the store already holds. No version-bump check, and no save.
///
/// The asymmetry is v1's and it is kept verbatim: enforcing the bump against a
/// replica-local runtime is how a replica wedges on a legal delete-and-redeclare
/// at the same version.
pub async fn declare_from_store(app: &Shared, doc: GraphDoc) -> Result<(), Refusal> {
    declare_locked(app, doc, false).await.map(|_| ())
}

pub async fn stop(rt: &Arc<GraphRuntime>) {
    supervisor::stop(rt).await;
}

/// Remove a graph.
///
/// The stored declaration goes FIRST: a delete that stops the stages and leaves
/// the document behind is undone by the next reconcile pass, on this replica and
/// on every other one. And a delete that cannot reach the store is refused
/// rather than half-applied.
pub async fn remove(app: &Shared, application: &str, name: &str) -> Result<Value, Refusal> {
    let _guard = app.declare_lock.lock().await;
    if let Err(e) = crate::store::forget(&app.queen, application, name).await {
        return Err(Refusal::Gateway(format!(
            "`{application}/{name}` was not deleted: the stored declaration could not be removed \
             ({e}), and a delete that stops the stages and leaves the document behind is undone by \
             the next reconcile pass"
        )));
    }
    match app.registry.get(application, name) {
        Some(rt) => {
            supervisor::stop(&rt).await;
            app.registry.remove(application, name);
            Ok(json!({ "ok": true, "registered": true }))
        }
        // Not running here is a success: the document is gone, which is what was
        // asked for, and a graph whose provisioning keeps failing must still be
        // removable.
        None => Ok(json!({ "ok": true, "registered": false })),
    }
}

fn join(problems: &[Problem]) -> String {
    problems
        .iter()
        .map(|p| p.to_string())
        .collect::<Vec<_>>()
        .join("; ")
}

/// What the declare answers: the whole compiled plan, so a caller never has to
/// reconstruct it and never has to guess a queue name.
pub fn resolved(rt: &Arc<GraphRuntime>, warnings: &[Problem]) -> Value {
    let plan = &rt.plan;
    json!({
        "ok": true,
        "application": rt.doc.application,
        "graph": rt.doc.graph,
        "version": rt.doc.version,
        "resolved": {
            "namespace": plan.namespace,
            "queues": plan.queues.iter().map(|q| json!({
                "name": q.name,
                "kind": match q.kind {
                    QueueKind::OwnedIngress => "ingress",
                    QueueKind::Interior => "interior",
                    QueueKind::Egress => "egress",
                    QueueKind::UserIngress => "ingress (yours)",
                },
                "ownedByGate": matches!(q.kind, QueueKind::OwnedIngress | QueueKind::Interior),
                "partitions": q.partitions,
            })).collect::<Vec<_>>(),
            "nodes": plan.nodes.iter().map(|(name, np)| json!({
                "node": name,
                "ingressQueue": np.ingress_queue,
                "httpPush": np.ingress_http,
                "egressQueue": np.egress_queue,
                "egressGroup": np.egress_group,
                "breakerKey": np.breaker_key,
                // The per-path ceilings, so §3.6 is legible from the response:
                // one counter, several ceilings, and the reserve is the gap
                // above the tallest lower one.
                "shares": np.shares,
                "budgets": np.budgets.iter().map(|b| json!({
                    "id": b.id,
                    "key": b.key,
                    "scopeBy": b.scope_by,
                    "sharedKey": b.shared_key,
                    "count": b.count,
                    "timeMs": b.time_ms,
                    "subWindows": b.sub_windows,
                    "countSub": b.count_sub,
                    "windowSubSeconds": b.window_sub_seconds,
                    "ceilings": np.shares.iter()
                        .map(|(p, s)| (p.clone(), b.max_for(*s)))
                        .collect::<std::collections::BTreeMap<_, _>>(),
                })).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
            "stages": plan.stages.iter().map(stage_view).collect::<Vec<_>>(),
            "counters": plan.counters_window_seconds,
        },
        "warnings": warnings.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
        // The trust model changed and it must be said out loud, not discovered.
        // v1 relied on its queues being undiscoverable (names derived, never
        // told); v2 NAMES the egress queue in the declaration.
        "trust": "write access to an interior or egress queue is admission bypass — the same trust \
                  model as any queen queue. Gate paces what flows through it; it does not defend a \
                  queue from a writer who already has the credentials.",
    })
}

fn stage_view(s: &gate_core::plan::Stage) -> Value {
    json!({
        "path": s.path,
        "node": s.node,
        "hop": s.hop,
        "priority": s.priority,
        "share": s.share,
        "source": s.source,
        "group": s.group,
        "batch": s.batch,
        "concurrency": s.concurrency,
        "checksForeign": s.check_foreign,
        "destinations": s.destinations.iter().map(|d| json!({
            "node": d.node,
            "queue": d.queue,
            "derivesTransactionId": d.derive_id,
            "terminal": d.terminal,
        })).collect::<Vec<_>>(),
    })
}

/// The topology, broker-free, so a drawing can poll it.
pub fn topology(rt: &Arc<GraphRuntime>) -> Value {
    json!({
        "application": rt.doc.application,
        "graph": rt.doc.graph,
        "version": rt.doc.version,
        "nodes": rt.plan.nodes.keys().map(|n| json!({
            "node": n,
            "ingress": rt.plan.nodes[n].ingress_queue.is_some(),
            "egress": rt.plan.nodes[n].egress_queue.is_some(),
            "paths": gate_core::plan::paths_through(&rt.plan, n),
            "shares": rt.plan.nodes[n].shares,
        })).collect::<Vec<_>>(),
        "paths": rt.doc.paths.iter().map(|p| json!({
            "name": p.name,
            "priority": p.priority,
            "share": p.share,
            "hops": gate_core::plan::hop_names(p),
        })).collect::<Vec<_>>(),
        "edges": gate_core::plan::edges(&rt.doc).iter()
            .map(|(a, b)| json!({ "from": a, "to": b }))
            .collect::<Vec<_>>(),
    })
}
