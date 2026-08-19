//! Declaring a graph: the nodes, the relays, and the atomicity of both.
//!
//! A graph is declared whole. Validation runs over nodes, edges, `consume` and
//! `breach` together — a cost ceiling that shrinks along an edge is not a property
//! of either node — and provisioning either brings every node up or puts the
//! previous topology back. Half a graph is the failure mode worth spending code to
//! avoid: work enters at an entry that is running and stops at a node that is not,
//! and from the outside that is indistinguishable from a limiter deciding to refuse
//! everything.
//!
//! The nodes themselves are ordinary targets named `{graph}.{node}`. Nothing in the
//! data plane learns what a graph is: the push, the gate runners, the admitted
//! queues, the lease and the ack are the ones that were already there. What a graph
//! adds is the relays between them, the entry/terminal rules, and one document that
//! owns the lot.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use gate_core::{utilisation_max, GraphSpec, TargetSpec};
use serde_json::{json, Value};

use crate::api::Shared;
use crate::edge;
use crate::registry::{GraphRuntime, RelayRuntime};
use crate::{now_ms, store, supervisor};

/// Why a declare was refused, in the caller's terms.
pub enum Refusal {
    /// The document is wrong. 422.
    Invalid(String),
    /// The document is fine but the cell cannot accept it: a name already owned,
    /// or a migration without a version. 409.
    Conflict(String),
    /// The broker refused to provision. 502.
    Gateway(String),
}

/// Who asked for this declare, which decides two things: whether the document is
/// written back, and whether a migration-class change needs a version bump.
#[derive(Clone, Copy, PartialEq)]
enum Source {
    /// A caller's `PUT`. The document is new to the cell, so both rules apply.
    Caller,
    /// The store, on a boot restore or a reconcile pass. The document is already
    /// authoritative — some other replica's declare was accepted — so neither applies.
    Store,
}

/// Declare a graph, atomically, as a caller asked. The caller holds the declare lock.
pub async fn declare(app: &Shared, spec: GraphSpec) -> Result<Arc<GraphRuntime>, Refusal> {
    declare_with(app, spec, Source::Caller).await
}


/// Bring up a graph the STORE already holds — a boot restore, or a reconcile pass
/// applying another replica's declare.
///
/// Two things differ from a caller's declare, and both are about the store being the
/// authority rather than this process:
///
/// * the document is NOT written back. Saving a document that came from the store
///   re-creates it, so a delete landing on another replica while this pass provisions
///   would be undone — the delete having been acknowledged.
/// * a migration-class change does NOT need a version bump. The bump rule protects a
///   caller from re-founding counters by accident, and that judgement was already made
///   where the declare was accepted. Enforcing it here against a replica-LOCAL runtime
///   is how a replica wedges: a delete-and-redeclare at the same version is legal for
///   the caller and refused for ever by every pod that still holds the old runtime,
///   which is precisely the indefinite divergence the reconcile exists to end. Targets
///   converge on the store for the same reason (their version check lives only in the
///   API handler), and graphs must not be the exception.
pub async fn declare_from_store(
    app: &Shared,
    spec: GraphSpec,
) -> Result<Arc<GraphRuntime>, Refusal> {
    declare_with(app, spec, Source::Store).await
}

async fn declare_with(
    app: &Shared,
    spec: GraphSpec,
    source: Source,
) -> Result<Arc<GraphRuntime>, Refusal> {


    let problems = gate_core::validate_graph(&spec);
    if !problems.is_empty() {
        return Err(Refusal::Invalid(
            problems
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join("; "),
        ));
    }

    let node_specs = spec.node_specs();
    let previous = app.registry.graph(&spec.application, &spec.name);

    // G10: one owner per queue family. A standalone target called `airbnb.ip` and
    // a node `ip` of graph `airbnb` are the same queues with two declarers, and
    // whichever wrote last would be enforcing.
    for (node, ns) in &node_specs {
        if let Some(existing) = app.registry.get(&spec.application, &ns.name) {
            let ours = existing.graph.as_deref() == Some(spec.key().as_str());
            if !ours {
                return Err(Refusal::Conflict(match &existing.graph {
                    Some(other) => format!(
                        "node `{node}` would be the target `{}`, which graph `{other}` already owns",
                        ns.name
                    ),
                    None => format!(
                        "node `{node}` would be the target `{}`, which is already declared as a \
                         standalone target: two owners of one queue family. Delete the target, or \
                         rename the graph",
                        ns.name
                    ),
                }));
            }
        }
    }

    // The same question of the store, for the same reason as the target side: a
    // standalone target declared on another replica is not in this registry yet, and
    // two owners of one queue family is the failure G10 exists to prevent.
    if let Ok(stored) = store::try_load_all(&app.queen).await {
        for (node, ns) in &node_specs {
            if stored
                .items
                .iter()
                .any(|t| t.application == ns.application && t.name == ns.name)
            {
                return Err(Refusal::Conflict(format!(
                    "node `{node}` would be the target `{}`, which is declared as a standalone target \
                     (on another replica): two owners of one queue family. Delete the target, or \
                     rename the graph",
                    ns.name
                )));
            }
        }
    }

    if let (Source::Caller, Some(old)) = (source, &previous) {
        if gate_core::needs_graph_version_bump(&old.spec, &spec) && spec.version <= old.spec.version {
            return Err(Refusal::Conflict(format!(

                "this change re-founds a counter or a path (a new partition starts at zero): bump \
                 version above {}",
                old.spec.version
            )));
        }
    }

    // Relays first, and before any node moves: a relay forwarding into a node that
    // is being restarted would push into a queue whose runner is down, which is
    // work parked for as long as the declare takes.
    if let Some(old) = &previous {
        edge::stop_all(&old.relays).await;
    }

    // ---- provision every node, or put the old ones back.
    let mut started: Vec<(String, Arc<crate::registry::TargetRuntime>)> = Vec::new();
    for (node, ns) in &node_specs {
        let old = app.registry.get(&spec.application, &ns.name);
        match supervisor::swap(
            &app.queen,
            app.meter.clone(),
            app.history.clone(),
            old.as_ref(),
            ns.clone(),
            Some(spec.key()),
        )
        .await
        {
            Ok(rt) => {
                app.registry.put(rt.clone());
                started.push((node.clone(), rt));
            }
            Err(failed) => {
                match failed.restored {
                    Some(rt) => {
                        app.registry.put(rt);
                    }
                    // Nothing is serving it: unregister, so a push is REFUSED rather
                    // than accepted into a queue nobody drains.
                    None => {
                        app.registry.remove(&spec.application, &ns.name);
                    }
                }
                rollback(app, &previous, &started).await;
                return Err(Refusal::Gateway(format!(
                    "node `{node}` could not be provisioned: {}",
                    failed.error
                )));
            }
        }
    }

    // ---- nodes the previous version had and this one does not.
    if let Some(old) = &previous {
        for (_, old_spec) in old.spec.node_specs() {
            if node_specs.iter().any(|(_, ns)| ns.name == old_spec.name) {
                continue;
            }
            if let Some(rt) = app.registry.remove(&spec.application, &old_spec.name) {
                supervisor::stop_with(&app.queen, &rt).await;
            }
        }
    }

    let relays = start_relays(app, &spec, &node_specs);

    let rt = Arc::new(GraphRuntime {
        spec: spec.clone(),
        relays,
        node_keys: node_specs.iter().map(|(_, ns)| ns.key()).collect(),
        persisted: std::sync::atomic::AtomicBool::new(false),
    });
    app.registry.put_graph(rt.clone());

    // Persisted only once it is actually up, like a target: a document saved for a
    // graph that failed to provision would come back on every boot and fail again.
    if source == Source::Store {
        // It came FROM the store, so it is already there — and writing it back would
        // undo a delete that landed while this was provisioning.
        rt.persisted.store(true, Ordering::Relaxed);
        return Ok(rt);
    }

    match store::save_graph(&app.queen, &spec).await {
        Ok(()) => rt.persisted.store(true, Ordering::Relaxed),
        // Same as a target: a document that did not persist is not durable, and the next
        // reconcile pass restores the stored one over it. The graph keeps running so the
        // failure costs no traffic, and the caller is told so it can retry.
        Err(e) => {
            tracing::warn!(graph = %spec.key(), error = %e, "declared but not persisted");
            return Err(Refusal::Gateway(format!(
                "graph `{}` is running but its document could not be stored ({e}), so it is NOT \
                 durable: the next reconcile pass will restore the stored one. Retry the declare",
                spec.name
            )));
        }
    }
    Ok(rt)
}



/// One relay per destination node, each draining its upstreams in priority order.
fn start_relays(
    app: &Shared,
    spec: &GraphSpec,
    node_specs: &[(String, TargetSpec)],
) -> Vec<Arc<RelayRuntime>> {
    let spec_of = |node: &str| -> Option<TargetSpec> {
        node_specs
            .iter()
            .find(|(n, _)| n == node)
            .map(|(_, s)| s.clone())
    };

    let mut relays = Vec::new();
    for dest in spec.merge_dests() {
        let Some(dest_spec) = spec_of(&dest) else { continue };
        // Lowest priority first; ties keep their declared order, which is the only
        // tie-break a document offers.
        let mut legs: Vec<edge::Leg> = spec
            .in_edges(&dest)
            .into_iter()
            .filter_map(|e| {
                spec_of(&e.from).map(|s| edge::Leg {
                    node: e.from.clone(),
                    priority: e.priority,
                    spec: s,
                })
            })
            .collect();
        legs.sort_by_key(|l| l.priority);
        relays.push(edge::spawn(
            app.queen.clone(),
            app.depths.clone(),
            edge::Plan {
                application: spec.application.clone(),
                graph: spec.name.clone(),
                dest_node: dest,
                dest: dest_spec,
                sources: legs,
            },
        ));
    }
    relays
}

/// Put the previous topology back after a failed declare: the nodes this attempt
/// had already replaced, then the relays it had already stopped.
async fn rollback(
    app: &Shared,
    previous: &Option<Arc<GraphRuntime>>,
    started: &[(String, Arc<crate::registry::TargetRuntime>)],
) {
    let Some(old) = previous else {
        // A first declare that failed leaves nothing behind, so the nodes it did
        // bring up are torn down rather than left running half a graph.
        for (_, rt) in started {
            supervisor::stop_with(&app.queen, rt).await;
            app.registry.remove(&rt.spec.application, &rt.spec.name);
        }
        return;
    };
    for (node, rt) in started {
        let Some(old_spec) = old.spec.node_spec(node) else { continue };
        if old_spec == rt.spec {
            continue;
        }
        match supervisor::swap(
            &app.queen,
            app.meter.clone(),
            app.history.clone(),
            Some(rt),
            old_spec,
            Some(old.spec.key()),
        )
        .await
        {
            Ok(restored) => {
                app.registry.put(restored);
            }
            // The same contract the declare loop honours, and for the same reason: a
            // runtime that could not be restarted has been STOPPED, and leaving it
            // registered is the one unrecoverable state — the node accepts pushes and
            // admits nothing, for ever, because every route gates on registry presence
            // and not on liveness. Unregistered it refuses pushes instead, which the
            // reconcile loop then repairs on its next pass.
            Err(f) => {
                match f.restored {
                    Some(restored) => {
                        app.registry.put(restored);
                    }
                    None => {
                        app.registry.remove(&rt.spec.application, &rt.spec.name);
                    }
                }
                tracing::error!(
                    graph = %old.spec.key(), node = %node, error = %f.error,
                    "could not restore a node after a failed declare"
                );
            }

        }
    }
    let relays = start_relays(app, &old.spec, &old.spec.node_specs());
    app.registry.put_graph(Arc::new(GraphRuntime {
        spec: old.spec.clone(),
        relays,
        node_keys: old.node_keys.clone(),
        persisted: std::sync::atomic::AtomicBool::new(old.persisted.load(Ordering::Relaxed)),
    }));
}

/// Stop the relays, stop the nodes, forget the document.
pub async fn remove(app: &Shared, application: &str, name: &str) -> Result<bool, Refusal> {
    // Not conditional on a runtime existing: a document whose provisioning keeps failing
    // is exactly the one an operator needs to delete, and it never reaches the registry.
    let registered = app.registry.graph(application, name).is_some();

    // The document first: it is what the fleet reconciles against, so a graph torn
    // down here but left in the store comes back within one interval — and the other
    // replicas never stopped it at all.
    store::forget_graph(&app.queen, application, name)
        .await
        .map_err(|e| {
            Refusal::Gateway(format!(
                "`{application}/{name}` was not deleted: the stored document could not be removed \
                 ({e}), and it would come back on the next reconcile"
            ))
        })?;
    if let Some(g) = app.registry.remove_graph(application, name) {
        // Every node's cancel first: a runner notices one between polls, so the
        // serial stops below then wait out ONE poll window between them all
        // instead of one each.
        for (_, ns) in g.spec.node_specs() {
            if let Some(rt) = app.registry.get(application, &ns.name) {
                supervisor::cancel(&rt);
            }
        }
        edge::stop_all(&g.relays).await;
        for (_, ns) in g.spec.node_specs() {
            if let Some(rt) = app.registry.remove(application, &ns.name) {
                supervisor::stop_with(&app.queen, &rt).await;
            }
        }
    }
    Ok(registered)
}



/// Stop a graph's relays and nodes without forgetting the document — what the
/// reconcile loop does before re-provisioning from a changed one.
pub async fn stop(app: &Shared, g: &Arc<GraphRuntime>) {
    // Cancel-all first — same reason as `remove`: serial stops after one
    // cancel pass cost the longest poll window, not the sum.
    for (_, ns) in g.spec.node_specs() {
        if let Some(rt) = app.registry.get(&ns.application, &ns.name) {
            supervisor::cancel(&rt);
        }
    }
    edge::stop_all(&g.relays).await;
    for (_, ns) in g.spec.node_specs() {
        if let Some(rt) = app.registry.remove(&ns.application, &ns.name) {
            supervisor::stop_with(&app.queen, &rt).await;
        }
    }
}

/// The declared graph plus what it is doing right now: depths per node, budget
/// utilisation, lane state, and the lag on every edge.
pub async fn view(app: &Shared, g: &Arc<GraphRuntime>) -> Value {
    let now = now_ms();
    let mut nodes = Vec::new();

    for (name, ns) in g.spec.node_specs() {
        let rt = app.registry.get(&ns.application, &ns.name);
        let states = rt
            .as_ref()
            .map(|r| r.last_state.read().clone())
            .unwrap_or_default();

        let budgets: Vec<Value> = ns
            .budgets
            .iter()
            .map(|b| {
                // The worst shard and the worst key: a budget is as spent as the
                // counter closest to refusing, and for a scoped budget there is no
                // single number that is not somebody's maximum.
                let used = states
                    .values()
                    .map(|s| utilisation_max(b, s, now))
                    .fold(0.0f64, f64::max);
                let keys: usize = states.values().map(|s| gate_core::key_count(b, s)).sum();
                json!({
                    "id": b.id,
                    "cap": b.cap,
                    "periodSeconds": b.period_seconds,
                    "alignment": b.alignment,
                    "scope": b.scope,
                    "store": b.store,
                    "confidence": b.confidence,
                    "match": b.matcher,
                    "maxKeys": b.max_keys,
                    "keys": keys,
                    "used": used * b.cap,
                    "utilisation": used,
                })
            })
            .collect();

        let lanes: Vec<Value> = ns
            .lanes
            .iter()
            .map(|l| {
                let stats = rt
                    .as_ref()
                    .and_then(|r| r.lanes.get(&l.name))
                    .map(|r| r.stats.read().clone())
                    .unwrap_or_default();
                json!({
                    "name": l.name,
                    "default": l.default,
                    "cap_policy": l.cap,
                    "admitted": stats.admitted,
                    "denied": stats.denied,
                    "calls": stats.calls,
                    "throttled": stats.throttled,
                    "retried": stats.retried,
                    "exhausted": stats.exhausted,
                    "last_denial_budget": stats.last_denial_budget,
                    "state": if stats.denied > 0 { "pacing" } else { "flowing" },
                })
            })
            .collect();

        // Two backlogs with two owners: gate holding work back on purpose, and the
        // caller's own consumers falling behind.
        let waiting_for_budget: u64 = app
            .depths
            .pending(&app.queen, &ns.push_queue())
            .await
            .values()
            .sum();
        let mut waiting_for_workers = 0u64;
        for l in &ns.lanes {
            waiting_for_workers += app
                .depths
                .pending(&app.queen, &ns.admitted_queue(&l.name))
                .await
                .values()
                .sum::<u64>();
        }

        nodes.push(json!({
            "name": name,
            "target": ns.name,
            "entry": g.spec.is_entry(&name),
            "consume": g.spec.is_consume(&name),
            "running": rt.is_some(),
            "shardBy": ns.shard_by,
            "shards": ns.shard_count(),
            "cost": ns.cost,
            "pacing": ns.pacing,
            "admitted": ns.admitted,
            "budgets": budgets,
            "lanes": lanes,
            "waiting_for_budget": waiting_for_budget,
            "waiting_for_workers": waiting_for_workers,
        }));
    }

    // Edge lag is what the relay has not moved yet: pending on the source's
    // admitted queues, which is the queue the relay reads.
    let mut edges = Vec::new();
    for e in &g.spec.edges {
        let mut lag = 0u64;
        if let Some(ns) = g.spec.node_spec(&e.from) {
            for l in &ns.lanes {
                lag += app
                    .depths
                    .pending(&app.queen, &ns.admitted_queue(&l.name))
                    .await
                    .values()
                    .sum::<u64>();
            }
        }
        edges.push(json!({
            "from": e.from,
            "to": e.to,
            "priority": e.priority,
            "lag": lag,
            "group": edge::group_of(&g.spec.application, &g.spec.name, &e.from, &e.to),
        }));
    }

    let relays: Vec<Value> = g
        .relays
        .iter()
        .map(|r| {
            json!({
                "dest": r.dest,
                "sources": r.sources.iter().map(|(n, p)| json!({ "node": n, "priority": p }))
                    .collect::<Vec<_>>(),
                "window": r.window,
                "forwarded": r.forwarded(),
                "unroutable": r.unroutable(),
                "duplicates": r.duplicates(),

            })
        })
        .collect();

    json!({
        "application": g.spec.application,
        "name": g.spec.name,
        "version": g.spec.version,
        "at": now,
        "spec": g.spec,
        "nodes": nodes,
        "edges": edges,
        "relays": relays,
        "consume": g.spec.consume,
        "breach": g.spec.breach,
        "persisted": g.persisted.load(Ordering::Relaxed),
        "warnings": gate_core::graph_warnings(&g.spec).iter().map(|p| p.to_string())
            .collect::<Vec<_>>(),
    })
}

/// Just the shape, for the console's diagram: no broker round trips, so it can be
/// polled at whatever rate a drawing wants.
pub fn topology(g: &GraphRuntime) -> Value {
    json!({
        "application": g.spec.application,
        "name": g.spec.name,
        "version": g.spec.version,
        "nodes": g.spec.nodes.keys().map(|n| {
            let spec = g.spec.node_spec(n).expect("declared");
            json!({
                "name": n,
                "target": spec.name,
                "entry": g.spec.is_entry(n),
                "consume": g.spec.is_consume(n),
                "budgets": spec.budgets.iter().map(|b| json!({
                    "id": b.id, "cap": b.cap, "periodSeconds": b.period_seconds,
                    "scope": b.scope, "confidence": b.confidence,
                })).collect::<Vec<_>>(),
                "shards": spec.shard_count(),
                "shardBy": spec.shard_by,
                "costMax": spec.cost.max,
            })
        }).collect::<Vec<_>>(),
        "edges": g.spec.edges,
        "consume": g.spec.consume,
        "breach": g.spec.breach,
    })
}
