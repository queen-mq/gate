//! Provisioning a graph: the queues Gate owns, then one consumer per stage.
//!
//! The caller never creates a queue Gate owns and never names one. That is not
//! politeness — it is what lets the topology change under a running caller. The
//! exception is deliberate and is the most important operational change in v2: a
//! node may name a queue the APPLICATION owns, and Gate then consumes it and
//! never configures it. Producers push with their normal SDK, so Gate can be
//! down without blocking ingest.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use queen_mq::{Cancel, Queen, QueueOptions, Result};

use gate_core::plan::{Plan, QueueKind};
use gate_core::GraphDoc;

use crate::budget::Budgets;
use crate::knobs::knobs;
use crate::obs::Traces;
use crate::registry::GraphRuntime;
use crate::relay::{self, StageRuntime};

/// Provision a graph and start its stages.
pub async fn start(
    queen: &Queen,
    budgets: &Budgets,
    traces: Arc<Traces>,
    doc: GraphDoc,
    plan: Plan,
) -> Result<Arc<GraphRuntime>> {
    // BEFORE the provisioning and before anything is spawned, and that ordering
    // is the whole point. A stage whose source is a Gate-owned interior queue
    // seeds a new group at this instant (`relay::interior_seed`), so it has to
    // be taken before any stage of this runtime can possibly have relayed a
    // frame into one — otherwise the frame would land below the cursor that is
    // about to be created, and be dropped.
    let started_at = std::time::SystemTime::now();

    provision(queen, &plan).await?;

    let cancel = Cancel::new();
    let mut stages = Vec::with_capacity(plan.stages.len());
    for s in &plan.stages {
        let Some(node) = plan.nodes.get(&s.node) else {
            continue;
        };
        stages.push(Arc::new(StageRuntime {
            application: doc.application.clone(),
            graph: doc.graph.clone(),
            stage: s.clone(),
            node: node.clone(),
            counters: Default::default(),
            last_refusal: parking_lot::RwLock::new(None),
            started_at,
            wedge: parking_lot::RwLock::new(None),
            cancel: cancel.clone(),
        }));
    }

    let stopped = Arc::new(AtomicBool::new(false));
    let rt = Arc::new(GraphRuntime {
        doc,
        plan,
        stages,
        handles: parking_lot::RwLock::new(Vec::new()),
        persisted: AtomicBool::new(false),
        stopped: stopped.clone(),
        cancel,
    });

    // Spawning cannot fail here — `consume_batch` reports a broker refusal from
    // inside the task, not from `spawn` — so there is no half-provisioned state
    // to unwind. The failure that DOES need unwinding is `provision` above, and
    // it happens before anything is running.
    let mut handles = Vec::with_capacity(rt.stages.len());
    for st in &rt.stages {
        handles.push(relay::spawn(
            queen.clone(),
            budgets.clone(),
            st.clone(),
            traces.clone(),
            stopped.clone(),
        ));
    }
    *rt.handles.write() = handles;

    tracing::info!(
        graph = %rt.key(), stages = rt.stages.len(), queues = rt.plan.queues.len(),
        "graph running"
    );
    Ok(rt)
}

/// The queues Gate owns, and only those.
async fn provision(queen: &Queen, plan: &Plan) -> Result<()> {
    let k = knobs();
    for q in &plan.queues {
        match q.kind {
            // A WORK lease, renewed while a handler runs — not a pacing quantum.
            // And `retry_limit` is a real number again: v1 had to set it to zero
            // because it paced by nacking and could not tell waiting from
            // failing. v2 paces by RELEASING, and queen charges no retry budget
            // on lease expiry, so the DLQ is back and means what it says.
            QueueKind::OwnedIngress | QueueKind::Interior => {
                let opts = QueueOptions {
                    lease_time: Some(k.lease_seconds),
                    retry_limit: Some(k.retry_limit),
                    ..Default::default()
                };
                queen
                    .queue(&q.name)
                    .namespace(&plan.namespace)
                    .configure(opts)
                    .await?;
            }
            // The application's queue. Created so its consumers can subscribe
            // before Gate has pushed anything — a group that finds no queue is a
            // 404 an application has to code around — and never configured: its
            // retention, its lease and its partition count belong to whoever
            // made it.
            QueueKind::Egress => {
                queen.queue(&q.name).create().await.ok();
            }
            // Consumed, never created. If it does not exist yet, Gate finds it
            // on its first message; declare-time validation says so as a
            // warning.
            QueueKind::UserIngress => {}
        }
    }
    Ok(())
}

/// What the broker knows about the queues a declaration names.
///
/// Read at declare time only — nothing in the hot path reads a queue's shape.
/// A broker that will not answer costs a caller some advice and never a
/// refusal, which is why every rule that reads this is a warning.
pub async fn probe(queen: &Queen, plan: &Plan) -> BTreeMap<String, gate_core::QueueFacts> {
    let mut out = BTreeMap::new();
    for q in &plan.queues {
        if q.kind != QueueKind::UserIngress {
            continue;
        }
        match queen.admin().queue(&q.name).await {
            Ok(v) => {
                let partitions = v
                    .get("partitions")
                    .and_then(|p| p.as_array())
                    .map(|a| a.len() as u32)
                    .unwrap_or(0);
                let retention = v
                    .get("queue")
                    .and_then(|q| q.get("retentionSeconds"))
                    .or_else(|| v.get("retentionSeconds"))
                    .and_then(|r| r.as_i64())
                    .filter(|r| *r > 0)
                    .map(|r| format!("{r} seconds"));
                out.insert(
                    q.name.clone(),
                    gate_core::QueueFacts {
                        exists: true,
                        partitions,
                        retention,
                    },
                );
            }
            // A 404 is BOTH "no such queue" and "this broker does not route the
            // detail"; either way there is nothing to report and nothing to
            // refuse.
            Err(_) => {
                out.insert(
                    q.name.clone(),
                    gate_core::QueueFacts {
                        exists: false,
                        partitions: 0,
                        retention: None,
                    },
                );
            }
        }
    }
    out
}

/// Fire every cancel a runtime owns, without waiting for anything.
///
/// Split out of [`stop`] so a caller tearing down MANY graphs can cancel them
/// all first and only then await: a stage notices its cancel between polls, so N
/// stops after one cancel-all pass cost the longest single poll window and not
/// the sum of N of them.
pub fn cancel(rt: &Arc<GraphRuntime>) {
    // Before the cancels, so nothing can observe a runtime whose stages are
    // going away and still believe it is serving.
    rt.stopped.store(true, Ordering::Relaxed);
    rt.cancel.cancel();
    for st in &rt.stages {
        st.cancel.cancel();
    }
}

pub async fn stop(rt: &Arc<GraphRuntime>) {
    cancel(rt);
    // Await the tasks themselves rather than sleeping a guess. v1's fixed 600ms
    // was sized for a 250ms poll window; the moment the window grew it stopped
    // covering it, and a swap could start the NEW consumer while the old one was
    // still parked. A stage notices its cancel between polls, so each await here
    // is bounded by one poll window; the timeout is a wedge guard for a
    // black-holed poll, and on expiry the task is merely detached — it still
    // exits at its next check.
    let handles: Vec<_> = std::mem::take(&mut *rt.handles.write());
    let budget = knobs().poll_timeout + std::time::Duration::from_secs(2);
    for h in handles {
        let _ = tokio::time::timeout(budget, h).await;
    }
}

/// Stop the old runtime and start a new document in its place, keeping the graph
/// serving whatever happens.
///
/// The failure this exists for: `start` fails half way (the broker refuses a
/// `configure`) and the graph is left stopped but still registered — it accepts
/// pushes and admits nothing, which is unrecoverable without an operator. So the
/// old document is restarted, and if even that fails the caller is told to
/// unregister: a graph that refuses pushes is recoverable, a queue nobody drains
/// is not.
pub async fn swap(
    queen: &Queen,
    budgets: &Budgets,
    traces: Arc<Traces>,
    old: Option<&Arc<GraphRuntime>>,
    doc: GraphDoc,
    plan: Plan,
) -> std::result::Result<Arc<GraphRuntime>, SwapFailed> {
    if let Some(old) = old {
        stop(old).await;
    }
    match start(queen, budgets, traces.clone(), doc, plan).await {
        Ok(rt) => Ok(rt),
        Err(e) => {
            let restored = match old {
                Some(old) => start(queen, budgets, traces, old.doc.clone(), old.plan.clone())
                    .await
                    .ok()
                    .inspect(|rt| {
                        // The old document's place in the store did not change
                        // because a new one failed to start. Carrying the flag over
                        // is what stops the reconcile loop reading this runtime as
                        // "declared here, never persisted" and re-saving a graph
                        // another replica has deleted.
                        rt.persisted
                            .store(old.persisted.load(Ordering::Relaxed), Ordering::Relaxed);
                    }),
                None => None,
            };
            Err(SwapFailed {
                error: e.to_string(),
                restored,
            })
        }
    }
}

/// A provisioning failure, and whatever is serving now.
pub struct SwapFailed {
    pub error: String,
    /// The old runtime, restarted. `None` means nothing is serving this graph
    /// and the caller must unregister it.
    pub restored: Option<Arc<GraphRuntime>>,
}
