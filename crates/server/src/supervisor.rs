//! Provisioning a target: the queues, then one pinned gate per lane.
//!
//! The caller never creates a queue and never names one. That is not politeness
//! — it is what lets the topology change under a running caller, and what keeps
//! the vendors out of this codebase entirely.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use queen_mq::{Queen, QueueOptions, Result};
use gate_core::TargetSpec;

use crate::gate;
use crate::registry::{LaneRuntime, TargetRuntime};

/// Provision a target and start its runners.
///
/// `owner` is `application/graph` when this target is a graph node. It changes
/// nothing about how the target runs — a node IS a target — and everything about
/// who may reap it and where its spec is kept.
pub async fn start(
    queen: &Queen,
    meter: Arc<crate::meter::Meter>,
    history: Option<Arc<crate::history::History>>,
    spec: TargetSpec,
    owner: Option<String>,
) -> Result<Arc<TargetRuntime>> {

    // The push queue's lease is the pacing quantum AND the failover window, so
    // it is set here, once, by the only owner. Nothing else may rewrite it.
    let mut opts = QueueOptions::default();
    opts.lease_time = Some(spec.pacing.lease_seconds as i32);
    opts.retry_limit = Some(0);
    queen
        .queue(spec.push_queue())
        .namespace(spec.namespace())
        .configure(opts.clone())
        .await?;

    for lane in &spec.lanes {
        let mut o = QueueOptions::default();
        o.lease_time = Some(60);
        queen
            .queue(spec.admitted_queue(&lane.name))
            .namespace(spec.namespace())
            .configure(o)
            .await?;
    }
    queen
        .queue(spec.calls_queue())
        .namespace(spec.namespace())
        .create()
        .await
        .ok();

    let mut lanes = HashMap::new();
    for lane in &spec.lanes {
        let effective = match &lane.cap {
            gate_core::CapPolicy::Absolute(n) => Some(*n),
            gate_core::CapPolicy::Share(f) => spec
                .budgets
                .iter()
                .map(|b| b.rate_per_sec())
                .fold(None, |a: Option<f64>, r| Some(a.map_or(r, |x| x.min(r))))
                .map(|r| r * f),
            // Ceiling and ceiling-minus-measured start unbounded: the target's
            // own budgets are the only limit until the meter says otherwise.
            _ => None,
        };
        lanes.insert(
            lane.name.clone(),
            Arc::new(LaneRuntime {
                name: lane.name.clone(),
                effective_cap: RwLock::new(effective),
                measured_share: RwLock::new(None),
                stats: RwLock::new(Default::default()),
                cancel: queen_mq::Cancel::new(),
            }),
        );
    }

    let rt = Arc::new(TargetRuntime {
        spec: spec.clone(),
        graph: owner,
        persisted: std::sync::atomic::AtomicBool::new(false),
        lanes,

        last_state: RwLock::new(HashMap::new()),
        last_breach: RwLock::new(None),
        handles: RwLock::new(Vec::new()),
        meter_cancel: RwLock::new(None),
        pools: spec
            .budgets
            .iter()
            .filter(|b| b.store == gate_core::Store::Kv)
            .map(|b| {
                Arc::new(crate::shared::Pool::new(crate::shared::SharedBudget {
                    scope: spec.application.clone(),
                    id: b.id.clone(),
                    cap: b.cap as i64,
                    period_seconds: b.period_seconds,
                }))
            })
            .collect(),
    });

    // One runner per lane, or per shard of a lane. Each one pins a partition, and
    // the partition lease is what makes its counter single-writer.
    //
    // A failure part way has to STOP what is already running before it returns. A
    // stream handle does not stop its runner when it is dropped — that is what the
    // cancel token is for — so an early `?` here would leave a detached runner
    // holding a partition of a spec nobody is registered against, and the next
    // successful declare would put a second runner on that partition enforcing a
    // different document.
    for lane in rt.lanes.values() {
        for shard in 0..spec.shard_count() {
            if let Err(e) = gate::spawn(queen, meter.clone(), rt.clone(), lane.clone(), shard).await
            {
                for l in rt.lanes.values() {
                    l.cancel.cancel();
                }
                return Err(e);
            }
        }
    }

    crate::meter::spawn(queen.clone(), meter, history, rt.clone());
    Ok(rt)
}


pub async fn stop_with(queen: &Queen, rt: &Arc<TargetRuntime>) {
    for pool in &rt.pools {
        pool.release(queen, crate::now_ms()).await;
    }
    stop(rt).await;
}

/// Stop the old runtime and start a new spec in its place, keeping the target
/// serving whatever happens.
///
/// The failure this exists for: `start` fails half way (the broker refuses a
/// `configure`), and the target is left stopped but still registered — it accepts
/// pushes and admits nothing, which is unrecoverable without an operator. So the
/// old spec is restarted, and if even that fails the caller is told to unregister:
/// a target that refuses pushes is recoverable, a queue nobody drains is not.
pub async fn swap(
    queen: &Queen,
    meter: Arc<crate::meter::Meter>,
    history: Option<Arc<crate::history::History>>,
    old: Option<&Arc<TargetRuntime>>,
    spec: TargetSpec,
    owner: Option<String>,
) -> std::result::Result<Arc<TargetRuntime>, SwapFailed> {
    if let Some(old) = old {
        stop_with(queen, old).await;
    }
    match start(queen, meter.clone(), history.clone(), spec, owner.clone()).await {
        Ok(rt) => Ok(rt),
        Err(e) => {
            let restored = match old {
                Some(old) => start(
                    queen,
                    meter,
                    history,
                    old.spec.clone(),
                    old.graph.clone(),
                )
                .await
                .ok()
                .inspect(|rt| {

                    // The old spec's place in the store did not change because a new
                    // one failed to start. Carrying the flag over is what stops the
                    // reconcile loop reading this runtime as "declared here, never
                    // persisted" and re-saving a target another replica has deleted.
                    rt.persisted.store(
                        old.persisted.load(std::sync::atomic::Ordering::Relaxed),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }),

                None => None,
            };

            Err(SwapFailed { error: e.to_string(), restored })
        }
    }
}

/// A provisioning failure, and whatever is serving now.
pub struct SwapFailed {
    pub error: String,
    /// The old runtime, restarted. `None` means nothing is serving this target and
    /// the caller must unregister it.
    pub restored: Option<Arc<TargetRuntime>>,
}

pub async fn stop(rt: &Arc<TargetRuntime>) {
    for lane in rt.lanes.values() {
        lane.cancel.cancel();
    }

    if let Some(c) = rt.meter_cancel.read().as_ref() {
        c.cancel();
    }
    // Give the runners a cycle to notice; they hold a lease each, and leaving
    // one held only delays the next holder by its own duration.
    tokio::time::sleep(std::time::Duration::from_millis(
        (rt.spec.pacing.lease_seconds as u64 * 1000).min(2_000),
    ))
    .await;
}
