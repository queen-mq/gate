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

pub async fn start(
    queen: &Queen,
    meter: Arc<crate::meter::Meter>,
    history: Option<Arc<crate::history::History>>,
    spec: TargetSpec,
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

    for lane in rt.lanes.values() {
        gate::spawn(queen, meter.clone(), rt.clone(), lane.clone()).await?;
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
