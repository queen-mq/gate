//! Edges: the relays that make a declared graph an actual one.
//!
//! An edge moves one item from a node's admitted queue to the next node's push
//! queue, and it does it in **one transaction** — the ack of the upstream message
//! and the push of the downstream one commit together or not at all. That is not
//! an optimisation. Two calls have two failure orders and both are wrong: ack
//! then push loses the item, push then ack duplicates it. The transaction also
//! carries the message's lease as a precondition, so a lease that expired while
//! the relay was working rolls the whole thing back instead of forwarding work
//! somebody else has already re-claimed.
//!
//! The transaction id is carried over from the upstream message rather than minted,
//! so a relay cannot forward the same item twice: the second push is refused by the
//! broker's dedup on `(queue, txn)`. Note what that refusal IS inside a
//! transaction — a hard error that rolls the bundle back, not the soft "duplicate"
//! verdict a plain push gets — so it is caught and settled item by item rather than
//! retried into a stall.

//!
//! # One relay per destination, not per edge
//!
//! Priority is only real where the streams meet. Two independent relays into one
//! push queue would each forward as fast as they could, and the destination's FIFO
//! would then order by arrival — which is precisely what a priority is meant to
//! override. So the tasks that feed a node are one task, draining its upstreams in
//! strict priority order, and it forwards only while the destination's push queue
//! is shallower than a `window`:
//!
//! * the bottleneck queue stays short, so priority at the entrance is priority in
//!   fact — a price push overtakes everything not yet forwarded;
//! * the destination's FIFO and its single counter stay exactly as they were, so
//!   nothing about the admission argument changes.
//!
//! # What a second replica changes, and what it does not
//!
//! Every replica runs this task, and they share the consumer group — so a message
//! is claimed by exactly one of them and nothing is forwarded twice. What they do
//! not share is the window: each reads the destination's depth and forwards
//! against it independently, so with `n` replicas the queue can transiently reach
//! `n` windows and a priority-0 item competes with whatever the other replicas
//! forwarded in the same instant. Both effects are bounded by the window and cost
//! sharpness, not correctness: the destination's own gate is still one writer per
//! partition, and its ceiling is unaffected.


use std::sync::atomic::Ordering;
use std::sync::Arc;

use gate_core::TargetSpec;
use queen_mq::{Queen, SubscriptionMode, TxnPushItem};
use serde_json::Value;

use crate::depth::Depths;
use crate::registry::RelayRuntime;

/// Everything one merge relay needs, resolved once at declare time so the loop
/// never touches the registry.
pub struct Plan {
    pub application: String,
    pub graph: String,
    pub dest_node: String,
    pub dest: TargetSpec,
    /// Lowest priority first — the order it drains in.
    pub sources: Vec<Leg>,
}

pub struct Leg {
    pub node: String,
    pub priority: u32,
    pub spec: TargetSpec,
}

/// How deep the destination's push queue may get before the relay stops feeding
/// it.
///
/// Two lease-windows of the node's most generous budget. The MOST generous, not
/// the tightest: what a node can admit inside one lease is governed by whichever
/// budget binds at that instant, and a wide window with a big cap has headroom to
/// spare — sizing on the daily average would starve the gate and make the relay
/// the limiter. Two windows so there is always a lease's worth of work in front of
/// the gate while the relay is between loops.
pub fn window_for(dest: &TargetSpec) -> u64 {
    // UNSCOPED budgets only. A scoped budget's rate is per key — a hundred photo
    // deletions per listing per week is not "one item per six thousand seconds of
    // node throughput" — and sizing the window on it collapses the window to one
    // item, which stalls the relay: the depth it compares against is the whole
    // node's, so a single item waiting anywhere stops everything.
    let per_sec = dest
        .budgets
        .iter()
        .filter(|b| b.store == gate_core::Store::Gate && b.scope.is_empty())
        .map(|b| b.rate_per_sec())
        .fold(0.0f64, f64::max);
    if per_sec <= 0.0 {
        // Nothing here bounds the node's total rate — it has no budget at all, or
        // only per-key ones. The relay is then not the pacer and must not pretend to
        // be: a cycle's worth of work per gate runner, so every shard can be fed.
        return (dest.pacing.batch.max(1) as u64).saturating_mul(dest.shard_count() as u64);
    }
    ((2.0 * per_sec * dest.pacing.lease_seconds.max(1) as f64).ceil() as u64).max(1)
}


/// The consumer group one leg reads under. Named for the edge, so two edges out of
/// one node each get the whole stream and an edge is never split with anybody.
pub fn group_of(application: &str, graph: &str, from: &str, to: &str) -> String {
    format!("gate.edge.{application}.{graph}.{from}.{to}")
}

/// Long enough that a commit is never racing its own lease, short enough that a
/// failed commit costs seconds rather than the admitted queue's whole minute.
const RELAY_LEASE_SECONDS: i32 = 15;

/// The largest transaction the relay will build. Bodies stay modest and a single
/// rollback costs at most this much re-work.
const MAX_FORWARD: u64 = 200;

pub fn spawn(queen: Queen, depths: Arc<Depths>, plan: Plan) -> Arc<RelayRuntime> {
    let window = window_for(&plan.dest);
    let relay = Arc::new(RelayRuntime {
        dest: plan.dest_node.clone(),
        sources: plan
            .sources
            .iter()
            .map(|l| (l.node.clone(), l.priority))
            .collect(),
        window,
        forwarded: Default::default(),
        unroutable: Default::default(),
        cancel: queen_mq::Cancel::new(),
    });

    let task = relay.clone();
    tokio::spawn(async move {
        // A quarter of the destination's pacing quantum: fast enough that the
        // window is refilled before the gate runs dry, slow enough that an idle
        // graph is not a busy loop.
        let pace = std::time::Duration::from_millis(
            ((plan.dest.pacing.lease_seconds.max(1) as u64 * 1000) / 4).clamp(50, 500),
        );
        let dest_lane = plan
            .dest
            .default_lane()
            .map(|l| l.name.clone())
            .unwrap_or_else(|| "default".to_string());
        let dest_push = plan.dest.push_queue();

        while !task.cancel.is_cancelled() {
            // A depth the broker would not report is NOT a depth of zero. Reading it
            // as one would forward a full window every loop for as long as the admin
            // API was unhappy, and the queue this exists to keep shallow would grow
            // without a bound.
            let Some(depth) = depths.try_pending_now(&queen, &dest_push).await else {
                tracing::debug!(
                    dest = %plan.dest_node,
                    "relay: the destination's depth is unknown; holding until it answers"
                );
                tokio::time::sleep(pace).await;
                continue;
            };
            let pending: u64 = depth.values().sum();
            let mut allowance = window.saturating_sub(pending);

            if allowance == 0 {
                tokio::time::sleep(pace).await;
                continue;
            }

            let mut moved = 0u64;
            // Strict priority: a lower number is drained to exhaustion — or to the
            // window's edge — before a higher one is looked at at all.
            for leg in &plan.sources {
                if allowance == 0 || task.cancel.is_cancelled() {
                    break;
                }
                for lane in &leg.spec.lanes {
                    if allowance == 0 {
                        break;
                    }
                    let take = allowance.min(MAX_FORWARD) as i32;
                    let msgs = match queen
                        .queue(leg.spec.admitted_queue(&lane.name))
                        .group(group_of(
                            &plan.application,
                            &plan.graph,
                            &leg.node,
                            &plan.dest_node,
                        ))
                        // `all`, always: a group created at the tail would skip
                        // every message already waiting, which for a relay means
                        // silently abandoning the backlog it exists to move.
                        .subscription_mode(SubscriptionMode::All)
                        .batch(take.max(1))
                        .partitions(leg.spec.admitted.partitions.max(1) as i32)
                        .lease_seconds(RELAY_LEASE_SECONDS)
                        // No long poll: a wait on an empty high-priority leg would
                        // hold the low-priority ones behind it for the whole
                        // timeout, which is head-of-line blocking dressed as
                        // priority.
                        .wait(false)
                        .poll_timeout(std::time::Duration::from_millis(2_000))
                        .pop()
                        .await
                    {
                        Ok(m) => m,
                        Err(e) => {
                            tracing::debug!(
                                edge = %format!("{} -> {}", leg.node, plan.dest_node),
                                error = %e,
                                "relay could not claim"
                            );
                            continue;
                        }
                    };
                    if msgs.is_empty() {
                        continue;
                    }

                    let mut tx = queen.transaction();
                    let mut forwarded = 0u64;
                    let mut unroutable = 0u64;
                    let mut staging_failed = false;

                    for m in &msgs {
                        match partition_for(&plan.dest, &dest_lane, &m.data) {
                            Some(partition) => {
                                tx = tx.ack(m);
                                match tx.push_item(TxnPushItem {
                                    queue: dest_push.clone(),
                                    partition: Some(partition),
                                    payload: m.data.clone(),
                                    // The upstream item's own id: a replayed relay
                                    // collapses instead of duplicating.
                                    transaction_id: Some(m.transaction_id.clone()),
                                    trace_id: None,
                                }) {
                                    Ok(next) => {
                                        tx = next;
                                        forwarded += 1;
                                    }
                                    // Nothing this loop passes can be refused here —
                                    // the only rejection is a malformed trace id and
                                    // there is none. Kept anyway, and kept as a
                                    // dropped BATCH rather than a returned task: a
                                    // relay that exits on an unexpected error stops
                                    // the graph for ever, while abandoning a batch
                                    // costs one lease.
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            "relay could not stage a push; the batch will be redelivered"
                                        );
                                        // A fresh, empty builder: `push_item` took
                                        // the old one and the batch is being
                                        // abandoned anyway.
                                        tx = queen.transaction();
                                        staging_failed = true;
                                        break;

                                    }

                                }
                            }
                            // The destination shards on a dimension this item does
                            // not carry, so there is no partition it could go to and
                            // no shard whose budget it could ever satisfy. Dead-
                            // lettered with the reason rather than dropped, in the
                            // same transaction as everything else.
                            None => {
                                tx = tx.nack(
                                    m,
                                    format!(
                                        "gate: `{}` shards by `{}` and this item carries none",
                                        plan.dest_node,
                                        plan.dest
                                            .shard_by
                                            .map(|d| d.as_str())
                                            .unwrap_or("?")
                                    ),
                                );
                                unroutable += 1;
                            }
                        }
                    }

                    if staging_failed {
                        // A half-built transaction would settle some messages and
                        // forward others; dropping it settles nothing, and the
                        // leases lapse in seconds.
                        continue;
                    }
                    match tx.commit().await {

                        Ok(_) => {
                            task.forwarded.fetch_add(forwarded, Ordering::Relaxed);
                            task.unroutable.fetch_add(unroutable, Ordering::Relaxed);
                            allowance = allowance.saturating_sub(forwarded);
                            moved += forwarded + unroutable;
                        }
                        // An item this relay has already forwarded, somehow: a
                        // duplicate transaction id is a soft verdict for a plain push
                        // but a HARD one inside a transaction, and it takes the whole
                        // batch down with it (queen `005_log_ack.sql`). Left alone
                        // that is a leg stalled for ever — the batch comes back, the
                        // same push is refused, nothing is ever settled. So the batch
                        // is retried one item at a time, and the one that is already
                        // downstream is simply acked.
                        Err(e) if e.to_string().contains("QDUP") => {
                            tracing::warn!(
                                edge = %format!("{} -> {}", leg.node, plan.dest_node),
                                "relay: an item was already forwarded; settling the batch one at a time"
                            );
                            for m in &msgs {
                                let Some(partition) =
                                    partition_for(&plan.dest, &dest_lane, &m.data)
                                else {
                                    continue;
                                };
                                let one = queen.transaction().ack(m).push_item(TxnPushItem {
                                    queue: dest_push.clone(),
                                    partition: Some(partition),
                                    payload: m.data.clone(),
                                    transaction_id: Some(m.transaction_id.clone()),
                                    trace_id: None,
                                });
                                let settled = match one {
                                    Ok(tx) => match tx.commit().await {
                                        Ok(_) => {
                                            task.forwarded.fetch_add(1, Ordering::Relaxed);
                                            true
                                        }
                                        // Already downstream: settle it and move on.
                                        Err(e) if e.to_string().contains("QDUP") => queen
                                            .transaction()
                                            .ack(m)
                                            .commit()
                                            .await
                                            .is_ok(),
                                        Err(_) => false,
                                    },
                                    Err(_) => false,
                                };
                                if settled {
                                    allowance = allowance.saturating_sub(1);
                                    moved += 1;
                                }
                            }
                        }
                        // Nothing was settled and nothing was pushed. The lease
                        // expires in seconds and the batch comes back.
                        Err(e) => tracing::warn!(
                            edge = %format!("{} -> {}", leg.node, plan.dest_node),
                            error = %e,
                            "relay transaction did not commit; the batch will be redelivered"
                        ),

                    }
                }
            }

            if moved == 0 {
                tokio::time::sleep(pace).await;
            }
        }
    });

    relay
}

/// Which push partition an item goes to at the destination.
///
/// `None` when the destination is sharded by a dimension the item does not carry:
/// there is no shard whose counter that item belongs to, and picking one would put
/// a key in two shards the next time it arrives with the field set.
fn partition_for(dest: &TargetSpec, lane: &str, payload: &Value) -> Option<String> {
    match dest.shard_by {
        None => Some(lane.to_string()),
        Some(dim) => {
            let value = payload.get(dim.as_str()).and_then(|v| v.as_str())?;
            Some(dest.push_partition(lane, dest.shard_of(value)))
        }
    }
}

/// Stop every relay of a graph and give the loops a moment to notice. Their leases
/// are seconds long, so a held one delays the next holder by that and nothing else.
pub async fn stop_all(relays: &[Arc<RelayRuntime>]) {
    for r in relays {
        r.cancel.cancel();
    }
    if !relays.is_empty() {
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    }
}
