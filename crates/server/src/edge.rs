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
//! override. So the tasks that feed a node are one relay, draining its upstreams in
//! strict priority order, and it forwards only while the destination's push queue
//! is shallower than a `window`:
//!
//! * the bottleneck queue stays short, so priority at the entrance is priority in
//!   fact — a price push overtakes everything not yet forwarded;
//! * the destination's FIFO and its single counter stay exactly as they were, so
//!   nothing about the admission argument changes.
//!
//! # Inside one leg: one runner per source partition
//!
//! One relay per destination is not one TASK per destination, and that is the whole
//! of this change. A leg — one in-edge, one lane of its source — is drained by one
//! runner per partition of that lane's admitted queue, several at a time, each
//! popping ONLY its own partition.
//!
//! It is the argument the admission gates already make, carried over unchanged
//! (`gate.rs`, `supervisor.rs`): the partition is what makes a claimer the only
//! claimer, so parallelism is expressed as more partitions rather than as more
//! runners on the same one. Here it buys three things at once:
//!
//! * **ordering survives.** Items are partitioned by the declared `partitionBy`, so
//!   a connection's work lives in exactly one source partition; that partition has
//!   exactly one runner, and one runner pops it in order and pushes it in order.
//!   The pin is a field of the runner, fixed when it is built — never an argument,
//!   never a loop variable — because "no two runners share a partition" has to be
//!   a property of the shape and not of the care taken by the next person to edit
//!   this file.
//! * **the transaction stays narrow.** See below; this is where the throughput was.
//! * **a failure is local.** A batch that will not commit stops one partition for
//!   the rest of the cycle. It used to stop the leg.
//!
//! "Several at a time" and not "all at once": a leg's partitions are worked through
//! by at most `MAX_IN_FLIGHT` runners. The bound is there because concurrency past it
//! buys nothing and costs something real — the measurements are on the constant.
//!
//! And not all of the partitions either — only the ones that hold something. A cycle
//! begins by asking the broker, in one watermark read per queue, which partitions
//! this relay's consumer group still owes work in; the runners are given those. It is
//! cheaper than polling a quiet ring sixty-four times to be told nothing, but the
//! reason it is there is that the window can be SMALLER than the ring — a node at 5/s
//! has a window of ten against sixty-four partitions — and a runner that reaches an
//! exhausted window never polls at all. Sweeping blindly, one item waiting in p35 was
//! reached about once in sixty cycles, which on an idle graph is minutes. Asked
//! directly, it is polled and forwarded in the cycle it arrived in.
//!
//! Three things keep that from being one number the whole edge hangs on. The answer
//! has to be the GROUP's own backlog — a queue-level pending is a different question
//! and on one broker version it answered zero for a queue holding thirty items — so a
//! broker that cannot answer THAT question is treated as not having answered at all,
//! and every partition is polled: silence is not emptiness. The list rotates, and by
//! as many partitions as the cycle actually reached, so a window too small to cover
//! the ring sweeps it rather than serving the same few. And one cycle in
//! `FULL_SWEEP_EVERY` polls everything regardless, so no depth answer can blind this
//! relay for longer than that.
//!
//! Nothing decides how many runners there are: it is the source's
//! `admitted.partitions`, one each. There is deliberately no knob for it. More
//! runners than partitions is two claimers on one partition, which is the ordering
//! guarantee gone; fewer is a number the caller can already express by declaring
//! fewer partitions. A declare-time warning says so when a source has one
//! partition and therefore one runner (`relay-parallelism`).
//!
//! One consequence, and it is why changing that number is migration-class: the
//! runners are built from the partitions the DOCUMENT names, so a ring that gets
//! narrower leaves whatever is still sitting in the partitions it dropped with
//! nobody to claim it. That is the same thing narrowing `shards` does to a push
//! queue and it gets the same answer — `needs_version_bump`, so the caller says the
//! change was meant. The stranded items stay visible as lag on the edge, and
//! widening the ring again picks them up.
//!
//! # Why the transaction is one source partition wide, and only one
//!
//! Measured on the broker, in this exact relay shape (`benchmark-queen/
//! 2026-08-20-gate`, loader `txnload/`): a transaction takes one row lock per
//! partition it touches, in canonical order. A relay that pops a batch spanning
//! all 16 partitions of its source and acks them in one transaction therefore
//! serialises against every other transaction on that queue — 64 workers, ONE
//! effective lane, 33 transactions a second with the machine 95% idle. The same
//! workers on disjoint partitions: 603 transactions a second, 23,000 items a
//! second. Gate's own measured ceiling before this change was 1,384 items/s per
//! hop, next to txnload's 1,654 for the fan-out shape — the same number, because
//! it was the same mistake.
//!
//! So: **one source partition per transaction**, which the pin gives for free, and
//! the downstream pushes grouped by destination partition with one transaction per
//! group. A batch that fans out to several destination partitions becomes several
//! narrow transactions rather than one wide one — more round trips, no convoy.
//! (Today a relay destination is never sharded — `shard-entry` refuses it, because
//! a relay cannot choose a shard for an item that does not carry the dimension —
//! so the grouping is a single group in practice. It is written to survive that
//! rule changing, not because the branch is hot.)
//!
//! What is left serialised is the destination's push partition: every runner
//! commits a push into it, so they queue on that one row lock. That is structural,
//! not an oversight — the destination's push partition IS its counter, one gate
//! runner and one state document, and spreading it would enforce the node's cap
//! once per partition. It is also where the edge's ceiling now is, measured rather
//! than assumed. Sixteen relay-shaped workers on disjoint source partitions, against
//! a 1.0.5 broker:
//!
//! | each pushing into | txn/s | items/s | txn p50 |
//! |---|---|---|---|
//! | its OWN destination partition | 126 | 12,346 | 100 ms |
//! | one shared destination partition | 16 | 2,953 | 1,017 ms |
//!
//! Four times the throughput and a tenth of the latency, for the same work — the
//! second row is what queueing on a row lock looks like, and it is the shape a
//! destination node has. This relay measures at 2,516-2,579 items/s in that shape,
//! which is 85% of what a loader with no other job gets from the same broker. The
//! relay is no longer what is in the way.
//!
//! # What the sharding was worth
//!
//! One backlog of ~300,000 items, drained by nothing but the relay, on a ten-core
//! laptop against a **1.0.5** broker (the decay across a run is the broker's log_txns
//! accumulation, which is why both a first- and a last-window rate are given):
//!
//! | source partitions | items/s mean | first 10s | last 10s |
//! |---|---|---|---|
//! | one loop across all of them (before) | 2,064 | 2,893 | 1,311 |
//! | 1 | 2,269 | 3,639 | 1,474 |
//! | 4 | 2,552 | 4,816 | 1,443 |
//! | 16 | 2,516 | 4,949 | 685 |
//! | 64 | 2,579 | 4,765 | 1,348 |
//!
//! The control is the second row: one partition is one runner, and one runner is not
//! faster than the loop it replaced. The gain is the partitions — 1.25x on the mean,
//! 1.7x on a fresh broker's first ten seconds — and it flattens at FOUR, where the
//! destination's single push partition takes over.
//!
//! Say the honest thing about that 1.25x: most of what this was meant to unblock was
//! not this relay at all. The same sweep against a 1.0.4 broker — the one whose
//! transaction wire re-scanned a partition's whole ack history per ack GROUP, and a
//! fan-out relay ack touches sixty-four of them — put the old loop at 981/s and this
//! one at 2,776/s, a 2.8x. 1.0.5 removed that scan, and with it most of the penalty
//! the old shape was paying. What is left is a relay that no longer serialises
//! against itself and no longer has one leg's failure stop the leg, running at the
//! broker's ceiling for the shape it has.
//!
//! # Priority is across legs; parallelism is inside one
//!
//! The legs are drained exactly as they were: lowest priority number first, that
//! leg taken to exhaustion or to the window's edge before the next is looked at,
//! ties in declared order. The parallelism goes INSIDE a leg — across the
//! partitions of one source — and never across two, because two legs running at
//! once is precisely the "each forwards as fast as it can" the single relay exists
//! to prevent: the destination's FIFO would order the merge by arrival and the
//! priority would mean nothing.
//!
//! Concretely, one leg at a time means the runners of a leg all finish before the
//! next leg's are started, so a priority-1 leg cannot take window from a
//! priority-0 leg that still has work. The cost is a barrier per leg per cycle —
//! the slowest partition holds up the next leg — and that cost is what strict
//! priority is.
//!
//! A leg hands the rest of the window on only when it is DRY, and dry means the
//! broker said so — an empty pop, or a depth read that named no partition holding
//! anything. A pop that errored and a transaction that would not commit are not dry,
//! they are unknown, and the difference is not academic: with both legs at 64 partitions
//! the pops that timed out read as "nothing here", and 188 of the first 300 items
//! forwarded came from the priority-1 leg while priority-0 held a backlog of 172. So
//! a leg that hit anything other than an empty partition keeps the remaining window,
//! nothing else forwards this cycle, and the next cycle asks the same leg again —
//! for `STALL_TOLERANCE` cycles, after which a leg that has never drained is treated
//! as broken rather than busy and the window goes on down the legs.
//!
//! # What a second replica changes, and what it does not
//!
//! Every replica runs this relay, and they share the consumer group — so a
//! partition is claimed by exactly one runner in the fleet at a time and nothing is
//! forwarded twice or out of order. What they do not share is the window: each
//! reads the destination's depth and forwards against it independently, so with `n`
//! replicas the queue can transiently reach `n` windows and a priority-0 item
//! competes with whatever the other replicas forwarded in the same instant. Both
//! effects are bounded by the window and cost sharpness, not correctness: the
//! destination's own gate is still one writer per partition, and its ceiling is
//! unaffected.
//!
//! Within ONE replica the window is not multiplied by the runner count, which it
//! would be if each runner read the depth for itself: the depth is probed once per
//! cycle and the allowance it yields is a single pool the runners claim from.


use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use gate_core::TargetSpec;
use queen_mq::{Message, Queen, SubscriptionMode, TxnPushItem};
use serde_json::Value;

use crate::depth::Depths;
use crate::registry::{RelayRuntime, RelaySource};

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
    // UNSCOPED budgets, whichever store holds them.
    //
    // Unscoped, because a scoped budget's rate is per key — a hundred photo deletions per
    // listing per week is not "one item per six thousand seconds of node throughput" — and
    // sizing the window on it collapses the window to one item, which stalls the relay:
    // the depth it compares against is the whole node's, so one item waiting anywhere
    // would stop everything.
    //
    // Either store, because a `store: kv` ceiling bounds this node's total rate exactly as
    // a gate-held one does; it is merely enforced from a local lease instead of from the
    // state document. Excluding it sent a node whose only total-rate bound is a shared
    // ceiling down the "nothing paces this" path and let its queue run deep, which is the
    // shallow-window property that makes priority real.
    let per_sec = dest
        .budgets
        .iter()
        .filter(|b| b.scope.is_empty())
        .map(|b| b.rate_per_sec())
        .fold(0.0f64, f64::max);
    if per_sec <= 0.0 {
        // Nothing here bounds the node's total rate — it has no budget at all, or only
        // per-key ones. The relay is then not the pacer and must not pretend to be: a
        // cycle's worth of work per gate runner, so every shard can be fed.
        return (dest.pacing.batch.max(1) as u64).saturating_mul(dest.shard_count() as u64);
    }

    ((2.0 * per_sec * dest.pacing.lease_seconds.max(1) as f64).ceil() as u64).max(1)
}


/// The consumer group one leg reads under. Named for the edge, so two edges out of
/// one node each get the whole stream and an edge is never split with anybody.
///
/// One group for the whole leg, not one per partition: a consumer group keeps a
/// cursor PER partition already, so pinned runners under one group are exactly as
/// independent as separate groups would make them — and the ETA, which asks the
/// broker what this group still owes, keeps working because there is still one
/// name to ask about.
pub fn group_of(application: &str, graph: &str, from: &str, to: &str) -> String {
    format!("gate.edge.{application}.{graph}.{from}.{to}")
}

/// Long enough that a commit is never racing its own lease, short enough that a
/// failed commit costs seconds rather than the admitted queue's whole minute.
const RELAY_LEASE_SECONDS: i32 = 15;

/// The largest transaction the relay will build. Bodies stay modest and a single
/// rollback costs at most this much re-work.
///
/// Left where it was after the sharding, and left there deliberately rather than by
/// omission. With the destination's push partition serialising the commits, a bigger
/// body looks like free throughput, and against a 1.0.4 broker it was not — 1,000
/// items ran 2,893 items/s against 2,621, a fifth of the gain for five times the
/// re-work on a rollback.
///
/// Against 1.0.5 the same experiment says something different and unfinished: 9,000
/// items/s in the first ten seconds against 4,949, nearly double — and the
/// destination's push queue then sat AT its window with the tail collapsing to
/// 406/s, which says the relay had stopped being the constraint and the node's own
/// gate had become one. That is a real lever and it is not a laptop's to pull: it
/// wants the bench VM, and it wants the destination side measured at the same time.
const MAX_FORWARD: u64 = 200;

/// How many of a leg's runners may be working at one instant.
///
/// The runner count is the ring width and the ring can be 256 wide, but the number
/// of them that should be TALKING to the broker at once is a different question and
/// it has a different answer. Two measurements set it:
///
/// * more concurrency than this buys nothing. Draining one backlog with 4, 16 and
///   64 source partitions moved 2,552 / 2,516 / 2,579 items a second — flat from four
///   onwards, because the transactions all commit into the destination's single push
///   partition and queue on that one row lock however many runners hold them. An
///   earlier build with all 64 running at once measured inside the same spread: the
///   bound costs nothing worth having.
/// * more concurrency than this costs correctness of a kind. With both legs of a
///   two-leg relay at 64 partitions, 128 pinned pops went out per cycle, the
///   broker's pop admission served what it could and the rest timed out at two
///   seconds and were retried. A leg whose pops time out looks drained, and a leg
///   that looks drained hands its window to the next priority: 188 of the first 300
///   forwarded items came from the priority-1 leg while priority-0 held a backlog of
///   172. Strict priority, lost to a queue of our own making.
///
/// So the partitions of a leg are worked through by at most this many runners at a
/// time, in a rotating order (below) so a window too small to reach them all does
/// not reach the same ones every cycle.
const MAX_IN_FLIGHT: usize = 16;

/// How many consecutive cycles a leg may hold a window it could not spend.
///
/// A leg that is not DRY keeps the rest of the window rather than handing it to a
/// lower priority (see the priority section above), and that is right for the case
/// it exists for: a pop that timed out, a transaction that lost its lease. Both are
/// over in a cycle or two.
///
/// Unbounded, though, it is the failure this file refuses everywhere else — one
/// error stopping the graph for ever. A partition that fails every cycle for seconds
/// is not a leg being drained, it is a leg that is broken, and holding every lower
/// priority behind it turns one bad partition into a stopped graph. So the hold has
/// a length: past this many cycles the window goes on down the legs and the log says
/// which leg gave up its claim on it.
const STALL_TOLERANCE: u32 = 8;

/// Every so often, poll the whole ring whatever the depth read said.
///
/// The depth read is what makes a cycle cheap and what makes an arriving item
/// reachable in the cycle it arrives in, and it is trusted BOTH ways — a partition it
/// does not name is not polled. That is a lot of weight on one number, and the failure
/// it would cause is the worst one available: a queue that is full and a relay that
/// has stopped asking. Measured once already on a broker whose queue-level pending
/// read zero for a queue holding thirty items (which is why this asks for the group's
/// own backlog and takes no substitute) — but "we found one" is a reason to bound the
/// damage of the next one, not a reason to believe there is no next one.
///
/// So one cycle in this many ignores the answer and asks every partition. It costs a
/// ring's worth of empty pops that often and nothing else, and it means no depth
/// answer, however wrong, can blind this relay for longer than that.
const FULL_SWEEP_EVERY: usize = 16;

/// One runner: one leg, one lane of its source, one partition of that lane's
/// admitted queue.
///
/// The partition is a field and not an argument, and that is the whole safety
/// argument: a runner is built pinned, so there is no code path that can hand two
/// of them the same partition and no ordering guarantee resting on remembering not
/// to.
#[derive(Clone)]
struct Runner {
    /// `from -> to`, for logs.
    edge: Arc<str>,
    group: Arc<str>,
    queue: String,
    partition: String,
}

/// The destination half of a relay, shared by every runner.
struct Dest {
    node: String,
    spec: TargetSpec,
    lane: String,
    push: String,
}

/// The window, as a pool the runners of one leg claim from.
///
/// The alternative — a slice of the window each — was rejected twice over: an idle
/// partition's slice is capacity the busy ones cannot have, and a slice rounded up
/// to at least one item makes `partitions` runners forward `partitions` items
/// against a window of ten. The bound the window exists to hold is the depth of the
/// destination's queue, and only a shared total holds it exactly.
struct Allowance {
    left: AtomicU64,
    /// Workers still drawing on it. A claim is a share of what is left divided by
    /// this, so a tight window is spread over the partitions that hold work instead
    /// of being taken whole by whichever worker asked first — with a window of ten,
    /// ten partitions get one item each rather than one partition getting ten. It
    /// also scales the other way for free: a wide window divided by a handful of
    /// workers is a claim of `MAX_FORWARD`, which is the full transaction the
    /// throughput depends on.
    live: AtomicU64,
}

impl Allowance {
    fn new(left: u64, runners: usize) -> Self {
        Self {
            left: AtomicU64::new(left),
            live: AtomicU64::new(runners.max(1) as u64),
        }
    }

    /// Reserve a batch's worth. Zero means the window is spent and the runner is
    /// done for this cycle.
    fn claim(&self) -> u64 {
        let live = self.live.load(Ordering::Relaxed).max(1);
        let mut got = 0u64;
        // `fetch_update` re-runs the closure on contention, so `got` is assigned on
        // every attempt including the losing ones — hence the explicit zero on the
        // "nothing left" path, which is the value that must survive.
        let _ = self
            .left
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| {
                if left == 0 {
                    got = 0;
                    return None;
                }
                got = (left / live).clamp(1, MAX_FORWARD).min(left);
                Some(left - got)
            });
        got
    }

    /// Hand back what a claim did not spend — the pop came up short, or the commit
    /// did not happen at all.
    fn give_back(&self, n: u64) {
        if n > 0 {
            self.left.fetch_add(n, Ordering::AcqRel);
        }
    }

    /// This runner is done draining, so everyone else's share gets bigger.
    fn retire(&self) {
        let _ = self
            .live
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                Some(n.saturating_sub(1))
            });
    }

    fn remaining(&self) -> u64 {
        self.left.load(Ordering::Acquire)
    }
}

pub fn spawn(queen: Queen, depths: Arc<Depths>, plan: Plan) -> Arc<RelayRuntime> {
    let window = window_for(&plan.dest);

    // One list of runners per LEG, in the plan's order (lowest priority first,
    // ties in declared order). Priority is expressed by draining these lists one
    // after another; parallelism by the length of one list.
    let legs: Vec<Arc<Vec<Runner>>> = plan
        .sources
        .iter()
        .map(|leg| {
            let group: Arc<str> = Arc::from(
                group_of(&plan.application, &plan.graph, &leg.node, &plan.dest_node).as_str(),
            );
            let edge: Arc<str> = Arc::from(format!("{} -> {}", leg.node, plan.dest_node).as_str());
            let mut runners = Vec::new();
            for lane in &leg.spec.lanes {
                for partition in leg.spec.admitted.partition_names() {
                    runners.push(Runner {
                        edge: edge.clone(),
                        group: group.clone(),
                        queue: leg.spec.admitted_queue(&lane.name),
                        partition,
                    });
                }
            }
            Arc::new(runners)
        })
        .collect();

    let relay = Arc::new(RelayRuntime {
        dest: plan.dest_node.clone(),
        sources: plan
            .sources
            .iter()
            .zip(legs.iter())
            .map(|(l, runners)| RelaySource {
                node: l.node.clone(),
                priority: l.priority,
                runners: runners.len() as u32,
            })
            .collect(),
        window,
        forwarded: Default::default(),
        commits: Default::default(),
        unroutable: Default::default(),
        duplicates: Default::default(),

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
        // What an IDLE relay may slow to.
        //
        // A cycle costs a depth read for the destination and one per leg for the
        // sources, and an idle graph paid all of them four times a second to be told
        // nothing. (The pops themselves are no longer part of that bill: a leg only
        // polls the partitions its depth read says hold something, so a quiet leg
        // costs one question rather than one per partition.)
        //
        // The cap is the same two seconds depth.rs argues its cache TTL from
        // ("shorter than any sane poll interval"), and it bounds what backing off
        // can cost: the first item after a quiet spell waits at most one of these,
        // on a path whose latency floor is already a lease per hop. Same trade the
        // gate's own poll window makes, same reason.
        let idle_cap = std::time::Duration::from_millis(2_000);
        let mut idle = pace;
        // Where in each leg's list this cycle starts, and how many cycles have run.
        // The first rotates the ring so a window too small to cover it does not serve
        // the same partitions every time; the second is the full-sweep counter below.
        let mut rotation: usize = 0;
        let mut cycle: usize = 0;
        // Consecutive cycles in which a leg held the window without draining it.
        let mut stalled: u32 = 0;
        let dest = Arc::new(Dest {
            node: plan.dest_node.clone(),
            lane: plan
                .dest
                .default_lane()
                .map(|l| l.name.clone())
                .unwrap_or_else(|| "default".to_string()),
            push: plan.dest.push_queue(),
            spec: plan.dest.clone(),
        });

        while !task.cancel.is_cancelled() {
            // A depth the broker would not report is NOT a depth of zero. Reading it
            // as one would forward a full window every loop for as long as the admin
            // API was unhappy, and the queue this exists to keep shallow would grow
            // without a bound.
            let Some(depth) = depths.try_pending_now(&queen, &dest.push).await else {
                tracing::debug!(
                    dest = %dest.node,
                    "relay: the destination's depth is unknown; holding until it answers"
                );
                // Back off here too: an admin API that did not answer is not
                // helped by being asked four times a second.
                idle = (idle * 2).min(idle_cap);
                tokio::select! {
                    _ = tokio::time::sleep(idle) => {}
                    _ = task.cancel.cancelled() => {}
                }
                continue;
            };
            let pending: u64 = depth.values().sum();
            let mut allowance = window.saturating_sub(pending);

            if allowance == 0 {
                // The destination is full, which means it is draining: freshness
                // is exactly what the refill depends on, so no backoff.
                idle = pace;
                tokio::select! {
                    _ = tokio::time::sleep(pace) => {}
                    _ = task.cancel.cancelled() => {}
                }
                continue;
            }

            let mut moved = 0u64;
            let mut stalled_this_cycle = false;
            // Strict priority: a lower number is drained to exhaustion — or to the
            // window's edge — before a higher one is looked at at all. One leg at a
            // time, however many partitions a leg has, because two legs draining at
            // once is the merge ordered by arrival.
            for runners in &legs {
                if allowance == 0 || task.cancel.is_cancelled() {
                    break;
                }
                // Which of this leg's partitions hold anything for US — and every
                // FULL_SWEEP_EVERY cycles, all of them, whatever the answer.
                //
                // One watermark read per queue, against one pop per partition. Both
                // ends of that trade matter. It is cheaper — a quiet 64-partition
                // source was 64 questions a cycle to be told nothing 64 times — but
                // the reason it is here is correctness of a sort: the window can be
                // smaller than the ring (a node at 5/s has a window of ten against
                // sixty-four partitions), and a runner that finds the window already
                // spent never polls at all. Sweeping the ring blindly, one item
                // waiting in p35 was reached about once every sixty cycles, which on
                // an idle graph is minutes; asked directly, it is polled this cycle
                // and forwarded this cycle.
                //
                // A queue the broker will not answer for is polled anyway. Silence is
                // not emptiness, and the safe direction to be wrong in is the one
                // where the relay does the work it did before it could ask.
                let hot = Arc::new(if cycle % FULL_SWEEP_EVERY == 0 {
                    runners.as_ref().clone()
                } else {
                    hot_partitions(&queen, &depths, runners).await
                });
                if hot.is_empty() {
                    continue;
                }
                let workers = hot.len().min(MAX_IN_FLIGHT);
                let budget = Arc::new(Allowance::new(allowance, workers));
                // Where in the list this cycle starts. The window can still be too
                // small to reach every partition that has work, and starting at zero
                // every cycle would mean the same ones are reached every cycle while
                // the rest wait — so it rotates, and every partition holding work
                // reaches the front. It costs nothing when the window is wide enough
                // to reach them all anyway.
                let start = rotation % hot.len();
                let cursor = Arc::new(AtomicUsize::new(0));
                let blocked = Arc::new(AtomicBool::new(false));
                let polled = Arc::new(AtomicUsize::new(0));

                let mut running = Vec::with_capacity(workers);
                for _ in 0..workers {
                    let (queen, task, dest) = (queen.clone(), task.clone(), dest.clone());
                    let (hot, budget) = (hot.clone(), budget.clone());
                    let (cursor, blocked) = (cursor.clone(), blocked.clone());
                    let polled = polled.clone();
                    running.push(tokio::spawn(async move {
                        let mut moved = 0u64;
                        loop {
                            // Each partition is taken by exactly one worker, once per
                            // cycle: the cursor hands out indices and never repeats
                            // one, which is what keeps a partition single-claimer even
                            // though the workers are fewer than the partitions.
                            let n = cursor.fetch_add(1, Ordering::Relaxed);
                            if n >= hot.len() || task.cancel.is_cancelled() {
                                break;
                            }
                            let runner = &hot[(start + n) % hot.len()];
                            let out = drain(&queen, &task, &dest, runner, &budget).await;
                            moved += out.moved;
                            if out.polled {
                                polled.fetch_add(1, Ordering::Relaxed);
                            }
                            if out.blocked {
                                blocked.store(true, Ordering::Relaxed);
                            }
                        }
                        budget.retire();
                        moved
                    }));
                }
                for handle in running {
                    // A panicking worker is a bug, and the answer to one is the same
                    // as the answer to a batch that would not commit: this pass
                    // forwarded less than it could have, the leases lapse in seconds
                    // and the work comes back. Never a relay that stops.
                    moved += handle.await.unwrap_or(0);
                }
                // Every worker has retired and handed back what it did not spend, so
                // this is what the leg actually left for the next one.
                allowance = budget.remaining();
                // Advance the rotation past what this cycle actually reached, not by
                // one. A window of ten against sixty-four partitions reaches ten of
                // them; stepping by one would take sixty cycles to come back round to
                // the sixty-fourth, which on an idle graph is minutes of an item just
                // sitting there. Stepping by ten sweeps the ring in seven.
                rotation = rotation.wrapping_add(polled.load(Ordering::Relaxed).max(1));

                // A leg yields its window to the next priority only when it is DRY.
                // A partition whose pop timed out or whose transaction would not
                // commit is not dry — it is unknown — and treating unknown as dry is
                // how a priority-1 leg came to take a window that priority-0 still
                // had a backlog for. So a leg that hit anything other than an empty
                // partition keeps the rest of the window: nothing else forwards this
                // cycle, and the next cycle asks the same leg again.
                if blocked.load(Ordering::Relaxed) {
                    stalled_this_cycle = true;
                    if stalled < STALL_TOLERANCE {
                        break;
                    }
                    // Held for long enough. A leg that has failed every cycle for
                    // seconds is broken rather than busy, and the graph does not stop
                    // for it.
                    tracing::warn!(
                        dest = %dest.node,
                        cycles = stalled,
                        "relay: a leg has not drained for several cycles; letting the \
                         lower priorities have the window"
                    );
                }
            }
            stalled = if stalled_this_cycle { stalled.saturating_add(1) } else { 0 };
            cycle = cycle.wrapping_add(1);

            if moved == 0 {
                // Nothing moved, so ask less often — whatever the destination's depth
                // says.
                //
                // This used to keep the full pace whenever the destination still held
                // work, on the grounds that a gate mid-drain wants a fresh depth for
                // the next refill. That reasoning does not survive the sources being
                // dry: there is nothing to refill the window WITH, and a fresh depth
                // buys a number nobody is going to spend.
                //
                // The bound this buys back is the cap below: an item arriving at a dry
                // source waits at most one of those before a runner sees it, on a path
                // whose floor is already a lease per hop.
                idle = (idle * 2).min(idle_cap);
                tokio::select! {
                    _ = tokio::time::sleep(idle) => {}
                    _ = task.cancel.cancelled() => {}
                }
            } else {
                idle = pace;
            }
        }
    });

    relay
}

/// The partitions of one leg that hold work for this relay's group, in the leg's own
/// order.
///
/// One depth read per QUEUE — a leg is one queue per lane of its source — and every
/// partition of a queue the broker would not answer for is kept, because silence is
/// not emptiness. The read is uncached on purpose: it decides what gets polled this
/// cycle, and a two-second-old answer is two seconds an arrived item is not looked
/// at.
async fn hot_partitions(queen: &Queen, depths: &Depths, runners: &[Runner]) -> Vec<Runner> {
    let mut depth: Vec<(String, Option<std::collections::HashMap<String, u64>>)> = Vec::new();
    let mut out = Vec::with_capacity(runners.len());
    for runner in runners {
        if !depth.iter().any(|(q, _)| *q == runner.queue) {
            let d = depths
                .try_pending_of_group_now(queen, &runner.queue, &runner.group)
                .await;
            depth.push((runner.queue.clone(), d));
        }
        let waiting = match depth.iter().find(|(q, _)| *q == runner.queue) {
            // The broker answered: take it at its word, both ways.
            Some((_, Some(map))) => map.get(&runner.partition).copied().unwrap_or(0) > 0,
            // It did not. Poll, as this loop did before it could ask.
            _ => true,
        };
        if waiting {
            out.push(runner.clone());
        }
    }
    out
}

/// Drain ONE partition of one leg until it runs dry, the window closes, or a pop or
/// a commit refuses. Returns how many items it settled — what the coordinator's idle
/// heuristic reads — and whether it ended on something that means "nothing left
/// here", which is what the leg's claim on the window rests on.
///
/// Every pop here is pinned to `runner.partition`, which is what keeps this the
/// only claimer of it, what keeps one connection's items in one order, and what
/// keeps the ack side of the transaction one partition wide.
async fn drain(
    queen: &Queen,
    task: &Arc<RelayRuntime>,
    dest: &Dest,
    runner: &Runner,
    budget: &Allowance,
) -> Drained {
    let mut out = Drained::default();
    loop {
        if task.cancel.is_cancelled() {
            break;
        }
        let take = budget.claim();
        if take == 0 {
            break;
        }
        out.polled = true;
        // Drain until the window closes or the partition runs dry.
        //
        // An EMPTY read is the only unambiguous "nothing left here": a short one can
        // mean the batch bumped into the claim's size, and stopping on it would hand
        // the rest of the allowance to the next leg while this one still had a
        // backlog — a third of the throughput handed to bulk under sustained load on
        // both legs, when that was measured.
        let msgs = match queen
            .queue(runner.queue.clone())
            // THE pin. One partition, this runner's, for as long as it lives.
            .partition(runner.partition.clone())
            .group(runner.group.to_string())
            // `all`, always: a group created at the tail would skip every message
            // already waiting, which for a relay means silently abandoning the
            // backlog it exists to move.
            .subscription_mode(SubscriptionMode::All)
            .batch(take.min(i32::MAX as u64) as i32)
            // One, and it is the pinned one. Anything higher would let a claim
            // wander into partitions another runner owns, and the transaction that
            // acks it would take a row lock per partition it wandered into.
            .partitions(1)
            .lease_seconds(RELAY_LEASE_SECONDS)
            // No long poll: a wait on an empty high-priority leg would hold the
            // low-priority ones behind it for the whole timeout, which is
            // head-of-line blocking dressed as priority.
            .wait(false)
            .poll_timeout(std::time::Duration::from_millis(2_000))
            .pop()
            .await
        {
            Ok(m) => m,
            // Not dry — unknown. The difference is what stops a lower-priority leg
            // taking this one's window: an empty read below is the only answer that
            // means "nothing left here".
            Err(e) => {
                tracing::debug!(
                    edge = %runner.edge,
                    partition = %runner.partition,
                    error = %e,
                    "relay could not claim"
                );
                budget.give_back(take);
                out.blocked = true;
                break;
            }
        };
        if msgs.is_empty() {
            budget.give_back(take);
            break;
        }

        let settled = forward(queen, task, dest, runner, &msgs).await;
        // Only what actually reached the destination spends window: an item nacked
        // as unroutable never arrives there, and one whose transaction rolled back
        // is still upstream.
        budget.give_back(take.saturating_sub(settled.forwarded));
        out.moved += settled.settled;
        if settled.stop {
            out.blocked = true;
            break;
        }
    }
    out
}

/// What one partition's pass did, and whether it ended for a reason that means
/// "nothing left here".
#[derive(Default)]
struct Drained {
    moved: u64,
    /// The window had room for this partition and it was actually asked. False means
    /// the window was spent before this partition's turn came — which is what the
    /// rotation below has to advance past, or the same partitions get asked every
    /// cycle and the rest never do.
    polled: bool,
    /// The pass stopped on something other than an empty partition or a spent
    /// window — a pop that errored, a transaction that would not commit. The leg is
    /// therefore NOT known to be drained, and its window must not pass to a lower
    /// priority on the strength of it.
    blocked: bool,
}

/// What one batch did.
#[derive(Default)]
struct Settled {
    /// Items now on the destination's queue — the only ones that spend window.
    forwarded: u64,
    /// Items that left the source queue at all, forwarded or dead-lettered.
    settled: u64,
    /// Stop draining this partition for the rest of the cycle: nothing was settled
    /// and the lease will bring the batch back.
    stop: bool,
}

/// Forward one claimed batch, in one transaction per destination partition.
///
/// The grouping is the narrowness rule from the module docs: a transaction takes a
/// row lock per partition it touches, so N destinations is N transactions of one
/// lock each, never one transaction of N. The acks ride with the pushes they
/// belong to, so a group that does not commit leaves exactly its own items
/// unsettled.
async fn forward(
    queen: &Queen,
    task: &Arc<RelayRuntime>,
    dest: &Dest,
    runner: &Runner,
    msgs: &[Message],
) -> Settled {
    // Grouped in arrival order, and the groups themselves in the order they were
    // first seen: a partition's items keep the order they were popped in, which is
    // the order they were admitted in.
    let mut groups: Vec<(Option<String>, Vec<&Message>)> = Vec::new();
    for m in msgs {
        let key = partition_for(&dest.spec, &dest.lane, &m.data);
        match groups.iter_mut().find(|(k, _)| *k == key) {
            Some((_, items)) => items.push(m),
            None => groups.push((key, vec![m])),
        }
    }

    let mut out = Settled::default();
    for (partition, items) in groups {
        let Some(partition) = partition else {
            // The destination shards on a dimension these items do not carry, so
            // there is no partition they could go to and no shard whose budget they
            // could ever satisfy. Dead-lettered with the reason rather than dropped
            // — and in their own transaction, which touches only the source
            // partition because it pushes nothing.
            let mut tx = queen.transaction();
            for m in &items {
                tx = tx.nack(m, unroutable(dest));
            }
            match tx.commit().await {
                Ok(_) => {
                    task.unroutable.fetch_add(items.len() as u64, Ordering::Relaxed);
                    out.settled += items.len() as u64;
                }
                Err(e) => {
                    tracing::warn!(
                        edge = %runner.edge, error = %e,
                        "relay could not dead-letter an unroutable batch; it will be redelivered"
                    );
                    out.stop = true;
                }
            }
            continue;
        };

        let mut tx = queen.transaction();
        let mut staged = 0u64;
        let mut staging_failed = false;
        for m in &items {
            tx = tx.ack(m);
            match tx.push_item(TxnPushItem {
                queue: dest.push.clone(),
                partition: Some(partition.clone()),
                payload: m.data.clone(),
                // The upstream item's own id: a replayed relay collapses instead of
                // duplicating.
                transaction_id: Some(m.transaction_id.clone()),
                trace_id: None,
            }) {
                Ok(next) => {
                    tx = next;
                    staged += 1;
                }
                // Nothing this loop passes can be refused here — the only rejection
                // is a malformed trace id and there is none. Kept anyway, and kept
                // as a dropped BATCH rather than a returned task: a relay that exits
                // on an unexpected error stops the graph for ever, while abandoning
                // a batch costs one lease.
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "relay could not stage a push; the batch will be redelivered"
                    );
                    // A fresh, empty builder: `push_item` took the old one and the
                    // batch is being abandoned anyway.
                    tx = queen.transaction();
                    staging_failed = true;
                    break;
                }
            }
        }
        if staging_failed {
            // A half-built transaction would settle some messages and forward
            // others; dropping it settles nothing, and the leases lapse in seconds.
            out.stop = true;
            continue;
        }

        match tx.commit().await {
            Ok(_) => {
                task.forwarded.fetch_add(staged, Ordering::Relaxed);
                task.commits.fetch_add(1, Ordering::Relaxed);
                out.forwarded += staged;
                out.settled += staged;
            }
            // An item this relay has already forwarded, somehow: a duplicate
            // transaction id is a soft verdict for a plain push but a HARD one
            // inside a transaction, and it takes the whole batch down with it
            // (queen `005_log_ack.sql`). Left alone that is a partition stalled for
            // ever — the batch comes back, the same push is refused, nothing is ever
            // settled. So the batch is retried one item at a time, and the one that
            // is already downstream is simply acked.
            Err(e) if e.to_string().contains("QDUP") => {
                task.duplicates.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    edge = %runner.edge,
                    partition = %runner.partition,
                    "relay: an item was already forwarded; settling the batch one at a time"
                );
                let one_by_one = settle_one_by_one(queen, task, dest, &partition, &items).await;
                out.forwarded += one_by_one.forwarded;
                out.settled += one_by_one.settled;
            }
            // Nothing was settled and nothing was pushed. The lease expires in
            // seconds and the batch comes back; this pass stops draining the
            // partition rather than hammering a broker that just refused it. Only
            // this partition: the other runners of the leg are untouched, which is
            // the second thing the pin buys.
            Err(e) => {
                tracing::warn!(
                    edge = %runner.edge,
                    partition = %runner.partition,
                    error = %e,
                    "relay transaction did not commit; the batch will be redelivered"
                );
                out.stop = true;
            }
        }
    }
    out
}

/// The recovery path: one transaction per item, so the one that is already
/// downstream can be acked on its own instead of taking its batch down with it.
async fn settle_one_by_one(
    queen: &Queen,
    task: &Arc<RelayRuntime>,
    dest: &Dest,
    partition: &str,
    items: &[&Message],
) -> Settled {
    let mut out = Settled::default();
    for m in items {
        // An item the destination cannot route gets the same treatment it gets on
        // the batch path — dead-lettered with the reason. Skipping it here left it
        // holding a lease until expiry and settled only by some later clean batch,
        // which is a different answer to the same question depending on how the
        // relay got here.
        if partition_for(&dest.spec, &dest.lane, &m.data).is_none() {
            if queen
                .transaction()
                .nack(m, unroutable(dest))
                .commit()
                .await
                .is_ok()
            {
                task.unroutable.fetch_add(1, Ordering::Relaxed);
                out.settled += 1;
            }
            continue;
        }
        let one = queen.transaction().ack(m).push_item(TxnPushItem {
            queue: dest.push.clone(),
            partition: Some(partition.to_string()),
            payload: m.data.clone(),
            transaction_id: Some(m.transaction_id.clone()),
            trace_id: None,
        });
        // A staging error here is the same non-event it is on the batch path, and it
        // leaves the item where it is: the lease lapses and it comes back.
        if let Ok(tx) = one {
            match tx.commit().await {
                Ok(_) => {
                    task.forwarded.fetch_add(1, Ordering::Relaxed);
                    task.commits.fetch_add(1, Ordering::Relaxed);
                    out.forwarded += 1;
                    out.settled += 1;
                }
                // Already downstream: settle it and move on. It does NOT count as
                // forwarded — it is not arriving at the destination a second time,
                // so it spends no window.
                Err(e) if e.to_string().contains("QDUP") => {
                    if queen.transaction().ack(m).commit().await.is_ok() {
                        out.settled += 1;
                    }
                }
                Err(_) => {}
            }
        }
    }
    out
}

fn unroutable(dest: &Dest) -> String {
    format!(
        "gate: `{}` shards by `{}` and this item carries none",
        dest.node,
        dest.spec.shard_by.map(|d| d.as_str()).unwrap_or("?")
    )
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
