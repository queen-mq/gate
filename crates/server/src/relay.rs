//! One stage: pop a batch, ask the broker for permission in one KV call, commit
//! `ack + push` in one transaction.
//!
//! This is the file the whole rewrite is about. It replaces `gate.rs` (237
//! lines, the streams gate), `edge.rs` (1132 lines, the relay), `shared.rs`
//! (183, the capacity-lease pool) and `meter.rs` (584, the top-up loop) with one
//! consumer per DAG edge set.
//!
//! # The three numbers it is built on
//!
//! **Prod, 2026-08-21, Query Insights, one hour.** Gate made roughly **275,000
//! "is there work?" calls** — `log_has_pending_v1` 138,656, `log_pop_specific_v1`
//! 86,927, depth 39,505, streams state 9,949 — to move messages **963** times
//! (`log_transaction_wire_v1`). That is **285 polls per relay**. Nothing was
//! broken; that is what the v1 design cost when idle, and idle is most of the
//! time. Here an idle stage is a parked long-poll: no connection held, no query,
//! woken by the push notifier.
//!
//! **Bench, 32-core VM, 2026-08-20.** The old counter-funnel relay topped out at
//! **2.8k items/s** into a single destination node with tuple lock waits at
//! 96–100%; capped shapes burned **6.5 PG cores to admit 172 items/s**, of which
//! `streams_cycle` alone was 3.2. `txnload` with **disjoint lanes** — the shape
//! this file adopts — did **23–34k items/s** on the same VM.
//!
//! **The lane discipline, measured.** 64 workers acking across 16 source
//! partitions in one transaction serialised to **33 txn/s with the machine 95%
//! idle**; the same workers on disjoint partitions did **603 txn/s and 23,000
//! items/s**. That is why `.partitions(1)` is on the consumer and why every push
//! goes to the SAME-NAMED partition on the destination: one claim touches one
//! source partition and one destination partition, and concurrent claims touch
//! different ones. It is a design constraint, not an optimisation.
//!
//! # The scheduler is the broker
//!
//! There is no `.partition()` on the consumer. The wildcard pop picks candidate
//! partitions in **randomised order** and claims with **`FOR UPDATE SKIP
//! LOCKED`** (`004_log_pop.sql`, `log_pop_wildcard_wire_v1`), precisely so
//! concurrent consumers of one group spread across partitions instead of
//! convoying. v1's pinned runners, hot-partition depth probe, `FULL_SWEEP_EVERY`,
//! rotation cursor and `MAX_IN_FLIGHT` all existed to do, badly, what the broker
//! does here for free.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, SecondsFormat, Utc};
use parking_lot::RwLock;
use queen_mq::{Cancel, Message, Queen, SubscriptionMode, TxnPushItem};
use serde_json::{json, Value};

use gate_core::plan::{NodePlan, Stage};
use gate_core::{cost_of, op_matches, op_of, scope_value, GATE_META};

use crate::budget::{Budgets, Charge, Ledger};
use crate::knobs::knobs;
use crate::obs::{StageCounters, Trace, Traces};

/// One stage, running.
pub struct StageRuntime {
    pub application: String,
    pub graph: String,
    pub stage: Stage,
    pub node: NodePlan,
    pub counters: StageCounters,
    /// `(budget id, at)` — what last said no here. The console's "why am I
    /// waiting" answer, without a query.
    pub last_refusal: RwLock<Option<(String, i64)>>,
    /// When this graph runtime started, taken ONCE in `supervisor::start`
    /// before anything was provisioned or spawned. It is what a new group on a
    /// Gate-owned interior queue is seeded from — see [`interior_seed`].
    pub started_at: SystemTime,
    /// A relay transaction the broker keeps refusing at the same claim head.
    /// See [`Wedge`].
    pub wedge: RwLock<Option<Wedge>>,
    pub cancel: Cancel,
}

impl StageRuntime {
    pub fn key(&self) -> String {
        format!("{}/{}", self.stage.path, self.stage.node)
    }
}

/// How far BEFORE this runtime's start an interior queue's new group is seeded.
///
/// # The clock this compares against is not ours
///
/// The seed is a wall-clock instant taken from **Gate's** clock, and the broker
/// resolves it against `log_segments.created_at`, stamped from **Postgres's**
/// `clock_timestamp()` (`003_log_push.sql:219-224`). The two can disagree. Gate
/// running AHEAD of the database is the direction that loses work — a frame
/// this runtime relays could carry a database timestamp below our seed and be
/// skipped — so the seed is pushed back by a margin that no plausible skew
/// crosses.
///
/// Two minutes, and it is bounded on BOTH sides:
///
/// * below, by the skew it has to absorb — and by the spread between replicas.
///   A declare lands on ONE replica and the others follow through the reconcile
///   loop, up to `GATE_RECONCILE_SECONDS` later, each starting its own runtime
///   with its own instant; whichever pops first is the one that creates the
///   group and seeds it. The margin has to cover that spread as well, or a peer
///   that seeded late could skip what the declaring replica had already
///   relayed. Two minutes against a fifteen-second reconcile is eight times the
///   room;
/// * above, by the broker's transaction window. Frames inside the margin are
///   foreign and are settled by ack, and an ack resolves by hash against
///   `queen.log_txns`, purged at
///   `now() - GREATEST(dedup_window, completed_retention, 900s)`
///   (`006_log_maintenance.sql:389`). A margin at or past that window would
///   re-create the very rollback loop this seeding exists to prevent, so it must
///   stay comfortably inside the 900-second floor.
///
/// `GATE_INTERIOR_SEED_SKEW_SECONDS` moves it; the ceiling above is why the knob
/// is clamped rather than free.
pub const INTERIOR_SEED_SKEW: Duration = Duration::from_secs(120);

/// The ISO-8601 instant a new group on a Gate-owned interior queue starts from.
///
/// `subscription_from` and not `SubscriptionMode::New`, and the difference is a
/// race. Every stage of a runtime is spawned at once (`supervisor::start`), so
/// the entry stage of a newly added path can relay a frame into an interior
/// queue **before** the downstream stage's first pop has created its group. With
/// `New` the broker seeds that group at `last_offset` as of the first pop
/// (`004_log_pop.sql:844-850`), so the frame lands BELOW the cursor and is
/// silently dropped — which is the failure the old blanket `All` was written to
/// prevent. A cursor seeded from an instant taken before any stage was spawned
/// has no such race: anything this runtime relays is at or after the seed.
///
/// The mode travels alongside as `New` only so that a broker which ignores
/// `subscriptionFrom` degrades to the race rather than to the incident; queen
/// 1.0.5 gives the timestamp precedence over the mode outright
/// (`004_log_pop.sql:779-790`), so on the broker we run against the mode is not
/// read at all.
///
/// # When `started_at` is old, which is exactly one case
///
/// The seed only ever applies to a group that does not exist yet, and in every
/// ordinary way that happens the runtime is SECONDS old: a graph's first declare
/// starts a runtime, adding a path swaps one for a new one, and a process
/// restart makes one. The group's absence and the runtime's youth are the same
/// event.
///
/// The exception is an operator DELETING a consumer group on a runtime that has
/// been up for weeks. The next pop then re-creates it seeded at that runtime's
/// start, which may be well outside the broker's transaction window — the
/// incident's own shape, arrived at from the other direction. This is not
/// closed, and it is not new: before this change the same deletion reseeded with
/// `All`, at the head of the whole retained log, which is strictly worse. What
/// is different is that the blast radius is now bounded by the runtime's age
/// rather than by the retention, and that the stage says so — see [`Wedge`],
/// which names the remedy. The remedy for a group that must be moved is
/// `seek`, never `delete`.
///
/// # Floored at the epoch, and the floor is not decoration
///
/// A seed before 1970 is
/// `All` wearing a timestamp — the broker's own "everything" sentinel IS the
/// epoch (`004_log_pop.sql`) — so a machine whose clock reads zero would quietly
/// restore the exact behaviour this function replaces. Such a clock breaks more
/// than Gate, but it must not break it in THIS direction.
pub fn interior_seed(started_at: SystemTime, skew: Duration) -> String {
    let at = started_at
        .checked_sub(skew)
        .unwrap_or(started_at)
        .max(SystemTime::UNIX_EPOCH);
    let at: DateTime<Utc> = at.into();
    at.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// A relay transaction the broker refuses, at the same claim head, over and
/// over.
///
/// It exists because the 2026-09-02 incident was invisible for hours: the stage
/// WARNed the identical rollback about ten times a second, and the only number
/// that moved was `waiting_for_budget`, which said the backlog was waiting for a
/// counter when nothing was. So a refusal that repeats at a head that never
/// advances is escalated ONCE to `ERROR`, with the remedy in the line, and
/// counted separately from a released batch.
pub struct Wedge {
    /// The transaction id of the claim's head. A different head means the
    /// cursor moved, which means this is not the same wedge.
    pub head: String,
    pub attempts: u64,
    /// Whether the `ERROR` line has already been written for this head. Once,
    /// not once per attempt: ten lines a second is how the last one hid.
    pub escalated: bool,
}

/// How many refusals at one head before the stage says so at `ERROR`.
///
/// Small, because the failure it names does not resolve on its own — the cursor
/// cannot move until an operator seeks the group — but not one, because a
/// single rolled-back transaction is an ordinary thing (a lapsed lease, a
/// concurrent seek) and does not deserve an `ERROR`.
const WEDGE_ESCALATE_AFTER: u64 = 5;

/// Start the consumer.
///
/// Every line of the builder below is load-bearing:
///
/// * **no `.partition()`** — the wildcard pop is the scheduler (module doc);
/// * **`.partitions(1)`** — one source partition per claim, which is the lane
///   discipline the 33-vs-603 txn/s measurement bought;
/// * **the subscription seed** — two rules, decided by
///   `Stage::source_is_interior` and by nothing else:
///   * an INGRESS source (user-owned or the Gate-owned HTTP door) gets
///     **`All`**, never `new`: an application producer writes it, so a group
///     created at the tail silently skips everything already waiting, which for
///     a limiter means silently dropping the backlog it exists to pace;
///   * a Gate-owned INTERIOR source gets **the tail as of this runtime's
///     start**, expressed as a `subscription_from` timestamp. Only Gate's own
///     relay writes such a queue, and only for a path that stamped its own name
///     on the frame, so a new group there can never need anything older than
///     the instant its own upstream stage started relaying. See
///     [`interior_seed`], `Stage::source_is_interior`, and the incident both
///     name;
/// * **`.auto_ack(false)`** — the relay settles inside its own transaction, so
///   an ack the client sent on its own would forward nothing and lose the item;
/// * **`.wait(true)` with a 30s poll timeout** — this is the line that turns
///   275,000 polls an hour into approximately zero;
/// * **`.renew_lease(...)`** — the handler may park in-line for up to a
///   sub-window, and without renewal the lease could lapse mid-park and the
///   batch be redelivered while this worker still holds it.
pub fn spawn(
    queen: Queen,
    budgets: Budgets,
    st: Arc<StageRuntime>,
    traces: Arc<Traces>,
) -> tokio::task::JoinHandle<()> {
    let k = knobs();
    let q = queen.clone();
    tokio::spawn(async move {
        let ctx = Ctx {
            queen: q.clone(),
            budgets,
            st: st.clone(),
            traces,
        };
        let ctx = Arc::new(ctx);
        // The seed is applied only where the group does not exist yet — the
        // broker never re-seeds a cursor it already has (`QueueBuilder::
        // subscription_mode`, and `004_log_pop.sql`'s `ON CONFLICT DO NOTHING`
        // on both the metadata row and the per-partition cursors) — so this is
        // a no-op on every restart of a graph that has run before.
        let subscribe = q.queue(&st.stage.source).group(&st.stage.group);
        let subscribe = if st.stage.source_is_interior {
            let from = interior_seed(st.started_at, k.interior_seed_skew);
            tracing::debug!(
                stage = %st.key(), queue = %st.stage.source, group = %st.stage.group, from = %from,
                "a Gate-owned interior source: a new group here starts at the tail as of this \
                 runtime's start, never at the head of another path's backlog"
            );
            subscribe
                .subscription_mode(SubscriptionMode::New)
                .subscription_from(from)
        } else {
            subscribe.subscription_mode(SubscriptionMode::All)
        };
        let res = subscribe
            .batch(st.stage.batch as i32)
            .partitions(1)
            .concurrency(st.stage.concurrency as usize)
            .auto_ack(false)
            .lease_seconds(k.lease_seconds)
            .renew_lease(k.renew_lease)
            .wait(true)
            .poll_timeout(k.poll_timeout)
            .cancel(st.cancel.clone())
            .consume_batch(move |msgs| {
                let ctx = ctx.clone();
                async move {
                    handle(&ctx, msgs).await;
                    Ok::<(), std::convert::Infallible>(())
                }
            })
            .await;
        match res {
            Ok(summary) => tracing::info!(
                stage = %st.key(), queue = %st.stage.source,
                processed = summary.processed, reason = ?summary.stopped_by,
                "stage stopped"
            ),
            // A stage that exits is a stopped graph, which is the failure v1's
            // edge.rs refuses everywhere. The consumer only returns on a
            // terminal refusal (a suspended cluster, a gated feature) or a
            // cancel, and both are worth a loud line rather than a silent task
            // going away.
            Err(e) => tracing::error!(
                stage = %st.key(), queue = %st.stage.source, error = %e,
                "stage exited on a broker refusal; nothing is draining this queue until it is \
                 re-declared"
            ),
        }
    })
}

struct Ctx {
    queen: Queen,
    budgets: Budgets,
    st: Arc<StageRuntime>,
    traces: Arc<Traces>,
}

// ------------------------------------------------------------------ the handler
//
// # One settle per claim, and why the whole handler is shaped around it
//
// Measured against the broker, not assumed: `queen.log_ack_v1` (and its
// `log_ack_at_v1` twin) commit the cursor **positionally** and then
//
//     UPDATE queen.log_consumers SET committed = p_upto,
//            worker_id = NULL, lease_expires_at = NULL, batch_end = NULL
//
// — **an ack RELEASES the claim.** Two consequences, and both are load-bearing:
//
// 1. A second transaction under the same lease answers `invalid or expired
//    lease`. So the relay settles a claim in EXACTLY ONE transaction: the
//    foreign acks, the poison nack and the admitted acks-and-pushes cannot be
//    three calls, they have to be one.
// 2. The commit is `committed = max(position acked)`. Acking a set with a gap
//    in it therefore commits **past** the gap and silently drops whatever was in
//    it. So what gets settled is a true PREFIX of the claimed batch in offset
//    order, foreign messages included — never a subset.
//
// And one more, measured against the broker and asserted by
// `an_ack_settles_the_whole_claim_or_pays_a_lease` in the live suite:
//
// 3. Settling a claim IN FULL re-arms its partition in about **seven
//    milliseconds**. Settling a PREFIX of one leaves it parked until the lease
//    expires — measured at exactly the lease, whatever the cursor says is
//    pending. (A nack re-arms it in about four milliseconds, and is not
//    available for pacing: it charges the retry budget and would dead-letter
//    work that is merely waiting.)
//
// So the happy path costs nothing and the deferral path costs one lease, which
// is why two things are the way they are: `plan::fitting_batch` sizes a claim to
// what one sub-window admits, so the deferral is rare, and
// `GATE_LEASE_SECONDS` is ten rather than thirty, so it is cheap when it
// happens.

/// What one claimed message is, decided once, in order.
enum Kind {
    /// Belongs to another path on a shared interior queue: settle it, never
    /// charge it, never forward it.
    Foreign,
    /// Declares a cost above this node's ceiling. It can NEVER be admitted, and
    /// left in place it parks the head of its partition for ever without ever
    /// reaching a DLQ, because a lease that expires charges no retry budget.
    Poison(String),
    Work,
}

async fn handle(ctx: &Ctx, msgs: Vec<Message>) {
    let st = &ctx.st;
    st.counters
        .popped
        .fetch_add(msgs.len() as u64, Ordering::Relaxed);
    if msgs.is_empty() {
        return;
    }

    let kinds: Vec<Kind> = msgs
        .iter()
        .map(|m| {
            // §6.7. Three groups read `ip.in` in the flagship graph and each sees
            // every message; only the one whose `_gate.path` matches forwards it.
            // The others must SETTLE it or their cursor never advances.
            if st.stage.check_foreign && !owns(&st.stage, &m.data) {
                return Kind::Foreign;
            }
            match cost_of(&st.node.cost, &m.data) {
                Ok(_) => Kind::Work,
                Err(e) => Kind::Poison(format!("gate: node `{}`: {e}", st.node.name)),
            }
        })
        .collect();

    // A poison message at the HEAD is nacked on its own, because a nack and an
    // ack in one transaction contradict each other: the nack releases the lease
    // without moving the cursor, the ack moves it. One at a time, and the rest of
    // the claim comes back.
    if let Kind::Poison(reason) = &kinds[0] {
        match ctx
            .queen
            .transaction()
            .nack(&msgs[0], reason.clone())
            .commit()
            .await
        {
            Ok(_) => {
                st.counters.deadlettered.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    stage = %st.key(), reason = %reason,
                    "dead-lettered an item that can never be admitted"
                );
            }
            Err(e) => tracing::warn!(
                stage = %st.key(), error = %e,
                "could not dead-letter an inadmissible item; it will be redelivered"
            ),
        }
        return;
    }

    // Everything up to the first poison message. Whatever follows comes back on
    // the next claim, with the poison at the head, where the arm above takes it.
    let cut = kinds
        .iter()
        .position(|k| matches!(k, Kind::Poison(_)))
        .unwrap_or(kinds.len());
    admit(ctx, &msgs[..cut], &kinds[..cut]).await;
}

/// §6.1 – §6.5. `window` is in offset order and all from one source partition.
async fn admit(ctx: &Ctx, window: &[Message], kinds: &[Kind]) {
    let st = &ctx.st;
    let k = knobs();

    let work: Vec<Message> = window
        .iter()
        .zip(kinds)
        .filter(|(_, k)| matches!(k, Kind::Work))
        .map(|(m, _)| m.clone())
        .collect();

    // Nothing of ours in this claim: settle the whole thing and move on.
    if work.is_empty() {
        settle(ctx, window, kinds, 0, &Ledger::default()).await;
        return;
    }

    let grouped = group(st, &work);

    // A batch that touches no counter at all — every budget of this node has a
    // `whenOp` that takes none of these ops. Nothing to ask, so nothing to wait
    // for.
    if grouped.keys.is_empty() {
        settle(ctx, window, kinds, work.len(), &Ledger::default()).await;
        return;
    }

    let mut parked = Duration::ZERO;
    loop {
        let charges = grouped.charges(work.len());
        let attempt = match ctx.budgets.charge(&charges).await {
            Ok(a) => a,
            // A KV call that FAILED is not a refusal. Reading it as one would
            // park the graph; reading it as an admission would breach the
            // ceiling. Neither is available, so the batch simply does not
            // happen: return without acking and let the lease redeliver.
            //
            // "Does not happen" is true of a call that failed BEFORE it
            // committed, and only of that one. A call the broker committed and
            // whose answer was then lost — a read timeout, a dropped
            // connection, a proxy 502 — has spent the budget with nothing left
            // that knows about it, and the redelivered batch charges it again.
            // That cannot be compensated safely (see `budget::Budgets::refund`),
            // so it is counted instead.
            Err(e) => {
                tracing::warn!(
                    stage = %st.key(), error = %e,
                    "the budget call failed; the batch will be redelivered. If the call had \
                     already committed, its charge is spent and nothing here can give it back"
                );
                st.counters.released.fetch_add(1, Ordering::Relaxed);
                st.counters.leaked.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };

        if attempt.all_applied() {
            let ledger = Ledger::of(&charges, &attempt);
            settle(ctx, window, kinds, work.len(), &ledger).await;
            return;
        }

        // ---- §6.3 step 1: refund what applied.
        //
        // A denial charges NOTHING. That is v1's most important property and it
        // is preserved here by giving back the ops that landed before one
        // refused — `min: 0`, which is a guard and not a clamp, so a refund
        // arriving after the window rotated is refused rather than handing out
        // free budget.
        refund_applied(ctx, &charges, &attempt).await;
        note_refusal(ctx, &charges, &attempt);

        // ---- §6.3 step 2: the PREFIX that fits.
        //
        // Prefix, not subset. A message that fits while an earlier one does not
        // is NOT admitted, even when they touch different scoped keys: order
        // inside a partition is the guarantee the whole passthrough design is
        // built on — and, since the cursor commits positionally, a subset would
        // not merely reorder, it would DROP what it skipped.
        let n = grouped.prefix(&charges, &attempt);
        if n == 0 {
            // Leading foreign messages can still be settled: they belong to
            // another path and cost nothing to let go of, and settling them is
            // progress the budget has no say in.
            if settle_end(kinds, 0) > 0 {
                settle(ctx, window, kinds, 0, &Ledger::default()).await;
                return;
            }
            match park_or_release(ctx, &charges, &attempt, &mut parked).await {
                Wait::Retry => continue,
                Wait::Release => return,
            }
        }

        st.counters.deferred.fetch_add(1, Ordering::Relaxed);
        let mut want = n;
        for attempt_no in 0..=k.max_prefix_retries {
            let prefix = grouped.charges(want);
            match ctx.budgets.charge(&prefix).await {
                Ok(a) if a.all_applied() => {
                    let ledger = Ledger::of(&prefix, &a);
                    settle(ctx, window, kinds, want, &ledger).await;
                    return;
                }
                Ok(a) => {
                    // Another worker took the headroom between the two calls.
                    refund_applied(ctx, &prefix, &a).await;
                    // §6.3 step 3: RECOMPUTE from the newly reported values.
                    // This retry exists precisely because somebody else took the
                    // headroom, so re-asking for the identical amount is
                    // near-certain to refuse again — and each burnt attempt is
                    // another charge/refund pair on a contended counter. Never
                    // upwards: the full-batch pass already said this much fits.
                    let next = grouped.prefix(&prefix, &a).min(want);
                    if next == 0 || attempt_no == k.max_prefix_retries {
                        // Treat it as `n == 0`: an unbounded retry loop against
                        // a contended counter is how a limiter turns into a
                        // spin.
                        match park_or_release(ctx, &prefix, &a, &mut parked).await {
                            Wait::Retry => break,
                            Wait::Release => return,
                        }
                    }
                    want = next;
                }
                Err(e) => {
                    tracing::warn!(
                        stage = %st.key(), error = %e,
                        "the prefix charge failed; the batch will be redelivered"
                    );
                    st.counters.released.fetch_add(1, Ordering::Relaxed);
                    st.counters.leaked.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            }
        }
    }
}

async fn refund_applied(ctx: &Ctx, charges: &[Charge], attempt: &crate::budget::Attempt) {
    let give_back = attempt.refunds(charges);
    if !give_back.is_empty() {
        ctx.budgets.refund(&give_back).await;
    }
}

/// How much of the claim can be settled, given how many WORK items were
/// admitted.
///
/// A true prefix: it stops at the first work item that was not admitted, and
/// carries any foreign messages that sit before or between the admitted ones.
/// The cursor commits to the highest position acked, so anything skipped inside
/// this range would be dropped rather than redelivered — which is why this
/// counts rather than filters.
fn settle_end(kinds: &[Kind], admitted: usize) -> usize {
    let mut seen = 0usize;
    let mut end = 0usize;
    for (i, k) in kinds.iter().enumerate() {
        if matches!(k, Kind::Work) {
            if seen == admitted {
                break;
            }
            seen += 1;
        }
        end = i + 1;
    }
    end
}

enum Wait {
    Retry,
    Release,
}

/// §6.5. Park in-handler for a short wait, release for a long one, and never
/// nack for pacing.
async fn park_or_release(
    ctx: &Ctx,
    charges: &[Charge],
    attempt: &crate::budget::Attempt,
    parked: &mut Duration,
) -> Wait {
    let st = &ctx.st;
    let k = knobs();
    let now = crate::now_ms();

    // MAX, not min. If key A frees in 100ms and key B in 5s, waking at 100ms
    // finds B still refusing. A missing `expiresAt` reads as 0 and means "retry
    // now" — the key was reaped between the incr and the read.
    let wait_ms = charges
        .iter()
        .zip(attempt.applied.iter())
        .filter(|(_, ok)| !**ok)
        .filter_map(|(c, _)| attempt.state(&c.key))
        .filter_map(|s| s.expires_at_ms)
        .map(|e| (e - now).max(0))
        .max()
        .unwrap_or(0);

    let wait = Duration::from_millis(wait_ms.max(0) as u64);
    // One comparison, and it is the whole rule: park while this handler can
    // afford to keep holding the claim. There is no separate cutoff on the
    // length of a single wait, because the measurement below says releasing
    // costs about a minute — so a wait is worth parking through whenever the
    // claim can be held that long, and worth releasing only when it cannot.
    if *parked + wait <= k.max_park {
        st.counters.parked.fetch_add(1, Ordering::Relaxed);
        // The jitter is not decoration. Every worker refused in the same
        // sub-window reads the SAME `expiresAt` and would otherwise stampede the
        // same row on the same millisecond. It is the difference between 33k
        // incr/s and a lock convoy.
        let jitter = jitter_ms(wait_ms);
        // Floored, because a MISSING deadline reads as "retry now" and "now"
        // plus a 20ms jitter span is roughly three thousand full kv batches over
        // the thirty-second parking budget. A key reaped between the incr and
        // the read is the ordinary way to get there, and a Gate clock running
        // ahead of the broker's deflates every wait towards the same spin.
        // Fifty milliseconds turns thousands of retries into tens and costs
        // nothing a park can notice.
        let sleep = Duration::from_millis((wait_ms + jitter).max(50) as u64);
        *parked += sleep;
        tokio::select! {
            _ = tokio::time::sleep(sleep) => Wait::Retry,
            // A redeclare must not wait out a park.
            _ = st.cancel.cancelled() => Wait::Release,
        }
    } else {
        // Return WITHOUT acking. The lease expires and the batch is
        // redelivered, and queen charges **no retry budget on lease expiry**
        // (`004_log_pop.sql`: "attempt_count is redelivery telemetry and never
        // consumes budget"), so this costs nothing and cannot dead-letter work
        // that is merely waiting.
        //
        // What it DOES cost is time, and more than the lease. An unsettled claim
        // is re-offered at exactly its lease when the poller is not parked —
        // measured at 10.3s for a 10s lease and 30.0s for a 30s one — but under
        // this consumer's own settings, with its workers parked on a long poll,
        // the same partition was observed taking about a MINUTE to come back
        // while those workers polled it five times a second and the group's
        // depth said it was owed. That is a broker behaviour this code cannot
        // change; what it can do is take this path only when the wait is long
        // enough for a minute not to matter, which is what `park_threshold`
        // decides. Raise `GATE_PARK_THRESHOLD_MS` if a node's sub-window sits
        // just above it and the stalls show.
        //
        // Never a nack. A nack IS re-offered at once — measured at 4ms — and it
        // is still not available here: an explicit `failed` ack charges the
        // retry budget, so pacing with one would dead-letter work that is
        // merely waiting. That is the line that gives Gate a working DLQ back.
        st.counters.released.fetch_add(1, Ordering::Relaxed);
        if *parked >= k.max_park {
            tracing::warn!(
                stage = %st.key(), parked_ms = parked.as_millis(), wait_ms,
                "held this claim to the parking budget and is still refused; releasing it, which \
                 costs about a minute before it is offered again"
            );
        }
        Wait::Release
    }
}

fn note_refusal(ctx: &Ctx, charges: &[Charge], attempt: &crate::budget::Attempt) {
    let st = &ctx.st;
    let Some(c) = charges
        .iter()
        .zip(attempt.applied.iter())
        .find(|(_, ok)| !**ok)
        .map(|(c, _)| c)
    else {
        return;
    };
    *st.last_refusal.write() = Some((c.budget_id.clone(), crate::now_ms()));
    ctx.traces.push(Trace {
        at: crate::now_ms(),
        application: st.application.clone(),
        graph: st.graph.clone(),
        node: st.stage.node.clone(),
        path: st.stage.path.clone(),
        op: String::new(),
        outcome: "denied",
        budget_id: Some(c.budget_id.clone()),
    });
}

// ------------------------------------------------------------------- charging

/// The batch, grouped by the counters it touches, with a running prefix sum per
/// key so the fallback can walk it.
struct Grouped {
    keys: Vec<KeySpec>,
    /// `per_msg[i]` is `(key index, cost)` for every counter message `i`
    /// charges.
    per_msg: Vec<Vec<(usize, i64)>>,
}

struct KeySpec {
    key: String,
    max: i64,
    ttl: i64,
    budget_id: String,
}

impl Grouped {
    /// The charges for the first `n` messages. A key nothing in the prefix
    /// touches is dropped rather than charged zero.
    fn charges(&self, n: usize) -> Vec<Charge> {
        let mut deltas = vec![0i64; self.keys.len()];
        for contributions in self.per_msg.iter().take(n) {
            for (idx, cost) in contributions {
                deltas[*idx] += cost;
            }
        }
        self.keys
            .iter()
            .zip(deltas.iter())
            .filter(|(_, d)| **d > 0)
            .map(|(k, d)| Charge {
                key: k.key.clone(),
                max: k.max,
                ttl: k.ttl,
                delta: *d,
                budget_id: k.budget_id.clone(),
            })
            .collect()
    }

    /// How many messages, in order, fit in what the counters have left.
    fn prefix(&self, charges: &[Charge], attempt: &crate::budget::Attempt) -> usize {
        // `remaining` per key index. The read in §6.2 was taken AFTER this
        // worker's own increments applied, and those are refunded — so its own
        // applied delta comes back off, or the batch would measure itself as
        // having filled the counter it is about to give back.
        let mut remaining = vec![i64::MAX; self.keys.len()];
        for (i, key) in self.keys.iter().enumerate() {
            let Some(pos) = charges.iter().position(|c| c.key == key.key) else {
                continue;
            };
            let mine = if attempt.applied.get(pos).copied().unwrap_or(false) {
                charges[pos].delta
            } else {
                0
            };
            let current = attempt
                .state(&key.key)
                .map(|s| s.value)
                .unwrap_or(0)
                .saturating_sub(mine);
            remaining[i] = (key.max - current).max(0);
        }

        let mut used = vec![0i64; self.keys.len()];
        for (n, contributions) in self.per_msg.iter().enumerate() {
            for (idx, cost) in contributions {
                if used[*idx] + cost > remaining[*idx] {
                    return n;
                }
            }
            for (idx, cost) in contributions {
                used[*idx] += cost;
            }
        }
        self.per_msg.len()
    }
}

fn group(st: &StageRuntime, msgs: &[Message]) -> Grouped {
    let mut keys: Vec<KeySpec> = Vec::new();
    let mut per_msg: Vec<Vec<(usize, i64)>> = Vec::with_capacity(msgs.len());

    for m in msgs {
        let cost = cost_of(&st.node.cost, &m.data).unwrap_or(1);
        let op = op_of(&m.data);
        let mut here: Vec<(usize, i64)> = Vec::new();
        for b in &st.node.budgets {
            if let Some(pats) = &b.when_op {
                if !op_matches(pats, op) {
                    continue;
                }
            }
            let key = match &b.scope_by {
                Some(path) => match scope_value(&m.data, path) {
                    Some(v) => b.key_for(Some(&v)),
                    // A counter keyed on an absent value measures the wrong
                    // thing. The HTTP front door refuses this with a 422; an
                    // item that arrived on a user-owned ingress queue without it
                    // is charged against the node's other budgets and skips this
                    // one, because dropping the item would be a limiter losing
                    // work it was asked to pace.
                    None => continue,
                },
                None => b.key.clone(),
            };
            let idx = match keys.iter().position(|k| k.key == key) {
                Some(i) => i,
                None => {
                    keys.push(KeySpec {
                        key,
                        max: b.max_for(st.stage.share),
                        ttl: b.window_sub_seconds,
                        budget_id: b.id.clone(),
                    });
                    keys.len() - 1
                }
            };
            // A shared key is one counter even when more than one budget on
            // this node names it. The validator permits identical declarations
            // deliberately; charging the same key once per declaration would
            // multiply this message's cost and enforce a smaller limit.
            if !here.iter().any(|(seen, _)| *seen == idx) {
                here.push((idx, cost));
            }
        }
        per_msg.push(here);
    }

    Grouped { keys, per_msg }
}

// ------------------------------------------------------------ the transaction

/// §6.4. The whole claim, settled in ONE transaction: `ack` a prefix, `push` the
/// admitted part of it, atomically.
///
/// Ack-then-push loses the item; push-then-ack duplicates it. The transaction
/// also carries the lease as a precondition, so a lease that lapsed while the
/// relay worked rolls the whole thing back instead of forwarding work somebody
/// else has re-claimed.
///
/// The budget is charged BEFORE this, not inside it: `applied` IS the decision,
/// so it must be known before the transaction is built. (The wire would let the
/// `incr` ride inside via `TransactionBuilder::kv`, and that is deliberately not
/// done — the decision would then be known only after the pushes were already
/// staged, which is the read-then-write shape `incr` exists to remove.) The
/// residual hazard is real and bounded, and it is NARROWED below rather than
/// closed: if the charge commits and the transaction then fails, the charge is
/// refunded — but a charge whose call the broker committed and whose answer was
/// lost is spent with nothing left that knows it, and is counted as `leaked`.
async fn settle(ctx: &Ctx, window: &[Message], kinds: &[Kind], admitted: usize, ledger: &Ledger) {
    let st = &ctx.st;
    let end = settle_end(kinds, admitted);
    if end == 0 {
        return;
    }
    let prefix = &window[..end];
    let kinds = &kinds[..end];
    // Exactly what the charge above paid for, and therefore exactly what a
    // failure has to give back. The foreign ones were never charged and must not
    // be refunded, or a rolled-back batch would hand out free budget.
    let charged: Vec<Message> = prefix
        .iter()
        .zip(kinds)
        .filter(|(_, k)| matches!(k, Kind::Work))
        .map(|(m, _)| m.clone())
        .take(admitted)
        .collect();

    match stage_and_commit(ctx, prefix, kinds, admitted).await {
        Settled::Committed => {}
        // A duplicate transaction id is a soft verdict for a plain push and a
        // HARD one inside a transaction: it rolls the whole bundle back
        // (`005_log_ack.sql`). Left alone that is a partition stalled for ever —
        // the batch comes back, the same push is refused, nothing is ever
        // settled.
        Settled::Duplicate => {
            st.counters.duplicates.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                stage = %st.key(), partition = %prefix[0].partition,
                "an item was already forwarded; halving the claim to find a prefix that commits"
            );
            settle_after_dup(ctx, prefix, kinds, admitted, &charged, ledger).await;
        }
        // Nothing was settled and nothing was pushed, but the budget WAS spent.
        // Give it back before returning, or a broker that refuses transactions
        // for a minute silently eats a minute of ceiling.
        Settled::Failed(why) => {
            note_failed_settle(ctx, prefix, &why);
            refund_for(ctx, &charged, ledger).await;
            st.counters.released.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Whether the broker refused the ACK itself, for a reason that cannot resolve
/// on its own.
///
/// `QTXN ack references unknown transactionId; transaction rolled back`
/// (`005_log_ack.sql:1451`), surfaced by the broker as the `ack_rejected`
/// failure kind (`handlers/data.rs`, `txn_reason_for`). It means the hash the
/// ack names resolves nowhere: either the transaction never existed, or — the
/// case that took Gate down — its `log_txns` row was purged, which happens at
/// `now() - GREATEST(dedup_window, completed_retention, 900s)`
/// (`006_log_maintenance.sql:389`) while the segment itself is retained for far
/// longer. Nothing Gate does moves that frame: the cursor is behind it, the ack
/// that would advance the cursor is the thing being refused, and lease expiry
/// hands the identical claim straight back.
///
/// Matched on the text because the text is what the client surfaces; both the
/// broker's kind and the SQL's own wording are accepted so a rewording on
/// either side does not silently switch the escalation off. A miss costs the
/// `ERROR` line, never correctness.
fn is_ack_rejected(why: &str) -> bool {
    let w = why.to_ascii_lowercase();
    w.contains("ack_rejected")
        || (w.contains("ack") && w.contains("unknown transactionid"))
        || w.contains("qtxn ack")
}

/// A settle the broker refused: warn always, escalate once.
///
/// The 2026-09-02 incident ran for hours at `WARN`, ten lines a second, saying
/// the same thing each time — and the only figure that moved was
/// `waiting_for_budget`, which attributed a wedged cursor to a counter that had
/// plenty left. So a refusal that repeats at a claim head that never advances is
/// counted as [`StageCounters::wedged`] and said ONCE at `ERROR`, with the
/// remedy in the line.
fn note_failed_settle(ctx: &Ctx, prefix: &[Message], why: &str) {
    let st = &ctx.st;
    let head = prefix
        .first()
        .map(|m| m.transaction_id.clone())
        .unwrap_or_default();

    // A different head means the cursor moved, which means whatever this is, it
    // is not the wedge we were counting.
    let (attempts, escalate) = {
        let mut guard = st.wedge.write();
        let fresh = !matches!(guard.as_ref(), Some(w) if w.head == head);
        if fresh {
            *guard = Some(Wedge {
                head: head.clone(),
                attempts: 0,
                escalated: false,
            });
        }
        let w = guard.as_mut().expect("just set");
        w.attempts += 1;
        let escalate = is_ack_rejected(why) && w.attempts >= WEDGE_ESCALATE_AFTER && !w.escalated;
        if escalate {
            w.escalated = true;
        }
        (w.attempts, escalate)
    };

    if escalate {
        st.counters.wedged.fetch_add(1, Ordering::Relaxed);
        tracing::error!(
            stage = %st.key(), queue = %st.stage.source, group = %st.stage.group,
            partition = %prefix.first().map(|m| m.partition.as_str()).unwrap_or(""),
            attempts, error = %why,
            "this stage is WEDGED: the broker refuses the ack that would advance the cursor, so \
             the same claim comes back for ever and nothing here can move it. This is a stuck \
             cursor and NOT a budget backlog, whatever `waitingForBudget` says. Remedy: POST \
             /api/v1/consumer-groups/{}/queues/{}/seek with {{\"toEnd\":true}} on the broker, \
             then check why this group is reading frames older than the broker's transaction \
             window",
            st.stage.group, st.stage.source
        );
        return;
    }

    tracing::warn!(
        stage = %st.key(), attempts, error = %why,
        "the relay transaction did not commit; refunding and letting the lease lapse"
    );
}

/// The QDUP recovery: halve the claim until a prefix commits.
///
/// The constraint is that only ONE transaction may commit per claim — but a
/// transaction that ROLLS BACK costs nothing and leaves the lease intact, so the
/// recovery may attempt as many as it likes provided at most one succeeds. That
/// makes the search one-sided: the first success ends the claim, so it walks
/// DOWN from half the batch rather than binary-searching for the maximum.
///
/// The broker's QDUP does not name the offending id — it says only *"duplicate
/// messages in queue X partition Y"* — so there is nothing to skip to. Halving
/// settles at least half of the duplicate-free prefix per claim in
/// `log2(batch)` rolled-back round trips, where settling only the head would
/// settle one; and the remainder comes back after one lease, which is what
/// `GATE_LEASE_SECONDS` is short for.
///
/// A single item that still refuses is already downstream: it is bare-acked,
/// counts as settled and not as forwarded, and its charge is given back — it is
/// not arriving at the destination a second time, so it spends no window.
async fn settle_after_dup(
    ctx: &Ctx,
    prefix: &[Message],
    kinds: &[Kind],
    admitted: usize,
    charged: &[Message],
    ledger: &Ledger,
) {
    let mut want = admitted / 2;
    loop {
        if want == 0 {
            break;
        }
        let end = settle_end(kinds, want);
        match stage_and_commit(ctx, &prefix[..end], &kinds[..end], want).await {
            Settled::Committed => {
                // Everything this transaction did NOT carry gives its charge
                // back; the rest of the claim comes back when the lease lapses.
                refund_for(ctx, &charged[want.min(charged.len())..], ledger).await;
                return;
            }
            Settled::Duplicate => want /= 2,
            // A failure that is not a duplicate: nothing was settled, so give
            // the whole charge back and let the lease lapse.
            Settled::Failed(why) => {
                tracing::warn!(stage = %ctx.st.key(), error = %why, "the halved settle did not commit");
                refund_for(ctx, charged, ledger).await;
                return;
            }
        }
    }
    // Down to one item, and the loop bottomed out before establishing that it is
    // the duplicate — so `settle_head` may genuinely FORWARD it. What is
    // refunded is what did not go downstream, which is everything behind the
    // head: refunding the head as well would give back a window an item is
    // spending at the vendor right now.
    let forwarded = settle_head(ctx, &prefix[0], &kinds[0]).await;
    let keep = if forwarded { 1 } else { 0 };
    refund_for(ctx, &charged[keep.min(charged.len())..], ledger).await;
}

/// What one settle attempt did.
enum Settled {
    Committed,
    /// Rolled back on a duplicate — so the lease is still ours and another
    /// attempt is allowed.
    Duplicate,
    Failed(String),
}

/// Build and commit one settle: `ack` the whole prefix, `push` the admitted part
/// of it, atomically.
///
/// Ack-then-push loses the item; push-then-ack duplicates it. The transaction
/// also carries the lease as a precondition, so a lease that lapsed while the
/// relay worked rolls the whole thing back instead of forwarding work somebody
/// else has re-claimed.
///
/// The budget is charged BEFORE this, not inside it: `applied` IS the decision,
/// so it must be known before the transaction is built. (The wire would let the
/// `incr` ride inside via `TransactionBuilder::kv`, and that is deliberately not
/// done — the decision would then be known only after the pushes were already
/// staged, which is the read-then-write shape `incr` exists to remove.) The
/// residual hazard is real and bounded, and the caller narrows it: a charge
/// whose transaction then fails is refunded, guarded on the value that charge
/// left behind.
async fn stage_and_commit(
    ctx: &Ctx,
    prefix: &[Message],
    kinds: &[Kind],
    admitted: usize,
) -> Settled {
    let st = &ctx.st;
    let mut tx = ctx.queen.transaction();
    let mut forwarded = 0u64;
    let mut foreign = 0u64;
    let mut cost = 0i64;
    for (m, kind) in prefix.iter().zip(kinds) {
        tx = tx.ack(m);
        if !matches!(kind, Kind::Work) {
            foreign += 1;
            continue;
        }
        if forwarded as usize >= admitted {
            continue;
        }
        forwarded += 1;
        cost += cost_of(&st.node.cost, &m.data).unwrap_or(1);
        for dest in &st.stage.destinations {
            match tx.push_item(push_for(st, m, dest)) {
                Ok(next) => tx = next,
                // Nothing this loop passes can be refused — the only rejection
                // is a malformed trace id and there is none. Kept anyway, and
                // kept as a dropped BATCH rather than a returned task: a relay
                // that exits on an unexpected error stops the graph for ever,
                // while abandoning a batch costs one lease.
                Err(e) => return Settled::Failed(format!("could not stage a push: {e}")),
            }
        }
    }
    match tx.commit().await {
        Ok(_) => {
            st.counters
                .forwarded
                .fetch_add(forwarded, Ordering::Relaxed);
            st.counters.admitted.fetch_add(forwarded, Ordering::Relaxed);
            st.counters.foreign.fetch_add(foreign, Ordering::Relaxed);
            st.counters.commits.fetch_add(1, Ordering::Relaxed);
            st.counters
                .cost
                .fetch_add(cost.max(0) as u64, Ordering::Relaxed);
            // The cursor moved, so whatever was being counted at the old head is
            // over. The head comparison in `note_failed_settle` would notice on
            // its own; this keeps the count honest without waiting for a second
            // failure to say so.
            *st.wedge.write() = None;
            Settled::Committed
        }
        Err(e) if e.to_string().contains("QDUP") => Settled::Duplicate,
        Err(e) => Settled::Failed(e.to_string()),
    }
}

/// One push, with the partition passed through and the transaction id decided at
/// declare time.
fn push_for(st: &StageRuntime, m: &Message, dest: &gate_core::Destination) -> TxnPushItem {
    TxnPushItem {
        queue: dest.queue.clone(),
        partition: Some(m.partition.clone()),
        payload: stamp(&m.data, st, dest.node.as_str()),
        transaction_id: Some(if dest.derive_id {
            gate_core::derive(&m.transaction_id, &dest.label)
        } else {
            m.transaction_id.clone()
        }),
        trace_id: None,
    }
}

/// The last resort: settle exactly the head of the claim.
///
/// Returns whether the head was FORWARDED, which is the difference between a
/// charge that has been spent at the vendor and one that has to go back.
async fn settle_head(ctx: &Ctx, m: &Message, kind: &Kind) -> bool {
    let st = &ctx.st;
    if !matches!(kind, Kind::Work) {
        let _ = ctx.queen.transaction().ack(m).commit().await;
        st.counters.foreign.fetch_add(1, Ordering::Relaxed);
        return false;
    }

    let mut tx = Some(ctx.queen.transaction().ack(m));
    for dest in &st.stage.destinations {
        let Some(builder) = tx.take() else { break };
        match builder.push_item(push_for(st, m, dest)) {
            Ok(next) => tx = Some(next),
            Err(_) => break,
        }
    }
    let Some(tx) = tx else {
        // Nack with the reason so it reaches the DLQ, never dropped.
        let _ = ctx
            .queen
            .transaction()
            .nack(m, "gate: this item cannot be staged for its destination")
            .commit()
            .await;
        st.counters.deadlettered.fetch_add(1, Ordering::Relaxed);
        return false;
    };
    match tx.commit().await {
        Ok(_) => {
            st.counters.forwarded.fetch_add(1, Ordering::Relaxed);
            st.counters.admitted.fetch_add(1, Ordering::Relaxed);
            st.counters.commits.fetch_add(1, Ordering::Relaxed);
            true
        }
        // Already downstream: settle it and move on. It does NOT count as
        // forwarded — it is not arriving at the destination a second time, so it
        // spends no window.
        Err(e) if e.to_string().contains("QDUP") => {
            if let Err(e) = ctx.queen.transaction().ack(m).commit().await {
                tracing::warn!(
                    stage = %st.key(), error = %e,
                    "could not settle an item that is already downstream; its lease will lapse and \
                     the claim will come back to the same place"
                );
            }
            false
        }
        // Leave it alone: its lease lapses and it comes back.
        Err(e) => {
            tracing::warn!(
                stage = %st.key(), error = %e,
                "could not settle the head of the claim; its lease will lapse"
            );
            false
        }
    }
}

/// Give back what these messages were charged, after a transaction that did not
/// commit.
///
/// The charges are recomputed from the messages, so the value each counter was
/// left at travels separately in `ledger` — a refund that cannot prove which
/// window it is giving back to is dropped rather than guessed at.
async fn refund_for(ctx: &Ctx, msgs: &[Message], ledger: &Ledger) {
    if msgs.is_empty() || ledger.is_empty() {
        return;
    }
    let work: Vec<Message> = msgs.to_vec();
    let grouped = group(&ctx.st, &work);
    let charges = grouped.charges(work.len());
    let refunds = ledger.refunds(&charges);
    if !refunds.is_empty() {
        ctx.budgets.refund(&refunds).await;
    }
}

// ---------------------------------------------------------------- the stamp

/// The one piece of per-item provenance v2 keeps.
///
/// One reserved object rather than four top-level keys, so it cannot collide
/// with a `scopeBy` path or a cost path. Stamped by the ingress push (the HTTP
/// front door) or by the first relay that handles an unstamped message — which
/// is how a user-owned ingress queue works, because its producers know nothing
/// about Gate. Carried verbatim by every relay and rewritten per hop.
///
/// It is **not signed and not verified**. It is trusted because it is written
/// server-side, and because writing to an interior or egress queue is already
/// admission bypass — the same trust model as any queen queue.
pub fn stamp(data: &Value, st: &StageRuntime, dest_node: &str) -> Value {
    let mut out = data.clone();
    let mut meta = json!({
        "graph": st.graph,
        "path": st.stage.path,
        "hop": st.stage.hop + 1,
        "node": dest_node,
        "at": crate::now_ms(),
    });
    // `attempt` is the one field that survives a hop rather than being rewritten
    // by it. It is set by `api::reenter` at the origin door, and if a relay
    // dropped it the item would reach the egress queue looking brand new — so
    // the next report would re-enter it as attempt 1, for ever. The bound in
    // §16.6 is only a bound because this line carries the count.
    if let Some(a) = data
        .get(GATE_META)
        .and_then(|g| g.get("attempt"))
        .and_then(|a| a.as_u64())
    {
        meta["attempt"] = json!(a);
    }
    // A payload that is not an object cannot carry the stamp. It is forwarded
    // as it is rather than wrapped: wrapping would change the shape the
    // application's own consumer reads, which is a breaking change Gate has no
    // business making on its behalf.
    if let Some(map) = out.as_object_mut() {
        map.insert(GATE_META.to_string(), meta);
    }
    out
}

/// Whether this stage owns a message on a shared interior queue.
///
/// An UNSTAMPED message belongs to the stage the compiler named as its owner
/// (`Stage::owns_unstamped`) and to no other. It cannot be left for whoever
/// finds it: a payload that is not a JSON object carries no stamp, every group
/// then reads it as its own, and because a converging queue derives its
/// transaction ids per path the copies do not dedup — one message becomes one
/// per path, each charged. Nor can every group skip it: with no owner it would
/// sit on the queue for ever. So exactly one forwards it and the rest ack it,
/// which is what they already do for a message stamped with another path.
fn owns(stage: &Stage, data: &Value) -> bool {
    match data
        .get(GATE_META)
        .and_then(|g| g.get("path"))
        .and_then(|p| p.as_str())
    {
        Some(p) => p == stage.path,
        None => stage.owns_unstamped,
    }
}

// ------------------------------------------------------------------- jitter

/// Uniform in `[0, min(200, waitMs/4 + 20))`, from a cheap xorshift.
///
/// Deliberately not a dependency: this is a de-stampeding smear, not a random
/// number anybody reasons about, and the quality that matters is only that two
/// workers refused in the same millisecond get different answers.
fn jitter_ms(wait_ms: i64) -> i64 {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let span = (wait_ms / 4 + 20).clamp(1, 200) as u64;
    let mut x = SEQ.fetch_add(1, Ordering::Relaxed)
        ^ std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64)
            .unwrap_or(0);
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    (x % span) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(id: &str, count_sub: i64) -> gate_core::CompiledBudget {
        gate_core::CompiledBudget {
            id: id.into(),
            key: format!("b:app:g:n:{id}"),
            scope_by: None,
            shared_key: None,
            when_op: None,
            count: count_sub,
            time_ms: 1000,
            sub_windows: 1,
            count_sub,
            window_sub_seconds: 1,
            confidence: gate_core::Confidence::Inferred,
        }
    }

    fn msg(txn: &str, data: Value) -> Message {
        Message {
            id: format!("m-{txn}"),
            transaction_id: txn.into(),
            trace_id: None,
            data,
            producer_sub: None,
            created_at: String::new(),
            partition_id: "pid".into(),
            partition: "p1".into(),
            lease_id: "lease".into(),
            consumer_group: "g".into(),
        }
    }

    fn runtime(budgets: Vec<gate_core::CompiledBudget>, share: f64) -> StageRuntime {
        StageRuntime {
            application: "app".into(),
            graph: "g".into(),
            stage: Stage {
                path: "p".into(),
                priority: 0,
                share,
                node: "n".into(),
                hop: 0,
                source: "src".into(),
                group: "grp".into(),
                first_hop: true,
                source_is_interior: false,
                check_foreign: false,
                owns_unstamped: true,
                batch: 200,
                concurrency: 4,
                destinations: vec![],
            },
            node: NodePlan {
                name: "n".into(),
                budgets,
                cost: gate_core::Cost::Path(gate_core::CostPath {
                    path: "payload.w".into(),
                    default: 1,
                    max: Some(100),
                }),
                ingress_queue: None,
                ingress_owned: false,
                ingress_http: false,
                ingress_shed: false,
                interior_queue: "n.in".into(),
                egress_queue: None,
                egress_group: None,
                breaker_key: "brk".into(),
                shares: Default::default(),
            },
            counters: Default::default(),
            last_refusal: RwLock::new(None),
            started_at: SystemTime::UNIX_EPOCH,
            wedge: RwLock::new(None),
            cancel: Cancel::new(),
        }
    }

    /// Cost is WEIGHTED, not counted: three items of weight 4 charge twelve, in
    /// one incr.
    #[test]
    fn a_batch_is_charged_once_at_its_total_weight() {
        let st = runtime(vec![budget("b", 100)], 1.0);
        let msgs: Vec<Message> = (0..3)
            .map(|i| msg(&format!("t{i}"), json!({ "w": 4 })))
            .collect();
        let charges = group(&st, &msgs).charges(3);
        assert_eq!(charges.len(), 1, "one key, one incr");
        assert_eq!(charges[0].delta, 12);
        assert_eq!(charges[0].max, 100);
    }

    /// Two identical declarations may intentionally share one counter. They
    /// remain one spend per message, not one spend per declaration.
    #[test]
    fn duplicate_shared_budgets_charge_their_counter_once() {
        let mut first = budget("first", 100);
        first.shared_key = Some("vendor".into());
        let mut second = budget("second", 100);
        second.key = first.key.clone();
        second.shared_key = first.shared_key.clone();
        let st = runtime(vec![first, second], 1.0);
        let msgs: Vec<Message> = (0..3)
            .map(|i| msg(&format!("t{i}"), json!({ "w": 4 })))
            .collect();

        let charges = group(&st, &msgs).charges(msgs.len());
        assert_eq!(charges.len(), 1, "one shared key must produce one incr");
        assert_eq!(charges[0].delta, 12, "the declarations doubled the cost");
    }

    /// A path's share IS the ceiling it carries: `round(count_sub * share)`.
    #[test]
    fn the_share_is_the_max_on_the_incr() {
        let st = runtime(vec![budget("b", 150)], 0.75);
        let charges = group(&st, &[msg("t", json!({ "w": 1 }))]).charges(1);
        assert_eq!(charges[0].max, 113);
    }

    /// PREFIX, not subset. Order inside a partition is the guarantee the
    /// passthrough design is built on.
    #[test]
    fn only_the_prefix_that_fits_is_admitted_and_it_is_a_prefix() {
        let st = runtime(vec![budget("b", 10)], 1.0);
        let msgs: Vec<Message> = vec![
            msg("t0", json!({ "w": 4 })),
            msg("t1", json!({ "w": 4 })),
            // This one would fit in the room left by the two above, and it is
            // still not admitted: an earlier message did not.
            msg("t2", json!({ "w": 9 })),
            msg("t3", json!({ "w": 1 })),
        ];
        let g = group(&st, &msgs);
        let charges = g.charges(4);
        assert_eq!(charges[0].delta, 18);

        // The counter already holds 2 of its 10, and our own 18 did not apply.
        let attempt = crate::budget::Attempt {
            applied: vec![false],
            post: vec![None],
            states: vec![crate::budget::State {
                key: charges[0].key.clone(),
                value: 2,
                expires_at_ms: None,
            }],
        };
        assert_eq!(
            g.prefix(&charges, &attempt),
            2,
            "4 + 4 fits in 8, 9 does not"
        );
        assert_eq!(g.charges(2)[0].delta, 8);
    }

    /// The read in §6.2 is taken AFTER this worker's own increments applied, and
    /// those are refunded — so a partial pass must not measure itself as having
    /// filled the counter it is about to give back.
    #[test]
    fn a_refunded_charge_does_not_count_against_its_own_prefix() {
        let st = runtime(vec![budget("a", 10), budget("b", 4)], 1.0);
        let msgs: Vec<Message> = (0..4)
            .map(|i| msg(&format!("t{i}"), json!({ "w": 1 })))
            .collect();
        let g = group(&st, &msgs);
        let charges = g.charges(4);
        assert_eq!(charges.len(), 2);

        // `a` applied (4 of 10, so the row now reads 4); `b` refused at 4 of 4.
        let attempt = crate::budget::Attempt {
            applied: vec![true, false],
            post: vec![Some(4), None],
            states: vec![
                crate::budget::State {
                    key: charges[0].key.clone(),
                    value: 4,
                    expires_at_ms: None,
                },
                crate::budget::State {
                    key: charges[1].key.clone(),
                    value: 0,
                    expires_at_ms: None,
                },
            ],
        };
        // Without subtracting our own applied 4, `a` would look full at 6 left
        // and the prefix would be cut short for no reason.
        assert_eq!(g.prefix(&charges, &attempt), 4);
    }

    /// A budget with a `whenOp` charges only the messages it selects, and the
    /// grouping is what makes that cheap: the batch is grouped before it is
    /// charged, so a selector costs one pass over the payloads and no extra
    /// round trip.
    #[test]
    fn when_op_selects_before_the_charge() {
        let mut b = budget("del", 100);
        b.when_op = Some(vec!["photo.delete".into()]);
        let st = runtime(vec![budget("all", 100), b], 1.0);
        let msgs = vec![
            msg("t0", json!({ "w": 1, "op": "photo.delete" })),
            msg("t1", json!({ "w": 1, "op": "photo.upload" })),
        ];
        let charges = group(&st, &msgs).charges(2);
        let all = charges.iter().find(|c| c.budget_id == "all").unwrap();
        let del = charges.iter().find(|c| c.budget_id == "del").unwrap();
        assert_eq!(all.delta, 2);
        assert_eq!(del.delta, 1);
    }

    /// A scoped budget is one counter per value, and two values in one batch are
    /// two keys in one call.
    #[test]
    fn a_scoped_budget_makes_one_key_per_value() {
        let mut b = budget("per-listing", 100);
        b.scope_by = Some("payload.listingId".into());
        let st = runtime(vec![b], 1.0);
        let msgs = vec![
            msg("t0", json!({ "w": 1, "listingId": "l1" })),
            msg("t1", json!({ "w": 1, "listingId": "l2" })),
            msg("t2", json!({ "w": 1, "listingId": "l1" })),
        ];
        let charges = group(&st, &msgs).charges(3);
        assert_eq!(charges.len(), 2);
        let l1 = charges
            .iter()
            .find(|c| c.key.ends_with(":l1"))
            .expect("one key per listing");
        assert_eq!(l1.delta, 2);
    }

    fn stage_named(path: &str, owns_unstamped: bool) -> Stage {
        let mut s = runtime(vec![budget("b", 10)], 1.0).stage;
        s.path = path.into();
        s.check_foreign = true;
        s.owns_unstamped = owns_unstamped;
        s
    }

    /// A stamped message belongs to the path it names.
    #[test]
    fn ownership_of_a_shared_interior_queue_reads_the_stamp() {
        let prices = stage_named("prices", true);
        assert!(owns(&prices, &json!({ "_gate": { "path": "prices" } })));
        assert!(!owns(&prices, &json!({ "_gate": { "path": "photos" } })));
    }

    /// An UNSTAMPED message — a payload that is not an object cannot carry the
    /// stamp — belongs to exactly ONE reader of the queue.
    ///
    /// Read as "mine" by every group, one message became one per converging
    /// path, each with its own derived id so dedup could not collapse them, and
    /// each charged: a limiter multiplying the traffic it exists to cap. Read as
    /// "nobody's" by every group, it would sit on the queue for ever.
    #[test]
    fn an_unstamped_message_has_exactly_one_owner() {
        let owner = stage_named("prices", true);
        let other = stage_named("photos", false);
        let scalar = json!("a bare string, which cannot carry _gate");

        assert!(owns(&owner, &scalar));
        assert!(!owns(&other, &scalar));
        assert!(owns(&owner, &json!([1, 2, 3])));
        assert!(!owns(&other, &json!([1, 2, 3])));
        // And an object a non-Gate producer wrote into an interior queue reads
        // the same way.
        assert!(owns(&owner, &json!({})));
        assert!(!owns(&other, &json!({})));
    }

    #[test]
    fn the_stamp_never_replaces_a_payload_it_cannot_carry() {
        let st = runtime(vec![budget("b", 10)], 1.0);
        let scalar = json!("just a string");
        assert_eq!(stamp(&scalar, &st, "ip"), scalar);

        let obj = stamp(&json!({ "a": 1 }), &st, "ip");
        assert_eq!(obj["a"], 1);
        assert_eq!(obj["_gate"]["path"], "p");
        assert_eq!(obj["_gate"]["hop"], 1);
    }

    /// The cursor commits to the HIGHEST position acked, so a settle with a gap
    /// in it would commit past the gap and DROP what was in it. This is the
    /// function that makes that impossible.
    #[test]
    fn a_settle_is_a_true_prefix_of_the_claim() {
        use Kind::*;
        // Foreign, work, foreign, work — admitting one work item settles up to
        // and including the foreign message that follows it, and stops at the
        // work item that was not admitted.
        let kinds = vec![Foreign, Work, Foreign, Work, Work];
        assert_eq!(
            settle_end(&kinds, 0),
            1,
            "the leading foreign run, and no more"
        );
        assert_eq!(
            settle_end(&kinds, 1),
            3,
            "one work item, and the foreign one behind it"
        );
        assert_eq!(settle_end(&kinds, 2), 4);
        assert_eq!(settle_end(&kinds, 3), 5, "everything");
    }

    /// A claim that is entirely another path's is settled whole: it costs one
    /// ack per message and it is what stops the cursor of a group on a shared
    /// interior queue from standing still.
    #[test]
    fn a_claim_of_only_foreign_messages_settles_whole() {
        let kinds = vec![Kind::Foreign, Kind::Foreign];
        assert_eq!(settle_end(&kinds, 0), 2);
    }

    /// Nothing admitted and a work item at the head: nothing to settle, so the
    /// handler parks or releases instead of committing an empty transaction.
    #[test]
    fn nothing_admitted_at_the_head_settles_nothing() {
        assert_eq!(settle_end(&[Kind::Work, Kind::Foreign], 0), 0);
    }

    // ------------------------------------------- where a new group starts

    /// The seed is the runtime's start, pushed back by the skew margin, as an
    /// ISO-8601 instant the broker parses with `::timestamptz`.
    #[test]
    fn the_interior_seed_is_the_runtimes_start_less_the_skew() {
        // 2026-09-02T12:00:00Z, in seconds since the epoch.
        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1_788_350_400);
        assert_eq!(
            interior_seed(start, Duration::ZERO),
            "2026-09-02T12:00:00.000Z"
        );
        assert_eq!(
            interior_seed(start, Duration::from_secs(120)),
            "2026-09-02T11:58:00.000Z"
        );
        assert_eq!(
            interior_seed(start, INTERIOR_SEED_SKEW),
            "2026-09-02T11:58:00.000Z",
            "the shipped margin is two minutes"
        );
    }

    /// A seed before the epoch would be `All` in disguise — the broker's own
    /// "deliver everything" sentinel IS the epoch — so the margin is floored
    /// there rather than allowed to walk back past it.
    #[test]
    fn the_interior_seed_never_underflows_the_epoch() {
        let seed = interior_seed(SystemTime::UNIX_EPOCH, Duration::from_secs(600));
        assert_eq!(seed, "1970-01-01T00:00:00.000Z");
        let seed = interior_seed(
            SystemTime::UNIX_EPOCH + Duration::from_secs(60),
            Duration::from_secs(600),
        );
        assert_eq!(seed, "1970-01-01T00:00:00.000Z");
    }

    /// The margin has a hard ceiling and it is the broker's, not ours.
    ///
    /// Frames inside the margin are foreign and are settled by ack; an ack
    /// resolves by hash against `queen.log_txns`, purged at `now() -
    /// GREATEST(dedup_window, completed_retention, 900s)`. A margin at or past
    /// that floor would seed the cursor among frames that can no longer be
    /// acked, which is precisely the rollback loop the seeding exists to
    /// prevent — so this is the assertion that stops somebody "fixing" a skew
    /// complaint by widening it to an hour.
    #[test]
    fn the_seed_margin_stays_inside_the_brokers_transaction_window() {
        const BROKER_TXNS_FLOOR: Duration = Duration::from_secs(900);
        assert!(
            INTERIOR_SEED_SKEW * 2 < BROKER_TXNS_FLOOR,
            "the default margin must sit well inside the broker's txns window"
        );
        assert!(knobs().interior_seed_skew <= Duration::from_secs(600));
    }

    /// Only the refusal that cannot resolve on its own escalates.
    ///
    /// A lapsed lease, a concurrent seek, a broker restart: those roll a
    /// transaction back too, and they clear by themselves. The ack the broker
    /// cannot resolve does not — the cursor is behind the frame, the ack that
    /// would move it is the thing being refused, and the lease hands the same
    /// claim straight back.
    #[test]
    fn only_an_unresolvable_ack_counts_as_a_wedge() {
        assert!(is_ack_rejected(
            "QTXN ack references unknown transactionId; transaction rolled back"
        ));
        assert!(is_ack_rejected("broker error: ack_rejected"));
        assert!(!is_ack_rejected("invalid or expired lease"));
        assert!(!is_ack_rejected(
            "QDUP duplicate messages in queue q partition p"
        ));
        assert!(!is_ack_rejected("connection reset by peer"));
    }

    /// The escalation is once per wedged head, not once per attempt — ten lines
    /// a second is how the last one hid — and a head that moves resets it.
    #[test]
    fn a_wedge_says_so_once_and_a_moving_cursor_resets_it() {
        let st = runtime(vec![budget("b", 10)], 1.0);
        let refusal = "QTXN ack references unknown transactionId; transaction rolled back";

        let mut escalations = 0;
        for _ in 0..50 {
            if wedge_step(&st, "head-1", refusal) {
                escalations += 1;
            }
        }
        assert_eq!(escalations, 1, "one ERROR for one wedge, not fifty");

        // The cursor moved: a different head is a different situation, and it
        // must be able to escalate on its own account.
        let mut again = 0;
        for _ in 0..WEDGE_ESCALATE_AFTER {
            if wedge_step(&st, "head-2", refusal) {
                again += 1;
            }
        }
        assert_eq!(again, 1);

        // And a refusal that resolves on its own never escalates, however often
        // it repeats at one head.
        let ordinary = runtime(vec![budget("b", 10)], 1.0);
        for _ in 0..50 {
            assert!(!wedge_step(&ordinary, "head-1", "invalid or expired lease"));
        }
    }

    /// The book-keeping half of `note_failed_settle`, without a broker: returns
    /// whether THIS refusal is the one that escalates.
    fn wedge_step(st: &StageRuntime, head: &str, why: &str) -> bool {
        let mut guard = st.wedge.write();
        let fresh = !matches!(guard.as_ref(), Some(w) if w.head == head);
        if fresh {
            *guard = Some(Wedge {
                head: head.to_string(),
                attempts: 0,
                escalated: false,
            });
        }
        let w = guard.as_mut().expect("just set");
        w.attempts += 1;
        let escalate = is_ack_rejected(why) && w.attempts >= WEDGE_ESCALATE_AFTER && !w.escalated;
        if escalate {
            w.escalated = true;
        }
        escalate
    }

    #[test]
    fn the_jitter_stays_inside_its_span() {
        for wait in [0i64, 100, 1000, 100_000] {
            for _ in 0..64 {
                let j = jitter_ms(wait);
                assert!((0..=200).contains(&j), "wait {wait} gave {j}");
            }
        }
    }
}
