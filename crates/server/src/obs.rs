//! What the hot path writes, which is nothing, and what it counts, which is
//! cheap.
//!
//! v1 wrote one trace row per decision through a `calls` queue, in the same
//! transaction as the ack, and one meter event per admission. v2 has no ack and
//! no calls queue: the hot path writes one KV batch and one transaction, and
//! that is the entire budget. Everything here is either an `AtomicU64` or a
//! bounded in-process ring.
//!
//! Prod, 2026-08-21, one hour: v1 made ~275,000 "is there work?" calls
//! (`log_has_pending_v1` 138,656, `log_pop_specific_v1` 86,927, depth 39,505,
//! streams state 9,949) to move messages **963** times. 285 polls per relay.
//! Nothing was broken; that is what the observability of the old design cost
//! while idle, and idle is most of the time.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use serde_json::{json, Value};

/// Per stage, lifetime, per replica.
///
/// `forwarded / commits` remains **the** number that explains a stage's
/// throughput: the destination partition takes one row lock per transaction
/// whoever holds it, so items-per-transaction is the multiplier on everything
/// the workers do in parallel. In v1 it sat near 1; here it should sit near the
/// batch.
#[derive(Debug, Default)]
pub struct StageCounters {
    pub popped: AtomicU64,
    pub admitted: AtomicU64,
    /// Batches where a refusal cut the batch short and the tail was left
    /// unacked, in order, for the next claim.
    pub deferred: AtomicU64,
    /// In-handler sleeps: the budget said "not yet" and the wait was short
    /// enough to hold the lease through.
    pub parked: AtomicU64,
    /// Returns without an ack: the wait was long, so the lease was allowed to
    /// lapse and the batch to be redelivered. Queen charges no retry budget on
    /// lease expiry, so this costs nothing and cannot dead-letter waiting work.
    pub released: AtomicU64,
    pub forwarded: AtomicU64,
    pub commits: AtomicU64,
    /// Batches found already partly forwarded, and settled one item at a time
    /// instead. Should be zero; it is here because "should be" is not a
    /// measurement, and a recovery path nobody can see is one nobody knows ran.
    pub duplicates: AtomicU64,
    /// Messages on a shared interior queue that belong to another path: acked,
    /// never charged, never forwarded.
    pub foreign: AtomicU64,
    /// Items that could never be admitted (a declared cost above the node's
    /// ceiling), nacked with a reason so they reach the DLQ rather than parking
    /// the head of a partition for ever.
    pub deadlettered: AtomicU64,
    /// Cost units admitted, so the console can divide by `admitted` and get the
    /// measured weight of an item.
    pub cost: AtomicU64,
    /// Charges that MAY have been spent with nothing left that knows it: a kv
    /// call that failed after the broker had already committed it — a read
    /// timeout, a dropped connection, a proxy 502.
    ///
    /// It cannot be compensated. A blind refund is unsound for the same reason
    /// `min: 0` was (see `budget::Budgets::refund`): `incr(-D)` cannot tell our
    /// own charge from another worker's traffic, so giving it back on a guess
    /// would hand out budget in the case where the call never landed. What is
    /// available is to COUNT it, so a broker that is dropping responses shows up
    /// as a number rather than as a limiter that quietly admits less than it
    /// should.
    pub leaked: AtomicU64,
    /// Times this stage was found WEDGED: the broker refusing the ack that
    /// would advance the cursor, at a claim head that never moves, so the same
    /// batch comes back for ever.
    ///
    /// It is here because the 2026-09-02 incident had no number of its own. A
    /// stage whose group had been seeded at the head of another path's twelve-
    /// day-old backlog could not ack a single frame — `log_txns` no longer held
    /// the hashes — and every figure the console could show said something else:
    /// `released` (which is ordinary pacing), `popped` (which was climbing), and
    /// worst of all `waitingForBudget`, which named a counter that was not the
    /// problem. Counted once per escalation, not once per refusal.
    pub wedged: AtomicU64,
}

impl StageCounters {
    pub fn view(&self) -> Value {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let forwarded = g(&self.forwarded);
        let commits = g(&self.commits);
        json!({
            "popped": g(&self.popped),
            "admitted": g(&self.admitted),
            "deferred": g(&self.deferred),
            "parked": g(&self.parked),
            "released": g(&self.released),
            "forwarded": forwarded,
            "commits": commits,
            "duplicates": g(&self.duplicates),
            "foreign": g(&self.foreign),
            "deadlettered": g(&self.deadlettered),
            "leaked": g(&self.leaked),
            "wedged": g(&self.wedged),
            "costAdmitted": g(&self.cost),
            // The ratio, computed here so nobody has to remember which two
            // numbers explain a stage's throughput.
            "itemsPerCommit": if commits == 0 { Value::Null } else { json!(forwarded as f64 / commits as f64) },
        })
    }

    pub fn bump(&self, f: impl Fn(&Self) -> &AtomicU64, n: u64) {
        f(self).fetch_add(n, Ordering::Relaxed);
    }
}

/// One refusal, kept.
///
/// **Denials only.** An admission is counted and never traced: it is the common
/// case and the uninteresting one, and v1's own trace stream was refusals in
/// practice despite documenting sampling. What is lost against v1 is the
/// estimate-versus-actual cost comparison, which had no source once the ack
/// went away — recorded in the design's §16.5 rather than smuggled through.
#[derive(Debug, Clone)]
pub struct Trace {
    pub at: i64,
    pub application: String,
    pub graph: String,
    pub node: String,
    pub path: String,
    pub op: String,
    pub outcome: &'static str,
    pub budget_id: Option<String>,
}

impl Trace {
    pub fn view(&self) -> Value {
        json!({
            "at": self.at,
            "application": self.application,
            // The console's column is still called `target`; a node IS one.
            "target": format!("{}.{}", self.graph, self.node),
            "graph": self.graph,
            "node": self.node,
            "path": self.path,
            "op": self.op,
            "outcome": self.outcome,
            // Durable traces have always used the schema/API spelling. Keep the
            // former live-only camelCase alias for one compatibility window.
            "budget_id": self.budget_id,
            "budgetId": self.budget_id,
        })
    }
}

/// Bounded, drop-oldest. 500 is a page of console, not a log.
pub const TRACE_RING: usize = 500;

#[derive(Default)]
pub struct Traces {
    ring: RwLock<VecDeque<Trace>>,
}

impl Traces {
    pub fn push(&self, t: Trace) {
        let mut r = self.ring.write();
        if r.len() >= TRACE_RING {
            r.pop_front();
        }
        r.push_back(t);
    }

    pub fn recent(&self, outcome: Option<&str>, limit: usize) -> Vec<Trace> {
        self.ring
            .read()
            .iter()
            .rev()
            .filter(|t| outcome.is_none_or(|o| t.outcome == o))
            .take(limit)
            .cloned()
            .collect()
    }

    /// Take everything, for the periodic flush to Postgres.
    pub fn drain(&self) -> Vec<Trace> {
        std::mem::take(&mut *self.ring.write())
            .into_iter()
            .collect()
    }

    pub fn len(&self) -> usize {
        self.ring.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
