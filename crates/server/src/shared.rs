//! Budgets that cross targets, which no gate can enforce.
//!
//! The gate's state is keyed `(query_id, partition_id, key)`: two targets are
//! two queries and do not share a row, so a ceiling they both draw on — an
//! egress IP, a shared account — has nowhere to live inside either of them.
//! That is a property of the model, not a gap, and it leaves exactly two
//! implementations. This is the exact one.
//!
//! `reserve` takes a whole cycle's worth in a single `incr` with `max`, where
//! `applied` IS the admission decision, and gives back what the cycle did not
//! spend. The refund is computed in memory and issued immediately, in the same
//! cycle: it must never depend on the gate's state, because a fully denied
//! cycle discards its state writes while the kv spend has already happened, and
//! the drift that leaves is permanent and invisible.
//!
//! Note what this cannot be: exact over a rolling window. `incr` carries a TTL
//! that is create-only, which makes it a FIXED window whatever the target
//! declares — so a rolling shared budget accepts up to twice its cap at the
//! boundary. The spec warns on that combination rather than forbidding it.

use std::sync::atomic::{AtomicI64, Ordering};

use queen_mq::{Expiry, Queen, Result};

/// A local lease on a shared budget.
///
/// The gate is a synchronous function — it cannot await, by the signature the
/// SDK gives it — so a budget that lives behind a network call cannot be
/// consulted per message. It is consulted in CHUNKS instead: a background task
/// reserves a block from kv and the gate spends it down in memory, which is the
/// capacity-lease pattern and the only shape that fits.
///
/// The chunk size is the trade: too small and the top-up is a round trip per
/// handful of messages, too large and a replica hoards a ceiling its neighbours
/// need. A second of the budget's own rate is the natural unit — it is what one
/// pacing quantum can spend anyway.
pub struct Pool {
    pub budget: SharedBudget,
    window: AtomicI64,
    allowance: AtomicI64,
}

impl Pool {
    pub fn new(budget: SharedBudget) -> Self {
        Self { budget, window: AtomicI64::new(-1), allowance: AtomicI64::new(0) }
    }

    fn window_of(&self, now_ms: i64) -> i64 {
        now_ms / (self.budget.period_seconds * 1000)
    }

    /// Spend from the local lease. Synchronous and allocation-free: this is on
    /// the gate's hot path, once per message.
    pub fn try_spend(&self, cost: i64, now_ms: i64) -> bool {
        let w = self.window_of(now_ms);
        // A rolled window invalidates whatever was left: the kv key it was
        // reserved against has rolled too, so holding it would be spending a
        // window that no longer exists.
        if self.window.swap(w, Ordering::Relaxed) != w {
            self.allowance.store(0, Ordering::Relaxed);
        }
        let mut cur = self.allowance.load(Ordering::Relaxed);
        loop {
            if cur < cost {
                return false;
            }
            match self.allowance.compare_exchange_weak(
                cur,
                cur - cost,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }

    pub fn remaining(&self) -> i64 {
        self.allowance.load(Ordering::Relaxed).max(0)
    }

    /// Top the lease up if it is running low. Called off the hot path.
    pub async fn top_up(&self, queen: &Queen, now_ms: i64) {
        let chunk = (self.budget.cap / self.budget.period_seconds.max(1)).max(1);
        let w = self.window_of(now_ms);
        if self.window.load(Ordering::Relaxed) != w {
            self.window.store(w, Ordering::Relaxed);
            self.allowance.store(0, Ordering::Relaxed);
        }
        // `max(1)`, because integer division made this a deadlock: a budget whose
        // chunk is one has a threshold of zero, "at least zero left" is always
        // true, and the top-up never fires — the pool admits nothing for ever.
        // Validation refuses such a spec now (`kv-chunk`), and this holds for the
        // ones already in the store.
        if self.allowance.load(Ordering::Relaxed) >= (chunk / 2).max(1) {
            return;
        }

        if reserve(queen, &self.budget, chunk, now_ms).await.unwrap_or(false) {
            self.allowance.fetch_add(chunk, Ordering::Relaxed);
        }
    }

    /// Hand back what this replica reserved and will not spend, so a neighbour
    /// can have it. Called when the window rolls; the alternative is that the
    /// slack sits here until the kv key expires, which is a ceiling nobody gets
    /// to use.
    pub async fn release(&self, queen: &Queen, now_ms: i64) {
        let left = self.allowance.swap(0, Ordering::Relaxed);
        if left > 0 {
            let _ = refund(queen, &self.budget, left, now_ms).await;
        }
    }
}

#[derive(Clone)]
pub struct SharedBudget {
    /// Scoped by application: two teams that both declare a budget called
    /// `egress-ip` are not sharing one, and a flat key would have silently made
    /// them share it.
    pub scope: String,
    pub id: String,
    pub cap: i64,
    pub period_seconds: i64,
}

const NS: &str = "gate";

fn window_key(b: &SharedBudget, now_ms: i64) -> String {
    // The window index is IN the key and the TTL is one period, so the row is
    // recycled rather than accumulated: the live set stays constant in time
    // instead of growing one row per window forever.
    let idx = now_ms / (b.period_seconds * 1000);
    format!("{}:{}:{}", b.scope, b.id, idx % 4)
}

/// Take `want` units. Returns how many were granted — all or nothing, because
/// a partial grant would need the caller to know how to spend a fraction of a
/// decision it has already made.
pub async fn reserve(queen: &Queen, b: &SharedBudget, want: i64, now_ms: i64) -> Result<bool> {
    if want <= 0 {
        return Ok(true);
    }
    let out = queen
        .kv()
        .incr(NS, &window_key(b, now_ms), want, Expiry::seconds(b.period_seconds * 2))
        .max(b.cap)
        .send()
        .await?;
    // `applied` is absent when the op never ran at all, which is a refusal as
    // far as an admission decision is concerned.
    Ok(out.applied.unwrap_or(false))
}

/// Give back what the cycle reserved and did not spend. `min(0)` so a refund
/// can never drive the counter negative — a floor is cheaper than a correctness
/// argument about racing refunds.
pub async fn refund(queen: &Queen, b: &SharedBudget, unspent: i64, now_ms: i64) -> Result<()> {
    if unspent <= 0 {
        return Ok(());
    }
    queen
        .kv()
        .incr(NS, &window_key(b, now_ms), -unspent, Expiry::seconds(b.period_seconds * 2))
        .min(0)
        .send()
        .await?;
    Ok(())
}

/// What the window currently holds, for the console. Absence reads as zero:
/// a window that has not been touched has not been spent.
pub async fn used(queen: &Queen, b: &SharedBudget, now_ms: i64) -> i64 {
    match queen.kv().get(NS, &window_key(b, now_ms)).await {
        Ok(r) => r
            .value
            .as_ref()
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        Err(_) => 0,
    }
}
