//! The limiter, which is one `kv.incr`.
//!
//! Every kv call in this crate goes through here, so there is exactly one place
//! that knows the namespace, the `min: 0` guard on a refund and what a refused
//! refund means.
//!
//! # Why `incr` with `max` IS the decision
//!
//! `server/sql/procedures/024_kv.sql`, the `WHEN 'incr'` arm: the call that
//! would break the ceiling **does not apply and returns the current value**.
//! There is no saturation and no truncation, so `applied` is the admission
//! verdict — one round trip, no CAS loop, no read-then-write race. That is the
//! whole limiter.
//!
//! Three more facts of that procedure this module is built on:
//!
//! * **the TTL is create-only.** A live row keeps its expiry; an expired row
//!   reads as zero and the next `incr` recreates it with a fresh one. Window
//!   rotation is therefore automatic and costs nothing — no window index in the
//!   key, no sweeper, no `% 4` recycling. v1 needed all four because it owned a
//!   JSON document nothing else pruned.
//! * **`min` is a guard, not a clamp.** `incr(-7, {min: 0})` against a current
//!   value of 5 is refused ENTIRELY, not clamped. But it is a guard on the
//!   RESULTING VALUE and not on the identity of the window — see
//!   [`Budgets::refund`], which is why a refund carries the value its charge
//!   left behind rather than a bare `min: 0`.
//! * **a refused `incr` carries no `expiresAt`.** So the wait deadline needs a
//!   separate read, and it rides in the same batch as a `getMany` — see
//!   [`Budgets::charge`].
//!
//! # Why one shared key is acceptable where one shared partition was not
//!
//! Measured, 32-core VM, 2026-08-20: the old counter-funnel relay topped out at
//! **2.8k items/s** with tuple lock waits at 96–100%, because every admission
//! was a write transaction on ONE partition row. `kv.incr` on one key does
//! **33k/s** — a HOT update on one narrow row with no lease, no segment and no
//! cursor. And the budget is charged once per BATCH, so at batch 200 and 34k
//! items/s the key sees 170 incr/s against that 33k/s ceiling: two orders of
//! magnitude of headroom.

use std::sync::Arc;

use queen_mq::{Expiry, KvOperation, Queen, Result};
use serde_json::json;

/// The kv namespace. Shared with the spec store, which is deliberate: one
/// namespace per deployment is one thing to look at in the console.
pub fn namespace() -> String {
    std::env::var("GATE_KV_NAMESPACE").unwrap_or_else(|_| "gate".to_string())
}

/// One counter, one delta, one ceiling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Charge {
    pub key: String,
    /// `round(count_sub * share)` — this path's ceiling on the shared counter.
    pub max: i64,
    /// The sub-window, in whole seconds. Create-only: it is written when the row
    /// is born and never extended.
    pub ttl: i64,
    pub delta: i64,
    /// For the refusal trace and the ETA's `boundBy`.
    pub budget_id: String,
}

/// What a counter holds right now, and when it rotates.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct State {
    pub key: String,
    pub value: i64,
    /// Epoch millis. `None` where the key was absent — which reads as *retry
    /// now*, not *wait for ever*.
    pub expires_at_ms: Option<i64>,
}

/// One charge attempt, index-aligned to the charges that produced it.
#[derive(Debug, Clone, Default)]
pub struct Attempt {
    pub applied: Vec<bool>,
    /// What the counter read the instant this charge landed on it. `None` where
    /// the charge did not apply, or where the broker did not say.
    ///
    /// This is the **identity of the window our delta went into**, and it is the
    /// only thing that distinguishes a live window from a rotated one — see
    /// [`Budgets::refund`]. Discarding it is what let a refund credit the next
    /// window.
    pub post: Vec<Option<i64>>,
    pub states: Vec<State>,
}

impl Attempt {
    pub fn all_applied(&self) -> bool {
        self.applied.iter().all(|a| *a)
    }

    /// The refunds for every charge that applied, each carrying the value it
    /// left behind. A charge whose post-value is unknown is **not** refunded:
    /// over-counting one sub-window is the safe direction, over-crediting the
    /// next one is not.
    pub fn refunds(&self, charges: &[Charge]) -> Vec<Refund> {
        charges
            .iter()
            .enumerate()
            .filter(|(i, _)| self.applied.get(*i).copied().unwrap_or(false))
            .filter_map(|(i, c)| {
                self.post
                    .get(i)
                    .copied()
                    .flatten()
                    .map(|was| Refund {
                        charge: c.clone(),
                        was,
                    })
                    .or_else(|| {
                        tracing::warn!(
                            key = %c.key, delta = c.delta, budget = %c.budget_id,
                            "budget: a charge applied without reporting its value, so it cannot \
                             be refunded safely; this sub-window is over-counted by that much"
                        );
                        None
                    })
            })
            .collect()
    }

    pub fn state(&self, key: &str) -> Option<&State> {
        self.states.iter().find(|s| s.key == key)
    }

    /// How much room is left on a key, given what the read said. Never
    /// negative: a counter above its own ceiling (a breaker spent the window)
    /// has no room, not negative room.
    pub fn remaining(&self, c: &Charge) -> i64 {
        let v = self.state(&c.key).map(|s| s.value).unwrap_or(0);
        (c.max - v).max(0)
    }
}

/// One charge, given back, with the proof that it is still there to give.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refund {
    pub charge: Charge,
    /// What the counter read straight after `charge` applied.
    pub was: i64,
}

/// What a claim's successful charge left on each counter.
///
/// The settle path recomputes its charges from the messages rather than
/// carrying them (a QDUP recovery refunds a SUFFIX of what it charged), so the
/// post-charge value has to travel with the claim separately. Keyed on the
/// counter, because that is what the guard is about.
#[derive(Debug, Clone, Default)]
pub struct Ledger(Vec<(String, i64)>);

impl Ledger {
    /// The applied part of an attempt, and nothing else.
    pub fn of(charges: &[Charge], a: &Attempt) -> Self {
        Ledger(
            charges
                .iter()
                .enumerate()
                .filter(|(i, _)| a.applied.get(*i).copied().unwrap_or(false))
                .filter_map(|(i, c)| a.post.get(i).copied().flatten().map(|v| (c.key.clone(), v)))
                .collect(),
        )
    }

    pub fn post(&self, key: &str) -> Option<i64> {
        self.0.iter().find(|(k, _)| k == key).map(|(_, v)| *v)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Turn recomputed charges into refunds. A key this ledger never saw is
    /// dropped, loudly: refunding a counter whose window we cannot identify is
    /// exactly the over-credit this type exists to stop.
    pub fn refunds(&self, charges: &[Charge]) -> Vec<Refund> {
        charges
            .iter()
            .filter_map(|c| match self.post(&c.key) {
                Some(was) => Some(Refund {
                    charge: c.clone(),
                    was,
                }),
                None => {
                    tracing::warn!(
                        key = %c.key, delta = c.delta, budget = %c.budget_id,
                        "budget: nothing recorded charging this counter, so it is not refunded"
                    );
                    None
                }
            })
            .collect()
    }
}

/// The broker's ceiling on ops in one HTTP `kv` call
/// (`024_kv.sql`, `C_MAX_OPS_HTTP`), minus the `getMany` that rides along.
///
/// A call above it does not degrade — it raises `kv_too_many_ops`, which the
/// relay reads as "try again later", and the identical batch is redelivered for
/// ever. A `scopeBy` budget mints one key per distinct scope value in the
/// claim, so the count is the caller's `batch` and not a constant.
const MAX_WRITES_PER_CALL: usize = 255;

#[derive(Clone)]
pub struct Budgets {
    queen: Queen,
    ns: Arc<String>,
}

impl Budgets {
    pub fn new(queen: Queen) -> Self {
        Self {
            queen,
            ns: Arc::new(namespace()),
        }
    }

    pub fn ns(&self) -> &str {
        &self.ns
    }

    /// Ask for the whole batch, in one round trip.
    ///
    /// The `getMany` rides along **always**, not only on refusal, for three
    /// reasons: a refused `incr` result carries no `expiresAt` and the wait
    /// deadline needs one; the rows are the ones the `incr`s just touched, so
    /// the read is index-only on a hot page; and asking only on refusal costs a
    /// second round trip at exactly the moment the system is saturated, which is
    /// the worst moment to add one.
    ///
    /// A transport failure is an `Err` and is **not** a refusal. Reading a
    /// failed charge as a refusal would park the graph; reading it as an
    /// admission would breach the ceiling. Neither is available, so the batch
    /// simply does not happen — see `relay::handle`.
    ///
    /// # Chunking, and why it changes nothing semantically
    ///
    /// One `kv` call carries at most 256 ops (`024_kv.sql`, `C_MAX_OPS_HTTP`),
    /// and a `scopeBy` budget mints one key per distinct scope value in the
    /// claim — so a node with a scoped budget and a large `batch` builds a call
    /// the broker REFUSES outright with `kv_too_many_ops`. The relay reads that
    /// as "try again later" and the identical claim comes back for ever, which
    /// is a livelocked partition with no dead letter and one repeating WARN.
    ///
    /// So the charges go out in chunks. That loses nothing, because the ops in
    /// ONE call are already applied independently of each other — a partial
    /// apply is the case the prefix arithmetic and the refund exist for — and
    /// the assembled [`Attempt`] is index-aligned to `charges` exactly as
    /// before. A chunk that fails after an earlier one landed gives the earlier
    /// one back before returning the error.
    pub async fn charge(&self, charges: &[Charge]) -> Result<Attempt> {
        if charges.is_empty() {
            return Ok(Attempt::default());
        }
        let mut applied: Vec<bool> = Vec::with_capacity(charges.len());
        let mut post: Vec<Option<i64>> = Vec::with_capacity(charges.len());
        let mut states: Vec<State> = Vec::with_capacity(charges.len());

        for (n, chunk) in charges.chunks(MAX_WRITES_PER_CALL).enumerate() {
            let mut ops: Vec<KvOperation> = Vec::with_capacity(chunk.len() + 1);
            for c in chunk {
                ops.push(
                    self.queen
                        .kv()
                        .incr(&self.ns, &c.key, c.delta, Expiry::seconds(c.ttl.max(1)))
                        .max(c.max)
                        .operation()?,
                );
            }
            ops.push(KvOperation::get_many(
                self.ns.as_str(),
                chunk.iter().map(|c| c.key.clone()).collect(),
            ));

            let out = match self.queen.kv().batch(ops).await {
                Ok(out) => out,
                Err(e) => {
                    // Everything an earlier chunk landed is known, and known
                    // exactly — so it goes back rather than being left to
                    // expire with the window.
                    if n > 0 {
                        let done = &charges[..n * MAX_WRITES_PER_CALL];
                        let a = Attempt {
                            applied: applied.clone(),
                            post: post.clone(),
                            states: Vec::new(),
                        };
                        self.refund(&a.refunds(done)).await;
                    }
                    return Err(e);
                }
            };
            let results = out.results();

            applied.extend(
                (0..chunk.len()).map(|i| results.get(i).and_then(|r| r.applied).unwrap_or(false)),
            );
            post.extend((0..chunk.len()).map(|i| {
                results
                    .get(i)
                    .filter(|r| r.applied == Some(true))
                    .and_then(|r| r.value.as_ref())
                    .and_then(|v| v.as_i64())
            }));

            // The read rides last. If it is missing (a broker that answered the
            // writes and not the read) the values from the `incr` results are
            // the same numbers, minus the expiry — so the prefix arithmetic
            // still works and only the park deadline degrades to "retry now".
            match results.last().and_then(|r| r.rows.as_ref()) {
                Some(rows) => states.extend(rows.iter().map(|r| State {
                    key: r.key.clone(),
                    value: r.value.as_ref().and_then(|v| v.as_i64()).unwrap_or(0),
                    expires_at_ms: r.expires_at.as_deref().and_then(parse_instant),
                })),
                None => states.extend(chunk.iter().enumerate().map(|(i, c)| {
                    State {
                        key: c.key.clone(),
                        value: results
                            .get(i)
                            .and_then(|r| r.value.as_ref())
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0),
                        expires_at_ms: None,
                    }
                })),
            }
        }

        Ok(Attempt {
            applied,
            post,
            states,
        })
    }

    /// Give back what applied when the batch as a whole did not.
    ///
    /// # `min: 0` is not enough, and the reason is in the procedure
    ///
    /// `024_kv.sql`, the `incr` UPDATE branch:
    ///
    /// ```text
    /// AND (v_min IS NULL OR queen.kv_num_v1(k.value, k.expires_at, v_now) + v_delta >= v_min)
    /// ```
    ///
    /// The guard is on the **resulting value**, not on the identity of the
    /// window. `min: 0` refuses a refund into a key that has been REAPED (the
    /// create branch is gated by the pure `delta >= min` comparison, and
    /// `-D >= 0` is false) — and happily applies one into a key another worker
    /// has just RECREATED. Sub-windows are a second wide by default and this
    /// path fires exactly when the counter is contended, which is exactly when
    /// the row is recreated at once: a batch that straddles a rotation used to
    /// hand its whole delta to the next window, which then admitted `cap + D`.
    ///
    /// So identity travels in the value guard instead. Each refund carries the
    /// number its charge left behind and asks for `min == max == was - delta`:
    /// **apply only if this counter still reads exactly what I left on it.**
    /// Anything else — a rotation, another worker's charge, another worker's
    /// refund — refuses, which is the safe direction. `was - delta` is never
    /// negative (our own charge is included in `was`), so the old `min: 0`
    /// property is kept as a consequence rather than as a rule.
    ///
    /// A refused refund is logged at WARN and otherwise dropped: it is at most
    /// one sub-window's over-count on one key, bounded and self-healing, and the
    /// alternative (a retry loop against a rotating window) is unbounded.
    pub async fn refund(&self, refunds: &[Refund]) {
        if refunds.is_empty() {
            return;
        }
        for chunk in refunds.chunks(MAX_WRITES_PER_CALL) {
            let mut ops = Vec::with_capacity(chunk.len());
            // Index-aligned to `ops`, because a stage that fails to build shifts
            // every result behind it — and this is the one place that tells an
            // operator WHICH counter is over-counted.
            let mut staged: Vec<&Refund> = Vec::with_capacity(chunk.len());
            for r in chunk {
                let c = &r.charge;
                let target = (r.was - c.delta).max(0);
                match self
                    .queen
                    .kv()
                    .incr(&self.ns, &c.key, -c.delta, Expiry::seconds(c.ttl.max(1)))
                    .min(target)
                    .max(target)
                    .operation()
                {
                    Ok(op) => {
                        ops.push(op);
                        staged.push(r);
                    }
                    Err(e) => {
                        tracing::warn!(key = %c.key, error = %e, "budget: could not stage a refund")
                    }
                }
            }
            if ops.is_empty() {
                continue;
            }
            match self.queen.kv().batch(ops).await {
                Ok(out) => {
                    for (i, res) in out.results().iter().enumerate() {
                        if res.applied == Some(false) {
                            let c = &staged[i].charge;
                            tracing::warn!(
                                key = %c.key, delta = c.delta, budget = %c.budget_id,
                                was = staged[i].was,
                                "budget: a refund was refused, which means the counter moved under \
                                 it — the window rotated, or another worker charged it. At most \
                                 one sub-window is over-counted on this key and the next rotation \
                                 clears it"
                            );
                        }
                    }
                }
                Err(e) => tracing::warn!(
                    error = %e, keys = chunk.len(),
                    "budget: the refund call failed; at most one sub-window is over-counted"
                ),
            }
        }
    }

    /// Credit a counter that this process never charged — the breaker giving
    /// back the token a reporter spent on the call a vendor refused.
    ///
    /// `min: 0` and nothing else, because there is no charge of ours to identify
    /// and therefore no window to prove. It is safe where [`Budgets::refund`] is
    /// not because the only caller spends the whole window immediately
    /// afterwards, which overwrites whatever this credited.
    pub async fn credit(&self, charges: &[Charge]) {
        let mut ops = Vec::with_capacity(charges.len());
        for c in charges {
            match self
                .queen
                .kv()
                .incr(&self.ns, &c.key, -c.delta, Expiry::seconds(c.ttl.max(1)))
                .min(0)
                .operation()
            {
                Ok(op) => ops.push(op),
                Err(e) => {
                    tracing::warn!(key = %c.key, error = %e, "budget: could not stage a credit")
                }
            }
        }
        if ops.is_empty() {
            return;
        }
        if let Err(e) = self.queen.kv().batch(ops).await {
            tracing::warn!(error = %e, keys = charges.len(), "budget: the credit call failed");
        }
    }

    /// Read counters without touching them. The ETA, the console and the
    /// breaker's report; never the hot path.
    pub async fn read(&self, keys: &[String]) -> Result<Vec<State>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let res = self.queen.kv().get_many(&self.ns, keys.to_vec()).await?;
        Ok(res
            .rows
            .unwrap_or_default()
            .iter()
            .map(|r| State {
                key: r.key.clone(),
                value: r.value.as_ref().and_then(|v| v.as_i64()).unwrap_or(0),
                expires_at_ms: r.expires_at.as_deref().and_then(parse_instant),
            })
            .collect())
    }

    /// Spend a window outright — the breaker.
    ///
    /// `put`'s TTL is **not** create-only (only `incr`'s is), so this rewrites
    /// both the value and the expiry in one call. That is what makes every
    /// parked consumer's `expiresAt` the vendor's own `Retry-After` deadline
    /// without anybody being told it.
    pub async fn spend(&self, keys: &[(String, i64)], ttl_seconds: i64) -> Result<()> {
        let mut ops = Vec::with_capacity(keys.len());
        for (key, value) in keys {
            ops.push(
                self.queen
                    .kv()
                    .put(
                        &self.ns,
                        key,
                        json!(value),
                        Expiry::seconds(ttl_seconds.max(1)),
                    )
                    .operation()?,
            );
        }
        self.queen.kv().batch(ops).await?;
        Ok(())
    }

    /// Delete counters. Un-breaking early: the next `incr` recreates them at
    /// zero with a fresh window.
    pub async fn clear(&self, keys: &[String]) -> Result<()> {
        let mut ops = Vec::with_capacity(keys.len());
        for key in keys {
            ops.push(self.queen.kv().delete(&self.ns, key).operation()?);
        }
        if ops.is_empty() {
            return Ok(());
        }
        self.queen.kv().batch(ops).await?;
        Ok(())
    }

    /// The rows as the broker holds them, values untouched. Everything else
    /// here reads a counter, which is an integer; the breaker's record is an
    /// object, so it needs the value rather than a coercion of it.
    pub async fn get_raw(&self, keys: &[String]) -> Result<Vec<queen_mq::KvRow>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let res = self.queen.kv().get_many(&self.ns, keys.to_vec()).await?;
        Ok(res.rows.unwrap_or_default())
    }

    pub async fn put_json(
        &self,
        key: &str,
        value: serde_json::Value,
        ttl_seconds: i64,
    ) -> Result<()> {
        self.queen
            .kv()
            .put(&self.ns, key, value, Expiry::seconds(ttl_seconds.max(1)))
            .send()
            .await?;
        Ok(())
    }

    pub async fn get_prefix(&self, prefix: &str, limit: u32) -> Result<Vec<queen_mq::KvRow>> {
        let res = self
            .queen
            .kv()
            .get_prefix(&self.ns, prefix)
            .limit(limit)
            .send()
            .await?;
        Ok(res.rows.unwrap_or_default())
    }
}

/// A broker timestamp, in epoch millis.
///
/// `expires_at` is a `TIMESTAMPTZ` rendered into jsonb, so it arrives as
/// RFC 3339 with an offset. The fallbacks are not defensive padding: a value
/// this cannot parse becomes "retry now", and a park loop that retries
/// immediately against a saturated counter is a spin — so an unusual rendering
/// has to be read rather than shrugged at.
pub fn parse_instant(s: &str) -> Option<i64> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_millis());
    }
    // A space instead of the `T`, which is how psql prints one.
    if let Ok(dt) = chrono::DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z") {
        return Some(dt.timestamp_millis());
    }
    // No offset at all: read it as UTC, which is what the broker's clock is.
    for fmt in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(s, fmt) {
            return Some(naive.and_utc().timestamp_millis());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_broker_timestamp_parses_in_every_shape_it_arrives_in() {
        let want = 1_755_763_200_000i64;
        assert_eq!(parse_instant("2025-08-21T08:00:00+00:00"), Some(want));
        assert_eq!(parse_instant("2025-08-21T08:00:00Z"), Some(want));
        assert_eq!(parse_instant("2025-08-21T08:00:00.000Z"), Some(want));
        assert_eq!(parse_instant("2025-08-21 08:00:00+00"), Some(want));
        assert_eq!(parse_instant("2025-08-21T08:00:00"), Some(want));
        assert_eq!(parse_instant("nonsense"), None);
    }

    /// The number an absent key answers. `None` means *retry now* — the key was
    /// reaped between the incr and the read — and never *wait for ever*.
    #[test]
    fn an_absent_key_has_no_deadline_and_full_room() {
        let a = Attempt::default();
        let c = Charge {
            key: "k".into(),
            max: 100,
            ttl: 1,
            delta: 5,
            budget_id: "b".into(),
        };
        assert_eq!(a.remaining(&c), 100);
        assert_eq!(a.state("k"), None);
    }

    /// A counter above its own ceiling has NO room, not negative room — which is
    /// exactly the state a breaker leaves behind.
    #[test]
    fn a_spent_window_leaves_no_room_rather_than_negative_room() {
        let a = Attempt {
            applied: vec![false],
            post: vec![None],
            states: vec![State {
                key: "k".into(),
                value: 150,
                expires_at_ms: None,
            }],
        };
        let c = Charge {
            key: "k".into(),
            max: 100,
            ttl: 1,
            delta: 5,
            budget_id: "b".into(),
        };
        assert_eq!(a.remaining(&c), 0);
    }

    fn charge(key: &str, delta: i64) -> Charge {
        Charge {
            key: key.into(),
            max: 100,
            ttl: 1,
            delta,
            budget_id: "b".into(),
        }
    }

    /// The refund a rotated window must not get.
    ///
    /// `min: 0` is a guard on the RESULTING VALUE, so a refund of −8 against a
    /// FRESH window holding 20 applies and hands the new window eight units of
    /// free budget. The guard that refuses it is the value the charge left:
    /// apply only if the counter still reads exactly that.
    #[test]
    fn a_refund_carries_the_value_its_charge_left_and_not_a_bare_floor() {
        let charges = vec![charge("k", 8)];
        let a = Attempt {
            applied: vec![true],
            post: vec![Some(30)],
            states: vec![State {
                key: "k".into(),
                value: 30,
                expires_at_ms: None,
            }],
        };
        let refunds = a.refunds(&charges);
        assert_eq!(refunds.len(), 1);
        assert_eq!(refunds[0].was, 30);
        // What goes on the wire: min == max == was − delta, so a counter that
        // reads anything but 30 refuses.
        assert_eq!(refunds[0].was - refunds[0].charge.delta, 22);
    }

    /// A charge that did not apply is not refunded, and neither is one whose
    /// value the broker did not report: over-counting one sub-window is the safe
    /// direction, over-crediting the next one is not.
    #[test]
    fn only_a_charge_with_a_known_landing_value_is_refundable() {
        let charges = vec![charge("a", 1), charge("b", 2), charge("c", 3)];
        let a = Attempt {
            applied: vec![true, false, true],
            post: vec![Some(10), Some(99), None],
            states: Vec::new(),
        };
        let refunds = a.refunds(&charges);
        assert_eq!(refunds.len(), 1, "only `a` applied AND reported a value");
        assert_eq!(refunds[0].charge.key, "a");
    }

    /// The settle path recomputes its charges from the messages, so the ledger
    /// is what carries the identity across — and a key it never saw is dropped
    /// rather than refunded on a guess.
    #[test]
    fn a_ledger_refunds_only_the_counters_it_watched_charge() {
        let charges = vec![charge("a", 4), charge("b", 6)];
        let a = Attempt {
            applied: vec![true, true],
            post: vec![Some(40), Some(60)],
            states: Vec::new(),
        };
        let ledger = Ledger::of(&charges, &a);
        assert_eq!(ledger.post("a"), Some(40));

        // A QDUP recovery refunds a SUFFIX: smaller deltas, same counters.
        let partial = vec![charge("a", 1), charge("z", 9)];
        let refunds = ledger.refunds(&partial);
        assert_eq!(refunds.len(), 1);
        assert_eq!(refunds[0].charge.key, "a");
        assert_eq!(
            refunds[0].was, 40,
            "the value the CHARGE left, not this one"
        );
    }
}
