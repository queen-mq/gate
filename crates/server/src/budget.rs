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
//!   value of 5 is refused ENTIRELY, not clamped. That is exactly the refund
//!   semantics wanted: a refund arriving after the window rotated is refused
//!   rather than handing out free budget.
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
    pub states: Vec<State>,
}

impl Attempt {
    pub fn all_applied(&self) -> bool {
        self.applied.iter().all(|a| *a)
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
    pub async fn charge(&self, charges: &[Charge]) -> Result<Attempt> {
        if charges.is_empty() {
            return Ok(Attempt::default());
        }
        let mut ops: Vec<KvOperation> = Vec::with_capacity(charges.len() + 1);
        for c in charges {
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
            charges.iter().map(|c| c.key.clone()).collect(),
        ));

        let out = self.queen.kv().batch(ops).await?;
        let results = out.results();

        let applied: Vec<bool> = (0..charges.len())
            .map(|i| results.get(i).and_then(|r| r.applied).unwrap_or(false))
            .collect();

        // The read rides last. If it is missing (a broker that answered the
        // writes and not the read) the values from the `incr` results are the
        // same numbers, minus the expiry — so the prefix arithmetic still works
        // and only the park deadline degrades to "retry now".
        let states = match results.last().and_then(|r| r.rows.as_ref()) {
            Some(rows) => rows
                .iter()
                .map(|r| State {
                    key: r.key.clone(),
                    value: r.value.as_ref().and_then(|v| v.as_i64()).unwrap_or(0),
                    expires_at_ms: r.expires_at.as_deref().and_then(parse_instant),
                })
                .collect(),
            None => charges
                .iter()
                .enumerate()
                .map(|(i, c)| State {
                    key: c.key.clone(),
                    value: results
                        .get(i)
                        .and_then(|r| r.value.as_ref())
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0),
                    expires_at_ms: None,
                })
                .collect(),
        };

        Ok(Attempt { applied, states })
    }

    /// Give back what applied when the batch as a whole did not.
    ///
    /// `min: 0` is a **guard, not a clamp**: if the window rotated between the
    /// charge and the refund, the refund is refused wholesale, which is correct
    /// — refunding into a fresh window would hand out free budget. A refused
    /// refund is logged at WARN and otherwise dropped: it is at most one
    /// sub-window's over-count on one key, bounded and self-healing, and the
    /// alternative (a retry loop against a rotating window) is unbounded.
    pub async fn refund(&self, charges: &[Charge]) {
        if charges.is_empty() {
            return;
        }
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
                    tracing::warn!(key = %c.key, error = %e, "budget: could not stage a refund")
                }
            }
        }
        match self.queen.kv().batch(ops).await {
            Ok(out) => {
                for (i, r) in out.results().iter().enumerate() {
                    if r.applied == Some(false) {
                        let c = &charges[i];
                        tracing::warn!(
                            key = %c.key, delta = c.delta, budget = %c.budget_id,
                            "budget: a refund was refused, which means the window rotated under it. \
                             At most one sub-window is over-counted on this key and the next \
                             rotation clears it"
                        );
                    }
                }
            }
            Err(e) => tracing::warn!(
                error = %e, keys = charges.len(),
                "budget: the refund call failed; at most one sub-window is over-counted"
            ),
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
}
