//! Rollups and traces, in a table.
//!
//! This started as an in-memory ring flushed to a queue and replayed at boot,
//! and that was wrong in three ways the moment there was more than one replica —
//! none of them a matter of weight:
//!
//! * the meter consumes `calls` under one shared group, so N replicas each see
//!   a FRACTION of the events and each wrote a partial bucket that nobody summed;
//! * each replica held its own ring, so the console showed a different history
//!   depending on which replica the load balancer picked;
//! * the boot replay opened a throwaway consumer group per target per start, and
//!   consumer groups persist — eighteen of them had accumulated in an afternoon.
//!
//! A table fixes all three at once, and the replica split stops being a bug and
//! becomes the mechanism: every replica upserts ITS increments and the row sums
//! them.
//!
//! The earlier objection to this — "no database" — was the wrong reading of a
//! good instinct. What is worth protecting is *no second data system to run*,
//! and a schema inside the PostgreSQL queen already uses keeps that intact:
//! same instance, same backup, same thing to page about.

use std::collections::HashMap;

use deadpool_postgres::{Config as PgConfig, Pool, Runtime};
use serde_json::{json, Value};
use tokio_postgres::NoTls;

use crate::meter::Bucket;

/// Always-virgin, like the broker's own schema: created on every boot, costs
/// nothing when it is already there, and removes the class of bug where two
/// deployments disagree about which migration they are on.
const SCHEMA: &str = r#"
CREATE SCHEMA IF NOT EXISTS gate;

CREATE TABLE IF NOT EXISTS gate.rollups (
    application  TEXT        NOT NULL,
    target       TEXT        NOT NULL,
    lane         TEXT        NOT NULL,
    minute       BIGINT      NOT NULL,
    admitted     BIGINT      NOT NULL DEFAULT 0,
    denied       BIGINT      NOT NULL DEFAULT 0,
    calls        BIGINT      NOT NULL DEFAULT 0,
    throttled    BIGINT      NOT NULL DEFAULT 0,
    cost_est     DOUBLE PRECISION NOT NULL DEFAULT 0,
    cost_actual  DOUBLE PRECISION NOT NULL DEFAULT 0,
    PRIMARY KEY (application, target, lane, minute)
);

-- The one access pattern: a contiguous range of minutes for one target, newest
-- first. Nothing here is ever queried by value.
CREATE INDEX IF NOT EXISTS rollups_by_time
    ON gate.rollups (application, target, minute DESC);

CREATE TABLE IF NOT EXISTS gate.traces (
    id           BIGSERIAL   PRIMARY KEY,
    at           BIGINT      NOT NULL,
    application  TEXT        NOT NULL,
    target       TEXT        NOT NULL,
    lane         TEXT        NOT NULL,
    op           TEXT        NOT NULL DEFAULT '',
    outcome      TEXT        NOT NULL,
    budget_id    TEXT,
    calls        BIGINT      NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS traces_by_time ON gate.traces (at DESC);
CREATE INDEX IF NOT EXISTS traces_by_outcome ON gate.traces (outcome, at DESC);
"#;

pub struct History {
    pool: Pool,
}

impl History {
    /// `None` when no database is configured, and that is a supported way to
    /// run: the gate itself needs nothing from this, so a cell without history
    /// limits traffic exactly as well and simply cannot answer "how were we
    /// doing last Tuesday".
    pub async fn connect() -> Option<Result<Self, String>> {
        let host = std::env::var("PG_HOST").ok()?;
        Some(Self::open(host).await)
    }

    async fn open(host: String) -> Result<Self, String> {
        let mut cfg = PgConfig::new();
        cfg.host = Some(host);
        cfg.port = std::env::var("PG_PORT").ok().and_then(|p| p.parse().ok());
        cfg.user = Some(std::env::var("PG_USER").unwrap_or_else(|_| "postgres".into()));
        cfg.password = std::env::var("PG_PASSWORD").ok();
        cfg.dbname = Some(std::env::var("PG_DATABASE").unwrap_or_else(|_| "postgres".into()));
        // Small on purpose: history writes must never compete with the message
        // path for connections, and everything here is batched or periodic.
        cfg.pool = Some(deadpool_postgres::PoolConfig::new(4));

        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .map_err(|e| format!("history pool: {e}"))?;
        let client = pool.get().await.map_err(|e| format!("history connect: {e}"))?;
        client
            .batch_execute(SCHEMA)
            .await
            .map_err(|e| format!("history schema: {e}"))?;
        Ok(Self { pool })
    }

    /// Add one minute's increments. Two replicas writing the same minute is the
    /// normal case, not a race: they saw different halves of the traffic, and
    /// the row is the sum.
    pub async fn add(&self, app: &str, target: &str, minute: i64, lanes: &HashMap<String, Bucket>) {
        let Ok(client) = self.pool.get().await else { return };
        for (lane, b) in lanes {
            let _ = client
                .execute(
                    "INSERT INTO gate.rollups
                       (application, target, lane, minute, admitted, denied, calls, throttled, cost_est, cost_actual)
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                     ON CONFLICT (application, target, lane, minute) DO UPDATE SET
                       admitted    = gate.rollups.admitted    + EXCLUDED.admitted,
                       denied      = gate.rollups.denied      + EXCLUDED.denied,
                       calls       = gate.rollups.calls       + EXCLUDED.calls,
                       throttled   = gate.rollups.throttled   + EXCLUDED.throttled,
                       cost_est    = gate.rollups.cost_est    + EXCLUDED.cost_est,
                       cost_actual = gate.rollups.cost_actual + EXCLUDED.cost_actual",
                    &[
                        &app, &target, lane, &minute,
                        &(b.admitted as i64), &(b.denied as i64),
                        &(b.calls as i64), &(b.throttled as i64),
                        &b.cost_estimated, &b.cost_actual,
                    ],
                )
                .await;
        }
    }

    pub async fn rollups(&self, app: &str, target: &str, minutes: i64) -> Vec<Value> {
        let Ok(client) = self.pool.get().await else { return vec![] };
        let since = (crate::now_ms() / 60_000 * 60_000) - minutes * 60_000;
        let rows = client
            .query(
                "SELECT minute, lane, admitted, denied, calls, throttled, cost_est, cost_actual
                 FROM gate.rollups
                 WHERE application = $1 AND target = $2 AND minute >= $3
                 ORDER BY minute",
                &[&app, &target, &since],
            )
            .await
            .unwrap_or_default();

        let mut by_minute: Vec<(i64, HashMap<String, Value>, [f64; 6])> = Vec::new();
        for r in rows {
            let m: i64 = r.get(0);
            let lane: String = r.get(1);
            let v = [
                r.get::<_, i64>(2) as f64,
                r.get::<_, i64>(3) as f64,
                r.get::<_, i64>(4) as f64,
                r.get::<_, i64>(5) as f64,
                r.get::<_, f64>(6),
                r.get::<_, f64>(7),
            ];
            if by_minute.last().map(|(mm, _, _)| *mm) != Some(m) {
                by_minute.push((m, HashMap::new(), [0.0; 6]));
            }
            let slot = by_minute.last_mut().expect("just pushed");
            for i in 0..6 {
                slot.2[i] += v[i];
            }
            slot.1.insert(
                lane,
                json!({ "t": m, "admitted": v[0], "denied": v[1], "calls": v[2],
                        "throttled": v[3], "cost_estimated": v[4], "cost_actual": v[5] }),
            );
        }

        by_minute
            .into_iter()
            .map(|(m, lanes, t)| {
                json!({
                    "t": m,
                    "total": { "t": m, "admitted": t[0], "denied": t[1], "calls": t[2],
                               "throttled": t[3], "cost_estimated": t[4], "cost_actual": t[5] },
                    "lanes": lanes,
                })
            })
            .collect()
    }

    /// Admissions per second for one lane, from the table rather than from
    /// whatever this replica happened to see.
    pub async fn rate_per_sec(&self, app: &str, target: &str, lane: &str, now_ms: i64) -> f64 {
        let Ok(client) = self.pool.get().await else { return 0.0 };
        let current = now_ms / 60_000 * 60_000;
        let rows = client
            .query(
                "SELECT minute, admitted FROM gate.rollups
                 WHERE application=$1 AND target=$2 AND lane=$3 AND minute >= $4
                 ORDER BY minute DESC LIMIT 2",
                &[&app, &target, &lane, &(current - 60_000)],
            )
            .await
            .unwrap_or_default();
        for r in &rows {
            let m: i64 = r.get(0);
            let a: i64 = r.get(1);
            // A complete minute needs no correction. Only fall back to the one
            // still filling — scaled, with a floor on the divisor — when there
            // is no complete one yet, which is exactly the first minute after a
            // declare and exactly when somebody is watching.
            if m < current {
                return a as f64 / 60.0;
            }
        }
        match rows.first() {
            Some(r) => {
                let a: i64 = r.get(1);
                let elapsed = (((now_ms - current) as f64) / 1000.0).max(5.0);
                a as f64 / elapsed
            }
            None => 0.0,
        }
    }

    /// The share of the ceiling the OTHER lanes spent in the last complete
    /// minute — the input to `ceiling-minus-measured`.
    ///
    /// This has to come from the table and not from the replica's own ring:
    /// with several replicas each sees a slice of the traffic, would each
    /// conclude the other lanes are idle, and would each hand the derived lane
    /// the whole residual. The lane would then oversubscribe by roughly the
    /// number of replicas — the same defect the lane shares were introduced to
    /// fix, arriving by a different door.
    pub async fn measured_share(
        &self,
        app: &str,
        target: &str,
        except_lane: &str,
        ceiling_per_min: f64,
        now_ms: i64,
    ) -> Option<f64> {
        if ceiling_per_min <= 0.0 {
            return None;
        }
        let client = match self.pool.get().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("measured_share: no connection: {e}");
                return None;
            }
        };
        let current = now_ms / 60_000 * 60_000;
        let row = match client
            .query_opt(
                "SELECT COALESCE(SUM(admitted), 0)::BIGINT FROM gate.rollups
                 WHERE application=$1 AND target=$2 AND lane <> $3 AND minute = $4",
                &[&app, &target, &except_lane, &(current - 60_000)],
            )
            .await
        {
            Ok(r) => r?,
            Err(e) => {
                tracing::warn!("measured_share: {e}");
                return None;
            }
        };
        let others: i64 = row.try_get(0).ok()?;
        Some((others as f64 / ceiling_per_min).clamp(0.0, 1.0))
    }

    /// Admissions per minute for every target, over the last `minutes`.
    ///
    /// One query for the whole deployment rather than one per target: the
    /// dashboard draws every application at once, and N round trips to draw one
    /// picture is how a console starts costing more than the thing it watches.
    pub async fn flow(&self, minutes: i64, now_ms: i64) -> Vec<(String, String, i64, i64)> {
        let Ok(client) = self.pool.get().await else { return vec![] };
        let since = now_ms / 60_000 * 60_000 - minutes * 60_000;
        let rows = client
            .query(
                "SELECT application, target, minute, COALESCE(SUM(admitted), 0)::BIGINT
                 FROM gate.rollups WHERE minute >= $1
                 GROUP BY application, target, minute
                 ORDER BY minute",
                &[&since],
            )
            .await
            .unwrap_or_default();
        rows.iter()
            .filter_map(|r| {
                Some((
                    r.try_get::<_, String>(0).ok()?,
                    r.try_get::<_, String>(1).ok()?,
                    r.try_get::<_, i64>(2).ok()?,
                    r.try_get::<_, i64>(3).ok()?,
                ))
            })
            .collect()
    }

    pub async fn add_traces(&self, rows: &[crate::meter::Trace]) {
        if rows.is_empty() {
            return;
        }
        let Ok(client) = self.pool.get().await else { return };
        for t in rows {
            let _ = client
                .execute(
                    "INSERT INTO gate.traces (at, application, target, lane, op, outcome, budget_id, calls)
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                    &[
                        &t.at, &t.application, &t.target, &t.lane, &t.op, &t.outcome,
                        &t.budget_id, &(t.calls as i64),
                    ],
                )
                .await;
        }
    }

    pub async fn traces(&self, outcome: Option<&str>, limit: i64) -> Vec<Value> {
        let Ok(client) = self.pool.get().await else { return vec![] };
        let rows = match outcome {
            Some(o) => {
                client
                    .query(
                        "SELECT at, application, target, lane, op, outcome, budget_id, calls
                         FROM gate.traces WHERE outcome = $1 ORDER BY at DESC LIMIT $2",
                        &[&o, &limit],
                    )
                    .await
            }
            None => {
                client
                    .query(
                        "SELECT at, application, target, lane, op, outcome, budget_id, calls
                         FROM gate.traces ORDER BY at DESC LIMIT $1",
                        &[&limit],
                    )
                    .await
            }
        }
        .unwrap_or_default();
        // `get` panics on a column type it did not expect, and a console page
        // is a poor place to discover that somebody upgraded the schema by
        // hand. A row that will not read is skipped.
        rows.iter()
            .filter_map(|r| {
                Some(json!({
                    "at": r.try_get::<_, i64>(0).ok()?,
                    "application": r.try_get::<_, String>(1).ok()?,
                    "target": r.try_get::<_, String>(2).ok()?,
                    "lane": r.try_get::<_, String>(3).ok()?,
                    "op": r.try_get::<_, String>(4).ok()?,
                    "outcome": r.try_get::<_, String>(5).ok()?,
                    "budget_id": r.try_get::<_, Option<String>>(6).ok()?,
                    "calls": r.try_get::<_, i64>(7).ok()?,
                }))
            })
            .collect()
    }

    /// Retention, on its own slow clock. `O(space)` work does not belong on the
    /// same tick as `O(work)` work — the same reason the broker's own purge has
    /// a separate interval.
    pub async fn prune(&self, rollup_days: i64, trace_days: i64) {
        let Ok(client) = self.pool.get().await else { return };
        let now = crate::now_ms();
        let _ = client
            .execute(
                "DELETE FROM gate.rollups WHERE minute < $1",
                &[&(now - rollup_days * 86_400_000)],
            )
            .await;
        let _ = client
            .execute(
                "DELETE FROM gate.traces WHERE at < $1",
                &[&(now - trace_days * 86_400_000)],
            )
            .await;
    }
}
