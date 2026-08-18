//! The meter: what actually left, aggregated.
//!
//! Layer 1 of the observability design is the `calls` queue — one event per
//! HTTP call, written in the same transaction as the ack so the count cannot
//! drift from what was settled. This module is layer 2 and 3: it drains that
//! queue into per-minute rollups, and keeps the sampled decision traces.
//!
//! Two rules from the design are enforced here rather than documented:
//!
//! * **rollups aggregate ABOVE the scope.** The dimensions a series may carry
//!   are target, lane, op, outcome — never entity, connection or tenant. A
//!   budget scoped to two hundred thousand listings is legitimate and would be
//!   a suicidal time series.
//! * **sampling is not uniform.** Admissions are 99% of the volume and 0% of
//!   the interest, so denials and breaches are kept whole and admissions are
//!   sampled.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use parking_lot::RwLock;
use queen_mq::Queen;
use serde::Serialize;
use serde_json::Value;

use crate::registry::TargetRuntime;

/// One minute of one `(lane, op, outcome)` series.
#[derive(Debug, Clone, Default, Serialize, serde::Deserialize)]
pub struct Bucket {
    /// Epoch milliseconds of the minute's start.
    pub t: i64,
    pub admitted: u64,
    pub denied: u64,
    pub calls: u64,
    pub throttled: u64,
    /// Cost declared at push time, and what the ack said it really was. Their
    /// drift is the number that says the cost model is broken — and a broken
    /// cost model silently blows every rate budget, because we are counting one
    /// thing and the vendor another.
    pub cost_estimated: f64,
    pub cost_actual: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Trace {
    pub at: i64,
    pub application: String,
    pub target: String,
    pub lane: String,
    pub op: String,
    pub outcome: String,
    pub budget_id: Option<String>,
    pub calls: u64,
}

/// The ring is this replica's own accumulator, not the store. It holds the
/// minute currently being filled plus a short tail, and every minute that
/// closes is written to `gate.rollups` in Postgres, where the rows from every
/// replica sum into one history.
///
/// It cannot be the history itself: each replica consumes a share of the call
/// events, so its ring is a slice of the truth, and the console would report a
/// different past depending on which pod answered.
const KEEP_MINUTES: usize = 720;
const KEEP_TRACES: usize = 500;

#[derive(Default)]
pub struct Meter {
    /// target -> minute -> lane -> bucket
    rollups: RwLock<HashMap<String, VecDeque<(i64, HashMap<String, Bucket>)>>>,
    traces: RwLock<VecDeque<Trace>>,
    breaches: RwLock<VecDeque<Trace>>,
    /// The gate reports lifetime counters; a bucket holds increments. Without
    /// somewhere to remember the last reading, `admitted` in a minute would be
    /// the total since boot — which reads as a staircase on a chart and, worse,
    /// makes `measured_share` divide a lifetime by a minute and saturate to the
    /// ceiling within seconds, pinning every derived lane at its floor forever.
    seen: RwLock<HashMap<(String, String), (u64, u64)>>,
    /// The last minute handed to the durable queue, per target. A minute is
    /// flushed once and only after it has closed, so a bucket on the queue is
    /// always final.
    flushed: RwLock<HashMap<String, i64>>,
}

/// The meter keys everything on `application/target`, because that is the
/// identity; the table keeps them apart so a range query can name one without
/// a LIKE.
fn split_key(key: &str) -> (String, String) {
    match key.split_once('/') {
        Some((a, t)) => (a.to_string(), t.to_string()),
        None => (gate_core::default_application(), key.to_string()),
    }
}

fn minute_of(ms: i64) -> i64 {
    ms - (ms % 60_000)
}

impl Meter {
    pub fn record(&self, target: &str, lane: &str, ev: &CallEvent, now_ms: i64) {
        let minute = minute_of(now_ms);
        let mut r = self.rollups.write();
        let series = r.entry(target.to_string()).or_default();
        if series.back().map(|(t, _)| *t) != Some(minute) {
            series.push_back((minute, HashMap::new()));
            while series.len() > KEEP_MINUTES {
                series.pop_front();
            }
        }
        let (_, lanes) = series.back_mut().expect("just pushed");
        let b = lanes.entry(lane.to_string()).or_default();
        b.t = minute;
        b.calls += ev.calls;
        b.cost_estimated += ev.cost_estimated;
        b.cost_actual += ev.calls as f64;
        if ev.outcome == "throttled" {
            b.throttled += 1;
        }

        let (application, tname) = split_key(target);
        let trace = Trace {
            at: now_ms,
            application,
            target: tname,
            lane: lane.to_string(),
            op: ev.op.clone(),
            outcome: ev.outcome.clone(),
            budget_id: None,
            calls: ev.calls,
        };
        // A breach is kept whole: it is the only evidence that the cap we
        // enforce is higher than the one the vendor enforces.
        if ev.outcome == "throttled" {
            let mut b = self.breaches.write();
            b.push_back(trace.clone());
            while b.len() > KEEP_TRACES {
                b.pop_front();
            }
        }
        let mut t = self.traces.write();
        t.push_back(trace);
        while t.len() > KEEP_TRACES {
            t.pop_front();
        }
    }

    /// A denial, recorded by the gate itself with the budget that caused it.
    ///
    /// Denials cannot arrive on the `calls` queue — nothing was called — so
    /// without this path the traces view would only ever show admissions, which
    /// is the 99% of the volume that carries none of the interest. Kept whole,
    /// never sampled, for the same reason.
    pub fn record_denial(&self, target: &str, lane: &str, op: &str, budget_id: &str, now_ms: i64) {
        let (application, tname) = split_key(target);
        let mut t = self.traces.write();
        t.push_back(Trace {
            at: now_ms,
            application,
            target: tname,
            lane: lane.to_string(),
            op: op.to_string(),
            outcome: "denied".to_string(),
            budget_id: Some(budget_id.to_string()),
            calls: 0,
        });
        while t.len() > KEEP_TRACES {
            t.pop_front();
        }
    }

    /// Fold the gate's own counters into the current minute. The gate counts
    /// admissions and denials; the `calls` queue counts what left. Both belong
    /// in the same series, and only one of them arrives as an event.
    ///
    /// The counters arrive CUMULATIVE and are stored as increments, so every
    /// field of a bucket means the same thing — a quantity that happened inside
    /// that minute — and nothing downstream has to know which fields need
    /// differencing and which do not.
    pub fn observe_gate(&self, target: &str, lane: &str, admitted: u64, denied: u64, now_ms: i64) {
        let (d_admitted, d_denied) = {
            let mut seen = self.seen.write();
            let key = (target.to_string(), lane.to_string());
            let prev = seen.insert(key, (admitted, denied)).unwrap_or((0, 0));
            // A counter that went backwards means the gate was restarted and
            // its totals re-founded; the delta is then the new value itself.
            (
                admitted.checked_sub(prev.0).unwrap_or(admitted),
                denied.checked_sub(prev.1).unwrap_or(denied),
            )
        };
        self.add_gate(target, lane, d_admitted, d_denied, now_ms);
    }

    fn add_gate(&self, target: &str, lane: &str, admitted: u64, denied: u64, now_ms: i64) {
        let minute = minute_of(now_ms);
        let mut r = self.rollups.write();
        let series = r.entry(target.to_string()).or_default();
        if series.back().map(|(t, _)| *t) != Some(minute) {
            series.push_back((minute, HashMap::new()));
            while series.len() > KEEP_MINUTES {
                series.pop_front();
            }
        }
        let (_, lanes) = series.back_mut().expect("just pushed");
        let b = lanes.entry(lane.to_string()).or_default();
        b.t = minute;
        b.admitted += admitted;
        b.denied += denied;
    }

    /// Hand over any minute that has closed and not yet been persisted.
    pub fn take_closed(&self, target: &str, now_ms: i64) -> Option<(i64, HashMap<String, Bucket>)> {
        let current = minute_of(now_ms);
        let r = self.rollups.read();
        let series = r.get(target)?;
        let last_flushed = self.flushed.read().get(target).copied().unwrap_or(i64::MIN);
        // The OLDEST unflushed minute, not the newest: the watermark only ever
        // moves forward, so taking the newest first would step over everything
        // behind it and lose those minutes for good. That is not hypothetical —
        // any lag on the calls queue fills several minutes before the loop
        // next wakes.
        let (t, lanes) = series
            .iter()
            .find(|(t, _)| *t < current && *t > last_flushed)?;
        self.flushed.write().insert(target.to_string(), *t);
        Some((*t, lanes.clone()))
    }

    pub fn rollups(&self, target: &str, minutes: usize) -> Vec<Value> {
        let r = self.rollups.read();
        let Some(series) = r.get(target) else {
            return vec![];
        };
        series
            .iter()
            .rev()
            .take(minutes)
            .map(|(t, lanes)| {
                let mut total = Bucket { t: *t, ..Default::default() };
                for b in lanes.values() {
                    total.admitted += b.admitted;
                    total.denied += b.denied;
                    total.calls += b.calls;
                    total.throttled += b.throttled;
                    total.cost_estimated += b.cost_estimated;
                    total.cost_actual += b.cost_actual;
                }
                serde_json::json!({
                    "t": t,
                    "total": total,
                    "lanes": lanes,
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    /// Admissions per second over the last few closed minutes, from this
    /// replica's own ring. Only used when no database is configured, where a
    /// single replica is the whole deployment and the ring is the truth.
    pub fn rate_per_sec(&self, target: &str, lane: &str, now_ms: i64) -> f64 {
        let r = self.rollups.read();
        let Some(series) = r.get(target) else {
            return 0.0;
        };
        let since = minute_of(now_ms) - 5 * 60_000;
        let mut admitted = 0u64;
        let mut minutes = 0u64;
        for (t, lanes) in series.iter().rev() {
            if *t < since {
                break;
            }
            minutes += 1;
            admitted += lanes.get(lane).map_or(0, |b| b.admitted);
        }
        if minutes == 0 {
            0.0
        } else {
            admitted as f64 / (minutes * 60) as f64
        }
    }

    /// The same shape `History::flow` returns, from this replica's own ring:
    /// (application, target, minute, admitted). Used only when no database is
    /// configured.
    pub fn flow(&self, minutes: usize, now_ms: i64) -> Vec<(String, String, i64, i64)> {
        let since = minute_of(now_ms) - (minutes as i64) * 60_000;
        let r = self.rollups.read();
        let mut out = Vec::new();
        for (key, series) in r.iter() {
            let (application, target) = split_key(key);
            for (t, lanes) in series.iter() {
                if *t < since {
                    continue;
                }
                let admitted: u64 = lanes.values().map(|b| b.admitted).sum();
                out.push((application.clone(), target.clone(), *t, admitted as i64));
            }
        }
        out.sort_by_key(|(_, _, t, _)| *t);
        out
    }

    /// Hand over the traces gathered since the last call. They live in memory
    /// only long enough to be batched into one write — a trace per row per
    /// decision, written in line, would put the history on the hot path.
    pub fn drain_traces(&self) -> Vec<Trace> {
        let mut t = self.traces.write();
        t.drain(..).collect()
    }

    pub fn traces(&self, outcome: Option<&str>, limit: usize) -> Vec<Trace> {
        self.traces
            .read()
            .iter()
            .rev()
            .filter(|t| outcome.is_none_or(|o| t.outcome == o))
            .take(limit)
            .cloned()
            .collect()
    }

    pub fn breaches(&self, limit: usize) -> Vec<Trace> {
        self.breaches.read().iter().rev().take(limit).cloned().collect()
    }

    /// What fraction of the target's ceiling the *other* lanes actually spent in
    /// the last complete minute. This is the number `ceiling-minus-measured`
    /// waits for: until it exists the lane runs on its declared floor.
    pub fn measured_share(&self, target: &str, except_lane: &str, ceiling_per_min: f64) -> Option<f64> {
        if ceiling_per_min <= 0.0 {
            return None;
        }
        let r = self.rollups.read();
        let series = r.get(target)?;
        // Buckets carry increments, so this is genuinely "what the other lanes
        // spent in that minute" and not a lifetime total.
        // Prefer the last COMPLETE minute, which needs no correction. Waiting
        // for one, though, leaves a lane running on its floor for up to two
        // minutes after a declare — so the current minute is used instead,
        // scaled by how much of it has elapsed. Extrapolating from a few
        // seconds is noisy early on, hence the floor on the divisor: a lane
        // that has barely started does not get to claim the whole ceiling.
        let now = crate::now_ms();
        if let Some((_, lanes)) = series.iter().rev().nth(1) {
            let others: u64 = lanes
                .iter()
                .filter(|(name, _)| name.as_str() != except_lane)
                .map(|(_, b)| b.admitted)
                .sum();
            return Some((others as f64 / ceiling_per_min).clamp(0.0, 1.0));
        }
        let (start, lanes) = series.back()?;
        let elapsed_frac = (((now - start) as f64) / 60_000.0).clamp(0.15, 1.0);
        let others: u64 = lanes
            .iter()
            .filter(|(name, _)| name.as_str() != except_lane)
            .map(|(_, b)| b.admitted)
            .sum();
        Some((others as f64 / (ceiling_per_min * elapsed_frac)).clamp(0.0, 1.0))
    }
}

/// The event an ack writes to `gate.{t}.calls`.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct CallEvent {
    pub target: String,
    pub lane: String,
    #[serde(default)]
    pub op: String,
    /// The REAL number of HTTP calls the work produced. At push time this was an
    /// estimate; this is what lets the meter correct the model.
    pub calls: u64,
    #[serde(default)]
    pub cost_estimated: f64,
    pub outcome: String,
    #[serde(default)]
    pub at: i64,
}

/// Drain `gate.{t}.calls` into the meter, and keep the derived lane caps fresh.
///
/// Deliberately a separate consumer with its own small concurrency: the design
/// says observability writes must never sit in line with an ack, because they
/// would then compete with the message path for the same database.
pub fn spawn(
    queen: Queen,
    meter: Arc<Meter>,
    history: Option<Arc<crate::history::History>>,
    target: Arc<TargetRuntime>,
) {
    let queue = target.spec.calls_queue();
    let cancel = queen_mq::Cancel::new();
    *target.meter_cancel.write() = Some(cancel.clone());

    tokio::spawn(async move {
        loop {
            if cancel.is_cancelled() {
                return;
            }
            let msgs = match queen
                .queue(&queue)
                .group("gate.meter")
                .batch(200)
                .wait(true)
                .poll_timeout(std::time::Duration::from_millis(1000))
                .pop()
                .await
            {
                Ok(m) => m,
                Err(_) => {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    continue;
                }
            };

            let now = crate::now_ms();
            let key = target.spec.key();
            for m in &msgs {
                if let Ok(ev) = serde_json::from_value::<CallEvent>(m.data.clone()) {
                    meter.record(&key, &ev.lane, &ev, if ev.at > 0 { ev.at } else { now });
                }
            }
            if !msgs.is_empty() {
                let _ = queen.ack_all(&msgs).await;
            }

            // Persist any minute that has closed. Two replicas writing the
            // same minute is the normal case and not a race: they saw different
            // halves of the traffic, and the row is the sum.
            if let Some(h) = history.as_ref() {
                while let Some((t, lanes)) = meter.take_closed(&key, now) {
                    h.add(&target.spec.application, &target.spec.name, t, &lanes).await;
                }
                h.add_traces(&meter.drain_traces()).await;
            }

            // Keep the cross-target leases topped up. Off the hot path by
            // construction: the gate only ever spends what is already local.
            for pool in &target.pools {
                pool.top_up(&queen, now).await;
            }

            // Fold in the gate's own counters and refresh the derived caps.
            let ceiling_per_min = target
                .spec
                .budgets
                .iter()
                .map(|b| b.rate_per_sec() * 60.0)
                .fold(f64::INFINITY, f64::min);
            for (name, lane) in &target.lanes {
                let (a, d) = {
                    let s = lane.stats.read();
                    (s.admitted, s.denied)
                };
                meter.observe_gate(&key, name, a, d, now);
                if matches!(
                    target.spec.lane(name).map(|l| &l.cap),
                    Some(gate_core::CapPolicy::CeilingMinusMeasured)
                ) {
                    // The table when there is one, because every replica must
                    // derive the same cap from the same numbers; this replica's
                    // ring only when there is no table, where one replica is
                    // the whole deployment anyway.
                    let m = match history.as_ref() {
                        Some(h) => {
                            h.measured_share(
                                &target.spec.application,
                                &target.spec.name,
                                name,
                                ceiling_per_min,
                                now,
                            )
                            .await
                        }
                        None => meter.measured_share(&key, name, ceiling_per_min),
                    };
                    *lane.measured_share.write() = m;
                }
            }
        }
    });
}
