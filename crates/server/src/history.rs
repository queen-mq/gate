//! Rollups and traces, in queen's own kv.
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
//! It then became a schema in the PostgreSQL the broker used, on the argument
//! that what is worth protecting is *no second data system to run*. Queen 2
//! keeps its state in its own log and has no PostgreSQL, so the same argument
//! now points here: history lives in the namespace the counters and the
//! declared graphs already live in, and Gate needs nothing but its broker.
//!
//! kv has no `SUM` and no upsert-and-add, so the shape is not a row per minute:
//!
//! * a **segment** is ONE key holding a run of consecutive minutes for one
//!   series. Its writer keeps it in memory, appends to it and rewrites it whole,
//!   and starts another when the value passes [`SOFT_BYTES`] or the run passes
//!   the series' span. A day of an idle target is one key, not 1,440.
//! * a segment has ONE writer: this process, named by a random id in the key.
//!   Two replicas writing the same minute is still the normal case rather than
//!   a race — they saw different halves of the traffic, each writes its own
//!   key, and the READER sums them. No read before the write, no lost update.
//! * a rewrite is a replacement, so repeating a write whose answer was lost
//!   cannot count anything twice.
//! * every key carries a TTL, so retention is not a job.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use queen_mq::{Expiry, KvRow, Queen};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// One window's increments for one (node, path).
///
/// `target` is `{graph}.{node}` and `lane` is the PATH — the names are v1's,
/// unchanged, and a node IS what a target was.
#[derive(Debug, Clone, Copy, Default)]
pub struct Bucket {
    pub admitted: u64,
    pub denied: u64,
    /// v1 counted vendor calls here, through the calls queue. There is no ack in
    /// v2, so nothing counts a call: it stays in the shape and stays zero rather
    /// than being repurposed into a number that would mean something else under
    /// the same name.
    pub calls: u64,
    pub throttled: u64,
    pub cost_estimated: f64,
    pub cost_actual: f64,
}

/// Everything history writes sits under this, beside `graph:` and the counters.
pub(crate) const PREFIX: &str = "hist:";
pub(crate) const MINUTE_MS: i64 = 60_000;

/// Where a segment stops growing. It is rewritten whole on every append, so
/// this is the price of one write; and the broker refuses a value above 64 KiB
/// by default, so staying well under leaves room for the append that crosses
/// the line.
pub(crate) const SOFT_BYTES: usize = 16 * 1024;

/// The longest run of minutes one rollup segment holds, and therefore how far
/// before `since` a reader has to start.
const ROLLUP_SPAN_MS: i64 = 24 * 3_600_000;

/// The console reads at most a day back (`MAX_HISTORY_MINUTES`, and the
/// timeline's own clamp). An hour of margin; raise it with them.
pub const KEEP_SECONDS: i64 = 25 * 3600;

/// A Watch session runs for up to thirty days and its summary reads all of it.
pub const KEEP_WATCH_SECONDS: i64 = 45 * 86_400;

const KEEP_TRACES_SECONDS: i64 = 7 * 86_400;

/// What one replica keeps of its refusals. Two replicas fill the console's
/// page (`TRACE_RING`), and the whole ring is one value.
const TRACE_ROWS: usize = 250;
const TRACE_BYTES: usize = 40 * 1024;

/// Rollups change once a minute and the console polls every few seconds: a
/// read is reused for this long, so a poll costs the broker nothing and never
/// competes with the limiter for kv's read rate.
const CACHE: Duration = Duration::from_secs(5);

// The order of a stored cell. The last three are written only when one is set.
const ADMITTED: usize = 0;
const DENIED: usize = 1;
const COST_EST: usize = 2;
const CALLS: usize = 3;
const THROTTLED: usize = 4;
const COST_ACTUAL: usize = 5;

type Cell = [f64; 6];
type Minutes = BTreeMap<i64, BTreeMap<String, Cell>>;

/// A stored number: an integer when it is one, so `[4,0,8]` rather than
/// `[4.0,0.0,8.0]` in a value that is rewritten every minute.
#[derive(Debug, Clone, Copy, Default, PartialEq, Deserialize)]
#[serde(transparent)]
struct Num(f64);

impl Serialize for Num {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        if self.0.fract() == 0.0 && self.0.abs() < 9.0e15 {
            s.serialize_i64(self.0 as i64)
        } else {
            s.serialize_f64(self.0)
        }
    }
}

/// One path's minutes inside a segment, keyed by minutes since the segment's
/// first.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct Lane {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    m: BTreeMap<i64, Vec<Num>>,
    /// Inclusive runs of minutes that were measured and were zero. An idle
    /// minute is still written — the dashboard must tell it from a missing or
    /// failed write — but a night of them is one pair, not 480 entries.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    z: Vec<(i64, i64)>,
}

impl Lane {
    /// Adds, like the upsert it replaces: the same minute twice is the sum.
    fn add(&mut self, offset: i64, b: &Bucket) {
        let mut cell: Cell = [0.0; 6];
        cell[ADMITTED] = b.admitted as f64;
        cell[DENIED] = b.denied as f64;
        cell[COST_EST] = b.cost_estimated;
        cell[CALLS] = b.calls as f64;
        cell[THROTTLED] = b.throttled as f64;
        cell[COST_ACTUAL] = b.cost_actual;
        // A number JSON cannot carry would make the whole segment unreadable.
        for x in cell.iter_mut().filter(|x| !x.is_finite()) {
            *x = 0.0;
        }
        if cell.iter().all(|x| *x == 0.0) {
            if self.m.contains_key(&offset) {
                return;
            }
            match self.z.last_mut() {
                Some((_, to)) if *to + 1 == offset => *to = offset,
                Some((from, to)) if (*from..=*to).contains(&offset) => {}
                _ => self.z.push((offset, offset)),
            }
            return;
        }
        let stored = self.m.entry(offset).or_default();
        let width = if cell[CALLS..].iter().any(|x| *x != 0.0) {
            6
        } else {
            3
        };
        stored.resize(stored.len().max(width), Num(0.0));
        for (s, x) in stored.iter_mut().zip(cell) {
            s.0 += x;
        }
    }

    fn each(&self, mut f: impl FnMut(i64, Cell)) {
        for (offset, stored) in &self.m {
            let mut cell: Cell = [0.0; 6];
            for (c, s) in cell.iter_mut().zip(stored) {
                *c = s.0;
            }
            f(*offset, cell);
        }
        for (from, to) in &self.z {
            // A run is at most a segment long; anything else is not ours.
            for offset in *from..=(*to).min(from.saturating_add(ROLLUP_SPAN_MS / MINUTE_MS)) {
                if !self.m.contains_key(&offset) {
                    f(offset, [0.0; 6]);
                }
            }
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct RollupSeg {
    /// The first minute, in milliseconds.
    t0: i64,
    l: BTreeMap<String, Lane>,
}

/// Sum every writer's segments into minutes, `since <= minute < until`.
fn fold<'a>(segments: impl IntoIterator<Item = &'a RollupSeg>, since: i64, until: i64) -> Minutes {
    let mut out = Minutes::new();
    for seg in segments {
        for (lane, minutes) in &seg.l {
            minutes.each(|offset, cell| {
                let minute = seg.t0.saturating_add(offset.saturating_mul(MINUTE_MS));
                if minute >= since && minute < until {
                    let sum = out
                        .entry(minute)
                        .or_default()
                        .entry(lane.clone())
                        .or_insert([0.0; 6]);
                    for (a, x) in sum.iter_mut().zip(cell) {
                        *a += x;
                    }
                }
            });
        }
    }
    out
}

fn cell_json(minute: i64, v: &Cell) -> Value {
    json!({ "t": minute, "admitted": v[ADMITTED], "denied": v[DENIED], "calls": v[CALLS],
            "throttled": v[THROTTLED], "cost_estimated": v[COST_EST], "cost_actual": v[COST_ACTUAL] })
}

/// A time inside a key, so that keys sort by it.
pub(crate) fn pad(ms: i64) -> String {
    format!("{:013}", ms.max(0))
}

/// `hist:r:{app}:{target}:{t0}:{writer}` → `{app}:{target}`. Names are single
/// lowercase segments (`ok_name`), so a colon only ever separates.
fn series_of(key: &str) -> Option<&str> {
    let rest = key.strip_prefix(PREFIX)?.strip_prefix("r:")?;
    let mut parts = rest.rsplitn(3, ':');
    let (_writer, _t0) = (parts.next()?, parts.next()?);
    parts.next()
}

pub(crate) fn decode<'a, T: Deserialize<'a>>(row: &'a KvRow) -> Result<T, String> {
    // A value that does not decode must be visible as a failed read rather than
    // quietly shortening the result and making an incident disappear from the
    // console.
    let value = row
        .value
        .as_ref()
        .ok_or_else(|| format!("history key {} has no value", row.key))?;
    T::deserialize(value).map_err(|e| format!("history key {}: {e}", row.key))
}

/// Drop the oldest refusals until the ring is one value the broker accepts.
fn trim(ring: &mut VecDeque<Value>) {
    while ring.len() > TRACE_ROWS {
        ring.pop_front();
    }
    let size = |row: &Value| row.to_string().len() + 1;
    let mut bytes: usize = ring.iter().map(size).sum();
    while bytes > TRACE_BYTES {
        match ring.pop_front() {
            Some(row) => bytes -= size(&row),
            None => break,
        }
    }
}

struct Cached {
    at: Instant,
    /// The cursor the scan started after; `None` is the whole prefix.
    floor: Option<String>,
    rows: Arc<Vec<KvRow>>,
}

pub struct History {
    queen: Queen,
    ns: String,
    /// This process, in every key it writes. Random per start and not the pod's
    /// name: two processes that shared a name would overwrite each other's
    /// segments, and a restarted one no longer holds what it wrote before.
    pub(crate) writer: String,
    rollups: Mutex<HashMap<String, RollupSeg>>,
    pub(crate) snapshots: Mutex<HashMap<String, crate::insight_history::SnapshotSeg>>,
    pub(crate) samples: Mutex<HashMap<String, crate::insight_history::SampleSeg>>,
    traces: Mutex<VecDeque<Value>>,
    cache: Mutex<HashMap<(String, bool), Cached>>,
}

impl History {
    /// Nothing to connect and nothing to create: the broker Gate cannot run
    /// without is the store, and a namespace exists from its first key.
    pub fn new(queen: Queen) -> Self {
        let mut random = [0u8; 6];
        getrandom::fill(&mut random).expect("OS randomness for the history writer identity");
        Self {
            queen,
            ns: crate::budget::namespace(),
            writer: random.iter().map(|b| format!("{b:02x}")).collect(),
            rollups: Default::default(),
            snapshots: Default::default(),
            samples: Default::default(),
            traces: Default::default(),
            cache: Default::default(),
        }
    }

    pub(crate) fn namespace(&self) -> &str {
        &self.ns
    }

    pub(crate) fn kv(&self) -> queen_mq::Kv {
        self.queen.kv()
    }

    pub(crate) async fn put(
        &self,
        key: &str,
        value: Value,
        keep_seconds: i64,
    ) -> Result<(), String> {
        self.queen
            .kv()
            .put(&self.ns, key, value, Expiry::seconds(keep_seconds))
            .send()
            .await
            .map_err(|e| format!("history write {key}: {e}"))?;
        // Whoever reads next sees this write, not the copy from before it.
        self.cache
            .lock()
            .retain(|(prefix, _), _| !key.starts_with(prefix.as_str()));
        Ok(())
    }

    /// Every page under a prefix, starting after `after`.
    ///
    /// The broker caps a page by rows and by bytes, and its exclusive
    /// `nextAfter` cursor is the only correct way to resume one. A truncated
    /// page without a cursor is an error rather than a short answer.
    pub(crate) async fn scan(
        &self,
        prefix: &str,
        after: Option<&str>,
        keys_only: bool,
    ) -> Result<Vec<KvRow>, String> {
        let mut rows = Vec::new();
        let mut cursor = after.map(str::to_string);
        loop {
            let mut query = self.queen.kv().get_prefix(&self.ns, prefix).limit(1000);
            if keys_only {
                query = query.keys_only();
            }
            if let Some(c) = &cursor {
                query = query.after(c);
            }
            let page = query
                .send()
                .await
                .map_err(|e| format!("history read {prefix}: {e}"))?;
            let truncated = page.truncated();
            let next = page.next_after.clone();
            rows.extend(page.rows.unwrap_or_default());
            if !truncated {
                return Ok(rows);
            }
            match next {
                Some(n) if cursor.as_ref().is_none_or(|c| n > *c) => cursor = Some(n),
                _ => return Err(format!("history read {prefix}: truncated without a cursor")),
            }
        }
    }

    /// [`Self::scan`], reused for [`CACHE`]. A cached scan that started earlier
    /// answers a later one; every reader filters by time anyway.
    pub(crate) async fn scan_cached(
        &self,
        prefix: &str,
        after: Option<String>,
        keys_only: bool,
    ) -> Result<Arc<Vec<KvRow>>, String> {
        let slot = (prefix.to_string(), keys_only);
        if let Some(c) = self.cache.lock().get(&slot) {
            let covers = match (&c.floor, &after) {
                (None, _) => true,
                (Some(floor), Some(after)) => floor <= after,
                (Some(_), None) => false,
            };
            if covers && c.at.elapsed() < CACHE {
                return Ok(c.rows.clone());
            }
        }
        let rows = Arc::new(self.scan(prefix, after.as_deref(), keys_only).await?);
        let mut cache = self.cache.lock();
        if cache.len() >= 512 {
            cache.retain(|_, c| c.at.elapsed() < CACHE);
        }
        cache.insert(
            slot,
            Cached {
                at: Instant::now(),
                floor: after,
                rows: rows.clone(),
            },
        );
        Ok(rows)
    }

    /// Add one minute's increments. Two replicas writing the same minute is the
    /// normal case, not a race: they saw different halves of the traffic, each
    /// holds its own segment, and the reader sums them.
    ///
    /// `watch` keeps the segment for as long as a Watch session can need it.
    pub async fn add(
        &self,
        app: &str,
        target: &str,
        minute: i64,
        lanes: &HashMap<String, Bucket>,
        watch: bool,
    ) -> bool {
        if lanes.is_empty() {
            return true;
        }
        let minute = minute.div_euclid(MINUTE_MS) * MINUTE_MS;
        let series = format!("{app}:{target}");
        // Built on a copy and kept only once the broker has it: a write that
        // failed leaves this minute out, and the caller sends its increments
        // again with the next one.
        let mut seg = self
            .rollups
            .lock()
            .get(&series)
            .filter(|s| (minute - s.t0).abs() < ROLLUP_SPAN_MS)
            .cloned()
            .unwrap_or(RollupSeg {
                t0: minute,
                l: BTreeMap::new(),
            });
        let offset = (minute - seg.t0) / MINUTE_MS;
        for (lane, b) in lanes {
            seg.l.entry(lane.clone()).or_default().add(offset, b);
        }
        let Ok(value) = serde_json::to_value(&seg) else {
            return false;
        };
        let full = value.to_string().len() >= SOFT_BYTES;
        let key = format!("{PREFIX}r:{series}:{}:{}", pad(seg.t0), self.writer);
        let keep = if watch {
            KEEP_WATCH_SECONDS
        } else {
            KEEP_SECONDS
        };
        if let Err(error) = self.put(&key, value, keep).await {
            tracing::warn!(%error, "could not write a rollup segment");
            return false;
        }
        let mut held = self.rollups.lock();
        if full {
            held.remove(&series);
        } else {
            held.insert(series, seg);
        }
        true
    }

    /// One series' minutes from `since`, summed across every writer.
    async fn minutes(&self, series: &str, since: i64, until: i64) -> Result<Minutes, String> {
        let prefix = format!("{PREFIX}r:{series}:");
        // The rate, the measured cost and the charts each ask for a different
        // part of the last day, in the same poll. Starting them all a day back
        // lets one read of a series answer all of them.
        let floor = since.min(crate::now_ms() - 24 * 60 * MINUTE_MS);
        // A segment that started more than a span before that holds nothing
        // this late.
        let after = format!("{prefix}{}", pad(floor.saturating_sub(ROLLUP_SPAN_MS)));
        let rows = self.scan_cached(&prefix, Some(after), false).await?;
        let mut segments = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            segments.push(decode::<RollupSeg>(row)?);
        }
        Ok(fold(&segments, since, until))
    }

    pub async fn rollups(
        &self,
        app: &str,
        target: &str,
        minutes: i64,
    ) -> Result<Vec<Value>, String> {
        let since = (crate::now_ms() / MINUTE_MS * MINUTE_MS) - minutes * MINUTE_MS;
        let data = self
            .minutes(&format!("{app}:{target}"), since, i64::MAX)
            .await?;
        Ok(data
            .into_iter()
            .map(|(minute, lanes)| {
                let mut total: Cell = [0.0; 6];
                let lanes: serde_json::Map<String, Value> = lanes
                    .into_iter()
                    .map(|(lane, cell)| {
                        for (t, x) in total.iter_mut().zip(cell) {
                            *t += x;
                        }
                        (lane, cell_json(minute, &cell))
                    })
                    .collect();
                json!({ "t": minute, "total": cell_json(minute, &total), "lanes": lanes })
            })
            .collect())
    }

    /// Completed minute buckets for one node, summed across its paths.
    /// Bounds exclude partial minutes and traffic preceding this watch session.
    pub async fn watch_minutes(
        &self,
        app: &str,
        target: &str,
        since: i64,
        until: i64,
    ) -> Result<Vec<(i64, f64)>, String> {
        let data = self
            .minutes(&format!("{app}:{target}"), since, until)
            .await?;
        Ok(data
            .values()
            .map(|lanes| {
                (
                    lanes.values().map(|c| c[ADMITTED]).sum::<f64>() as i64,
                    lanes.values().map(|c| c[COST_EST]).sum::<f64>(),
                )
            })
            .collect())
    }

    /// Admissions per second for one lane, from what every replica wrote rather
    /// than from whatever this one happened to see.
    pub async fn rate_per_sec(
        &self,
        app: &str,
        target: &str,
        lane: &str,
        now_ms: i64,
    ) -> Result<f64, String> {
        let current = now_ms / MINUTE_MS * MINUTE_MS;
        let data = self
            .minutes(&format!("{app}:{target}"), current - MINUTE_MS, i64::MAX)
            .await?;
        let newest: Vec<(i64, f64)> = data
            .iter()
            .rev()
            .filter_map(|(minute, lanes)| lanes.get(lane).map(|c| (*minute, c[ADMITTED])))
            .take(2)
            .collect();
        for (minute, admitted) in &newest {
            // A complete minute needs no correction. Only fall back to the one
            // still filling — scaled, with a floor on the divisor — when there
            // is no complete one yet, which is exactly the first minute after a
            // declare and exactly when somebody is watching.
            if *minute < current {
                return Ok(admitted / 60.0);
            }
        }
        Ok(match newest.first() {
            Some((_, admitted)) => {
                let elapsed = (((now_ms - current) as f64) / 1000.0).max(5.0);
                admitted / elapsed
            }
            None => 0.0,
        })
    }

    /// What one item of this lane charges a budget, measured over the last few
    /// minutes: declared cost over items admitted.
    ///
    /// From what the fleet wrote rather than a replica's own counters: a replica
    /// holds its own share of both, and the sum of the shares is the ratio.
    pub async fn avg_cost(&self, app: &str, target: &str, lane: &str, now_ms: i64) -> Option<f64> {
        let current = now_ms / MINUTE_MS * MINUTE_MS;
        let data = self
            .minutes(
                &format!("{app}:{target}"),
                current - 5 * MINUTE_MS,
                i64::MAX,
            )
            .await
            .ok()?;
        let (mut cost, mut items) = (0.0, 0.0);
        for cell in data.values().filter_map(|lanes| lanes.get(lane)) {
            cost += cell[COST_EST];
            items += cell[ADMITTED];
        }
        // Nothing admitted yet says nothing about cost, and a zero here would say
        // the backlog is free. The caller falls back to the declared default.
        if items <= 0.0 || cost <= 0.0 {
            return None;
        }
        Some(cost / items)
    }

    /// Admissions and admitted cost per minute for every target, over the last
    /// `minutes`.
    ///
    /// The series come from the keys, not from the registry: a target that was
    /// deleted an hour ago still has an hour on the chart.
    pub async fn flow(
        &self,
        minutes: i64,
        now_ms: i64,
    ) -> Result<Vec<(String, String, i64, i64, f64)>, String> {
        let since = now_ms / MINUTE_MS * MINUTE_MS - minutes * MINUTE_MS;
        let keys = self.scan_cached(&format!("{PREFIX}r:"), None, true).await?;
        let series: BTreeSet<&str> = keys.iter().filter_map(|row| series_of(&row.key)).collect();
        let mut out = Vec::new();
        for s in series {
            let Some((application, target)) = s.split_once(':') else {
                continue;
            };
            for (minute, lanes) in self.minutes(s, since, i64::MAX).await? {
                let admitted: f64 = lanes.values().map(|c| c[ADMITTED]).sum();
                let cost: f64 = lanes.values().map(|c| c[COST_EST]).sum();
                out.push((
                    application.to_string(),
                    target.to_string(),
                    minute,
                    admitted as i64,
                    // A graph that declares no cost charges one per item.
                    if cost != 0.0 { cost } else { admitted },
                ));
            }
        }
        out.sort_by_key(|row| row.2);
        Ok(out)
    }

    /// The newest refusals of this replica, as one value: the ones just drained
    /// from the ring behind the ones it already wrote.
    pub async fn add_traces(&self, rows: &[crate::obs::Trace]) -> bool {
        if rows.is_empty() {
            return true;
        }
        let mut ring = self.traces.lock().clone();
        for t in rows {
            ring.push_back(json!([
                t.at,
                t.application,
                format!("{}.{}", t.graph, t.node),
                t.path,
                t.op,
                t.outcome,
                t.budget_id,
            ]));
        }
        trim(&mut ring);
        let key = format!("{PREFIX}t:{}", self.writer);
        if let Err(error) = self
            .put(&key, json!({ "rows": ring }), KEEP_TRACES_SECONDS)
            .await
        {
            tracing::warn!(%error, "could not write the refusal ring");
            return false;
        }
        *self.traces.lock() = ring;
        true
    }

    pub async fn traces(&self, outcome: Option<&str>, limit: i64) -> Result<Vec<Value>, String> {
        let rings = self
            .scan_cached(&format!("{PREFIX}t:"), None, false)
            .await?;
        let mut all: Vec<(i64, &Value)> = Vec::new();
        for ring in rings.iter() {
            let rows = ring
                .value
                .as_ref()
                .and_then(|v| v["rows"].as_array())
                .ok_or_else(|| format!("history key {}: not a refusal ring", ring.key))?;
            for row in rows {
                let at = row[0]
                    .as_i64()
                    .ok_or_else(|| format!("history key {}: a refusal without a time", ring.key))?;
                if outcome.is_none_or(|o| row[5] == o) {
                    all.push((at, row));
                }
            }
        }
        all.sort_by_key(|(at, _)| std::cmp::Reverse(*at));
        Ok(all
            .into_iter()
            .take(limit.max(0) as usize)
            .map(|(at, row)| {
                json!({ "at": at, "application": row[1], "target": row[2], "lane": row[3],
                        "op": row[4], "outcome": row[5], "budget_id": row[6], "calls": 0 })
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket(admitted: u64, denied: u64, cost: f64) -> Bucket {
        Bucket {
            admitted,
            denied,
            cost_estimated: cost,
            ..Bucket::default()
        }
    }

    #[test]
    fn an_idle_night_is_one_pair_and_still_reads_as_measured_zeros() {
        let mut lane = Lane::default();
        for offset in 0..480 {
            lane.add(offset, &Bucket::default());
        }
        lane.add(480, &bucket(4, 1, 8.0));
        lane.add(481, &Bucket::default());
        assert_eq!(lane.z, vec![(0, 479), (481, 481)]);
        assert_eq!(
            serde_json::to_string(&lane).unwrap(),
            r#"{"m":{"480":[4,1,8]},"z":[[0,479],[481,481]]}"#
        );
        let mut seen = BTreeMap::new();
        lane.each(|offset, cell| {
            seen.insert(offset, cell);
        });
        assert_eq!(seen.len(), 482, "every written minute is present");
        assert_eq!(seen[&0], [0.0; 6]);
        assert_eq!(seen[&480][ADMITTED], 4.0);
    }

    #[test]
    fn the_same_minute_twice_is_the_sum_and_a_later_zero_changes_nothing() {
        let mut lane = Lane::default();
        lane.add(3, &bucket(2, 0, 2.5));
        lane.add(3, &bucket(3, 1, 0.5));
        lane.add(3, &Bucket::default());
        assert_eq!(lane.m[&3], vec![Num(5.0), Num(1.0), Num(3.0)]);
        assert!(lane.z.is_empty());
        lane.add(
            4,
            &Bucket {
                calls: 7,
                ..Bucket::default()
            },
        );
        assert_eq!(
            lane.m[&4].len(),
            6,
            "the rare fields widen only their own cell"
        );
    }

    #[test]
    fn two_writers_minutes_are_summed_and_the_window_is_half_open() {
        let segment = |t0: i64, cells: &[(i64, Bucket)]| {
            let mut seg = RollupSeg {
                t0,
                l: BTreeMap::new(),
            };
            for (offset, b) in cells {
                seg.l.entry("main".into()).or_default().add(*offset, b);
            }
            // Through JSON, as a reader gets it.
            serde_json::from_value::<RollupSeg>(serde_json::to_value(&seg).unwrap()).unwrap()
        };
        let a = segment(600_000, &[(0, bucket(4, 0, 8.0)), (1, bucket(1, 0, 1.0))]);
        let b = segment(660_000, &[(0, bucket(2, 3, 2.0)), (1, Bucket::default())]);
        let all = fold([&a, &b], 660_000, 780_000);
        assert_eq!(
            all.keys().copied().collect::<Vec<_>>(),
            vec![660_000, 720_000]
        );
        assert_eq!(all[&660_000]["main"][ADMITTED], 3.0);
        assert_eq!(all[&660_000]["main"][DENIED], 3.0);
        assert_eq!(all[&660_000]["main"][COST_EST], 3.0);
        assert_eq!(all[&720_000]["main"], [0.0; 6], "a measured zero is a row");
        assert!(fold([&a, &b], 0, 660_000).contains_key(&600_000));
    }

    #[test]
    fn a_busy_day_of_one_path_fits_under_the_value_ceiling() {
        let mut seg = RollupSeg::default();
        let lane = seg.l.entry("main".into()).or_default();
        for offset in 0..600 {
            lane.add(offset, &bucket(12_345, 678, 12_345.0));
        }
        let bytes = serde_json::to_string(&seg).unwrap().len();
        assert!(bytes < 2 * SOFT_BYTES, "{bytes} bytes for ten busy hours");
    }

    #[test]
    fn the_series_is_read_back_from_a_key() {
        assert_eq!(
            series_of("hist:r:channel-go:airbnb.limit:0001760000000:a1b2c3d4e5f6"),
            Some("channel-go:airbnb.limit")
        );
        assert_eq!(series_of("hist:t:a1b2c3d4e5f6"), None);
        assert_eq!(series_of("graph:channel-go:airbnb"), None);
        assert_eq!(pad(-5), "0000000000000");
        assert!(pad(1_760_000_000_000) < pad(1_760_000_060_000));
    }

    #[test]
    fn the_refusal_ring_drops_its_oldest_by_count_and_by_bytes() {
        let mut ring: VecDeque<Value> = (0..TRACE_ROWS as i64 + 10)
            .map(|at| json!([at, "app", "g.n", "main", "", "denied", null]))
            .collect();
        trim(&mut ring);
        assert_eq!(ring.len(), TRACE_ROWS);
        assert_eq!(ring.front().unwrap()[0], 10);

        let fat = "x".repeat(1024);
        let mut ring: VecDeque<Value> = (0..100)
            .map(|at| json!([at, "app", "g.n", "main", fat, "denied", null]))
            .collect();
        trim(&mut ring);
        assert!(json!({ "rows": ring }).to_string().len() <= TRACE_BYTES + 64);
        assert_eq!(ring.back().unwrap()[0], 99, "the newest survive");
    }
}
