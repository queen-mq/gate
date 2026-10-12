//! Watch samples, queue samples and configuration events, as kv segments.
//! The shape and its reasons are in [`crate::history`].
use crate::history::{decode, pad, History, KEEP_SECONDS, MINUTE_MS, PREFIX, SOFT_BYTES};
use crate::observation::{Observation, Sample};
use queen_mq::Expiry;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

/// A simulation reads the last day of a session, and people come back to a
/// finished Watch after a weekend, not after a month.
const KEEP_SAMPLES_SECONDS: i64 = 7 * 86_400;
/// One segment of per-second samples: at most this many seconds with traffic,
/// and never a longer stretch than the span.
const SAMPLE_ROWS: usize = 600;
const SAMPLE_SPAN_S: i64 = 3600;
const SNAPSHOT_SPAN_MS: i64 = 6 * 3_600_000;
/// `positions` has one entry per partition. Past this the sample is kept
/// without it, and the console shows the incoming rate as unknown, which is
/// what it already does for a node that is not a single ingress.
const SNAPSHOT_BODY_BYTES: usize = 4096;

/// A recorder's seconds, keyed by seconds since the segment's first.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct SampleSeg {
    t0: i64,
    s: BTreeMap<i64, (i64, i64)>,
}

/// Lay pending seconds onto the segment a recorder is filling. Returns the
/// segments to write, oldest first, and the one to keep filling.
///
/// Replacement, not addition: retrying an ambiguous write is idempotent.
fn pack(
    mut current: Option<SampleSeg>,
    rows: &[(i64, Sample)],
) -> (Vec<SampleSeg>, Option<SampleSeg>) {
    let mut writes = Vec::new();
    let mut dirty = false;
    for (second, sample) in rows {
        let full = current.as_ref().is_some_and(|seg| {
            let offset = (second - seg.t0) / 1000;
            offset.abs() >= SAMPLE_SPAN_S
                || (seg.s.len() >= SAMPLE_ROWS && !seg.s.contains_key(&offset))
        });
        if full {
            // An untouched segment is already in the broker as it is.
            if let Some(seg) = current.take().filter(|_| dirty) {
                writes.push(seg);
            }
        }
        let seg = current.get_or_insert_with(|| SampleSeg {
            t0: *second,
            s: BTreeMap::new(),
        });
        seg.s
            .insert((second - seg.t0) / 1000, (sample.items, sample.cost));
        dirty = true;
    }
    if dirty {
        writes.extend(current.clone());
    }
    (writes, current)
}

/// A node's queue samples: runs of consecutive minutes that read the same.
/// `(first, last, at of the last, body)`, in minutes since the segment's first.
/// An empty queue reads the same all night, and that is one run.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct SnapshotSeg {
    t0: i64,
    r: Vec<(i64, i64, i64, Value)>,
}

impl SnapshotSeg {
    fn push(&mut self, offset: i64, at: i64, body: Value) {
        match self.r.last_mut() {
            // The same minute again: the later reading wins.
            Some(run) if run.1 == offset => {
                if at <= run.2 {
                    return;
                }
                if run.0 == run.1 {
                    (run.2, run.3) = (at, body);
                } else {
                    run.1 -= 1;
                    self.r.push((offset, offset, at, body));
                }
            }
            Some(run) if run.1 + 1 == offset && run.3 == body => (run.1, run.2) = (offset, at),
            _ => self.r.push((offset, offset, at, body)),
        }
    }

    /// Every minute with its reading. Only the last minute of a run kept its
    /// exact time; the others are given the same second of their own minute,
    /// which is when the sampler ran.
    fn each(&self, mut f: impl FnMut(i64, i64, &Value)) {
        for (first, last, at, body) in &self.r {
            for offset in *first..=(*last).min(first.saturating_add(SNAPSHOT_SPAN_MS / MINUTE_MS)) {
                let minute = self.t0.saturating_add(offset.saturating_mul(MINUTE_MS));
                let at = if offset == *last {
                    *at
                } else {
                    minute + at.rem_euclid(MINUTE_MS)
                };
                f(minute, at, body);
            }
        }
    }
}

fn fit(body: &Value) -> Value {
    let mut body = body.clone();
    if body.to_string().len() > SNAPSHOT_BODY_BYTES && body.get("positions").is_some() {
        body["positions"] = Value::Null;
    }
    body
}

impl History {
    pub async fn flush_observation(&self, rec: &Observation) -> Result<(), String> {
        let now = crate::now_ms();
        let through = rec.through(now);
        let rows = rec.pending(now / 1000 * 1000);
        let root = format!(
            "{}:{}:{}:{}",
            rec.application,
            rec.graph,
            rec.node,
            pad(rec.session)
        );
        let held = self.samples.lock().get(&rec.id).cloned();
        let (writes, current) = pack(held, &rows);
        let op = |key: String, value: Value| {
            self.kv()
                .put(
                    self.namespace(),
                    &key,
                    value,
                    Expiry::seconds(KEEP_SAMPLES_SECONDS),
                )
                .operation()
                .map_err(|e| e.to_string())
        };
        let mut ops = Vec::with_capacity(writes.len() + 1);
        for seg in &writes {
            ops.push(op(
                format!("{PREFIX}w:{root}:{}:{}", pad(seg.t0), rec.id),
                serde_json::to_value(seg).map_err(|e| e.to_string())?,
            )?);
        }
        // What this recorder covers, in the same call as the seconds it covers.
        ops.push(op(
            format!("{PREFIX}o:{root}:{}", rec.id),
            json!([
                rec.started_at,
                through,
                rec.lost.load(Ordering::Relaxed).min(i64::MAX as u64) as i64
            ]),
        )?);
        self.kv().batch(ops).await.map_err(|e| e.to_string())?;
        rec.acknowledge(&rows);
        let done = (rec.closed_at.load(Ordering::Relaxed) > 0 || now >= rec.until)
            && rec.pending(i64::MAX).is_empty();
        let mut samples = self.samples.lock();
        match current {
            Some(seg) if !done => samples.insert(rec.id.clone(), seg),
            _ => samples.remove(&rec.id),
        };
        Ok(())
    }

    pub async fn observation_data(
        &self,
        app: &str,
        graph: &str,
        node: &str,
        session: i64,
        since: i64,
        until: i64,
    ) -> Result<(Vec<(i64, Sample)>, Value), String> {
        let root = format!("{app}:{graph}:{node}:{}", pad(session));
        let mut coverage = Vec::new();
        for row in self
            .scan(&format!("{PREFIX}o:{root}:"), None, false)
            .await?
        {
            let (started, through, lost): (i64, i64, i64) = decode(&row)?;
            if through >= started {
                coverage.push((started, through, lost));
            }
        }
        coverage.sort();
        let spans: Vec<(i64, i64)> = coverage.iter().map(|r| (r.0, r.1)).collect();
        let start = spans
            .iter()
            .map(|(a, _)| *a)
            .min()
            .unwrap_or(until)
            .max(since)
            .saturating_add(999)
            / 1000
            * 1000;
        let end = spans
            .iter()
            .map(|(_, b)| *b)
            .max()
            .unwrap_or(start)
            .min(until)
            .max(start);
        // Union coverage: any known missing interval is reported, never filled as idle.
        let mut covered_until = start;
        let mut gaps = 0i64;
        for (a, b) in spans {
            let a = a.saturating_add(999) / 1000 * 1000;
            if a > covered_until {
                gaps += (a.min(end) - covered_until).max(0);
            }
            covered_until = covered_until.max(b.min(end));
        }
        gaps += (end - covered_until).max(0);

        let prefix = format!("{PREFIX}w:{root}:");
        let after = format!(
            "{prefix}{}",
            pad(start.saturating_sub(SAMPLE_SPAN_S * 1000))
        );
        let mut seconds: BTreeMap<i64, Sample> = BTreeMap::new();
        for row in self.scan(&prefix, Some(&after), false).await? {
            let seg: SampleSeg = decode(&row)?;
            for (offset, (items, cost)) in &seg.s {
                let second = seg.t0.saturating_add(offset.saturating_mul(1000));
                if second >= start && second < end {
                    let sum = seconds.entry(second).or_default();
                    sum.items = sum.items.saturating_add(*items);
                    sum.cost = sum.cost.saturating_add(*cost);
                }
            }
        }
        let lost: i64 = coverage.iter().map(|r| r.2).sum();
        Ok((
            seconds.into_iter().collect(),
            json!({"since":start,"until":end,"gapSeconds":gaps/1000,"lostItems":lost,"resolutionSeconds":1}),
        ))
    }

    pub async fn snapshot(
        &self,
        app: &str,
        graph: &str,
        node: &str,
        at: i64,
        body: &Value,
    ) -> Result<(), String> {
        let minute = at.div_euclid(MINUTE_MS) * MINUTE_MS;
        let series = format!("{app}:{graph}:{node}");
        let mut seg = self
            .snapshots
            .lock()
            .get(&series)
            .filter(|s| (minute - s.t0).abs() < SNAPSHOT_SPAN_MS)
            .cloned()
            .unwrap_or(SnapshotSeg {
                t0: minute,
                r: Vec::new(),
            });
        seg.push((minute - seg.t0) / MINUTE_MS, at, fit(body));
        let value = serde_json::to_value(&seg).map_err(|e| e.to_string())?;
        let full = value.to_string().len() >= SOFT_BYTES;
        // Time before node: the timeline reads a whole graph from a minute on.
        let key = format!(
            "{PREFIX}s:{app}:{graph}:{}:{node}:{}",
            pad(seg.t0),
            self.writer
        );
        self.put(&key, value, KEEP_SECONDS).await?;
        let mut held = self.snapshots.lock();
        if full {
            held.remove(&series);
        } else {
            held.insert(series, seg);
        }
        Ok(())
    }

    pub async fn config_event(
        &self,
        doc: &gate_core::GraphDoc,
        previous: Option<&gate_core::GraphDoc>,
    ) -> Result<(), String> {
        if previous == Some(doc) {
            return Ok(());
        }
        let label = match previous {
            None => "Created",
            Some(old) if old.watch.is_some() != doc.watch.is_some() => {
                if doc.watch.is_some() {
                    "Watch started"
                } else {
                    "Limits activated"
                }
            }
            _ => "Configuration updated",
        };
        let body = json!({"label":label,"version":doc.version,"mode":if doc.watch.is_some() {"watch"} else {"enforce"}});
        let key = format!(
            "{PREFIX}c:{}:{}:{}:{}",
            doc.application,
            doc.graph,
            pad(crate::now_ms()),
            self.writer
        );
        self.put(&key, body, KEEP_SECONDS).await
    }

    pub async fn insights(&self, app: &str, graph: &str, minutes: i64) -> Result<Value, String> {
        let since = crate::now_ms() - minutes * MINUTE_MS;
        let prefix = format!("{PREFIX}s:{app}:{graph}:");
        let after = format!("{prefix}{}", pad(since.saturating_sub(SNAPSHOT_SPAN_MS)));
        let rows = self.scan_cached(&prefix, Some(after), false).await?;
        // Every replica samples every node each minute; the later reading wins.
        let mut newest: BTreeMap<(i64, String), (i64, Value)> = BTreeMap::new();
        for row in rows.iter() {
            let node = row.key.rsplit(':').nth(1).unwrap_or_default();
            decode::<SnapshotSeg>(row)?.each(|minute, at, body| {
                if minute < since {
                    return;
                }
                match newest.get(&(minute, node.to_string())) {
                    Some((seen, _)) if *seen >= at => {}
                    _ => {
                        newest.insert((minute, node.to_string()), (at, body.clone()));
                    }
                }
            });
        }
        let prefix = format!("{PREFIX}c:{app}:{graph}:");
        let after = format!("{prefix}{}", pad(since));
        let rows = self.scan_cached(&prefix, Some(after), false).await?;
        let mut events: Vec<(i64, &Value)> = rows
            .iter()
            .filter_map(|row| {
                let at = row.key.rsplit(':').nth(1)?.parse::<i64>().ok()?;
                Some((at, row.value.as_ref()?))
            })
            .filter(|(at, _)| *at >= since)
            .collect();
        events.sort_by_key(|(at, _)| std::cmp::Reverse(*at));
        events.truncate(200);
        Ok(
            json!({"samples":newest.iter().map(|((minute,node),(at,body))| json!({"node":node,"t":minute,"at":at,"value":body})).collect::<Vec<_>>(),"events":events.iter().map(|(at,body)| json!({"at":at,"change":body})).collect::<Vec<_>>()}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(items: i64) -> Sample {
        Sample { items, cost: items }
    }

    #[test]
    fn a_resent_second_replaces_itself_and_a_quiet_flush_writes_nothing() {
        let (writes, held) = pack(None, &[(5000, sample(2)), (6000, sample(3))]);
        assert_eq!(writes.len(), 1);
        let (again, same) = pack(held.clone(), &[(6000, sample(3))]);
        assert_eq!(again, writes, "the same seconds are the same value");
        assert_eq!(same, held);
        let (none, kept) = pack(held.clone(), &[]);
        assert!(none.is_empty());
        assert_eq!(kept, held);
    }

    #[test]
    fn an_outage_backlog_is_split_into_segments_that_each_fit() {
        let rows: Vec<_> = (0..1500).map(|s| (s * 1000, sample(1))).collect();
        let (writes, held) = pack(None, &rows);
        assert_eq!(writes.len(), 3);
        assert_eq!(
            writes.iter().map(|w| w.s.len()).collect::<Vec<_>>(),
            vec![SAMPLE_ROWS, SAMPLE_ROWS, 300]
        );
        assert_eq!(writes[1].t0, 600_000);
        assert_eq!(held.as_ref(), writes.last());
        assert!(writes
            .iter()
            .all(|w| serde_json::to_string(w).unwrap().len() < SOFT_BYTES));

        // A second past the span starts a segment even when the last has room.
        let (writes, held) = pack(held, &[(1_500_000 + SAMPLE_SPAN_S * 1000, sample(1))]);
        assert_eq!(
            writes.len(),
            1,
            "the untouched segment is not written again"
        );
        assert_eq!(held.unwrap().s.len(), 1);
    }

    #[test]
    fn an_unchanged_queue_is_one_run_and_every_minute_reads_back() {
        let idle = json!({"available":true,"before":0,"workers":0});
        let mut seg = SnapshotSeg::default();
        for minute in 0..180 {
            seg.push(minute, minute * MINUTE_MS + 4000, idle.clone());
        }
        seg.push(
            180,
            180 * MINUTE_MS + 4100,
            json!({"available":true,"before":7,"workers":0}),
        );
        assert_eq!(seg.r.len(), 2);
        let mut seen = Vec::new();
        seg.each(|minute, at, body| seen.push((minute, at, body["before"].clone())));
        assert_eq!(seen.len(), 181);
        assert_eq!(seen[10], (10 * MINUTE_MS, 10 * MINUTE_MS + 4000, json!(0)));
        assert_eq!(
            seen[180],
            (180 * MINUTE_MS, 180 * MINUTE_MS + 4100, json!(7))
        );
    }

    #[test]
    fn the_same_minute_again_keeps_the_later_reading() {
        let (a, b) = (json!({"before":1}), json!({"before":2}));
        let mut seg = SnapshotSeg::default();
        seg.push(0, 1000, a.clone());
        seg.push(1, 61_000, a.clone());
        seg.push(1, 62_000, b.clone());
        seg.push(1, 61_500, a.clone());
        assert_eq!(seg.r, vec![(0, 0, 61_000, a), (1, 1, 62_000, b)]);
    }

    #[test]
    fn a_node_with_many_partitions_is_kept_without_its_positions() {
        let positions: serde_json::Map<String, Value> = (0..400)
            .map(|p| (format!("queue/group/partition-{p}"), json!(p)))
            .collect();
        let body = json!({"available":true,"before":3,"positions":positions});
        let kept = fit(&body);
        assert_eq!(kept["before"], 3);
        assert!(kept["positions"].is_null());
        let small = json!({"available":true,"positions":{"q/g/p0":4}});
        assert_eq!(fit(&small), small);
    }
}
