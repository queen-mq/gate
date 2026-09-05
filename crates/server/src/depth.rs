//! How much work is waiting, and what it is waiting for.
//!
//! Two backlogs, and a product surface has to tell them apart because they have
//! different owners:
//!
//! * **waiting for budget** — pending on the push queue. Gate is holding this
//!   back on purpose; the vendor's ceiling is the reason and there is nothing
//!   the caller can do to speed it up.
//! * **waiting for workers** — pending on the admitted queue. Gate has already
//!   said yes; this is the caller's own consumers not keeping up, and adding
//!   concurrency fixes it.
//!
//! Reporting one number for both would tell a hotel that its prices are late
//! because of Airbnb when the truth is that channel-manager is short of workers.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use queen_mq::Queen;

/// Depth is read from the broker, so it costs a round trip per queue. A caller
/// polling this every few seconds would multiply that by the number of targets,
/// so the answer is held briefly — two seconds is shorter than any sane poll
/// interval and long enough to collapse a burst of them.
const TTL: Duration = Duration::from_secs(2);

type Depth = HashMap<String, u64>;

#[derive(Clone)]
enum Cached {
    Value(Depth),
    Unavailable(String),
}

#[derive(Default)]
pub struct Depths {
    // One map, keyed by queue (or `queue\u{1f}group`), holding either the last
    // answer or a recent failure. Failures are cached too: reporting an outage
    // honestly must not turn a page with a dozen graphs into an admin-API retry
    // storm.
    cache: RwLock<HashMap<String, (Cached, Instant)>>,
}

impl Depths {
    fn cached(&self, key: &str) -> Option<queen_mq::Result<Depth>> {
        let cache = self.cache.read();
        let (entry, at) = cache.get(key)?;
        if at.elapsed() >= TTL {
            return None;
        }
        Some(match entry {
            Cached::Value(value) => Ok(value.clone()),
            Cached::Unavailable(error) => Err(queen_mq::Error::Network(format!(
                "cached depth read failure: {error}"
            ))),
        })
    }

    fn remember_value(&self, key: &str, value: &Depth) {
        self.cache.write().insert(
            key.to_string(),
            (Cached::Value(value.clone()), Instant::now()),
        );
    }

    fn remember_failure(&self, key: &str, error: &queen_mq::Error) {
        self.cache.write().insert(
            key.to_string(),
            (Cached::Unavailable(error.to_string()), Instant::now()),
        );
    }

    /// Pending count per partition of one queue. A confirmed absent queue is
    /// zero; a broker that did not answer is an error. Both successful answers
    /// and failures are held briefly.
    pub async fn pending(&self, queen: &Queen, queue: &str) -> queen_mq::Result<Depth> {
        if let Some(cached) = self.cached(queue) {
            return cached;
        }
        self.try_pending_now(queen, queue).await
    }

    /// The same read with the cache skipped, and the answer left in it.
    ///
    /// The merge relay bounds the destination queue's depth, and it forwards on
    /// every loop — a couple of hundred milliseconds. A two-second-old depth would
    /// let it overshoot the window by everything it forwarded in the meantime,
    /// which is the one number the window exists to hold down.
    pub async fn pending_now(&self, queen: &Queen, queue: &str) -> queen_mq::Result<Depth> {
        self.try_pending_now(queen, queue).await
    }

    /// The same read, with the failure kept.
    ///
    /// A caller that BOUNDS something on the answer must be able to tell "nothing is
    /// waiting" from "the broker did not say". The merge relay is one: reading a
    /// failed depth as zero would let it forward a full window on every loop, for as
    /// long as the admin API is unhappy, and the queue it is supposed to keep shallow
    /// would grow without a bound.
    pub async fn try_pending_now(&self, queen: &Queen, queue: &str) -> queen_mq::Result<Depth> {
        // The depth route first (broker >= 1.0.4): watermark arithmetic only —
        // measured at ~1ms on a gate-sized queue, against two console-grade
        // queries for the detail below. No group, on purpose: queue-level
        // pending under the worst-cursor precedence is exactly what the old
        // detail reported, so the relay's window bound does not move.
        let result = match queen.admin().queue_depth(queue, None).await {
            Ok(v) => depth_route(&v),
            // A 404 here is BOTH "this broker predates the route" and "no such
            // queue", and they cannot be told apart. The queue detail below
            // answers both the same way this function always has — it exists
            // on every broker version, and it 404s a missing queue too — so
            // one fallback covers both, at the old price only on old brokers.
            Err(e) if e.status() == Some(404) => match queen.admin().queue(queue).await {
                Ok(v) => queue_detail(&v),
                // The detail route exists on every supported broker, so its
                // own 404 confirms that the queue is absent. That is a known
                // empty backlog, unlike a transport or server failure.
                Err(e) if e.status() == Some(404) => Ok(HashMap::new()),
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        };
        match &result {
            Ok(value) => self.remember_value(queue, value),
            Err(error) => self.remember_failure(queue, error),
        }
        result
    }

    /// The same read as [`Self::pending_of_group`], with the cache skipped and the
    /// failure kept.
    ///
    /// The relay chooses which partitions to poll from this, and both properties
    /// matter for that. Cached, a two-second-old answer would leave an item that
    /// has just arrived unpolled for as long as the entry is warm — on a path whose
    /// whole job is to move work along. And `None` has to be tellable from "nothing
    /// waiting": a relay that read a broker's silence as an empty queue would stop
    /// polling every partition it has, which is not a slower relay, it is a stopped
    /// one.
    pub async fn try_pending_of_group_now(
        &self,
        queen: &Queen,
        queue: &str,
        group: &str,
    ) -> queen_mq::Result<Depth> {
        let key = format!("{queue}\u{1f}{group}");
        let result = match queen.admin().queue_depth(queue, Some(group)).await {
            Ok(v) => depth_route(&v),
            // No fallback to the queue-level number, and this one is measured. The
            // queue-level pending is not this group's backlog on every broker: on
            // 1.0.3 an admitted queue that a second consumer group had read to the
            // end reported ZERO while the relay's own group still owed thirty items.
            // A caller that BOUNDS work on this — the relay decides which partitions
            // to poll — would have stopped polling a queue that was full, which is
            // not a slower relay but a stopped graph. So an answer that is not this
            // group's own is no answer, and the caller decides what it can safely
            // do without it.
            Err(e) => Err(e),
        };
        // This exact read deliberately has no legacy fallback. Do not let its
        // 404 poison the normal reader's cache: that reader can still answer
        // safely from the queue-level route on an older broker.
        if let Ok(value) = &result {
            self.remember_value(&key, value);
        }
        result
    }

    /// The same read, scoped to ONE consumer group's own backlog.
    ///
    /// Queue-level pending answers "is anything waiting"; this answers "is
    /// anything waiting for ME", and an ETA needs the second. The two differ
    /// wherever a queue has more than one reader — an admitted queue drained by
    /// a relay and never popped by a caller reads as a mountain of work under
    /// the executor's group and as nothing under the relay's.
    ///
    /// Requires the depth route (broker >= 1.0.4). An older broker and an absent
    /// queue both answer 404 and cannot be told apart, so both fall back to the
    /// queue-level number — which is the WORST cursor across every group, and so
    /// is at or above the group being asked about. The fallback can therefore
    /// only make an ETA later, never earlier, which is the safe direction for an
    /// answer that promises "no earlier than".
    pub async fn pending_of_group(
        &self,
        queen: &Queen,
        queue: &str,
        group: &str,
    ) -> queen_mq::Result<Depth> {
        // The group belongs in the cache key: two answers about one queue that
        // mean different things must not serve each other's entry.
        let key = format!("{queue}\u{1f}{group}");
        if let Some(cached) = self.cached(&key) {
            return cached;
        }
        let result = match queen.admin().queue_depth(queue, Some(group)).await {
            Ok(v) => depth_route(&v),
            Err(e) if e.status() == Some(404) => {
                let out = self.pending(queen, queue).await;
                // Stamped under the GROUP key as well, and not only under the
                // queue's. Both reasons for a 404 here persist — a broker older
                // than 1.0.4 stays old, and a queue that does not exist yet
                // stays absent for as long as the target has had no push — so
                // without this the probe is re-issued on every request for ever:
                // one round trip per caller, which is the thing the TTL exists
                // to stop. Re-probed once per TTL, so an upgrade or a first push
                // is noticed two seconds later.
                out
            }
            Err(e) => Err(e),
        };
        match &result {
            Ok(value) => self.remember_value(&key, value),
            Err(error) => self.remember_failure(&key, error),
        }
        result
    }
}

/// `{partitions: [{partition, pending}]}` — what the depth route answers, with
/// or without a group.
fn depth_route(v: &serde_json::Value) -> queen_mq::Result<Depth> {
    let mut out = HashMap::new();
    let parts = v
        .get("partitions")
        .and_then(|p| p.as_array())
        .ok_or_else(|| queen_mq::Error::Decode("depth response has no partitions array".into()))?;
    for (index, p) in parts.iter().enumerate() {
        let name = p.get("partition").and_then(|n| n.as_str()).ok_or_else(|| {
            queen_mq::Error::Decode(format!(
                "depth response partition {index} has no string partition"
            ))
        })?;
        let pending = p.get("pending").and_then(|n| n.as_u64()).ok_or_else(|| {
            queen_mq::Error::Decode(format!(
                "depth response partition {index} has no unsigned pending"
            ))
        })?;
        if out.insert(name.to_string(), pending).is_some() {
            return Err(queen_mq::Error::Decode(format!(
                "depth response repeats partition {name}"
            )));
        }
    }
    Ok(out)
}

/// `{partitions: [{name, stats: {pending}}]}` — the console-grade queue detail,
/// which every broker version has and which knows nothing about groups.
fn queue_detail(v: &serde_json::Value) -> queen_mq::Result<Depth> {
    let mut out = HashMap::new();
    let parts = v
        .get("partitions")
        .and_then(|p| p.as_array())
        .ok_or_else(|| queen_mq::Error::Decode("queue detail has no partitions array".into()))?;
    for (index, p) in parts.iter().enumerate() {
        let name = p.get("name").and_then(|n| n.as_str()).ok_or_else(|| {
            queen_mq::Error::Decode(format!("queue detail partition {index} has no string name"))
        })?;
        let pending = p
            .get("stats")
            .and_then(|s| s.get("pending"))
            .and_then(|n| n.as_u64())
            .ok_or_else(|| {
                queen_mq::Error::Decode(format!(
                    "queue detail partition {index} has no unsigned stats.pending"
                ))
            })?;
        if out.insert(name.to_string(), pending).is_some() {
            return Err(queen_mq::Error::Decode(format!(
                "queue detail repeats partition {name}"
            )));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{depth_route, queue_detail};

    #[test]
    fn depth_route_requires_every_value_it_reports() {
        let parsed = depth_route(&json!({
            "partitions": [
                { "partition": "p0", "pending": 3 },
                { "partition": "p1", "pending": 0 }
            ]
        }))
        .expect("valid depth response");
        assert_eq!(parsed.get("p0"), Some(&3));
        assert_eq!(parsed.get("p1"), Some(&0));

        assert!(depth_route(&json!({ "partitions": [{ "partition": "p0" }] })).is_err());
        assert!(depth_route(&json!({})).is_err());
    }

    #[test]
    fn legacy_queue_detail_does_not_turn_schema_drift_into_zero() {
        let parsed = queue_detail(&json!({
            "partitions": [{ "name": "p0", "stats": { "pending": 7 } }]
        }))
        .expect("valid queue detail");
        assert_eq!(parsed.get("p0"), Some(&7));

        assert!(queue_detail(&json!({
            "partitions": [{ "name": "p0", "stats": {} }]
        }))
        .is_err());
    }
}
