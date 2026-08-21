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

#[derive(Default)]
pub struct Depths {
    // One map, keyed by queue (or `queue\u{1f}group`), holding the last answer
    // and when it was given. Spelling the pair out as a type alias would name
    // something nobody says out loud.
    #[allow(clippy::type_complexity)]
    cache: RwLock<HashMap<String, (HashMap<String, u64>, Instant)>>,
}

impl Depths {
    /// Pending count per partition of one queue. An absent queue is zero rather
    /// than an error: a target declared a moment ago has no queue yet, and that
    /// is a true statement about its backlog.
    pub async fn pending(&self, queen: &Queen, queue: &str) -> HashMap<String, u64> {
        if let Some((v, at)) = self.cache.read().get(queue) {
            if at.elapsed() < TTL {
                return v.clone();
            }
        }
        match self.try_pending_now(queen, queue).await {
            Some(v) => v,
            // The broker did not answer. Serve the last thing it DID say rather than a
            // zero, and stamp it so an outage costs one round trip per TTL instead of one
            // per caller: a console polling every few seconds across a dozen targets would
            // otherwise hammer an admin API that is already unhappy.
            None => {
                let stale = self
                    .cache
                    .read()
                    .get(queue)
                    .map(|(v, _)| v.clone())
                    .unwrap_or_default();
                self.cache
                    .write()
                    .insert(queue.to_string(), (stale.clone(), Instant::now()));
                stale
            }
        }
    }

    /// The same read with the cache skipped, and the answer left in it.
    ///
    /// The merge relay bounds the destination queue's depth, and it forwards on
    /// every loop — a couple of hundred milliseconds. A two-second-old depth would
    /// let it overshoot the window by everything it forwarded in the meantime,
    /// which is the one number the window exists to hold down.
    pub async fn pending_now(&self, queen: &Queen, queue: &str) -> HashMap<String, u64> {
        self.try_pending_now(queen, queue).await.unwrap_or_default()
    }

    /// The same read, with the failure kept.
    ///
    /// A caller that BOUNDS something on the answer must be able to tell "nothing is
    /// waiting" from "the broker did not say". The merge relay is one: reading a
    /// failed depth as zero would let it forward a full window on every loop, for as
    /// long as the admin API is unhappy, and the queue it is supposed to keep shallow
    /// would grow without a bound.
    pub async fn try_pending_now(
        &self,
        queen: &Queen,
        queue: &str,
    ) -> Option<HashMap<String, u64>> {
        let mut out = HashMap::new();
        let mut answered = false;

        // The depth route first (broker >= 1.0.4): watermark arithmetic only —
        // measured at ~1ms on a gate-sized queue, against two console-grade
        // queries for the detail below. No group, on purpose: queue-level
        // pending under the worst-cursor precedence is exactly what the old
        // detail reported, so the relay's window bound does not move.
        match queen.admin().queue_depth(queue, None).await {
            Ok(v) => {
                answered = true;
                out = depth_route(&v);
            }
            // A 404 here is BOTH "this broker predates the route" and "no such
            // queue", and they cannot be told apart. The queue detail below
            // answers both the same way this function always has — it exists
            // on every broker version, and it 404s a missing queue too — so
            // one fallback covers both, at the old price only on old brokers.
            Err(e) if e.status() == Some(404) => {
                if let Ok(v) = queen.admin().queue(queue).await {
                    answered = true;
                    out = queue_detail(&v);
                }
            }
            Err(_) => {}
        }
        if !answered {
            // An absent queue answers, and answers zero — a target declared a moment
            // ago has no queue yet, and that is a true statement about its backlog.
            // This is the other case: the broker did not answer at all.
            return None;
        }
        self.cache
            .write()
            .insert(queue.to_string(), (out.clone(), Instant::now()));
        Some(out)
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
    ) -> Option<HashMap<String, u64>> {
        let key = format!("{queue}\u{1f}{group}");
        match queen.admin().queue_depth(queue, Some(group)).await {
            Ok(v) => {
                let out = depth_route(&v);
                self.cache
                    .write()
                    .insert(key, (out.clone(), Instant::now()));
                Some(out)
            }
            // No fallback to the queue-level number, and this one is measured. The
            // queue-level pending is not this group's backlog on every broker: on
            // 1.0.3 an admitted queue that a second consumer group had read to the
            // end reported ZERO while the relay's own group still owed thirty items.
            // A caller that BOUNDS work on this — the relay decides which partitions
            // to poll — would have stopped polling a queue that was full, which is
            // not a slower relay but a stopped graph. So an answer that is not this
            // group's own is no answer: `None`, and the caller does what it did
            // before it could ask.
            Err(_) => None,
        }
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
    ) -> HashMap<String, u64> {
        // The group belongs in the cache key: two answers about one queue that
        // mean different things must not serve each other's entry.
        let key = format!("{queue}\u{1f}{group}");
        if let Some((v, at)) = self.cache.read().get(&key) {
            if at.elapsed() < TTL {
                return v.clone();
            }
        }
        match queen.admin().queue_depth(queue, Some(group)).await {
            Ok(v) => {
                let out = depth_route(&v);
                self.cache
                    .write()
                    .insert(key, (out.clone(), Instant::now()));
                out
            }
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
                self.cache
                    .write()
                    .insert(key, (out.clone(), Instant::now()));
                out
            }
            // The broker did not answer. Serve the last thing it DID say, stamped,
            // for the reason `pending` does it: an outage costs one round trip per
            // TTL rather than one per caller.
            Err(_) => {
                let stale = self
                    .cache
                    .read()
                    .get(&key)
                    .map(|(v, _)| v.clone())
                    .unwrap_or_default();
                self.cache
                    .write()
                    .insert(key, (stale.clone(), Instant::now()));
                stale
            }
        }
    }
}

/// `{partitions: [{partition, pending}]}` — what the depth route answers, with
/// or without a group.
fn depth_route(v: &serde_json::Value) -> HashMap<String, u64> {
    let mut out = HashMap::new();
    if let Some(parts) = v.get("partitions").and_then(|p| p.as_array()) {
        for p in parts {
            let name = p.get("partition").and_then(|n| n.as_str()).unwrap_or("");
            let pending = p.get("pending").and_then(|n| n.as_u64()).unwrap_or(0);
            out.insert(name.to_string(), pending);
        }
    }
    out
}

/// `{partitions: [{name, stats: {pending}}]}` — the console-grade queue detail,
/// which every broker version has and which knows nothing about groups.
fn queue_detail(v: &serde_json::Value) -> HashMap<String, u64> {
    let mut out = HashMap::new();
    if let Some(parts) = v.get("partitions").and_then(|p| p.as_array()) {
        for p in parts {
            let name = p.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let pending = p
                .get("stats")
                .and_then(|s| s.get("pending"))
                .and_then(|n| n.as_u64())
                .unwrap_or(0);
            out.insert(name.to_string(), pending);
        }
    }
    out
}
