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

        if let Ok(v) = queen.admin().queue(queue).await {
            answered = true;

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
}
