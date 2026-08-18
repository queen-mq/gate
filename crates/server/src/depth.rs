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
        let mut out = HashMap::new();
        if let Ok(v) = queen.admin().queue(queue).await {
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
        self.cache
            .write()
            .insert(queue.to_string(), (out.clone(), Instant::now()));
        out
    }
}
