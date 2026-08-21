//! The knobs, read once at boot.
//!
//! v1's rule and v1's exception: everything is read once, because a value that
//! can change under a running loop is a value nobody can reason about from a
//! log line — except `GATE_ADMIN_EMAILS` and `GATE_DEV_EMAIL`, which are read
//! per request so an operator can grant themselves access without a restart.

use std::sync::OnceLock;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct Knobs {
    /// The per-claim batch when a node declares none. The budget is charged
    /// ONCE per batch, so this is also the divisor on the shared counter's
    /// traffic.
    pub batch: u32,
    /// Fleet-wide worker-count override. `None` leaves the derived default of
    /// `max(4, partitions of the source)`.
    pub concurrency: Option<u32>,
    /// A **work** lease, not a pacing quantum. v1's was one second because the
    /// lease WAS the pacing; here the budget window is the pacing and the lease
    /// only has to outlive a handler.
    pub lease_seconds: i32,
    /// The parked long-poll window. It can be thirty seconds rather than five
    /// because a parked poll **releases its pooled PG connection before
    /// parking** and is woken by the push notifier (`server/src/handlers/data.rs`
    /// §10). The ceiling on it is shutdown latency, and the supervisor waits
    /// this plus two seconds for a stage to stop.
    ///
    /// # This, times the worker count, IS the idle cost
    ///
    /// A parked poll that times out re-issues its pop, so an idle graph costs
    ///
    /// ```text
    ///     stages × concurrency ÷ poll_timeout   pops per second
    /// ```
    ///
    /// and nothing else — no depth probe, no state read, no meter tick. For the
    /// flagship seven-stage graph at the derived default of sixteen workers that
    /// is 112 parked polls, or **~13,400 pops an hour**: twenty times less than
    /// the ~275,000 "is there work?" calls v1 was measured making in prod, and
    /// not the zero the design's acceptance criterion asks for. Both knobs move
    /// it and both cost something: `GATE_POLL_TIMEOUT_SECONDS` is paid in
    /// shutdown latency (the pop does not notice a cancel until it returns), and
    /// `GATE_STAGE_CONCURRENCY` is paid in how many partitions a stage can drain
    /// at once. `gate-bench idle` measures the number rather than asserting it.
    pub poll_timeout: Duration,
    /// How often a held claim is renewed while the handler is parked in-line.
    pub renew_lease: Duration,
    /// Park below this, release above it. Just above the one-second sub-window
    /// floor, so an ordinary rotation always parks rather than releasing.
    pub park_threshold: Duration,
    /// In-handler parks before the handler gives up and releases. An unbounded
    /// park loop is a worker that never notices its graph was redeclared.
    pub max_parks: u32,
    /// Retries of the prefix charge when another worker takes the headroom
    /// between the two calls. An unbounded retry loop against a contended
    /// counter is how a limiter turns into a spin.
    pub max_prefix_retries: u32,
    /// The DLQ is BACK. v1 had to set `retry_limit: 0` on its push queue
    /// because it could not tell waiting from failing — it paced by nacking.
    /// v2 paces by releasing, and queen charges no retry budget on lease expiry
    /// (`004_log_pop.sql`), so an explicit failed ack is reserved for real
    /// poison and a retry budget means what it says.
    pub retry_limit: i32,
}

impl Default for Knobs {
    fn default() -> Self {
        Self {
            batch: 200,
            concurrency: None,
            lease_seconds: 30,
            poll_timeout: Duration::from_secs(30),
            renew_lease: Duration::from_secs(10),
            park_threshold: Duration::from_millis(1500),
            max_parks: 3,
            max_prefix_retries: 2,
            retry_limit: 3,
        }
    }
}

fn env_u32(name: &str) -> Option<u32> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

static KNOBS: OnceLock<Knobs> = OnceLock::new();

pub fn knobs() -> &'static Knobs {
    KNOBS.get_or_init(|| {
        let d = Knobs::default();
        Knobs {
            batch: env_u32("GATE_STAGE_BATCH")
                .unwrap_or(d.batch)
                .clamp(1, 1000),
            concurrency: env_u32("GATE_STAGE_CONCURRENCY").filter(|n| *n > 0),
            lease_seconds: env_u32("GATE_LEASE_SECONDS")
                .map(|n| n as i32)
                .unwrap_or(d.lease_seconds)
                .max(1),
            poll_timeout: Duration::from_secs(
                env_u32("GATE_POLL_TIMEOUT_SECONDS").unwrap_or(30).max(1) as u64,
            ),
            renew_lease: d.renew_lease,
            park_threshold: Duration::from_millis(
                env_u32("GATE_PARK_THRESHOLD_MS").unwrap_or(1500) as u64,
            ),
            max_parks: env_u32("GATE_MAX_PARKS").unwrap_or(d.max_parks),
            max_prefix_retries: d.max_prefix_retries,
            retry_limit: env_u32("GATE_RETRY_LIMIT")
                .map(|n| n as i32)
                .unwrap_or(d.retry_limit),
        }
    })
}
