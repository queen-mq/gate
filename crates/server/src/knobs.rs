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
    /// only has to outlive a handler — a charge, a transaction, and up to
    /// `max_park` of parking, which renewal covers anyway.
    ///
    /// Ten and not thirty, for a measured reason. Settling a claim IN FULL
    /// re-arms its partition in about seven milliseconds; settling a PREFIX of
    /// one leaves the partition parked for exactly this long. The prefix path is
    /// rare — `plan::fitting_batch` sizes a claim to what a sub-window admits —
    /// but when it is taken, this is what the tail waits, so it is as short as a
    /// handler can safely live with.
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
    /// How long one handler may hold its claim across in-handler parks before
    /// it gives up and releases.
    ///
    /// **This is the whole park-or-release rule**, and it retires both of the
    /// design's knobs for it — `GATE_MAX_PARKS`, a count, which bounds nothing
    /// an operator cares about; and `GATE_PARK_THRESHOLD_MS`, a cutoff on the
    /// length of one wait, which asks the wrong question. The right question is
    /// "can I afford to hold this claim that long", and the answer is one
    /// comparison: park while `parked + wait <= max_park`, release otherwise.
    ///
    /// Releasing costs about a MINUTE, not a lease (see
    /// `relay::park_or_release`). Sixteen workers on a node with a one-second
    /// window means fifteen of them find the counter full on every rotation; at
    /// three parks they would each release and strand a whole claim for a
    /// minute, and the node's throughput would collapse to one window's worth
    /// per minute per partition. With thirty seconds of parking they wait their
    /// turn instead, at one second a turn.
    ///
    /// The original reason for a small bound — "a worker that never notices its
    /// graph was redeclared" — does not apply: every park is a `tokio::select!`
    /// against the stage's cancel token, so a redeclare interrupts one
    /// immediately. What the bound really buys is that a claim is not held for
    /// ever by a handler waiting on a counter nobody is refilling.
    pub max_park: Duration,
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
            // Kept in step with `gate_core::plan::DEFAULT_LEASE_SECONDS`, which
            // the v1 migration quotes back at a caller whose `pacing.leaseSeconds`
            // became a no-op.
            lease_seconds: gate_core::plan::DEFAULT_LEASE_SECONDS as i32,
            poll_timeout: Duration::from_secs(30),
            renew_lease: Duration::from_secs(3),
            max_park: Duration::from_secs(30),
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
            max_park: env_u32("GATE_MAX_PARK_MS")
                .map(|n| Duration::from_millis(n as u64))
                .unwrap_or(d.max_park),
            max_prefix_retries: d.max_prefix_retries,
            retry_limit: env_u32("GATE_RETRY_LIMIT")
                .map(|n| n as i32)
                .unwrap_or(d.retry_limit),
        }
    })
}
