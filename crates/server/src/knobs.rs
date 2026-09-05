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
    /// Fleet-wide worker-count override. `None` leaves the derived default —
    /// `gate_core::plan::fitting_workers`, which divides the stage's own ceiling
    /// by `lane_capacity`.
    pub concurrency: Option<u32>,
    /// What one consumer lane drains, items per second, for deciding how many
    /// lanes a stage needs. `GATE_LANE_CAPACITY`; see
    /// `gate_core::plan::LANE_CAPACITY` for why it is a thousand.
    pub lane_capacity: u32,
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
    /// and nothing else — no depth probe, no state read, no meter tick.
    ///
    /// The worker count is DERIVED from the budget now
    /// (`plan::fitting_workers`), and that is what makes this number small. The
    /// three graphs we actually run — sixteen stages across airbnb, vrbo and
    /// google, with caps between 1.7 and 400 items a second — derive **one**
    /// worker per stage. Sixteen parked polls per replica, or about **1,900 pops
    /// an hour**, against the ~275,000 "is there work?" calls v1 was measured
    /// making in prod: a factor of a hundred and forty.
    ///
    /// It used to be `max(4, partitions)`, which is a throughput rule and the
    /// wrong one here — the same sixteen stages parked 128 consumers for
    /// ceilings a single lane covers with room to spare. Fleet-wide, the number
    /// of consumers PARKED at any instant is
    ///
    /// ```text
    ///     stages × derived_workers × replicas
    /// ```
    ///
    /// so a graph that genuinely admits tens of thousands a second still gets
    /// its lanes, up to the partition count — this shrinks the idle floor
    /// without capping a graph that is actually busy.
    ///
    /// Three knobs move it and each costs something: `GATE_POLL_TIMEOUT_SECONDS`
    /// is paid in shutdown latency (the pop does not notice a cancel until it
    /// returns), `GATE_LANE_CAPACITY` is paid in how much of a burst one lane
    /// must absorb, and `GATE_STAGE_CONCURRENCY` overrides the derivation
    /// outright. `gate-bench idle` measures the number rather than asserting it.
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
    /// How far before a graph runtime's start a NEW group on a Gate-owned
    /// interior queue is seeded. See `relay::INTERIOR_SEED_SKEW` for both bounds
    /// — the clock skew it absorbs below, the broker's transaction window above.
    ///
    /// `GATE_INTERIOR_SEED_SKEW_SECONDS`, clamped to ten minutes: a margin at or
    /// past the broker's 900-second `log_txns` floor would put the seed back
    /// among frames whose acks can no longer resolve, which is the rollback loop
    /// the seeding exists to prevent.
    pub interior_seed_skew: Duration,
    /// The DLQ is BACK. v1 had to set `retry_limit: 0` on its push queue
    /// because it could not tell waiting from failing — it paced by nacking.
    /// v2 paces by releasing, and queen charges no retry budget on lease expiry
    /// (`004_log_pop.sql`), so an explicit failed ack is reserved for real
    /// poison and a retry budget means what it says.
    pub retry_limit: i32,
    /// The largest body a PUSH route will buffer, in bytes.
    ///
    /// axum's own default is 2 MiB and nothing here ever overrode it, so that
    /// number was the real ceiling on everything a caller can hand this service
    /// — silently, because the refusal it produces says
    /// `Failed to buffer the request body: length limit exceeded` and names
    /// neither the limit nor the fact that it is ours.
    ///
    /// It was measured from prod on 2026-09-04, from both sides of the wall, by
    /// a caller that had spent a week failing against it: pushes of 11,408 /
    /// 10,387 / 8,976 records went through, and pushes of 12,000 and 16,096 did
    /// not. Divide, and 2 MiB is exactly where those cross — the payloads run
    /// about 130 to 175 bytes a record depending on the vendor.
    ///
    /// 8 MiB, and the ceiling on the ceiling is memory rather than taste: the
    /// service runs with a 512 MiB limit, and a body limit is a per-request
    /// buffer. Four times the old value keeps a large caller comfortably inside
    /// it while a burst of ten concurrent pushes still costs under a sixth of
    /// the pod.
    ///
    /// PUSH ROUTES ONLY. Declaring a graph or reading the console has no reason
    /// to accept megabytes, and axum's default is the right answer everywhere
    /// the body is a document rather than a batch.
    pub max_push_body: usize,
}

impl Default for Knobs {
    fn default() -> Self {
        Self {
            batch: 200,
            concurrency: None,
            lane_capacity: gate_core::plan::LANE_CAPACITY,
            // Kept in step with `gate_core::plan::DEFAULT_LEASE_SECONDS`, which
            // the v1 migration quotes back at a caller whose `pacing.leaseSeconds`
            // became a no-op.
            lease_seconds: gate_core::plan::DEFAULT_LEASE_SECONDS as i32,
            poll_timeout: Duration::from_secs(30),
            renew_lease: Duration::from_secs(3),
            max_park: Duration::from_secs(30),
            max_prefix_retries: 2,
            interior_seed_skew: crate::relay::INTERIOR_SEED_SKEW,
            retry_limit: 3,
            max_push_body: 8 * 1024 * 1024,
        }
    }
}

/// axum's own `DefaultBodyLimit`, which applied to every route here until
/// 2026-09-04 because nothing set one. Named so the floor below says why it is
/// where it is.
pub const AXUM_DEFAULT_BODY_LIMIT: usize = 2 * 1024 * 1024;

/// The largest a push body may be configured to be.
///
/// The limit is a per-request memory reservation, not a per-request cost: the
/// buffered bytes, the `serde_json::Value` they parse into, the copy the
/// envelope is built on and the body sent to the broker are all live at once,
/// and there is no concurrency limiter in front of any of it. A typo in a
/// deployment manifest should not be able to ask one pod to hold gigabytes.
///
/// 64 MiB is eight times the default and far past any real push; a deployment
/// that wants more than this wants a different shape, not a bigger number.
pub const MAX_PUSH_BODY_CEILING: usize = 64 * 1024 * 1024;

fn env_u32(name: &str) -> Option<u32> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

static KNOBS: OnceLock<Knobs> = OnceLock::new();

pub fn knobs() -> &'static Knobs {
    KNOBS.get_or_init(|| {
        let d = Knobs::default();
        Knobs {
            lane_capacity: env_u32("GATE_LANE_CAPACITY")
                .filter(|n| *n > 0)
                .unwrap_or(d.lane_capacity),
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
            // Clamped, not free: see the field. Zero is allowed and means "no
            // margin", which is the right answer only where Gate and the broker
            // share a clock.
            interior_seed_skew: env_u32("GATE_INTERIOR_SEED_SKEW_SECONDS")
                .map(|n| Duration::from_secs(n.min(600) as u64))
                .unwrap_or(d.interior_seed_skew),
            retry_limit: env_u32("GATE_RETRY_LIMIT")
                .map(|n| n as i32)
                .unwrap_or(d.retry_limit),
            // Floored at axum's own default rather than at zero: a typo in the
            // environment must not be able to make this service refuse bodies it
            // accepted before anybody set the variable.
            max_push_body: env_u32("GATE_MAX_PUSH_BODY_BYTES")
                .map(|n| (n as usize).clamp(AXUM_DEFAULT_BODY_LIMIT, MAX_PUSH_BODY_CEILING))
                .unwrap_or(d.max_push_body),
        }
    })
}
