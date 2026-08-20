//! Validation at declare time.
//!
//! Every rule here turns a silent runtime failure into a rejected `PUT`. That
//! is the whole value of the file: none of these are style checks, and each one
//! corresponds to a way the limiter breaks in production without saying so.

use std::collections::HashSet;

use crate::spec::{Alignment, CapPolicy, Confidence, Store, TargetSpec};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Problem {
    pub rule: &'static str,
    pub detail: String,
}

impl std::fmt::Display for Problem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.rule, self.detail)
    }
}

/// Above this, a scoped budget's counters do not belong in the gate's state
/// document: it is re-read in full on every cycle, so a big document is a big
/// re-read, every time. High cardinality goes to kv, which is one row per key.
pub const GATE_MAX_KEYS: u64 = 5_000;

/// One gate runner, one partition lease and one state document per shard, so the
/// count is a resource number and not a taste one. Sixty-four is the working
/// figure; this is the wall.
pub const GATE_MAX_SHARDS: u32 = 256;

/// The same wall, for the other partitioned queue.
///
/// An admitted partition used to be nothing but a hash bucket that kept one
/// connection's work in order, and its count cost nothing to raise. It is now
/// also the unit of relay parallelism — a node with an out-edge gets one relay
/// runner per admitted partition per lane, because the partition is what makes
/// a runner the only claimer of what it forwards — so the number buys claims,
/// pinned polls and tasks exactly as `shards` buys gate runners. Same argument,
/// same wall.
pub const GATE_MAX_ADMITTED_PARTITIONS: u32 = 256;

/// What a caller is allowed to relax, and nothing more.
#[derive(Debug, Clone, Copy, Default)]
pub struct ValidateOpts {
    /// A graph node with out-edges may hold no budget of its own: it exists to
    /// isolate a traffic class and to carry a priority, and the limit it is
    /// checked against lives downstream. A standalone target with no budget
    /// limits nothing, which is why this is off by default.
    pub allow_empty_budgets: bool,
}

/// One lowercase segment: what a queue name and a kv key can carry without
/// quoting.
fn ok_segment(s: &str) -> bool {
    !s.is_empty()
        && s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && s.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
}

/// A single segment: applications, lanes and graph names.
pub fn ok_name(s: &str) -> bool {
    s.len() <= 63 && ok_segment(s)
}

/// A target name, which may be dotted because a graph node IS a target called
/// `{graph}.{node}`. Each segment is still a segment, so nothing that reaches a
/// queue name or a kv key changes shape.
pub fn ok_target_name(s: &str) -> bool {
    s.len() <= 63 && !s.is_empty() && s.split('.').all(ok_segment)
}

pub fn validate(spec: &TargetSpec) -> Vec<Problem> {
    validate_with(spec, ValidateOpts::default())
}

pub fn validate_with(spec: &TargetSpec, opts: ValidateOpts) -> Vec<Problem> {
    let mut out = Vec::new();
    let p = |rule, detail: String| Problem { rule, detail };

    if !ok_name(&spec.application) {

        out.push(p(
            "application",
            format!("`{}` is not a usable application name: lowercase, digits and dashes", spec.application),
        ));
    }
    if !ok_target_name(&spec.name) {
        out.push(p(
            "name",
            format!(
                "`{}` is not a usable target name: lowercase, digits, dashes, and a dot only \
                 between segments (a graph node is the target `{{graph}}.{{node}}`)",
                spec.name
            ),
        ));
    }

    if spec.budgets.is_empty() && !opts.allow_empty_budgets {
        out.push(p("budgets", "a target with no budget does not limit anything".into()));
    }

    if spec.lanes.is_empty() {
        out.push(p("lanes", "a target needs at least one lane".into()));
    }

    let defaults = spec.lanes.iter().filter(|l| l.default).count();
    if defaults != 1 {
        out.push(p(
            "default-lane",
            format!("exactly one lane must be default, found {defaults}"),
        ));
    }

    let mut lane_names = HashSet::new();
    for lane in &spec.lanes {
        if !lane_names.insert(&lane.name) {
            out.push(p("lane-unique", format!("duplicate lane `{}`", lane.name)));
        }
        if lane.concurrency == 0 {
            out.push(p("lane-concurrency", format!("lane `{}` has no consumers", lane.name)));
        }
        if let CapPolicy::CeilingMinusMeasured = lane.cap {
            if !(0.0..=1.0).contains(&lane.floor) {
                out.push(p("lane-floor", format!("lane `{}` floor must be 0..1", lane.name)));
            }
        }
    }

    // Each lane is its own partition and its own copy of the counters, so the
    // shares must divide the ceiling rather than each claim it. Measured, not
    // theorised: two lanes both at `ceiling` peaked at 93/s against a declared
    // 50/s before this rule existed.
    let reserved: f64 = spec
        .lanes
        .iter()
        .map(|l| match &l.cap {
            CapPolicy::Share(f) => *f,
            CapPolicy::CeilingMinusMeasured => l.floor.max(0.0),
            _ => 0.0,
        })
        .sum();
    if reserved > 1.0 + 1e-9 {
        out.push(p(
            "lane-shares",
            format!("lane reservations sum to {reserved:.2} of the ceiling; they must not exceed 1.0"),
        ));
    }
    let takers = spec
        .lanes
        .iter()
        .filter(|l| matches!(l.cap, CapPolicy::Ceiling | CapPolicy::Absolute(_)))
        .count();
    if takers > 1 {
        out.push(p(
            "lane-shares",
            format!(
                "{takers} lanes claim the whole ceiling; at most one may, the rest declare a share or a floor"
            ),
        ));
    }
    // The other end of the same rule, and the one that validated clean until
    // now. With NO lane claiming the ceiling, the residual belongs to nobody —
    // and `ceiling-minus-measured` reads the residual as its own, so two derived
    // lanes each take everything the other did not reserve and the ceiling is
    // enforced twice. `takers == 1` closes it because the residual then has an
    // owner.
    if takers == 0 && reserved < 1.0 - 1e-9 {
        out.push(p(
            "lane-shares",
            format!(
                "no lane claims the ceiling and the reservations only add up to {reserved:.2}: \
                 the remaining {:.2} has no owner, and a `ceiling-minus-measured` lane would \
                 claim all of it. Give one lane `ceiling`, or make the reservations sum to 1.0",
                1.0 - reserved
            ),
        ));
    }
    // The property, checked directly rather than inferred from the rules above:
    // whatever the lanes declare, their shares of one ceiling must not add up to
    // more than that ceiling. Measured with no meter reading, which is the
    // largest a derived lane can ever be (the meter may only shrink it).
    let allocated: f64 = spec
        .lanes
        .iter()
        .map(|l| spec.lane_share(&l.name, None))
        .sum();
    if allocated > 1.0 + 1e-9 {
        out.push(p(
            "lane-shares",
            format!(
                "the lanes divide {allocated:.2} of one ceiling between them: each lane is its own \
                 partition with its own copy of the counters, so anything above 1.0 is the ceiling \
                 enforced more than once"
            ),
        ));
    }

    if spec.lanes.len() > 1 {
        for l in &spec.lanes {
            if matches!(l.cap, CapPolicy::CeilingMinusMeasured) && l.floor <= 0.0 {
                out.push(p(
                    "lane-floor",
                    format!(
                        "lane `{}` is ceiling-minus-measured with no floor: until a meter runs it would admit nothing",
                        l.name
                    ),
                ));
            }
        }
    }

    if spec.cost.max < spec.cost.default {
        out.push(p("cost", "cost.max is below cost.default".into()));
    }

    let mut ids = HashSet::new();
    for b in &spec.budgets {
        if !ids.insert(&b.id) {
            out.push(p("budget-unique", format!("duplicate budget id `{}`", b.id)));
        }
        if b.cap <= 0.0 {
            out.push(p("budget-cap", format!("`{}` has a non-positive cap", b.id)));
        }
        if b.period_seconds < 1 {
            out.push(p("budget-period", format!("`{}` has a period under one second", b.id)));
        }

        // The one that matters most. An item costing more than a cap can never
        // be admitted, blocks the head of its lane forever, and never reaches a
        // DLQ because lease expiry does not charge retries.
        if b.cap < spec.cost.max {
            out.push(p(
                "cost-fits",
                format!(
                    "budget `{}` caps at {} but a single item may cost {}: that item would block the lane forever",
                    b.id, b.cap, spec.cost.max
                ),
            ));
        }

        if !b.scope.is_empty() && b.max_keys.is_none() {
            out.push(p(
                "max-keys",
                format!("scoped budget `{}` must declare maxKeys", b.id),
            ));
        }
        if let Some(max) = b.max_keys {
            // Per SHARD, because a shard is a state document: the bound that
            // matters is what one cycle re-reads, and sharding is the only way a
            // high-cardinality budget fits. Never kv — see `kv-scope` below for
            // why that recommendation was wrong.
            let shards = spec.shard_count() as u64;
            let per_shard = max.div_ceil(shards.max(1));
            if b.store == Store::Gate && per_shard > GATE_MAX_KEYS {
                out.push(p(
                    "store-fits",
                    format!(
                        "budget `{}` declares {max} keys over {shards} shard(s), {per_shard} per gate \
                         state document; above {GATE_MAX_KEYS} the document is re-read whole every \
                         cycle at that size. Shard the target (`shardBy` on the scope dimension, more \
                         `shards`) or narrow the scope — kv cannot hold a scoped budget",
                        b.id
                    ),
                ));
            }
        }

        // ---- kv quarantine. Everything a `store: kv` budget silently is not.
        if b.store == Store::Kv {
            if !b.scope.is_empty() {
                out.push(p(
                    "kv-scope",
                    format!(
                        "budget `{}` is `store: kv` with a scope: the shared pool keys on \
                         `(application, id)` alone and DROPS the scope, so every key would spend one \
                         counter — the per-key limit would not exist. Shard the target instead",
                        b.id
                    ),
                ));
            }
            if b.matcher.is_some() {
                out.push(p(
                    "kv-match",
                    format!(
                        "budget `{}` is `store: kv` with a `match`: the pool is spent from a local \
                         lease before any op is looked at, so it charges every op and the selector \
                         would not exist",
                        b.id
                    ),
                ));
            }
            // The lease is a chunk of one second's worth of the budget, topped up
            // when less than half remains. At `cap < 2 x periodSeconds` the chunk
            // is one, half of it is zero, the top-up never fires and the budget
            // admits nothing for ever.
            let chunk = (b.cap / b.period_seconds.max(1) as f64).floor();
            if b.cap < 2.0 * b.period_seconds as f64 {
                out.push(p(
                    "kv-chunk",
                    format!(
                        "budget `{}` is `store: kv` with cap {} over {}s: the shared lease is a \
                         chunk of one second's rate and needs at least 2 x periodSeconds of cap to \
                         top itself up. Below that it deadlocks at zero",
                        b.id, b.cap, b.period_seconds
                    ),
                ));
            } else if chunk < spec.cost.max {
                // `cost-fits` compares an item against the whole cap, which is the
                // right test for a budget the gate holds in memory. A kv budget is
                // never spent from its cap: it is spent from a local lease of one
                // second's rate, so an item costing more than that lease can never
                // be admitted — and it blocks the head of its lane for ever, which
                // is exactly the failure `cost-fits` exists to prevent.
                out.push(p(
                    "kv-chunk",
                    format!(
                        "budget `{}` is `store: kv` and leases {chunk} per top-up (cap {} over {}s), \
                         but a single item may cost {}: that item can never be spent from the shared \
                         lease and would block the lane for ever. Raise the cap, shorten the period, \
                         or lower cost.max",
                        b.id, b.cap, b.period_seconds, spec.cost.max
                    ),
                ));
            }

        }


        if b.confidence == Confidence::Documented && (b.source.is_none() || b.as_of.is_none()) {
            out.push(p(
                "provenance",
                format!("`{}` claims documented but carries no source and asOf", b.id),
            ));
        }

        if let Some(m) = &b.matcher {
            if m.op.is_empty() {
                out.push(p("match", format!("`{}` has an empty match", b.id)));
            }
        }
    }

    // If the batch is smaller than what a lease-window's worth of budget would
    // allow, the batch is the limiter and the vendor's ceiling is never reached.
    if let Some(tightest) = spec
        .budgets
        .iter()
        .filter(|b| b.store == Store::Gate)
        .map(|b| b.rate_per_sec())
        .fold(None, |acc: Option<f64>, r| Some(acc.map_or(r, |a| a.min(r))))
    {
        let per_lease = tightest * spec.pacing.lease_seconds as f64;
        if (spec.pacing.batch as f64) < per_lease {
            out.push(p(
                "batch-fits",
                format!(
                    "batch {} is below the {:.0} items a lease of {}s allows: the batch would limit, not the budget",
                    spec.pacing.batch, per_lease, spec.pacing.lease_seconds
                ),
            ));
        }
    }

    if spec.pacing.lease_seconds < 1 {
        out.push(p("pacing", "leaseSeconds is an integer of at least 1".into()));
    }

    // ---- admitted partitions. A hash bucket, and now also a relay runner.
    //
    // Both ends are refused rather than clamped. A zero used to be read as a one
    // everywhere it was used, which is a decision nobody made surviving as a
    // `.max(1)` in five places; and the ceiling is the same resource argument
    // `shard-count` makes — every partition of a node with an out-edge is a relay
    // runner holding a claim on the broker, so a number chosen by pasting is a
    // number the broker pays for.
    if spec.admitted.partition_by != crate::spec::PartitionBy::None {
        if spec.admitted.partitions < 1 {
            out.push(p(
                "admitted-partitions",
                format!(
                    "`partitionBy: {}` needs `partitions` of at least 1: a ring of no buckets has \
                     nowhere to put an item",
                    match spec.admitted.partition_by {
                        crate::spec::PartitionBy::Entity => "entity",
                        _ => "connection",
                    }
                ),
            ));
        }
        if spec.admitted.partitions > GATE_MAX_ADMITTED_PARTITIONS {
            out.push(p(
                "admitted-partitions",
                format!(
                    "{} admitted partitions is above the {GATE_MAX_ADMITTED_PARTITIONS} this build \
                     runs: a node with an out-edge gets one relay runner per admitted partition per \
                     lane, each holding its own claim on the broker. Raise the batch or the lease \
                     before raising this",
                    spec.admitted.partitions
                ),
            ));
        }
    }

    // ---- sharding. A shard is a partition, and a partition is a counter.
    if let Some(dim) = spec.shard_by {
        let shards = spec.shards.unwrap_or(0);
        if shards < 1 {
            out.push(p(
                "shard-count",
                format!("`shardBy: {}` needs `shards` of at least 1", dim.as_str()),
            ));
        }
        if shards > GATE_MAX_SHARDS {
            out.push(p(
                "shard-count",
                format!(
                    "{shards} shards is above the {GATE_MAX_SHARDS} this build runs: each one is a \
                     gate runner holding a partition lease"
                ),
            ));
        }
        for b in spec.budgets.iter().filter(|b| b.store == Store::Gate) {
            if !b.scope.contains(&dim) {
                out.push(p(
                    "shard-scope",
                    format!(
                        "budget `{}` does not carry `{}` in its scope, but the target is sharded by \
                         it: the budget would get one counter per shard and its cap would be \
                         enforced {} times over. Scope it by `{}`, or move it to a node that is not \
                         sharded",
                        b.id,
                        dim.as_str(),
                        spec.shard_count(),
                        dim.as_str()
                    ),
                ));
            }
        }
    } else if spec.shards.is_some() {
        out.push(p(
            "shard-count",
            "`shards` without `shardBy` shards nothing: name the dimension".into(),
        ));
    }

    out
}


/// Fields whose change re-founds the state and therefore needs a version bump:
/// a new `partition_id` is a counter that restarts at zero, and changing a
/// period or an alignment changes what the accumulated state *means*.
pub fn needs_version_bump(old: &TargetSpec, new: &TargetSpec) -> bool {
    // The admitted ring: both how it is keyed and how wide it is.
    //
    // The width used to be free to change, because the ring was only a hash bucket
    // and whoever drained the queue drained whatever partitions the broker offered.
    // It is now the claim topology: a relay runs one runner per partition it can
    // NAME, so narrowing the ring leaves whatever is still sitting in the partitions
    // that no longer exist in the document with nobody to claim it — the same thing
    // narrowing `shards` does to the push queue, and it gets the same answer. A bump
    // does not move that work; it makes the caller say the change was meant, and the
    // stranded items stay visible as lag on the edge until the ring is widened again.
    if old.admitted.partition_by != new.admitted.partition_by
        || old.admitted.count() != new.admitted.count()
    {
        return true;
    }
    // Re-sharding moves keys between shards, and a shard is a counter: the same
    // listing would land on a document that has never seen it and start from
    // zero, while the document that holds its spend is no longer consulted.
    if old.shard_by != new.shard_by || old.shard_count() != new.shard_count() {
        return true;
    }

    if old.lanes.len() > new.lanes.len()
        || old.lanes.iter().any(|l| new.lane(&l.name).is_none())
    {
        return true;
    }
    for ob in &old.budgets {
        match new.budgets.iter().find(|nb| nb.id == ob.id) {
            None => return true,
            Some(nb) => {
                if nb.period_seconds != ob.period_seconds
                    || nb.alignment != ob.alignment
                    || nb.scope != ob.scope
                    || nb.store != ob.store
                {
                    return true;
                }
            }
        }
    }
    false
}

/// A budget declared `assumed` is arithmetic on a guess, so it is enforced
/// below its stated cap. The console draws those bars hatched for the same
/// reason: a guess must never look like a measurement.
pub const ASSUMED_FACTOR: f64 = 0.7;

pub fn effective_cap(cap: f64, confidence: Confidence) -> f64 {
    match confidence {
        Confidence::Assumed => cap * ASSUMED_FACTOR,
        _ => cap,
    }
}

/// The lease is the pacing quantum: a lane wakes once per lease and, if it was
/// denied, not again until the next one. When the lease is as long as the
/// tightest budget's window, the two beat against each other — the lane wakes
/// at the top of a window that has not decayed yet, admits almost nothing,
/// denies, and parks for another whole window.
///
/// Measured, on a 200/s ceiling: a 1s window with a 1s lease held 152/s, and
/// the same ceiling expressed over 10s held 205/s. The rule of thumb the
/// numbers give is a lease no longer than a fifth of the tightest window.
///
/// A budget whose period is one second cannot be paced better than that, since
/// the broker's lease is an integer number of seconds and one is the floor.
/// That is a property of the design, not a bug, and it belongs in the warning
/// rather than in a footnote nobody reads.
pub fn pacing_warnings(spec: &TargetSpec) -> Vec<Problem> {
    let mut out = Vec::new();
    let tightest = spec
        .budgets
        .iter()
        .filter(|b| b.store == Store::Gate)
        .map(|b| b.period_seconds)
        .min();
    if let Some(period) = tightest {
        if spec.pacing.lease_seconds * 5 > period {
            out.push(Problem {
                rule: "lease-beats-window",
                detail: format!(
                    "a lease of {}s against a tightest window of {}s: the lane wakes about once per window and cannot recover the decayed budget, so expect roughly three quarters of the declared ceiling{}",
                    spec.pacing.lease_seconds,
                    period,
                    if period <= 1 {
                        " — and a one-second window cannot do better, because the lease floor is one second"
                    } else {
                        ""
                    }
                ),
            });
        }
    }
    out
}

/// A shared budget on kv is a fixed window whatever it declares, so a rolling
/// one accepts up to twice its cap at the boundary. Not an error — a warning
/// the API returns and the console shows.
pub fn warnings(spec: &TargetSpec) -> Vec<Problem> {
    let mut out = pacing_warnings(spec);
    out.extend(kv_warnings(spec));
    out
}

fn kv_warnings(spec: &TargetSpec) -> Vec<Problem> {
    spec.budgets
        .iter()
        .filter(|b| b.store == Store::Kv && b.alignment == Alignment::Rolling)
        .map(|b| Problem {
            rule: "kv-rolling",
            detail: format!(
                "`{}` is rolling on kv, which is a fixed window: up to 2x the cap at the boundary",
                b.id
            ),
        })
        .collect()
}
