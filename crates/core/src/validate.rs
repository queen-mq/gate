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

pub fn validate(spec: &TargetSpec) -> Vec<Problem> {
    let mut out = Vec::new();
    let p = |rule, detail: String| Problem { rule, detail };

    // The application and the name both become queue names and kv keys, so both
    // are constrained rather than trusted.
    let ok_name = |s: &str| {
        !s.is_empty()
            && s.len() <= 63
            && s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
            && s.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
    };
    if !ok_name(&spec.application) {
        out.push(p(
            "application",
            format!("`{}` is not a usable application name: lowercase, digits and dashes", spec.application),
        ));
    }
    if !ok_name(&spec.name) {
        out.push(p(
            "name",
            format!("`{}` is not a usable target name: lowercase, digits and dashes", spec.name),
        ));
    }

    if spec.budgets.is_empty() {
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
            if b.store == Store::Gate && max > GATE_MAX_KEYS {
                out.push(p(
                    "store-fits",
                    format!(
                        "budget `{}` declares {max} keys in the gate state; above {GATE_MAX_KEYS} it belongs on kv",
                        b.id
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

    out
}

/// Fields whose change re-founds the state and therefore needs a version bump:
/// a new `partition_id` is a counter that restarts at zero, and changing a
/// period or an alignment changes what the accumulated state *means*.
pub fn needs_version_bump(old: &TargetSpec, new: &TargetSpec) -> bool {
    if old.admitted.partition_by != new.admitted.partition_by {
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
