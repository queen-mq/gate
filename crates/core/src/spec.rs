//! The target spec: what a caller declares, and nothing else.
//!
//! Mirrors `TARGET_SPEC.md`. Every field that document calls obligatory is
//! non-`Option` here, so a missing one is a deserialization error rather than a
//! default nobody chose — the whole point of `alignment` having no default is
//! lost if serde invents one.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TargetSpec {
    /// Which application owns this target.
    ///
    /// Applications do not share ceilings — that is the whole point of the
    /// concept and the reason it can stay this thin. Two teams calling the same
    /// vendor with their own credentials have two ceilings, so they get two
    /// targets and never coordinate. Two callers that DO share a credential are
    /// not two applications: they are two lanes of one target, because the
    /// gate's state is per partition and two gates each holding "the ceiling"
    /// enforce it twice.
    ///
    /// So this is an envelope for ownership and naming, never a budget concept:
    /// it scopes the name, the queues, the stored spec, the observability, and
    /// — the part that matters most — which targets a sync is allowed to reap.
    #[serde(default = "default_application")]
    pub application: String,
    pub name: String,
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress: Option<String>,
    pub budgets: Vec<Budget>,
    pub lanes: Vec<Lane>,
    pub cost: Cost,
    #[serde(default)]
    pub pacing: Pacing,
    #[serde(default)]
    pub admitted: Admitted,
    /// Split this target's push queue by a scope dimension: `shards` partitions
    /// per lane, each with its own gate runner, its own partition lease and its
    /// own state document.
    ///
    /// This is what makes a per-key limit expressible at a cardinality one state
    /// document could never hold: the document is re-read whole every cycle, so
    /// 200,000 keys in one is 200,000 keys re-read every cycle. Sharded, it is
    /// `maxKeys / shards` per document, and the single-writer argument survives
    /// because a key hashes to exactly one shard — never two.
    ///
    /// The dimension must appear in the `scope` of EVERY budget in the target
    /// (`shard-scope`): an unscoped budget in a sharded target is one counter per
    /// shard, which is the ceiling enforced `shards` times.
    #[serde(default, rename = "shardBy", skip_serializing_if = "Option::is_none")]
    pub shard_by: Option<Dim>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shards: Option<u32>,
}


#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Budget {
    pub id: String,
    pub cap: f64,
    #[serde(rename = "periodSeconds")]
    pub period_seconds: i64,
    /// No default, deliberately: rolling and calendar differ by a factor of two
    /// at the window boundary, and guessing is a silent overshoot under load.
    pub alignment: Alignment,
    #[serde(default, rename = "match", skip_serializing_if = "Option::is_none")]
    pub matcher: Option<Match>,
    #[serde(default)]
    pub scope: Vec<Dim>,
    /// How many scope keys this budget's counter is expected to hold at once.
    ///
    /// The LIVE set, not the set per window: a rolling budget reads its own window and the
    /// one before it, so the state document carries up to two windows' worth of keys until
    /// the older ones are swept. A number chosen per window will be met at twice itself.
    #[serde(default, rename = "maxKeys", skip_serializing_if = "Option::is_none")]
    pub max_keys: Option<u64>,

    #[serde(default)]
    pub store: Store,
    pub confidence: Confidence,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, rename = "asOf", skip_serializing_if = "Option::is_none")]
    pub as_of: Option<String>,
}

pub fn default_application() -> String {
    "default".to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Alignment {
    Rolling,
    Calendar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Store {
    #[default]
    Gate,
    Kv,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Confidence {
    Documented,
    Inferred,
    Assumed,
}

/// Scope dimensions form the counter key. They must be present on the work item
/// when a budget names them; a missing one is a rejected push, not a zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Dim {
    Host,
    Entity,
    Account,
    Connection,
    Tenant,
}

impl Dim {
    pub fn as_str(&self) -> &'static str {
        match self {
            Dim::Host => "host",
            Dim::Entity => "entity",
            Dim::Account => "account",
            Dim::Connection => "connection",
            Dim::Tenant => "tenant",
        }
    }
}

/// Selection is on a declared `op`, never on a URL: the gate decides before the
/// HTTP call exists, so there is no path to match against.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Match {
    pub op: Vec<String>,
}

impl Match {
    /// Suffix glob on dot-separated segments: `listing.*` takes `listing.create`.
    /// No regex — a pattern language in a config file is a failure surface we
    /// do not pay for.
    pub fn matches(&self, op: &str) -> bool {
        self.op.iter().any(|p| match p.strip_suffix(".*") {
            Some(prefix) => op.strip_prefix(prefix).is_some_and(|r| r.starts_with('.')),
            None => p == op || p == "*",
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Lane {
    pub name: String,
    pub cap: CapPolicy,
    pub concurrency: u32,
    #[serde(default)]
    pub floor: f64,
    #[serde(default)]
    pub default: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CapPolicy {
    /// The lane may use the whole target budget.
    Ceiling,
    /// Ceiling minus what the other lanes measured, floored at `floor`.
    CeilingMinusMeasured,
    /// A fixed rate, in cost units per second.
    Absolute(f64),
    /// A fraction of the binding budget's rate.
    Share(f64),
}

impl Serialize for CapPolicy {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&match self {
            CapPolicy::Ceiling => "ceiling".into(),
            CapPolicy::CeilingMinusMeasured => "ceiling-minus-measured".into(),
            CapPolicy::Absolute(n) => format!("absolute:{n}"),
            CapPolicy::Share(f) => format!("share:{f}"),
        })
    }
}

impl<'de> Deserialize<'de> for CapPolicy {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;
        let s = String::deserialize(d)?;
        match s.as_str() {
            "ceiling" => Ok(CapPolicy::Ceiling),
            "ceiling-minus-measured" => Ok(CapPolicy::CeilingMinusMeasured),
            other => {
                let (kind, val) = other
                    .split_once(':')
                    .ok_or_else(|| D::Error::custom(format!("bad cap policy: {other}")))?;
                let n: f64 = val
                    .parse()
                    .map_err(|_| D::Error::custom(format!("bad cap value: {val}")))?;
                match kind {
                    "absolute" => Ok(CapPolicy::Absolute(n)),
                    "share" => Ok(CapPolicy::Share(n)),
                    _ => Err(D::Error::custom(format!("bad cap policy: {other}"))),
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Cost {
    pub field: String,
    pub default: f64,
    /// A gate cannot admit an item that costs more than the smallest cap, and
    /// such an item blocks its lane forever without ever reaching a DLQ. This
    /// number turns that into a rejected PUT.
    pub max: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Pacing {
    #[serde(rename = "leaseSeconds")]
    pub lease_seconds: i64,
    pub batch: u32,
}

impl Default for Pacing {
    fn default() -> Self {
        // One second: the shortest lease the broker accepts, and the lease is
        // both the pacing quantum and the failover window.
        Self { lease_seconds: 1, batch: 200 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Admitted {
    #[serde(rename = "partitionBy")]
    pub partition_by: PartitionBy,
    pub partitions: u32,
}

impl Default for Admitted {
    fn default() -> Self {
        Self { partition_by: PartitionBy::Connection, partitions: 64 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PartitionBy {
    Connection,
    Entity,
    None,
}

impl TargetSpec {
    pub fn lane(&self, name: &str) -> Option<&Lane> {
        self.lanes.iter().find(|l| l.name == name)
    }

    pub fn default_lane(&self) -> Option<&Lane> {
        self.lanes.iter().find(|l| l.default)
    }

    /// Budgets whose selector takes this op. A budget with no `match` takes
    /// everything, which is how a global ceiling is expressed.
    pub fn budgets_for<'a>(&'a self, op: &'a str) -> impl Iterator<Item = &'a Budget> {
        self.budgets
            .iter()
            .filter(move |b| b.matcher.as_ref().is_none_or(|m| m.matches(op)))
    }

    /// The identity of a target is the pair, never the name alone: two teams may
    /// both have something they call `airbnb`, and they are not the same thing.
    pub fn key(&self) -> String {
        format!("{}/{}", self.application, self.name)
    }

    /// Queue names are derived, never declared: the caller names an
    /// application, a target and a lane, and never learns there is a queue at
    /// all. The application is in the name so two teams cannot collide on a
    /// queue, a consumer group or a stream's state.
    /// Queen's own namespace for every queue this target owns.
    ///
    /// The dotted prefix on the queue name already carries the application, but
    /// a prefix is a convention this codebase invented and the broker cannot
    /// read. The namespace is a field the broker keeps, so the console can
    /// filter by it and an operator looking at a shared broker sees one team's
    /// queues without knowing how Gate spells its names.
    pub fn namespace(&self) -> String {
        format!("gate.{}", self.application)
    }

    pub fn push_queue(&self) -> String {
        format!("gate.{}.{}.push", self.application, self.name)
    }
    pub fn admitted_queue(&self, lane: &str) -> String {
        format!("gate.{}.{}.admitted.{}", self.application, self.name, lane)
    }
    pub fn calls_queue(&self) -> String {
        format!("gate.{}.{}.calls", self.application, self.name)
    }
    pub fn query_id(&self, lane: &str) -> String {
        format!("gate.{}.{}.{}", self.application, self.name, lane)
    }

    // ------------------------------------------------------------- sharding

    pub fn is_sharded(&self) -> bool {
        self.shard_by.is_some()
    }

    /// One for an unsharded target, so every caller can multiply by it without
    /// branching.
    pub fn shard_count(&self) -> u32 {
        if self.is_sharded() {
            self.shards.unwrap_or(1).max(1)
        } else {
            1
        }
    }

    /// Which shard a dimension value belongs to. A fixed ring: an unmeasured
    /// cardinality cannot become an unbounded partition count, and a collision
    /// serialises two keys that need not have been — never the reverse.
    pub fn shard_of(&self, value: &str) -> u32 {
        shard_index(value, self.shard_count())
    }

    /// The push-queue partition one lane's work goes to. Unsharded this IS the
    /// lane name, which is why nothing about an existing target changes when this
    /// field is absent.
    pub fn push_partition(&self, lane: &str, shard: u32) -> String {
        if self.is_sharded() {
            format!("{lane}:{shard}")
        } else {
            lane.to_string()
        }
    }

    /// Every push partition a lane owns — one gate runner each.
    pub fn lane_partitions(&self, lane: &str) -> Vec<String> {
        (0..self.shard_count())
            .map(|s| self.push_partition(lane, s))
            .collect()
    }
}

/// FNV-1a, modulo the ring size. Written out rather than pulled from a hasher
/// crate because the same number has to come out of the server's push route and
/// the gate's own partitioner, in this process and in the next release.
pub fn shard_index(value: &str, shards: u32) -> u32 {
    let mut h: u64 = 1469598103934665603;
    for b in value.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    (h % shards.max(1) as u64) as u32
}


impl Budget {
    pub fn rate_per_sec(&self) -> f64 {
        self.cap / self.period_seconds as f64
    }
}

/// How many consumers a lane assumes when nobody said. Only ever a default for
/// the batch size of a `next`, so it is a comfort number rather than a limit.
pub const DEFAULT_CONCURRENCY: u32 = 8;

impl Lane {
    /// The single lane a target gets when it declares none.
    ///
    /// One, never two: lanes DIVIDE a ceiling rather than replicate it (see
    /// [`TargetSpec::lane_share`]), so an implicit second lane would halve a
    /// limit nobody asked to halve.
    pub fn sole() -> Self {
        Self {
            name: "default".to_string(),
            cap: CapPolicy::Ceiling,
            concurrency: DEFAULT_CONCURRENCY,
            floor: 0.0,
            default: true,
        }
    }
}


impl TargetSpec {
    /// The fraction of every target budget a lane may spend.
    ///
    /// This exists because of a measured fact, not a theory: each lane is its
    /// own partition, so each lane's gate holds its own copy of the counters.
    /// Two lanes both told "you may use the ceiling" enforce the ceiling twice
    /// and the fleet spends double — an e2e run against a declared 50/s peaked
    /// at 93/s before this was here.
    ///
    /// So the ceiling is *divided*, never replicated. A lane that reserves a
    /// share takes it off the top; `ceiling` takes what is left; and
    /// `ceiling-minus-measured` falls back to its floor until a meter has
    /// something to say. The shares sum to one by construction.
    pub fn lane_share(&self, lane_name: &str, measured: Option<f64>) -> f64 {
        let lane = match self.lane(lane_name) {
            Some(l) => l,
            None => return 0.0,
        };
        // What every other lane is ALLOCATED — not what it reserves statically.
        // The two are different for a `ceiling` lane, which reserves nothing and
        // is allocated the residual, and mixing them is how the shares came to
        // sum above one: a load run at a declared 5000 per ten seconds carried
        // 7131, because `ceiling` subtracted the static reservations while
        // `ceiling-minus-measured` subtracted the measured spend, and neither
        // knew what the other had taken.
        let allocated_to_others: f64 = self
            .lanes
            .iter()
            .filter(|l| l.name != lane_name)
            .map(|l| self.allocation(l))
            .sum();

        match &lane.cap {
            CapPolicy::Ceiling | CapPolicy::Absolute(_) => (1.0 - allocated_to_others).max(0.0),
            CapPolicy::Share(f) => *f,
            // The residual, and ONLY the residual.
            //
            // The name promises borrowing — take what the others measured
            // themselves not using — and the measurement is available, so the
            // first implementation did exactly that. A load run at a declared
            // 5000 per ten seconds then carried 7131, and the reason is not a
            // bug in the arithmetic: an idle neighbour is still ENTITLED to its
            // allocation the instant it wakes, so a lane that takes the idle
            // slack and a lane that keeps its entitlement are counting the same
            // capacity twice.
            //
            // Borrowing across lanes needs a reclaim protocol — the borrower
            // must be told to give the capacity back, and must do so before the
            // lender needs it. There is nowhere to put that: each lane is its
            // own partition with its own counters and no channel between them.
            // So the measured figure may SHRINK this lane and may never grow it
            // past its residual, which makes the shares sum to one by
            // construction and costs the idle capacity of a quiet neighbour.
            //
            // That cost is real and should be spent deliberately: a target
            // whose lanes have very different duty cycles wants `share` with
            // numbers chosen for the traffic, not this.
            CapPolicy::CeilingMinusMeasured => {
                let residual = (1.0 - allocated_to_others).max(lane.floor);
                match measured {
                    Some(m) => residual.min((1.0 - m).max(lane.floor)),
                    None => residual,
                }
            }
        }
    }

    /// What a lane is allocated before any measurement — the floor of what it
    /// may claim. A `ceiling` lane is allocated the residual of the others'
    /// static reservations, which is what makes the sum close.
    fn allocation(&self, lane: &Lane) -> f64 {
        match &lane.cap {
            CapPolicy::Share(f) => *f,
            CapPolicy::CeilingMinusMeasured => lane.floor.max(0.0),
            CapPolicy::Ceiling | CapPolicy::Absolute(_) => {
                let others: f64 = self
                    .lanes
                    .iter()
                    .filter(|l| l.name != lane.name)
                    .map(|l| match &l.cap {
                        CapPolicy::Share(f) => *f,
                        CapPolicy::CeilingMinusMeasured => l.floor.max(0.0),
                        _ => 0.0,
                    })
                    .sum();
                (1.0 - others).max(0.0)
            }
        }
    }


}
