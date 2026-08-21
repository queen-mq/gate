//! The declaration: one document type, and nothing else.
//!
//! v1 had two — a `TargetSpec` and a `GraphSpec` that projected each node back
//! into a `TargetSpec` — and the split cost a projection function, a
//! run-every-target-rule-per-node hack, a one-owner-per-queue-family conflict
//! check and a name resolver. **A graph is the only object here.** A standalone
//! target is a one-node graph, declared through the sugar endpoint that still
//! answers on `/v1/apps/:app/targets/:name`.
//!
//! `deny_unknown_fields` everywhere, exactly as v1: a document a newer build
//! wrote must be UNREADABLE by an older one, because that is what makes the
//! store's `complete: false` honest. A field silently dropped on read is a
//! configuration silently downgraded on the next reconcile pass.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The reserved envelope Gate stamps into a payload it is routing.
///
/// One object rather than four top-level keys, so it cannot collide with a
/// `scopeBy` path, a cost path or the `op` a `whenOp` selects on. Unsigned and
/// unverified: it is trusted because Gate writes it server-side, and because
/// write access to an interior or egress queue is admission bypass anyway (see
/// the trust-model note in the README).
pub const GATE_META: &str = "_gate";

/// The root segment every payload path must start with.
///
/// `payload` names the message's own `data`, so `payload.listingId` reads
/// `msg.data.listingId`. Requiring the prefix is not ceremony: it keeps
/// `_gate.path` unaddressable from a declaration, so a budget can never be
/// scoped on Gate's own provenance stamp.
pub const PAYLOAD_ROOT: &str = "payload";

pub fn default_application() -> String {
    "default".to_string()
}

// ----------------------------------------------------------------- the document

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GraphDoc {
    /// Which application owns this graph.
    ///
    /// Applications do not share ceilings — that is the whole point of the
    /// concept and the reason it can stay this thin. It scopes the name, the
    /// queues, the kv keys, the stored document, the observability, and the part
    /// that matters most: which graphs a sync is allowed to reap.
    #[serde(default = "default_application")]
    pub application: String,

    /// Defaulted so a caller does not have to repeat what the route already
    /// says. Every route that accepts a document overwrites it from the path,
    /// and an empty one is refused by validation — so it is never a name nobody
    /// chose.
    #[serde(default)]
    pub graph: String,

    /// One version for the whole document: a graph is declared atomically, so a
    /// migration-class change to any node is a change to the graph.
    pub version: u32,

    /// Ordered, so a declare, a GET and the console all see the nodes in the
    /// same sequence — and so provisioning is deterministic when something fails
    /// half way.
    pub nodes: BTreeMap<String, Node>,

    pub paths: Vec<Path>,

    /// Opt-in per-graph counters stream (§10.3 of the design). Off by default,
    /// on purpose: observability is a thing you switch on, not a thing that runs
    /// whether or not anyone is looking. Prod, 2026-08-21: v1's always-on
    /// machinery made ~275,000 "is there work?" calls an hour to move messages
    /// 963 times.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub counters: Option<Counters>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Counters {
    #[serde(rename = "windowSeconds")]
    pub window_seconds: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct Node {
    /// At least one, and at least one of them unscoped — see `node-budget` and
    /// `node-unscoped-budget`.
    #[serde(default)]
    pub budgets: Vec<Budget>,

    #[serde(default)]
    pub cost: Cost,

    /// Present on a node work may ENTER at. Absent means the node is fed only by
    /// the paths that relay into it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress: Option<Ingress>,

    /// Required on a terminal node: the queue the application's own consumers
    /// pop with their own SDK.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress: Option<Egress>,

    /// How many messages one claim carries. Defaults to `GATE_STAGE_BATCH`.
    ///
    /// The budget is charged ONCE per batch, so this is also the divisor on the
    /// shared counter's traffic: at batch 200 and 34k items/s the key sees 170
    /// incr/s against a measured 33k/s ceiling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch: Option<u32>,

    /// How many workers drain this node's stages. Defaults to
    /// `max(4, source partitions)`. More workers than partitions is harmless
    /// (the extras find nothing and park); fewer is a throughput ceiling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<u32>,
}

// ------------------------------------------------------------------- budgets

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Budget {
    /// Part of the kv key, so changing it re-founds the counter. Defaults to
    /// `b{index}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    pub count: i64,

    #[serde(rename = "timeMs")]
    pub time_ms: i64,

    /// Smoothing. A declared window longer than a second is SUBDIVIDED: the
    /// budget enforces `count/N` per `timeMs/N`, so a burst cannot take a
    /// ten-second allowance in the first 200ms and starve the rest.
    #[serde(
        default,
        rename = "subWindows",
        skip_serializing_if = "Option::is_none"
    )]
    pub sub_windows: Option<u32>,

    /// One counter per distinct value of this payload path. Replaces the whole
    /// of v1's `scope[]` + `maxKeys` + `shardBy` + `shards` + store-fits
    /// subsystem: cardinality is Postgres rows with a TTL now, not entries in a
    /// document re-read whole on every cycle, so there is no shard to allocate.
    #[serde(default, rename = "scopeBy", skip_serializing_if = "Option::is_none")]
    pub scope_by: Option<String>,

    /// One counter across every node and graph of this application that names
    /// it. Replaces `store: kv`, which was a second KIND of budget with its own
    /// capacity-lease machinery; every budget is a kv counter now and this only
    /// changes which one.
    #[serde(default, rename = "sharedKey", skip_serializing_if = "Option::is_none")]
    pub shared_key: Option<String>,

    /// Charge only for a matching `payload.op`. Suffix globs on dot-separated
    /// segments; a bare `*` matches all; absence takes everything.
    #[serde(default, rename = "whenOp", skip_serializing_if = "Option::is_none")]
    pub when_op: Option<Vec<String>>,

    #[serde(default)]
    pub confidence: Confidence,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,

    #[serde(default, rename = "asOf", skip_serializing_if = "Option::is_none")]
    pub as_of: Option<String>,
}

impl Budget {
    /// The id as it reaches a kv key. `index` is this budget's position in its
    /// node, which is what `b{index}` means.
    pub fn id_or(&self, index: usize) -> String {
        self.id.clone().unwrap_or_else(|| format!("b{index}"))
    }

    /// The declared rate, in cost units per second. Only ever used for display
    /// and for the migration's `absolute:` mapping — the enforcement is
    /// `count_sub` per `window_sub`, which is a different pair of numbers.
    pub fn rate_per_sec(&self) -> f64 {
        self.count as f64 / (self.time_ms.max(1) as f64 / 1000.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Confidence {
    Documented,
    #[default]
    Inferred,
    Assumed,
}

// ---------------------------------------------------------------------- cost

/// What one item spends. `delta = cost`.
///
/// **Integers.** `kv.incr`'s delta is `i64` on this wire, which v1's `f64` cost
/// was not — see the migration notes and the `cost-integer` rule. A fractional
/// weight is expressed by counting tenths and multiplying the budget by ten.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Cost {
    Fixed(i64),
    Path(CostPath),
}

impl Default for Cost {
    fn default() -> Self {
        Cost::Fixed(1)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CostPath {
    pub path: String,
    #[serde(default = "one")]
    pub default: i64,
    /// The largest an item may cost. An item costing more than a budget's
    /// sub-window can NEVER be admitted: it parks the head of its partition for
    /// ever and never reaches a DLQ, because a lease that expires charges no
    /// retry. That is v1's `cost-fits` rule and it survives verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<i64>,
}

fn one() -> i64 {
    1
}

impl Cost {
    /// What the declaration says an item may cost at worst — the number every
    /// `cost-fits`-shaped rule compares against.
    pub fn max(&self) -> i64 {
        match self {
            Cost::Fixed(n) => *n,
            Cost::Path(c) => c.max.unwrap_or(c.default),
        }
    }

    pub fn default_value(&self) -> i64 {
        match self {
            Cost::Fixed(n) => *n,
            Cost::Path(c) => c.default,
        }
    }

    pub fn path(&self) -> Option<&str> {
        match self {
            Cost::Fixed(_) => None,
            Cost::Path(c) => Some(&c.path),
        }
    }
}

// ------------------------------------------------------------------- ingress

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Ingress {
    /// `true` — Gate creates and owns `gate.{app}.{graph}.{node}.ingress`.
    Owned(bool),
    /// Gate CONSUMES a queue the application already owns. Producers push with
    /// their normal SDK, so Gate can be down without blocking ingest. This is
    /// the single most important operational change in v2 and it is what makes
    /// the HTTP push endpoint optional.
    Named(IngressSpec),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct IngressSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
    /// Only meaningful when Gate creates the queue. For a user-owned queue the
    /// partition count is read from the broker at declare time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partitions: Option<u32>,
    /// Keep the `POST .../push` front door. Defaults to `true` for an owned
    /// queue and `false` for a named one — a caller who already pushes with the
    /// SDK did not ask for a second door.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http: Option<bool>,
}

impl Ingress {
    pub fn is_owned(&self) -> bool {
        match self {
            Ingress::Owned(_) => true,
            Ingress::Named(s) => s.queue.is_none(),
        }
    }

    pub fn declared_queue(&self) -> Option<&str> {
        match self {
            Ingress::Owned(_) => None,
            Ingress::Named(s) => s.queue.as_deref(),
        }
    }

    pub fn partitions(&self) -> Option<u32> {
        match self {
            Ingress::Owned(_) => None,
            Ingress::Named(s) => s.partitions,
        }
    }

    pub fn http(&self) -> bool {
        match self {
            Ingress::Owned(_) => true,
            Ingress::Named(s) => s.http.unwrap_or_else(|| s.queue.is_none()),
        }
    }

    /// `ingress: false` is a declaration that this is not an entry, and reads
    /// exactly like the field being absent.
    pub fn is_enabled(&self) -> bool {
        !matches!(self, Ingress::Owned(false))
    }
}

// -------------------------------------------------------------------- egress

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Egress {
    Name(String),
    Spec(EgressSpec),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct EgressSpec {
    pub queue: String,
    /// The consumer group Gate asks about for the "waiting for workers" half of
    /// the ETA. Without it the ETA reports the queue-level (worst-cursor) number
    /// and says so in `assumes`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
}

impl Egress {
    pub fn queue(&self) -> &str {
        match self {
            Egress::Name(q) => q,
            Egress::Spec(s) => &s.queue,
        }
    }
    pub fn group(&self) -> Option<&str> {
        match self {
            Egress::Name(_) => None,
            Egress::Spec(s) => s.group.as_deref(),
        }
    }
}

// ---------------------------------------------------------------------- path

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Path {
    /// Unique. It names the consumer groups, which is why two paths of one name
    /// would share a cursor and SPLIT the stream instead of each receiving it.
    pub name: String,

    /// Lower is sooner. Kept for rank and display only: priority is expressed as
    /// a `share` ceiling on one shared counter, and there is no scheduler
    /// anywhere in this codebase that reads it.
    #[serde(default)]
    pub priority: u32,

    /// The fraction of a node's counter this path may spend. Defaults to equal
    /// steps by priority rank — see `plan::default_shares`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub share: Option<f64>,

    /// A sequence of nodes. Element `i` relays into element `i+1`; a nested
    /// array is a fan-out and the relay transaction pushes to every branch
    /// atomically.
    pub nodes: Vec<PathElem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum PathElem {
    One(String),
    FanOut(Vec<String>),
}

impl PathElem {
    pub fn names(&self) -> Vec<&str> {
        match self {
            PathElem::One(n) => vec![n.as_str()],
            PathElem::FanOut(v) => v.iter().map(|s| s.as_str()).collect(),
        }
    }
    pub fn is_fanout(&self) -> bool {
        matches!(self, PathElem::FanOut(_))
    }
}

// ------------------------------------------------------------------ accessors

impl GraphDoc {
    /// The identity of a graph is the PAIR, never the name alone: two teams may
    /// both have something they call `airbnb`, and they are not the same thing.
    pub fn key(&self) -> String {
        format!("{}/{}", self.application, self.graph)
    }

    /// Queen's own namespace for every queue this graph owns.
    ///
    /// The dotted prefix on a queue name already carries the application, but a
    /// prefix is a convention this codebase invented and the broker cannot read.
    /// The namespace is a field the broker keeps, so an operator on a shared
    /// broker sees one team's queues without knowing how Gate spells its names.
    pub fn namespace(&self) -> String {
        format!("gate.{}", self.application)
    }

    pub fn node(&self, name: &str) -> Option<&Node> {
        self.nodes.get(name)
    }

    pub fn path(&self, name: &str) -> Option<&Path> {
        self.paths.iter().find(|p| p.name == name)
    }

    /// Every node named by any path, in path order then hop order.
    pub fn visited(&self) -> Vec<&str> {
        let mut out: Vec<&str> = Vec::new();
        for p in &self.paths {
            for e in &p.nodes {
                for n in e.names() {
                    if !out.contains(&n) {
                        out.push(n);
                    }
                }
            }
        }
        out
    }

    /// Which paths visit a node — the set whose shares divide that node's one
    /// counter.
    pub fn paths_at(&self, node: &str) -> Vec<&Path> {
        self.paths
            .iter()
            .filter(|p| p.nodes.iter().any(|e| e.names().contains(&node)))
            .collect()
    }
}

// ---------------------------------------------------------------------- names

/// One lowercase segment: what a queue name and a kv key can carry without
/// quoting.
fn ok_segment(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && s.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
}

/// A single segment: applications, graphs, nodes and paths.
pub fn ok_name(s: &str) -> bool {
    s.len() <= 63 && ok_segment(s)
}

/// A dotted name, each segment of which is still a segment. Queue names Gate
/// does not own are not held to this — the application named them.
pub fn ok_target_name(s: &str) -> bool {
    s.len() <= 63 && !s.is_empty() && s.split('.').all(ok_segment)
}
