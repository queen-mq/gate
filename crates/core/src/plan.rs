//! The compiler: a declaration in, a runtime plan out.
//!
//! **Start here.** This file is the new `spec.rs`: it is where every queue name,
//! every consumer group and every kv key in this codebase is minted, and it is
//! the only thing the supervisor reads. `compile` is pure and deterministic, so
//! the whole topology of a graph is testable without a broker — and the declare
//! response echoes the plan, so a caller never has to reconstruct it.
//!
//! # Why one function mints every name
//!
//! v1's rule, kept for v1's measured reason: a near-miss on a consumer group
//! does not fail loudly. The broker answers a group with no cursor with the
//! queue's WHOLE retained range, so an ETA built on a misspelt group reports
//! every message ever pushed as waiting for budget — plausibly, and for ever.
//!
//! # The shape it compiles to
//!
//! A **stage** is one hop of one path: `(path, node, destinations)`. It consumes
//! one queue under one group, charges one node's budgets, and pushes into the
//! next hop's queues. That is the entire runtime: for the seven-stage `airbnb`
//! graph, v1 ran 66 gate runners, 5 meter tasks, 2 merge relays spawning up to
//! 16 workers per cycle across 2 legs each, a reconcile loop, a history prune
//! and a depth cache serving four read shapes. v2 runs seven consumers.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use crate::doc::{Confidence, Cost, GraphDoc, Node, Path, PathElem};

/// How many partitions Gate gives an ingress queue it owns, when the
/// declaration does not say.
///
/// Sixteen because that is the shape the throughput was measured on: `txnload`
/// with sixteen source partitions and disjoint lanes did 23–34k items/s and
/// 603 txn/s, against 33 txn/s for the same workers contending on shared ones.
/// Partitions are lanes here, not counters — the counter is one kv row — so the
/// number buys parallelism and costs nothing else.
pub const DEFAULT_INGRESS_PARTITIONS: u32 = 16;

/// The per-claim batch when nothing declares one.
///
/// The budget is charged ONCE per batch, which is the sentence that makes a
/// single shared counter acceptable where a single shared partition was not: at
/// batch 200 and 34k items/s the key sees 170 incr/s against a measured 33k/s
/// ceiling.
pub const DEFAULT_BATCH: u32 = 200;

/// How many times an item may re-enter a graph when the declaration does not
/// say. v1's `breach[].maxAttempts` default, kept.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;

/// The work lease a stage holds, in seconds, when nothing overrides it.
///
/// Lives here rather than only in `gate-server`'s knobs because the v1 migration
/// has to tell a caller what their `pacing.leaseSeconds` became, and a warning
/// quoting a number the build does not use is worse than no warning. Kept in
/// step with `knobs::Knobs::default().lease_seconds`, which is the authority.
pub const DEFAULT_LEASE_SECONDS: i64 = 10;

/// What one consumer lane drains, in items per second, for the purpose of
/// deciding how many lanes a stage needs.
///
/// **A rate limiter's drain never needs to exceed its own cap.** Workers used to
/// come from the partition count, which is a THROUGHPUT rule: size the consumer
/// to the width of the queue, because the queue is what bounds you. Here it is
/// not — the budget is. Admissible work is bounded by construction, so a stage
/// whose ceiling is 200 items a second needs one lane, not sixteen, however many
/// partitions the ordering happens to be spread over. Stage, measured: about 200
/// gate consumers parked for a system whose largest declared budget is 400/s —
/// one lane's worth, with the rate to spare.
///
/// One thousand, and deliberately pessimistic. The bench measured 3–13k items/s
/// per lane batched, so this keeps at least 3x headroom against the low end.
///
/// **The burst, which is the case worth checking.** The worst a stage can be
/// handed at once is a full window released in one go — `count` tokens. One lane
/// at this rate drains a 2000-token window in about two seconds, well inside any
/// pacing cadence, and the batch is the divisor on everything the lane actually
/// does: at batch 200 that burst is ten claims.
///
/// Partitions do **not** shrink to match. They are the ordering identity — one
/// partition is one order, end to end — and a single wildcard consumer drains
/// all of them, one per claim, in randomised order under `FOR UPDATE SKIP
/// LOCKED`. Fewer lanes than partitions costs latency at saturation and nothing
/// else; fewer PARTITIONS would cost ordering, which is not available.
pub const LANE_CAPACITY: u32 = 1000;

/// The id the v1 migration gives the budget it invents for a node that declared
/// none — a "pass-through" of a million a second, which exists only to satisfy
/// `node-unscoped-budget` and limits nothing.
///
/// Named here because [`fitting_workers`] has to ignore it: it is a sentinel,
/// not a measurement, and dividing it by a lane's capacity would ask for a
/// thousand lanes for a node that paces nothing.
pub const PASSTHROUGH_BUDGET_ID: &str = "passthrough";

/// A budget declared `assumed` is arithmetic on a guess.
///
/// v1 defined this, unit-tested it, documented it in the README — *"an assumed
/// cap is enforced at 70% of what it claims"* — and never applied it:
/// `effective_cap` had no caller. It is wired here but **not switched on**:
/// `PlanOpts::assumed_factor` defaults to 1.0, so this build enforces exactly
/// what a declaration says. Turning it on is a product decision (design §16.3)
/// and it changes what every existing `assumed` budget admits, so it is not one
/// an implementer makes on the way past.
pub const ASSUMED_FACTOR: f64 = 0.7;

/// What the compiler needs that the document does not say.
#[derive(Debug, Clone)]
pub struct PlanOpts {
    pub batch: u32,
    /// A fleet-wide override for the per-stage worker count. `None` leaves the
    /// derived default — see [`fitting_workers`].
    pub concurrency: Option<u32>,
    /// What one consumer lane can drain, in items per second. See
    /// [`LANE_CAPACITY`].
    pub lane_capacity: u32,
    /// Partition counts observed at the broker, by queue name. Empty in a pure
    /// test; filled at declare time for user-owned ingress queues, whose width
    /// Gate reads rather than chooses.
    pub partitions: BTreeMap<String, u32>,
    /// See [`ASSUMED_FACTOR`]. 1.0 applies nothing.
    pub assumed_factor: f64,
}

impl Default for PlanOpts {
    fn default() -> Self {
        Self {
            batch: DEFAULT_BATCH,
            concurrency: None,
            lane_capacity: LANE_CAPACITY,
            partitions: BTreeMap::new(),
            assumed_factor: 1.0,
        }
    }
}

// --------------------------------------------------------------------- output

#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub application: String,
    pub graph: String,
    pub version: u32,
    pub namespace: String,
    pub nodes: BTreeMap<String, NodePlan>,
    pub stages: Vec<Stage>,
    pub queues: Vec<QueueSpec>,
    pub counters_window_seconds: Option<u32>,
    /// The re-entry bound, resolved (design §16.6). See
    /// [`DEFAULT_MAX_ATTEMPTS`].
    pub max_attempts: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NodePlan {
    pub name: String,
    pub budgets: Vec<CompiledBudget>,
    pub cost: Cost,
    /// The queue work enters this node by, when it is an entry.
    pub ingress_queue: Option<String>,
    /// Whether Gate created it. A user-owned queue is consumed and never
    /// configured: its retention, its lease and its partition count belong to
    /// the application that made it.
    pub ingress_owned: bool,
    pub ingress_http: bool,
    /// Whether the front door refuses with a 429 rather than queueing. Off
    /// unless the declaration asks for it — see `IngressSpec::shed`.
    pub ingress_shed: bool,
    /// Where relays into this node push. Always named, even for a node no path
    /// relays into — the name is cheap and a plan with a hole in it is not.
    pub interior_queue: String,
    pub egress_queue: Option<String>,
    pub egress_group: Option<String>,
    /// `brk:{app}:{graph}:{node}` — the breaker record's key.
    pub breaker_key: String,
    /// What fraction of this node's counter each path that visits it may spend.
    pub shares: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Stage {
    pub path: String,
    pub priority: u32,
    /// This path's fraction of `node`'s counter. `max = round(count_sub *
    /// share)`; the higher-priority paths' headroom above it is an exact,
    /// atomic reserve held by the same row lock that does the counting.
    pub share: f64,
    pub node: String,
    /// 0-based position in the path.
    pub hop: usize,
    pub source: String,
    pub group: String,
    /// The first hop reads an ingress queue, where a producer that has never
    /// heard of Gate is the writer. Nothing there carries a `_gate` stamp Gate
    /// wrote, so nothing there can be foreign.
    pub first_hop: bool,
    /// Whether this stage's source is a Gate-owned INTERIOR queue — one written
    /// **only** by Gate's own relay and never by an application producer.
    ///
    /// It decides where a NEW group's cursor is seeded, and it is the one fact
    /// that makes seeding at the tail safe. See `relay::spawn`.
    ///
    /// # The incident, 2026-09-02
    ///
    /// `channel-go` redeclared its `vrbo` graph adding a path `reviews` through
    /// the pre-existing terminal node `partner`. That compiled a stage reading
    /// the INTERIOR queue `gate.channel-go.vrbo.partner.in` under a brand-new
    /// group, and every group Gate owned was seeded with `All` — so the group's
    /// cursor started at the oldest retained frame of all 105 partitions, about
    /// 19,800 frames belonging to the other three paths, the oldest twelve days
    /// old. Every one of them was foreign, foreign frames are settled by ack
    /// inside the relay's single transaction (§6.4/§6.7), and the broker
    /// resolves those acks by transaction hash against `queen.log_txns` — which
    /// it purges after `GREATEST(dedup_window, completed_retention, 900s)`
    /// (`006_log_maintenance.sql:389`), far short of the segment retention. A
    /// twelve-day-old hash resolves nowhere, so the broker raised `QTXN ack
    /// references unknown transactionId; transaction rolled back`
    /// (`005_log_ack.sql:1451`) and rolled the whole relay transaction back. The
    /// cursor never moved, the stage retried about ten times a second per
    /// replica for ever, both replicas OOMed, and the console reported the
    /// backlog as `waiting_for_budget` — which nothing was.
    ///
    /// # Why the rule is exact and not a heuristic
    ///
    /// A frame lands on an interior queue only because a stage of some path `P`
    /// relayed it there, and it carries `P`'s `_gate.path` stamp. A group for
    /// path `P` on that queue can therefore never need anything older than the
    /// instant `P`'s own upstream stage started relaying. Seeding it at the tail
    /// as of *this runtime's start* skips exactly the other paths' backlog and
    /// nothing of its own.
    ///
    /// An INGRESS queue is the opposite case and keeps `All`: an application
    /// producer writes it, a backlog there is real work the limiter exists to
    /// pace, and two paths entering at one node is pub-sub by design
    /// (§4.2/§6.7). That holds for a user-owned ingress (`DocIngress.queue`) and
    /// for the Gate-owned `gate.{app}.{graph}.{node}.ingress` HTTP door alike.
    ///
    /// Derived from the plan, never from the shape of the name: a stage's source
    /// is interior exactly when it is the node's [`NodePlan::interior_queue`],
    /// which is true at every hop after the first and at a first hop whose node
    /// declares no ingress at all (a shape `path-entry` refuses, and one where
    /// the stage could never own a frame anyway).
    pub source_is_interior: bool,
    /// Whether this stage shares its source queue with another path's group, and
    /// therefore has to recognise and settle messages that are not its own.
    pub check_foreign: bool,
    /// Whether an UNSTAMPED message on this stage's source belongs to it.
    ///
    /// A payload that is not a JSON object cannot carry `_gate` (see
    /// `relay::stamp`), so on a shared interior queue there is nothing to read
    /// ownership off. Reading "unstamped" as "mine" in every group made every
    /// path forward the same message, under distinct derived ids that dedup
    /// cannot collapse — a limiter multiplying a message by the number of
    /// converging paths. So exactly ONE stage per shared source owns the
    /// unstamped ones and the rest settle them as foreign, which is the same
    /// machinery and costs nothing new. Always true where the source is read by
    /// one stage.
    pub owns_unstamped: bool,
    pub batch: u32,
    pub concurrency: u32,
    pub destinations: Vec<Destination>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Destination {
    /// The node this push belongs to — the next hop's node, or this stage's own
    /// node when the push is the terminal one into an egress queue.
    pub node: String,
    pub queue: String,
    /// `{path}/{node}`, the label a derived transaction id hashes.
    pub label: String,
    /// Whether this push gets a DERIVED transaction id rather than the
    /// upstream's own.
    ///
    /// True at a fan-out (two branches must not carry one id, or a later
    /// convergence dedups one of them away); true where several stages of this
    /// plan push into one queue (two messages that entered by different paths
    /// carrying the same upstream id — which is exactly what pub-sub over a
    /// shared ingress produces — would otherwise collapse on arrival); and
    /// **always at a terminal push**.
    ///
    /// The last one is the case a count cannot see. `converging` is counted over
    /// the stages of ONE plan, so two separate graphs that both name
    /// `channel.airbnb.out` as an egress each counted one and each reused the
    /// upstream id verbatim. Partition passthrough puts both copies on the
    /// same-named partition, and the broker's dedup probes by hash per partition
    /// (`003_log_push.sql`) over the transaction id's bytes alone — so if one
    /// producer event id enters both graphs, the second push is a dedup hit and
    /// that message is SILENTLY LOST. §11's `egress-owner` is only a warning, so
    /// nothing prevents the topology.
    ///
    /// Deriving there costs nothing and keeps every property the reuse was for:
    /// the id is still deterministic in the message's own, so a redelivered
    /// relay still computes the same one and dedup still refuses the second
    /// push, and a producer's own coalescing id still collapses two pushes into
    /// one. What changes is the value an application's consumer reads off the
    /// egress queue, which is why this is recorded in §7 rather than done
    /// quietly.
    pub derive_id: bool,
    pub terminal: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum QueueKind {
    /// Gate created it and owns its options.
    OwnedIngress,
    /// Gate created it and owns its options; it is where relays put work.
    Interior,
    /// The application's. Gate pushes; the application pops.
    Egress,
    /// The application's. Gate consumes; the application pushes. **Never**
    /// created or configured here.
    UserIngress,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueueSpec {
    pub name: String,
    pub kind: QueueKind,
    pub partitions: Option<u32>,
}

/// One budget, with the subdivision arithmetic already done.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledBudget {
    pub id: String,
    /// The kv key, or — for a scoped budget — everything before the scope value.
    pub key: String,
    pub scope_by: Option<String>,
    pub shared_key: Option<String>,
    pub when_op: Option<Vec<String>>,
    pub count: i64,
    pub time_ms: i64,
    pub sub_windows: u32,
    /// What one sub-window admits, before a path's share is applied.
    pub count_sub: i64,
    /// The kv TTL, in whole seconds. **A kv TTL cannot be shorter than one
    /// second** (`queen-protocol/src/kv.rs`, `Expiry`), which is why this is
    /// floored rather than expressed at the declared width.
    pub window_sub_seconds: i64,
    pub confidence: Confidence,
}

impl CompiledBudget {
    pub fn is_scoped(&self) -> bool {
        self.scope_by.is_some()
    }

    /// The key one charge lands on. A scoped budget gets one row per distinct
    /// value, reaped by its own TTL — 200,000 live listings is 200,000 Postgres
    /// rows, where v1 needed 64 shards, 64 gate runners, 64 partition leases and
    /// 64 state documents to hold the same thing.
    pub fn key_for(&self, scope: Option<&str>) -> String {
        match (self.scope_by.as_ref(), scope) {
            (Some(_), Some(v)) => format!("{}:{v}", self.key),
            _ => self.key.clone(),
        }
    }

    /// `round(count_sub * share)` — the `max` a path's `incr` carries.
    ///
    /// Rounded, not floored, because a share IS the declared intent and the
    /// floor is already applied twice on the way here (once to `count_sub`, once
    /// to `window_sub_seconds`). Never negative.
    pub fn max_for(&self, share: f64) -> i64 {
        ((self.count_sub as f64) * share).round().max(0.0) as i64
    }
}

// -------------------------------------------------------------------- naming
//
// Every one of these is called from exactly one place. See the module doc for
// why that is a rule and not a preference.

pub fn namespace(app: &str) -> String {
    format!("gate.{app}")
}

pub fn owned_ingress_queue(app: &str, graph: &str, node: &str) -> String {
    format!("gate.{app}.{graph}.{node}.ingress")
}

pub fn interior_queue(app: &str, graph: &str, node: &str) -> String {
    format!("gate.{app}.{graph}.{node}.in")
}

/// One group per **(path, node)**, not per node.
///
/// Two paths sharing an ingress node is pub-sub: each path's group gets EVERY
/// message, so the message traverses both paths. That is the documented,
/// intended semantics and it composes with fan-out — and it is why the path is
/// in the name.
pub fn stage_group(app: &str, graph: &str, path: &str, node: &str) -> String {
    format!("gate.{app}.{graph}.{path}.{node}")
}

pub fn budget_key(app: &str, graph: &str, node: &str, bid: &str) -> String {
    format!("b:{app}:{graph}:{node}:{bid}")
}

pub fn shared_budget_key(app: &str, shared: &str) -> String {
    format!("b:{app}:shared:{shared}")
}

pub fn breaker_key(app: &str, graph: &str, node: &str) -> String {
    format!("brk:{app}:{graph}:{node}")
}

/// The label a derived transaction id hashes: `{app}/{graph}/{path}/{node}`.
///
/// The application and the graph are in it because `converging` can only count
/// the stages of ONE plan. Two graphs that both name `channel.airbnb.out` as an
/// egress each counted one, so each thought it had nothing to distinguish itself
/// from — and a label of `{path}/{node}` can coincide across graphs anyway. With
/// both in the name, ids minted by different graphs cannot collide by
/// construction rather than by a count.
pub fn label(app: &str, graph: &str, path: &str, node: &str) -> String {
    format!("{app}/{graph}/{path}/{node}")
}

/// The label a TERMINAL push hashes: [`label`] with `/out` on the end.
///
/// A terminal push's destination node is the stage's own node, so it would
/// otherwise carry the identical label to the hop that fed it — which is
/// harmless, because the parent ids differ, and confusing to anyone
/// reproducing a value by hand.
pub fn egress_label(app: &str, graph: &str, path: &str, node: &str) -> String {
    format!("{}/out", label(app, graph, path, node))
}

// ------------------------------------------------------------------- windows

/// How many sub-windows a budget is enforced in, when it does not say.
///
/// Aim for a one-second sub-window, and never more sub-windows than the count:
/// above that `count_sub` floors to 1 and the budget enforces `N` per window
/// instead of `count` per window, which is the opposite of what was declared.
pub fn default_sub_windows(count: i64, time_ms: i64) -> u32 {
    if time_ms < 2000 {
        return 1;
    }
    let want = (time_ms / 1000).clamp(1, count.max(1));
    want.clamp(1, u32::MAX as i64) as u32
}

/// `(count_sub, window_sub_seconds)`.
///
/// # The property, and it is the only one that matters
///
/// ```text
/// count_sub / window_sub_seconds  <=  count / (time_ms / 1000)
/// ```
///
/// **The enforced rate is at or below the declared one, for every input.**
/// Enforcing tighter than declared is the safe direction; enforcing looser is a
/// vendor block, which is the failure this whole service exists to prevent.
///
/// This used to say *"rounding is always down, in both terms, so the enforced
/// ceiling is at or below the declared one"*, and that was wrong — the two terms
/// are not on the same side of the fraction. Rounding the COUNT down lowers a
/// numerator, which is tighter; rounding the WINDOW down lowers a DENOMINATOR,
/// which is **looser**. `subdivide(200000, 3600000, 2000)` gave `100` per
/// `floor(1800ms) = 1s`, enforcing 100/s against a declared 55.6/s — very nearly
/// twice the ceiling the caller wrote down.
///
/// So the window is not derived from `time_ms / n` at all. It is derived from
/// the count that was actually chosen, rounded **up**:
///
/// ```text
/// window_sub_seconds = max(1, ceil(count_sub * time_ms / (count * 1000)))
/// ```
///
/// which is the inequality above, solved for the window. Deriving it from
/// `count_sub` rather than from `n` also closes the second leak: `count_sub` has
/// a floor of 1, so a budget with more sub-windows than count (`count: 5` over
/// ten of them) enforced `1` per `1s` where `1` per `2s` was declared —
/// `subwindow-fits` refuses that document, but this function is public and is
/// called before anything has validated.
///
/// The one-second floor is not a choice: a kv TTL is whole seconds with a
/// minimum of one, so a budget declared over 200ms is enforced as `count` per
/// SECOND — tighter than `count` per 200ms, never looser, but slower. The
/// declare response says so loudly (`window-sub-second`).
///
/// Rounding up costs exactness where the division is not clean: 20000 per 300s
/// over 150 sub-windows is `133` per `2s` (66.5/s) against a declared 66.67/s.
/// That is the trade, taken deliberately in the safe direction. `migrate`
/// avoids paying it at all by choosing an `n` that DIVIDES the period (§12.2),
/// and a hand-written document can do the same.
pub fn subdivide(count: i64, time_ms: i64, sub_windows: u32) -> (i64, i64) {
    let n = sub_windows.max(1) as i64;
    let count_sub = (count / n).max(1);
    // i128 because the numerator is a product of two declared numbers and this
    // must not be the place a large budget wraps. Ceiling division, written out
    // rather than as floats: the whole point is an exact comparison.
    let num = (count_sub as i128) * (time_ms.max(1) as i128);
    let den = (count.max(1) as i128) * 1000;
    let window_sub_seconds = ((num + den - 1) / den).max(1) as i64;
    (count_sub, window_sub_seconds)
}

// ------------------------------------------------------------------- shares

/// The default share of a node's counter for each path that visits it.
///
/// With `K` distinct priority ranks present at the node, rank `r` (0 = highest)
/// gets `(K - r) / K`. Three ranks → 1.0, 0.667, 0.333. The top rank is always
/// 1.0, which is the `share-top` rule: the highest priority must be able to
/// reach the whole ceiling, or the headroom above every other path's share
/// belongs to nobody.
///
/// **The shares do not have to sum to 1, and normally will not.** They overlap
/// on purpose, and the total is still bounded because there is ONE counter.
/// Every v1 rule about oversubscription existed to police an invariant that is
/// structural here — a load run at a declared 5000 per ten seconds carried 7131
/// because two lanes each held their own copy of the counter, and that shape is
/// no longer expressible.
pub fn default_shares(priorities: &[u32]) -> BTreeMap<u32, f64> {
    let mut ranks: Vec<u32> = priorities.to_vec();
    ranks.sort_unstable();
    ranks.dedup();
    let k = ranks.len().max(1) as f64;
    ranks
        .iter()
        .enumerate()
        .map(|(r, p)| (*p, (k - r as f64) / k))
        .collect()
}

// ------------------------------------------------------------------- compile

pub fn compile(doc: &GraphDoc) -> Plan {
    compile_with(doc, &PlanOpts::default())
}

pub fn compile_with(doc: &GraphDoc, opts: &PlanOpts) -> Plan {
    let app = doc.application.as_str();
    let graph = doc.graph.as_str();

    // ---- per-node: budgets, queues, shares.
    let mut nodes: BTreeMap<String, NodePlan> = BTreeMap::new();
    for (name, node) in &doc.nodes {
        let visiting = doc.paths_at(name);
        let defaults = default_shares(&visiting.iter().map(|p| p.priority).collect::<Vec<_>>());
        let shares: BTreeMap<String, f64> = visiting
            .iter()
            .map(|p| {
                // A share is a fraction of a SHARED counter. Where a node is
                // crossed by exactly one path there is nothing to share, and a
                // fraction there would be capacity nobody can reach: the
                // headroom above it belongs to no other path, so it is simply
                // lost. So a sole occupant always gets the whole ceiling, and
                // the declared fraction takes effect at the nodes where the
                // paths actually meet — which is what settled point 3 means by
                // "per-caller max ceilings on the SHARED node key".
                let s = if visiting.len() <= 1 {
                    1.0
                } else {
                    p.share
                        .unwrap_or_else(|| defaults.get(&p.priority).copied().unwrap_or(1.0))
                };
                (p.name.clone(), s)
            })
            .collect();

        let ingress_queue = node.ingress.as_ref().filter(|i| i.is_enabled()).map(|i| {
            i.declared_queue()
                .map(|q| q.to_string())
                .unwrap_or_else(|| owned_ingress_queue(app, graph, name))
        });

        nodes.insert(
            name.clone(),
            NodePlan {
                name: name.clone(),
                budgets: compile_budgets(app, graph, name, node, opts.assumed_factor),
                cost: node.cost.clone(),
                ingress_queue,
                ingress_owned: node
                    .ingress
                    .as_ref()
                    .filter(|i| i.is_enabled())
                    .is_some_and(|i| i.is_owned()),
                ingress_http: node
                    .ingress
                    .as_ref()
                    .filter(|i| i.is_enabled())
                    .is_some_and(|i| i.http()),
                ingress_shed: node
                    .ingress
                    .as_ref()
                    .filter(|i| i.is_enabled())
                    .is_some_and(|i| i.shed()),
                interior_queue: interior_queue(app, graph, name),
                egress_queue: node.egress.as_ref().map(|e| e.queue().to_string()),
                egress_group: node
                    .egress
                    .as_ref()
                    .and_then(|e| e.group().map(String::from)),
                breaker_key: breaker_key(app, graph, name),
                shares,
            },
        );
    }

    // ---- stages, in path order then hop order.
    let mut stages: Vec<Stage> = Vec::new();
    for p in &doc.paths {
        let hops = p.nodes.len();
        for (i, elem) in p.nodes.iter().enumerate() {
            let last = i + 1 == hops;
            for node_name in elem.names() {
                let Some(np) = nodes.get(node_name) else {
                    // An unknown node. Validation refuses the document; the
                    // compiler simply does not invent a stage for it, so a
                    // caller that skipped validation gets a short plan rather
                    // than a panic.
                    continue;
                };
                let source = if i == 0 {
                    np.ingress_queue
                        .clone()
                        .unwrap_or_else(|| np.interior_queue.clone())
                } else {
                    np.interior_queue.clone()
                };
                // The `unwrap_or_else` above is why this is a comparison and not
                // `i > 0`: a first hop whose node declares an ingress reads a
                // queue somebody else writes, and a first hop whose node
                // declares none falls back to the interior queue. Only the
                // second is Gate-written. (`path-entry` refuses the second, so
                // this arm exists for a caller who skipped validation.)
                let source_is_interior = source == np.interior_queue;

                let destinations: Vec<Destination> = if last {
                    match &np.egress_queue {
                        Some(q) => vec![Destination {
                            node: node_name.to_string(),
                            queue: q.clone(),
                            label: egress_label(app, graph, &p.name, node_name),
                            // ALWAYS at a terminal. See the note on `derive_id`.
                            derive_id: true,
                            terminal: true,
                        }],
                        None => Vec::new(),
                    }
                } else {
                    p.nodes[i + 1]
                        .names()
                        .iter()
                        .filter_map(|d| {
                            nodes.get(*d).map(|dn| Destination {
                                node: (*d).to_string(),
                                queue: dn.interior_queue.clone(),
                                label: label(app, graph, &p.name, d),
                                derive_id: false,
                                terminal: false,
                            })
                        })
                        .collect()
                };

                let share = np.shares.get(&p.name).copied().unwrap_or(1.0);
                let node_doc = doc.nodes.get(node_name);
                let partitions_hint = partitions_hint(opts, &source, node_doc);
                let declared_batch = node_doc.and_then(|n| n.batch).unwrap_or(opts.batch).max(1);
                stages.push(Stage {
                    path: p.name.clone(),
                    priority: p.priority,
                    share,
                    node: node_name.to_string(),
                    hop: i,
                    source,
                    group: stage_group(app, graph, &p.name, node_name),
                    first_hop: i == 0,
                    source_is_interior,
                    check_foreign: false,  // filled below
                    owns_unstamped: false, // filled below
                    batch: fitting_batch(np, share, declared_batch),
                    concurrency: node_doc
                        .and_then(|n| n.concurrency)
                        .or(opts.concurrency)
                        .unwrap_or_else(|| {
                            fitting_workers(np, share, partitions_hint, opts.lane_capacity)
                        })
                        .max(1),
                    destinations,
                });
            }
        }
    }

    // ---- the two facts a stage cannot know on its own.
    //
    // `converging` is why §7's middle arm exists: several stages pushing into one
    // queue means reuse of the upstream id would silently collapse two legitimate
    // messages. Counted once, here, so the relay reads a bool.
    let mut converging: HashMap<&str, usize> = HashMap::new();
    for s in &stages {
        for d in &s.destinations {
            *converging.entry(d.queue.as_str()).or_insert(0) += 1;
        }
    }
    let mut readers: HashMap<&str, usize> = HashMap::new();
    for s in &stages {
        *readers.entry(s.source.as_str()).or_insert(0) += 1;
    }
    let converging: HashMap<String, usize> = converging
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
    let readers: HashMap<String, usize> = readers
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

    // One owner per source for the messages nobody stamped. Deterministic and
    // decided here, so the relay reads a bool: the FIRST stage in plan order
    // that reads a queue owns them, and every other reader of that queue settles
    // them as foreign. Stages are built in path order then hop order, and
    // `doc.paths` is a list, so this is stable across replicas compiling the
    // same document.
    let mut claimed: HashSet<String> = HashSet::new();
    for s in &mut stages {
        s.check_foreign = !s.first_hop && readers.get(&s.source).copied().unwrap_or(1) > 1;
        s.owns_unstamped = claimed.insert(s.source.clone());
        let fanout = s.destinations.len() > 1;
        for d in &mut s.destinations {
            d.derive_id =
                d.terminal || fanout || converging.get(&d.queue).copied().unwrap_or(1) > 1;
        }
    }

    // ---- queues, deduplicated, in a deterministic order.
    let mut queues: Vec<QueueSpec> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for (name, np) in &nodes {
        if let Some(q) = &np.ingress_queue {
            if seen.insert(q.clone()) {
                queues.push(QueueSpec {
                    name: q.clone(),
                    kind: if np.ingress_owned {
                        QueueKind::OwnedIngress
                    } else {
                        QueueKind::UserIngress
                    },
                    partitions: doc
                        .nodes
                        .get(name)
                        .and_then(|n| n.ingress.as_ref())
                        .and_then(|i| i.partitions())
                        .or(if np.ingress_owned {
                            Some(DEFAULT_INGRESS_PARTITIONS)
                        } else {
                            None
                        }),
                });
            }
        }
    }
    // Only the interior queues a stage actually reads. A node no path relays
    // into has a NAME for one and no queue: provisioning a queue nothing writes
    // and nothing drains is how a broker fills up with a topology's ghosts.
    for s in &stages {
        if !s.first_hop && seen.insert(s.source.clone()) {
            queues.push(QueueSpec {
                name: s.source.clone(),
                kind: QueueKind::Interior,
                partitions: None,
            });
        }
    }
    for np in nodes.values() {
        if let Some(q) = &np.egress_queue {
            if seen.insert(q.clone()) {
                queues.push(QueueSpec {
                    name: q.clone(),
                    kind: QueueKind::Egress,
                    partitions: None,
                });
            }
        }
    }

    Plan {
        application: doc.application.clone(),
        graph: doc.graph.clone(),
        version: doc.version,
        namespace: namespace(app),
        nodes,
        stages,
        queues,
        counters_window_seconds: doc.counters.as_ref().map(|c| c.window_seconds),
        max_attempts: doc.max_attempts.unwrap_or(DEFAULT_MAX_ATTEMPTS).max(1),
    }
}

/// How many messages one claim should carry, given what this path may spend in
/// one sub-window.
///
/// **This is the number that keeps the deferral path rare, and the reason it
/// exists is a measurement of the broker.** Settling a claim in full re-arms its
/// partition in about **seven milliseconds**; settling a PREFIX of one leaves the
/// partition parked until the lease expires — measured at exactly the lease, and
/// asserted by `an_ack_settles_the_whole_claim_or_pays_a_lease` in the live
/// suite. So a claim that routinely exceeds what the window admits would pace
/// itself by the LEASE, which is the thing this design set out to remove.
///
/// So the batch is clamped to what one sub-window admits at the typical item
/// cost: `round(count_sub × share) / cost.default`, over the tightest UNSCOPED
/// budget. Scoped budgets are excluded on purpose — a batch of two hundred
/// messages across two hundred different keys spends one unit of each, and
/// sizing on a per-key count would shrink every claim to a per-key allowance.
///
/// It is a floor of one and a ceiling of what the declaration asked for: a wide
/// budget leaves the declared batch untouched, and a tight one gets a claim that
/// fits.
fn fitting_batch(np: &NodePlan, share: f64, declared: u32) -> u32 {
    let per_item = np.cost.default_value().max(1);
    let fits = np
        .unscoped()
        .map(|b| (b.max_for(share) / per_item).max(1))
        .min();
    match fits {
        Some(n) => declared.min(n.clamp(1, u32::MAX as i64) as u32),
        None => declared,
    }
}

/// How many workers one stage needs, given what it is allowed to admit.
///
/// ```text
/// workers = clamp(ceil(cap_rate_per_sec / LANE_CAPACITY), 1, partitions)
/// ```
///
/// `cap_rate_per_sec` is this stage's BINDING budget — the tightest of them,
/// measured as what one sub-window admits over how long that sub-window is,
/// because that is what is actually enforced — with this path's `share` applied,
/// because that is this path's own ceiling on the shared counter.
///
/// **This replaces `max(4, partitions)`, which was the wrong rule for this
/// service.** Sizing a consumer to the width of its queue is a throughput rule:
/// it assumes the queue is what bounds you. In a rate limiter the BUDGET bounds
/// you, by construction, so lanes beyond what the cap can feed are parked polls
/// that can never have work. Stage, measured: ~200 gate consumers for a system
/// whose largest declared budget is 400 items a second.
///
/// Only the UNSCOPED budgets, for the same reason `fitting_batch` uses them: a
/// per-key budget is not a rate the node has. *100 photo deletions per listing
/// per week* says nothing about how fast the node drains.
///
/// And not the migration's [`PASSTHROUGH_BUDGET_ID`], which is a sentinel and
/// not a measurement: a million a second means "this node limits nothing", and
/// dividing it by a lane would ask for a thousand lanes for a class node that
/// paces no traffic of its own. A node left with no rate at all gets ONE worker,
/// which is the honest answer — what such a node forwards is bounded by the
/// tight node downstream of it, not by itself.
///
/// Clamped UP to `partitions` and never above it: a lane with no partition to
/// claim finds nothing, for ever. Clamped DOWN to one: a stage with no workers
/// is a queue nobody drains.
fn fitting_workers(np: &NodePlan, share: f64, partitions: u32, lane_capacity: u32) -> u32 {
    let capacity = lane_capacity.max(1) as f64;
    let tightest = np
        .unscoped()
        .filter(|b| b.id != PASSTHROUGH_BUDGET_ID)
        .map(|b| b.max_for(share) as f64 / b.window_sub_seconds.max(1) as f64)
        .fold(f64::INFINITY, f64::min);
    if !tightest.is_finite() {
        return 1;
    }
    let want = (tightest / capacity).ceil().max(1.0);
    (want as u32).clamp(1, partitions.max(1))
}

fn partitions_hint(opts: &PlanOpts, source: &str, node: Option<&Node>) -> u32 {
    if let Some(n) = opts.partitions.get(source) {
        return *n;
    }
    node.and_then(|n| n.ingress.as_ref())
        .and_then(|i| i.partitions())
        .unwrap_or(DEFAULT_INGRESS_PARTITIONS)
}

fn compile_budgets(
    app: &str,
    graph: &str,
    node: &str,
    n: &Node,
    assumed_factor: f64,
) -> Vec<CompiledBudget> {
    n.budgets
        .iter()
        .enumerate()
        .map(|(i, b)| {
            let id = b.id_or(i);
            let sub_windows = b
                .sub_windows
                .unwrap_or_else(|| default_sub_windows(b.count, b.time_ms))
                .max(1);
            let count = match b.confidence {
                Confidence::Assumed => ((b.count as f64) * assumed_factor).floor().max(1.0) as i64,
                _ => b.count,
            };
            let (count_sub, window_sub_seconds) = subdivide(count, b.time_ms, sub_windows);
            CompiledBudget {
                key: match &b.shared_key {
                    Some(k) => shared_budget_key(app, k),
                    None => budget_key(app, graph, node, &id),
                },
                id,
                scope_by: b.scope_by.clone(),
                shared_key: b.shared_key.clone(),
                when_op: b.when_op.clone(),
                count: b.count,
                time_ms: b.time_ms,
                sub_windows,
                count_sub,
                window_sub_seconds,
                confidence: b.confidence,
            }
        })
        .collect()
}

// -------------------------------------------------------------- graph algebra

/// The edges every path implies, as `(from, to)` pairs. A fan-out contributes
/// one edge per branch.
pub fn edges(doc: &GraphDoc) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for p in &doc.paths {
        for i in 0..p.nodes.len().saturating_sub(1) {
            for from in p.nodes[i].names() {
                for to in p.nodes[i + 1].names() {
                    out.push((from.to_string(), to.to_string()));
                }
            }
        }
    }
    out
}

/// A cycle in the union of every path's edges, if there is one. Kahn, and the
/// leftovers ARE the cycle.
pub fn find_cycle(doc: &GraphDoc) -> Option<Vec<String>> {
    let edges = edges(doc);
    let mut names: Vec<&str> = doc.nodes.keys().map(|s| s.as_str()).collect();
    for (a, b) in &edges {
        for n in [a.as_str(), b.as_str()] {
            if !names.contains(&n) {
                names.push(n);
            }
        }
    }
    let mut indeg: HashMap<&str, usize> = names.iter().map(|n| (*n, 0)).collect();
    let mut out: HashMap<&str, Vec<&str>> = HashMap::new();
    let mut seen_edge: HashSet<(&str, &str)> = HashSet::new();
    for (a, b) in &edges {
        if !seen_edge.insert((a.as_str(), b.as_str())) {
            continue;
        }
        *indeg.entry(b.as_str()).or_insert(0) += 1;
        out.entry(a.as_str()).or_default().push(b.as_str());
    }
    let mut q: VecDeque<&str> = names
        .iter()
        .copied()
        .filter(|n| indeg.get(n) == Some(&0))
        .collect();
    let mut done = 0usize;
    while let Some(n) = q.pop_front() {
        done += 1;
        for m in out.get(n).cloned().unwrap_or_default() {
            if let Some(d) = indeg.get_mut(m) {
                *d -= 1;
                if *d == 0 {
                    q.push_back(m);
                }
            }
        }
    }
    if done == names.len() {
        return None;
    }
    let mut stuck: Vec<String> = names
        .iter()
        .filter(|n| indeg.get(*n).copied().unwrap_or(0) > 0)
        .map(|n| n.to_string())
        .collect();
    stuck.sort();
    Some(stuck)
}

impl Plan {
    pub fn key(&self) -> String {
        format!("{}/{}", self.application, self.graph)
    }

    pub fn node(&self, name: &str) -> Option<&NodePlan> {
        self.nodes.get(name)
    }

    pub fn stages_of_node<'a>(&'a self, node: &'a str) -> impl Iterator<Item = &'a Stage> {
        self.stages.iter().filter(move |s| s.node == node)
    }

    /// The stage that charges `node` on `path`, at whichever hop that is.
    pub fn stage(&self, path: &str, node: &str) -> Option<&Stage> {
        self.stages
            .iter()
            .find(|s| s.path == path && s.node == node)
    }

    pub fn queue(&self, name: &str) -> Option<&QueueSpec> {
        self.queues.iter().find(|q| q.name == name)
    }
}

impl NodePlan {
    /// The budgets that live on the node itself, rather than one per key.
    ///
    /// The breaker spends these. ETA and other node-wide calculations must
    /// additionally exclude budgets with `whenOp`: a conditional counter is
    /// not a rate every item meets.
    pub fn unscoped(&self) -> impl Iterator<Item = &CompiledBudget> {
        self.budgets.iter().filter(|b| !b.is_scoped())
    }

    /// The widest ceiling any path can reach at this node — what the breaker
    /// writes when it spends the window, so no path can slip under it.
    pub fn widest_share(&self) -> f64 {
        self.shares
            .values()
            .copied()
            .fold(0.0f64, f64::max)
            .max(1.0)
    }
}

/// Every path a node is on, by name. Used by the console and the ETA's
/// `assumes`.
pub fn paths_through(plan: &Plan, node: &str) -> Vec<String> {
    let mut v: Vec<String> = plan
        .stages
        .iter()
        .filter(|s| s.node == node)
        .map(|s| s.path.clone())
        .collect();
    v.sort();
    v.dedup();
    v
}

/// Whether this element is a fan-out of at least two branches.
pub fn is_fanout(e: &PathElem) -> bool {
    matches!(e, PathElem::FanOut(v) if v.len() > 1)
}

/// The path's own view of its hops, for messages and for the console.
pub fn hop_names(p: &Path) -> Vec<String> {
    p.nodes
        .iter()
        .map(|e| match e {
            PathElem::One(n) => n.clone(),
            PathElem::FanOut(v) => format!("[{}]", v.join(", ")),
        })
        .collect()
}
