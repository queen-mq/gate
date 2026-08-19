//! The graph: a declared DAG of nodes, each of which is a target.
//!
//! A single-stage limiter can express one thing at a time — either a limit is
//! exact (every budget evaluated at one instant, in one node) or it is isolated
//! (each class parks its own queue). The composition algebra is what forces the
//! choice:
//!
//! * budgets in ONE node, selected by `match`, are ANDed at one instant: exact,
//!   with no isolation — a denial parks the whole lane batch behind it;
//! * an EDGE between nodes ANDs in sequence: fully isolated, each node parking
//!   its own queue, but *smeared* — downstream queueing ages the upstream
//!   certificate, so whichever limit is enforced LAST is the exact one;
//! * parallel-AND across two nodes — exact and isolated at once — is not
//!   expressible without distributed transactions, and this file will not
//!   pretend otherwise.
//!
//! Which is the whole design rule: the severe limit (an egress IP block takes
//! out the fleet) goes in the TERMINAL node where it is enforced last and
//! exactly; the mild ones (a per-endpoint 429) go upstream where isolation is
//! worth more than exactness. Paths stay short because smear composes per hop.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use serde::{Deserialize, Serialize};

use crate::spec::{default_application, Admitted, Budget, Cost, Dim, Lane, Pacing, TargetSpec};
use crate::validate::{ok_name, ok_target_name, validate_with, Problem, ValidateOpts};

/// `retryTo: origin-entry` — the entry the item came in at, stamped on its first
/// push. Never a fixed node: a 429'd call re-enters where it entered and re-pays
/// every budget on its path, because the vendor counted the call that failed.
pub const ORIGIN_ENTRY: &str = "origin-entry";

/// Smear composes per hop and the latency floor is one lease per hop, so a long
/// path is a slow path AND a vague one. Three is the wall.
pub const MAX_HOPS: usize = 3;

/// The outcome a caller acks when the vendor refused the call it had been
/// admitted to make. `status: 429` in a breach rule matches it, because that is
/// how a throttle arrives on the wire today.
pub const THROTTLED: &str = "throttled";

/// The reserved envelope Gate stamps into a payload it is routing: the entry the
/// item came in at, and how many times it has been retried. One object rather
/// than three top-level keys, so it cannot collide with a scope dimension
/// (`entity`, `connection`, …) or with the cost field.
pub const GATE_META: &str = "_gate";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GraphSpec {
    #[serde(default = "default_application")]
    pub application: String,
    /// Defaulted so a caller does not have to repeat what the route already says.
    /// Every route that accepts a document overwrites it from the path, and an empty
    /// one is refused by validation — so it is never a name nobody chose.
    #[serde(default)]
    pub name: String,

    /// One version for the whole document: a graph is declared atomically, so a
    /// migration-class change to any node is a change to the graph.
    pub version: u32,
    /// Ordered, so a declare, a GET and the console all see the nodes in the same
    /// sequence — and so provisioning is deterministic when something fails half
    /// way.
    pub nodes: BTreeMap<String, Node>,
    #[serde(default)]
    pub edges: Vec<Edge>,
    /// The nodes a caller may pop. Anything else is interior and its admitted
    /// queue belongs to a relay.
    #[serde(default)]
    pub consume: Vec<String>,
    #[serde(default)]
    pub breach: Vec<BreachRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Node {
    /// Callers may push here. An interior node's push queue belongs to its
    /// in-edges.
    #[serde(default)]
    pub entry: bool,
    /// May be empty for a node with out-edges: a class node exists to isolate and
    /// to carry a priority, and is checked against the budgets downstream.
    #[serde(default)]
    pub budgets: Vec<Budget>,
    pub cost: Cost,
    /// Defaults to one lane taking the whole ceiling. Declaring two DIVIDES every
    /// budget in the node between them — see `TargetSpec::lane_share`.
    #[serde(default)]
    pub lanes: Vec<Lane>,
    #[serde(default)]
    pub pacing: Pacing,
    #[serde(default)]
    pub admitted: Admitted,
    #[serde(default, rename = "shardBy", skip_serializing_if = "Option::is_none")]
    pub shard_by: Option<Dim>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shards: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Edge {
    pub from: String,
    pub to: String,
    /// Strict, and lower is sooner: the merge relay into a node drains priority 0
    /// to exhaustion before it looks at priority 1. Equal priorities are drained
    /// in declared order.
    #[serde(default)]
    pub priority: u32,
}

/// A vendor throttle routed back into the graph.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BreachRule {
    pub when: BreachWhen,
    #[serde(rename = "retryTo")]
    pub retry_to: String,
    /// Never optional. A retro edge without one is a retry livelock with a
    /// vendor's rate limit as the only brake.
    #[serde(rename = "maxAttempts")]
    pub max_attempts: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BreachWhen {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
}

impl BreachWhen {
    /// Does an ack match this rule?
    ///
    /// `status: 429` also matches an ack that carries no status but says
    /// `outcome: throttled`, because that is the shape the contract already has:
    /// the consumer classifies the vendor's refusal and acks the truth, and
    /// "throttled" IS a 429 in every caller wired to this so far. Stated here so
    /// the equivalence is a decision and not an accident.
    pub fn matches(&self, outcome: &str, status: Option<i64>) -> bool {
        let by_outcome = self.outcome.as_deref().map(|o| o == outcome);
        let by_status = self.status.map(|want| match status {
            Some(got) => got == want,
            None => want == 429 && outcome == THROTTLED,
        });
        match (by_outcome, by_status) {
            (None, None) => false,
            (a, b) => a.unwrap_or(true) && b.unwrap_or(true)
        }
    }
}

impl GraphSpec {
    pub fn key(&self) -> String {
        format!("{}/{}", self.application, self.name)
    }

    /// A node IS a target, named for its graph. Nothing else in the server has to
    /// learn what a graph is: the queues, the gate runners, the leases, the push
    /// and the ack are the ones that were already there.
    pub fn node_target_name(&self, node: &str) -> String {
        format!("{}.{}", self.name, node)
    }

    pub fn node(&self, node: &str) -> Option<&Node> {
        self.nodes.get(node)
    }

    pub fn node_spec(&self, node: &str) -> Option<TargetSpec> {
        let n = self.nodes.get(node)?;
        Some(TargetSpec {
            application: self.application.clone(),
            name: self.node_target_name(node),
            version: self.version,
            egress: n.egress.clone(),
            budgets: n.budgets.clone(),
            lanes: if n.lanes.is_empty() {
                vec![Lane::sole()]
            } else {
                n.lanes.clone()
            },
            cost: n.cost.clone(),
            pacing: n.pacing.clone(),
            admitted: n.admitted.clone(),
            shard_by: n.shard_by,
            shards: n.shards,
        })
    }

    pub fn node_specs(&self) -> Vec<(String, TargetSpec)> {
        self.nodes
            .keys()
            .filter_map(|n| self.node_spec(n).map(|s| (n.clone(), s)))
            .collect()
    }

    pub fn is_entry(&self, node: &str) -> bool {
        self.nodes.get(node).is_some_and(|n| n.entry)
    }

    pub fn is_consume(&self, node: &str) -> bool {
        self.consume.iter().any(|c| c == node)
    }

    pub fn entries(&self) -> Vec<&str> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.entry)
            .map(|(k, _)| k.as_str())
            .collect()
    }

    pub fn out_edges(&self, node: &str) -> Vec<&Edge> {
        self.edges.iter().filter(|e| e.from == node).collect()
    }

    pub fn in_edges(&self, node: &str) -> Vec<&Edge> {
        self.edges.iter().filter(|e| e.to == node).collect()
    }

    /// Every node something relays INTO — one merge relay each.
    pub fn merge_dests(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for e in &self.edges {
            if !out.contains(&e.to) {
                out.push(e.to.clone());
            }
        }
        out.sort();
        out
    }

    /// The rule that takes this ack, if any.
    pub fn breach_for(&self, outcome: &str, status: Option<i64>) -> Option<&BreachRule> {
        self.breach.iter().find(|r| r.when.matches(outcome, status))
    }

    /// Where a breached item re-enters. `origin-entry` resolves to the entry
    /// stamped on the item; failing that (an item pushed before the graph carried
    /// a breach rule) to the sole entry, and if there are several we do not guess.
    pub fn retry_entry(&self, rule: &BreachRule, origin: Option<&str>) -> Option<String> {
        if rule.retry_to != ORIGIN_ENTRY {
            return self.is_entry(&rule.retry_to).then(|| rule.retry_to.clone());
        }
        if let Some(o) = origin.filter(|o| self.is_entry(o)) {
            return Some(o.to_string());
        }
        let entries = self.entries();
        (entries.len() == 1).then(|| entries[0].to_string())
    }
}

/// Everything a graph declare refuses. Numbered as `G1`..`G10` in the plan; the
/// rule names are what the caller sees.
pub fn validate_graph(g: &GraphSpec) -> Vec<Problem> {
    let mut out = Vec::new();
    let p = |rule, detail: String| Problem { rule, detail };

    if !ok_name(&g.application) {
        out.push(p(
            "application",
            format!("`{}` is not a usable application name", g.application),
        ));
    }
    if !ok_name(&g.name) {
        out.push(p(
            "name",
            format!(
                "`{}` is not a usable graph name: lowercase, digits and dashes, and no dot — the \
                 dot is what joins a graph to its node",
                g.name
            ),
        ));
    }
    if g.nodes.is_empty() {
        out.push(p("nodes", "a graph with no nodes routes nothing".into()));
        return out;
    }
    for name in g.nodes.keys() {
        if !ok_name(name) {
            out.push(p(
                "node-name",
                format!("node `{name}`: lowercase, digits and dashes, and no dot"),
            ));
        }
        if !ok_target_name(&g.node_target_name(name)) {
            out.push(p(
                "node-name",
                format!(
                    "`{}` is too long to be a target name once the graph name is on it",
                    g.node_target_name(name)
                ),
            ));
        }
    }

    // ---- edges reference real nodes, exactly once each, and never a node itself.
    let mut seen_edges: HashSet<(String, String)> = HashSet::new();
    for e in &g.edges {
        if !g.nodes.contains_key(&e.from) {
            out.push(p("edge-node", format!("edge from unknown node `{}`", e.from)));
        }
        if !g.nodes.contains_key(&e.to) {
            out.push(p("edge-node", format!("edge to unknown node `{}`", e.to)));
        }
        if e.from == e.to {
            out.push(p(
                "edge-self",
                format!("`{}` relays into itself: work would never leave it", e.from),
            ));
        }
        if !seen_edges.insert((e.from.clone(), e.to.clone())) {
            out.push(p(
                "edge-unique",
                format!(
                    "`{} -> {}` is declared twice: two relays on one queue pair would each forward \
                     the same message",
                    e.from, e.to
                ),
            ));
        }
    }

    if g.entries().is_empty() {
        out.push(p(
            "entry",
            "no node is an `entry`: nothing could ever be pushed into this graph".into(),
        ));
    }
    if g.consume.is_empty() {
        out.push(p(
            "consume",
            "no node is in `consume`: work would enter and never be popped by anyone".into(),
        ));
    }
    for c in &g.consume {
        if !g.nodes.contains_key(c) {
            out.push(p("consume", format!("`{c}` is in `consume` but is not a node")));
            continue;
        }
        // The 409 on interior queues is not cosmetic, and this is the same rule at
        // declare time: a caller popping a node that also feeds a relay steals
        // from the graph, and the two consumers would split the work at random.
        if !g.out_edges(c).is_empty() {
            out.push(p(
                "consume-terminal",
                format!(
                    "`{c}` is in `consume` but has out-edges: its admitted queue already belongs to \
                     the relay, and a caller popping it would take work the graph is routing"
                ),
            ));
        }
    }

    // ---- G1: the forward edges are a DAG.
    let cyclic = match topo_order(g) {
        Ok(_) => false,
        Err(cycle) => {
            out.push(p(
                "acyclic",
                format!(
                    "the forward edges contain a cycle ({}): an item could traverse for ever, \
                     re-paying every budget on the way round",
                    cycle.join(" -> ")
                ),
            ));
            true
        }
    };

    // ---- G2: reachable from an entry, and able to reach a consume node.
    let reachable = reachable_from(g, g.entries());
    let draining = reaches_consume(g);
    for name in g.nodes.keys() {
        if !reachable.contains(name.as_str()) {
            out.push(p(
                "reachable",
                format!("`{name}` is not an entry and nothing relays into it: it can never hold work"),
            ));
        }
        if !cyclic && !draining.contains(name.as_str()) {
            out.push(p(
                "drains",
                format!(
                    "no path from `{name}` ends in a `consume` node: work that reaches it is never \
                     popped by anyone"
                ),
            ));
        }
    }

    // ---- G3: cost.max never decreases along an edge.
    for e in &g.edges {
        let (Some(from), Some(to)) = (g.nodes.get(&e.from), g.nodes.get(&e.to)) else {
            continue;
        };
        if to.cost.max < from.cost.max {
            out.push(p(
                "cost-monotonic",
                format!(
                    "`{}` admits items costing up to {} but `{}` caps at {}: an item admitted \
                     upstream would sit at the head of the downstream lane for ever, and never reach \
                     a DLQ",
                    e.from, from.cost.max, e.to, to.cost.max
                ),
            ));
        }
    }

    // ---- G4: a node with no budget must have somewhere to send its work.
    for (name, n) in &g.nodes {
        if n.budgets.is_empty() && g.out_edges(name).is_empty() {
            out.push(p(
                "budgets",
                format!(
                    "`{name}` declares no budget and has no out-edge: it would admit everything \
                     straight to a consumer, which is a queue with extra steps"
                ),
            ));
        }
    }

    // ---- one way out of a node, because two would be a broadcast.
    //
    // Each edge is its own consumer group on the source's admitted queue, and a
    // consumer group gets every message — so two out-edges do not SPLIT the
    // stream, they COPY it, and one pushed item becomes one vendor call per
    // branch. A limiter that silently doubles traffic is worse than no limiter.
    for name in g.nodes.keys() {
        let outs = g.out_edges(name);

        if outs.len() > 1 {
            out.push(p(
                "edge-fanout",
                format!(
                    "`{name}` relays into {}: each edge is its own consumer group and would receive \
                     EVERY item, so this is a broadcast and not a split — one push would become one \
                     call per branch. Use one out-edge, and separate entry nodes for separate classes",
                    outs.iter().map(|e| e.to.as_str()).collect::<Vec<_>>().join(" and ")
                ),
            ));
        }
    }

    // ---- what a relay cannot decide for itself.
    for (name, n) in &g.nodes {
        if g.in_edges(name).is_empty() {
            continue;
        }
        // A relay has nobody to ask which lane a relayed item belongs in, so it
        // uses the default one — and lanes DIVIDE the node's budgets, so every
        // other lane's share would be capacity nothing can reach.
        if n.lanes.len() > 1 {
            out.push(p(
                "relay-lane",
                format!(
                    "`{name}` declares {} lanes and is fed by a relay: a relay has nobody to ask \
                     which lane an item belongs in, and lanes divide this node's budgets — so the \
                     other lanes' shares would be capacity nothing can reach. Split the classes into \
                     separate upstream nodes instead",
                    n.lanes.len()
                ),
            ));
        }
        // A shard is chosen from a dimension ON the item. A relay cannot invent one
        // for an item that does not carry it, and picking any shard would put one
        // key in two counters — so a sharded node is where work ENTERS.
        if let Some(dim) = n.shard_by {
            out.push(p(
                "shard-entry",
                format!(
                    "`{name}` is sharded by `{}` and is fed by {}: a relay cannot choose a shard for \
                     an item that does not carry the dimension, and choosing one anyway would put a \
                     key in two counters. A sharded node takes its work from a push, not from an edge",
                    dim.as_str(),
                    g.in_edges(name).iter().map(|e| e.from.as_str()).collect::<Vec<_>>().join(", ")
                ),
            ));
        }
    }

    // ---- one cost field for the whole graph.
    //
    // The push stamps the item's weight under the ENTRY node's `cost.field` and
    // every relay forwards the payload verbatim, so a downstream node reading a
    // different field name finds nothing and charges its `cost.default` — a
    // hundred-call item counted as one, all the way to the vendor's ceiling.
    let mut fields: Vec<(&str, &str)> = g
        .nodes
        .iter()
        .map(|(name, n)| (name.as_str(), n.cost.field.as_str()))
        .collect();
    fields.dedup_by_key(|(_, f)| *f);
    if fields.len() > 1 {
        out.push(p(
            "cost-field",
            format!(
                "the nodes name {} different cost fields ({}): the weight is stamped once, at the \
                 push, and forwarded verbatim — a node reading another name would charge its \
                 cost.default for every item. One field per graph",
                fields.len(),
                fields
                    .iter()
                    .map(|(n, f)| format!("`{n}`: {f}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ));
    }


    // ---- G6: path length.
    if !cyclic {
        if let Some((hops, path)) = longest_path(g) {
            if hops > MAX_HOPS {
                out.push(p(
                    "path-length",
                    format!(
                        "`{}` is {hops} forward hops: the latency floor is one lease per hop and the \
                         upstream certificate ages at every one, so {MAX_HOPS} is the limit",
                        path.join(" -> ")
                    ),
                ));
            }
        }
    }

    // ---- the ceiling is declared once, or it is not a ceiling.
    //
    // Two nodes holding the same unscoped budget id are two counters with one
    // name: each admits the full cap and the vendor sees the sum. Detected by id
    // because that is the only thing a document says twice.
    // A `store: kv` budget is exempt, and it is the exception that proves the
    // rule: the shared pool keys on `(application, id)`, so two nodes declaring
    // the same kv id draw down ONE counter. That is how a ceiling spanning nodes
    // is expressed at all.
    let mut unscoped: HashMap<&str, Vec<&str>> = HashMap::new();
    for (name, n) in &g.nodes {
        for b in n
            .budgets
            .iter()
            .filter(|b| b.scope.is_empty() && b.store != crate::spec::Store::Kv)
        {
            unscoped.entry(b.id.as_str()).or_default().push(name.as_str());
        }
    }

    let mut dupes: Vec<(&str, Vec<&str>)> = unscoped
        .into_iter()
        .filter(|(_, nodes)| nodes.len() > 1)
        .collect();
    dupes.sort();
    for (id, mut nodes) in dupes {
        nodes.sort();
        out.push(p(
            "budget-once",
            format!(
                "budget `{id}` is declared unscoped in {}: each node keeps its OWN counter, so the \
                 cap would be enforced once per node and the vendor would see the sum. Declare it in \
                 the terminal node only, or give the copies distinct ids because they are distinct \
                 limits",
                nodes.join(" and ")
            ),
        ));
    }

    // ---- G5: retro edges.
    for r in &g.breach {
        if r.when.status.is_none() && r.when.outcome.is_none() {
            out.push(p(
                "breach-when",
                "a breach rule needs a `status` or an `outcome` to match on".into(),
            ));
        }
        if r.max_attempts < 1 {
            out.push(p(
                "breach-attempts",
                format!("`retryTo: {}` needs maxAttempts of at least 1", r.retry_to),
            ));
        }
        // A fixed re-entry has to be able to admit anything that could arrive
        // there. `origin-entry` is safe by construction — an item goes back to the
        // door it came in at, which already accepted its cost — but a NAMED entry
        // receives items pushed at every other entry, and one costing more than
        // its `cost.max` can never be admitted: it parks the head of that entry's
        // lane for ever and never reaches a DLQ.
        if r.retry_to != ORIGIN_ENTRY {
            if let Some(to) = g.nodes.get(&r.retry_to) {
                let worst = g
                    .nodes
                    .iter()
                    .filter(|(_, n)| n.entry)
                    .map(|(name, n)| (name.as_str(), n.cost.max))
                    .fold(("", 0.0f64), |acc, x| if x.1 > acc.1 { x } else { acc });
                if to.cost.max < worst.1 {
                    out.push(p(
                        "retry-cost",
                        format!(
                            "`retryTo: {}` caps an item at {} but `{}` admits items costing up to {}: \
                             a breached item from there could never be admitted at the re-entry and \
                             would park its lane for ever. Use `{ORIGIN_ENTRY}`, which sends an item \
                             back to the door that accepted it",
                            r.retry_to, to.cost.max, worst.0, worst.1
                        ),
                    ));
                }
            }
        }
        if r.retry_to != ORIGIN_ENTRY && !g.is_entry(&r.retry_to) {

            out.push(p(
                "retry-entry",
                format!(
                    "`retryTo: {}` is not an entry node of this graph: a re-entry that skips the \
                     upstream budgets would spend a path it never paid for, and a throttled call is \
                     a NEW call that owes the whole path. Use `{ORIGIN_ENTRY}` or name an entry",
                    r.retry_to
                ),
            ));
        }
    }

    // ---- G9: every target-level rule, per node.
    for (name, spec) in g.node_specs() {
        let opts = ValidateOpts {
            allow_empty_budgets: !g.out_edges(&name).is_empty(),
        };
        for problem in validate_with(&spec, opts) {
            out.push(Problem {
                rule: problem.rule,
                detail: format!("node `{name}`: {}", problem.detail),
            });
        }
    }

    out
}

/// Warnings, per node, with the node named. Same contract as a target's: the
/// declare succeeds and the caller is told what it bought.
pub fn graph_warnings(g: &GraphSpec) -> Vec<Problem> {
    let mut out = Vec::new();
    for (name, spec) in g.node_specs() {
        for w in crate::validate::warnings(&spec) {
            out.push(Problem {
                rule: w.rule,
                detail: format!("node `{name}`: {}", w.detail),
            });
        }
    }
    out
}

/// Changes that re-found a counter somewhere, and therefore need a version.
pub fn needs_graph_version_bump(old: &GraphSpec, new: &GraphSpec) -> bool {
    for (name, _) in old.nodes.iter() {
        match (old.node_spec(name), new.node_spec(name)) {
            // A node that has gone takes its queues and its counters with it.
            (Some(_), None) => return true,
            (Some(o), Some(n)) => {
                if crate::validate::needs_version_bump(&o, &n) {
                    return true;
                }
            }
            _ => {}
        }
    }
    // Rewiring is migration-class even when every node is untouched: an item in
    // flight was admitted against a path that no longer exists.
    let old_edges: HashSet<(&str, &str)> = old
        .edges
        .iter()
        .map(|e| (e.from.as_str(), e.to.as_str()))
        .collect();
    let new_edges: HashSet<(&str, &str)> = new
        .edges
        .iter()
        .map(|e| (e.from.as_str(), e.to.as_str()))
        .collect();
    old_edges != new_edges
}

/// Kahn, and the leftovers ARE the cycle.
fn topo_order(g: &GraphSpec) -> Result<Vec<String>, Vec<String>> {
    let mut indegree: HashMap<&str, usize> = g.nodes.keys().map(|n| (n.as_str(), 0)).collect();
    for e in &g.edges {
        if g.nodes.contains_key(&e.to) && g.nodes.contains_key(&e.from) {
            *indegree.entry(e.to.as_str()).or_insert(0) += 1;
        }
    }
    let mut queue: VecDeque<&str> = g
        .nodes
        .keys()
        .filter(|n| indegree.get(n.as_str()) == Some(&0))
        .map(|n| n.as_str())
        .collect();
    let mut order = Vec::new();
    while let Some(n) = queue.pop_front() {
        order.push(n.to_string());
        for e in g.out_edges(n) {
            if let Some(d) = indegree.get_mut(e.to.as_str()) {
                *d -= 1;
                if *d == 0 {
                    queue.push_back(e.to.as_str());
                }
            }
        }
    }
    if order.len() == g.nodes.len() {
        Ok(order)
    } else {
        let mut stuck: Vec<String> = g
            .nodes
            .keys()
            .filter(|n| !order.contains(n))
            .cloned()
            .collect();
        stuck.sort();
        Err(stuck)
    }
}

fn reachable_from<'a>(g: &'a GraphSpec, roots: Vec<&'a str>) -> HashSet<&'a str> {
    let mut seen: HashSet<&str> = roots.iter().copied().collect();
    let mut queue: VecDeque<&str> = roots.into_iter().collect();
    while let Some(n) = queue.pop_front() {
        for e in g.out_edges(n) {
            if let Some((k, _)) = g.nodes.get_key_value(&e.to) {
                if seen.insert(k.as_str()) {
                    queue.push_back(k.as_str());
                }
            }
        }
    }
    seen
}

/// Which nodes have a path to a `consume` node — walked backwards from the
/// terminals, so a fan-in costs one traversal rather than one per node.
fn reaches_consume(g: &GraphSpec) -> HashSet<&str> {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut queue: VecDeque<&str> = VecDeque::new();
    for c in &g.consume {
        if let Some((k, _)) = g.nodes.get_key_value(c) {
            if seen.insert(k.as_str()) {
                queue.push_back(k.as_str());
            }
        }
    }
    while let Some(n) = queue.pop_front() {
        for e in g.in_edges(n) {
            if let Some((k, _)) = g.nodes.get_key_value(&e.from) {
                if seen.insert(k.as_str()) {
                    queue.push_back(k.as_str());
                }
            }
        }
    }
    seen
}

/// The longest forward path, in hops, with the path itself for the message.
/// Assumes acyclic — the caller checks that first.
fn longest_path(g: &GraphSpec) -> Option<(usize, Vec<String>)> {
    let order = topo_order(g).ok()?;
    // node -> (hops to here, path to here)
    let mut best: HashMap<&str, (usize, Vec<String>)> = HashMap::new();
    for name in &order {
        let here = best
            .get(name.as_str())
            .cloned()
            .unwrap_or((0, vec![name.clone()]));
        for e in g.out_edges(name) {
            let mut path = here.1.clone();
            path.push(e.to.clone());
            let cand = (here.0 + 1, path);
            match best.get(e.to.as_str()) {
                Some((h, _)) if *h >= cand.0 => {}
                _ => {
                    if let Some((k, _)) = g.nodes.get_key_value(&e.to) {
                        best.insert(k.as_str(), cand);
                    }
                }
            }
        }
        if let Some((k, _)) = g.nodes.get_key_value(name.as_str()) {
            best.entry(k.as_str()).or_insert(here);
        }
    }
    best.into_values().max_by_key(|(h, _)| *h)
}
