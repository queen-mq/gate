//! Validation at declare time.
//!
//! Every rule here turns a silent runtime failure into a rejected `PUT`. That is
//! the whole value of the file: none of these are style checks, and each one
//! corresponds to a way the limiter breaks in production without saying so.
//!
//! The loud-422 philosophy is v1's and is kept exactly: a `Problem { rule,
//! detail }`, all problems joined with `; `, and a message that names the
//! number, names the consequence and names the fix. **The rule names are
//! asserted on in tests, so they are API.**
//!
//! Roughly half of v1's rules are gone, and none of them because they were
//! wrong. Each one policed an invariant this architecture makes structural
//! (`lane-shares` and its four siblings: there is one counter now, so N ceilings
//! cannot oversubscribe it) or a resource it no longer allocates (`shard-count`,
//! `max-keys`, `store-fits`, `kv-chunk`: cardinality is Postgres rows with a
//! TTL, not entries in a document Gate re-reads whole every cycle).

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use crate::cost::ok_payload_path;
use crate::doc::{ok_name, Confidence, Cost, GraphDoc, PathElem};
use crate::plan::{self, Plan};

/// The largest claim a node may ask for. §12.2's clamp on v1's `pacing.batch`,
/// enforced as a refusal.
pub const MAX_BATCH: u32 = 1000;

/// The most re-entries a document may allow one item (§16.6). v1's
/// `breach-attempts` policed the same number for the same reason.
pub const MAX_ATTEMPTS_CEILING: u32 = 20;

/// Above this, a scoped budget's sub-window is long enough that one full key
/// holding the head of a partition is an operational surprise rather than a
/// pacing decision. An hour: the same order as the broker's own default dedup
/// window, and far above any window an operator watches in real time.
pub const HEAD_OF_LINE_WARN_SECONDS: i64 = 3600;

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

/// What the broker knows and a document cannot say.
///
/// Empty is a legal answer and means "we did not ask" — every rule that reads
/// this is a WARNING, so a broker that will not answer costs a caller some
/// advice and never a refusal.
#[derive(Debug, Clone, Default)]
pub struct ExternalFacts {
    pub queues: BTreeMap<String, QueueFacts>,
    /// Stage source queues already claimed elsewhere in the fleet (both
    /// ingress and Gate-owned interior queues):
    /// `(queue, "app/graph", node)`. This graph's own entries must be excluded
    /// by the caller, or a redeclare would collide with itself.
    pub ingress_owners: Vec<(String, String, String)>,
    /// `(queue, "app/graph")` for every egress queue declared elsewhere.
    pub egress_owners: Vec<(String, String)>,
}

#[derive(Debug, Clone, Default)]
pub struct QueueFacts {
    pub exists: bool,
    pub partitions: u32,
    /// However the broker spells it. Rendered into the warning verbatim, because
    /// a retention Gate paraphrased is a retention nobody checks.
    pub retention: Option<String>,
}

fn p(rule: &'static str, detail: String) -> Problem {
    Problem { rule, detail }
}

// -------------------------------------------------------------------- refusals

pub fn validate(doc: &GraphDoc) -> Vec<Problem> {
    validate_with(doc, &ExternalFacts::default())
}

pub fn validate_with(doc: &GraphDoc, facts: &ExternalFacts) -> Vec<Problem> {
    let mut out = Vec::new();
    naming(doc, &mut out);
    if doc.nodes.is_empty() {
        out.push(p("nodes", "a graph with no nodes limits nothing.".into()));
        return out;
    }
    if doc.paths.is_empty() {
        out.push(p(
            "paths",
            "a graph with no paths has no way in and no way out: declare at least one path naming \
             the nodes a message visits, in order."
                .into(),
        ));
        return out;
    }

    let plan = plan::compile(doc);
    shape(doc, &mut out);
    budgets(doc, &plan, &mut out);
    shares(doc, &plan, &mut out);
    ownership(&plan, facts, &mut out);
    out
}

fn naming(doc: &GraphDoc, out: &mut Vec<Problem>) {
    if !ok_name(&doc.application) {
        out.push(p(
            "application",
            format!(
                "`application` must be one lowercase segment (letters, digits and dashes, starting \
                 with a letter or digit, at most 63 characters): it becomes part of a queue name \
                 and a kv key, which cannot carry anything else. Got `{}`.",
                doc.application
            ),
        ));
    }
    if !ok_name(&doc.graph) {
        out.push(p(
            "graph-name",
            format!(
                "a graph name is one segment, because the dot is what joins a graph to its node: \
                 `{{graph}}.{{node}}` is the target of every queue this declaration creates. Got \
                 `{}`.",
                doc.graph
            ),
        ));
    }
    for name in doc.nodes.keys() {
        if !ok_name(name) {
            out.push(p(
                "node-name",
                format!(
                    "node `{name}`: one lowercase segment (letters, digits and dashes). It becomes \
                     part of `gate.{}.{}.{name}.in`, which is a queue name.",
                    doc.application, doc.graph
                ),
            ));
            continue;
        }
        let q = plan::interior_queue(&doc.application, &doc.graph, name);
        if q.len() > 63 {
            out.push(p(
                "node-name",
                format!(
                    "node `{name}`: the queue name this becomes is `{q}`, which is {} characters. \
                     Shorten the graph or the node.",
                    q.len()
                ),
            ));
        }
    }
    let mut seen: HashSet<&str> = HashSet::new();
    for path in &doc.paths {
        if !ok_name(&path.name) {
            out.push(p(
                "path-name",
                format!(
                    "path `{}`: one lowercase segment (letters, digits and dashes). It names a \
                     consumer group per node it visits.",
                    path.name
                ),
            ));
        }
        if !seen.insert(path.name.as_str()) {
            out.push(p(
                "path-name",
                format!(
                    "path `{}` is declared twice. A path names a consumer group per node it \
                     visits, so two paths of one name would share a cursor and split the stream \
                     instead of each receiving it.",
                    path.name
                ),
            ));
        }
    }
}

fn shape(doc: &GraphDoc, out: &mut Vec<Problem>) {
    let declared: Vec<&str> = doc.nodes.keys().map(|s| s.as_str()).collect();

    for path in &doc.paths {
        if path.nodes.is_empty() {
            out.push(p("path-length", format!("path `{}` is empty.", path.name)));
            continue;
        }
        let hops = path.nodes.len();
        for (i, elem) in path.nodes.iter().enumerate() {
            match elem {
                PathElem::FanOut(v) if v.len() < 2 => out.push(p(
                    "fanout-branch",
                    format!(
                        "path `{}` hop {i}: a fan-out is a flat array of at least two node names.",
                        path.name
                    ),
                )),
                PathElem::FanOut(_) if i + 1 != hops => out.push(p(
                    "fanout-terminal",
                    format!(
                        "path `{}` fans out to {} at hop {i}, which is not the last hop. After a \
                         fan-out the branches are separate streams; give each one its own path.",
                        path.name,
                        elem.names().join(", ")
                    ),
                )),
                _ => {}
            }
            for n in elem.names() {
                if !declared.contains(&n) {
                    out.push(p(
                        "path-node",
                        format!(
                            "path `{}` visits `{n}`, which is not a declared node. Declared nodes \
                             are: {}.",
                            path.name,
                            declared.join(", ")
                        ),
                    ));
                    continue;
                }
                let node = &doc.nodes[n];
                if i == 0 && !node.ingress.as_ref().is_some_and(|g| g.is_enabled()) {
                    out.push(p(
                        "path-entry",
                        format!(
                            "path `{}` starts at `{n}`, which declares no ingress. Work cannot \
                             enter a node that has no queue to enter by: give `{n}` an `ingress`, \
                             or start the path at a node that has one.",
                            path.name
                        ),
                    ));
                }
                if i + 1 == hops && node.egress.is_none() {
                    out.push(p(
                        "path-terminal",
                        format!(
                            "path `{}` ends at `{n}`, which declares no egress. Work would be \
                             admitted and then have nowhere to go. Name the queue your consumers \
                             read: `\"egress\": \"{}.{}.out\"`.",
                            path.name, doc.application, doc.graph
                        ),
                    ));
                }
            }
        }
    }

    if let Some(cycle) = plan::find_cycle(doc) {
        // The pair a reader can act on: the first edge inside the stuck set.
        let arrow = plan::edges(doc)
            .into_iter()
            .find(|(a, b)| cycle.contains(a) && cycle.contains(b))
            .map(|(a, b)| format!("{a} -> {b} -> {a}"))
            .unwrap_or_else(|| cycle.join(" -> "));
        out.push(p(
            "acyclic",
            format!(
                "these nodes form a cycle: {arrow}. An item would traverse it for ever, re-paying \
                 every budget on the way round."
            ),
        ));
    }

    let visited = doc.visited();
    for name in doc.nodes.keys() {
        if !visited.contains(&name.as_str()) {
            out.push(p(
                "node-orphan",
                format!("node `{name}` is declared and no path visits it: it can never hold work."),
            ));
        }
    }
}

fn budgets(doc: &GraphDoc, plan: &Plan, out: &mut Vec<Problem>) {
    // A shared key is one counter across the whole application, so two
    // declarations of it that disagree about what it enforces are not two
    // budgets — one of them is a lie about the ceiling.
    let mut shared: HashMap<&str, (&str, &str, i64, i64, u32)> = HashMap::new();

    // The re-entry bound (§16.6). v1's `breach[].maxAttempts` policed the
    // same number under `breach-attempts`, and it is policed for the same
    // reason: zero is a re-entry endpoint that always refuses, and a large
    // one is a livelock the limiter pays for.
    if let Some(n) = doc.max_attempts {
        if !(1..=MAX_ATTEMPTS_CEILING).contains(&n) {
            out.push(p(
                "max-attempts-range",
                format!(
                    "`maxAttempts` is {n}; it must be between 1 and {MAX_ATTEMPTS_CEILING}. \
                     Zero is a re-entry that always refuses, and an unbounded one is a \
                     livelock this limiter would be paying for."
                ),
            ));
        }
    }

    for (name, node) in &doc.nodes {
        let Some(np) = plan.node(name) else { continue };
        cost_rules(name, &node.cost, out);

        // §12.2 maps v1's `pacing.batch` to this, "clamped to [1, 1000]". A
        // declaration is refused rather than clamped, because a caller who asks
        // for 5000 and is silently given 1000 has no way to find out.
        //
        // There is a ceiling here at all because a claim is what sizes the kv
        // call: `Budgets::charge` chunks so no batch can exceed the broker's
        // 256-op limit, but a scoped budget still mints one key per distinct
        // value in the claim, so an unbounded batch is an unbounded number of
        // round trips holding one lease.
        if let Some(b) = node.batch {
            if !(1..=MAX_BATCH).contains(&b) {
                out.push(p(
                    "batch-range",
                    format!(
                        "node `{name}` declares batch {b}. It must be between 1 and {MAX_BATCH}: \
                         a scoped budget mints one counter per distinct value in the claim, so \
                         the claim size is also the width of the kv call that pays for it."
                    ),
                ));
            }
        }

        if node.budgets.is_empty() {
            out.push(p(
                "node-budget",
                format!(
                    "node `{name}` declares no budget, so it limits nothing — it would admit \
                     everything straight through, which is a queue with extra steps."
                ),
            ));
            continue;
        }
        if np.unscoped().next().is_none() {
            out.push(p(
                "node-unscoped-budget",
                format!(
                    "node `{name}` has only per-key budgets. It needs at least one budget on the \
                     node itself: it is what the ETA measures a rate against and what the breaker \
                     spends when a vendor says 429."
                ),
            ));
        }

        let mut ids: HashSet<&str> = HashSet::new();
        for (b, cb) in node.budgets.iter().zip(np.budgets.iter()) {
            if !ids.insert(cb.id.as_str()) {
                out.push(p(
                    "budget-unique",
                    format!(
                        "node `{name}` declares budget `{}` twice: the id is the kv key, so the \
                         second would spend the first's counter.",
                        cb.id
                    ),
                ));
            }
            if b.count < 1 {
                out.push(p(
                    "budget-count",
                    format!(
                        "budget `{}` of node `{name}` has count {}. A budget that cannot admit \
                         anything never will — no schedule refills it.",
                        cb.id, b.count
                    ),
                ));
            }
            if b.time_ms < 100 {
                out.push(p(
                    "budget-window",
                    format!(
                        "budget `{}` of node `{name}` declares timeMs {}. The floor is 100.",
                        cb.id, b.time_ms
                    ),
                ));
            }
            if let Some(n) = b.sub_windows {
                if !(1..=3600).contains(&n) {
                    out.push(p(
                        "subwindow-range",
                        format!(
                            "budget `{}` of node `{name}`: subWindows must be between 1 and 3600.",
                            cb.id
                        ),
                    ));
                } else if (n as i64) > b.count.max(1) {
                    out.push(p(
                        "subwindow-fits",
                        format!(
                            "budget `{}` of node `{name}` asks for {n} sub-windows of a count of \
                             {}: each would carry {}/{n} < 1, so the budget would enforce {n} per \
                             window instead of {}. Lower subWindows to at most {}, or raise count.",
                            cb.id, b.count, b.count, b.count, b.count
                        ),
                    ));
                }
            }
            let item_max = node.cost.max();
            if item_max > cb.count_sub {
                out.push(p(
                    "cost-fits",
                    format!(
                        "node `{name}`: an item may cost up to {item_max} and budget `{}` admits \
                         {} per sub-window. An item that cannot fit a window can never be admitted \
                         — it parks the head of its partition for ever and never reaches a DLQ, \
                         because a lease that expires charges no retry. Raise the budget, lower \
                         cost.max, or lower subWindows.",
                        cb.id, cb.count_sub
                    ),
                ));
            }
            if let Some(path) = &b.scope_by {
                if !ok_payload_path(path) {
                    out.push(p(
                        "scope-path",
                        format!(
                            "budget `{}` of node `{name}`: scopeBy `{path}` is not a payload path. \
                             Write it as `payload.field` or `payload.a.b`.",
                            cb.id
                        ),
                    ));
                }
            }
            if let Some(ops) = &b.when_op {
                if ops.is_empty() {
                    out.push(p(
                        "whenop-empty",
                        format!(
                            "budget `{}` of node `{name}`: an empty whenOp matches nothing, so the \
                             budget charges nothing. Drop the field to take everything.",
                            cb.id
                        ),
                    ));
                }
            }
            if b.confidence == Confidence::Documented && (b.source.is_none() || b.as_of.is_none()) {
                let missing = match (b.source.is_none(), b.as_of.is_none()) {
                    (true, true) => "source or asOf",
                    (true, false) => "source",
                    _ => "asOf",
                };
                out.push(p(
                    "provenance",
                    format!(
                        "budget `{}` of node `{name}` claims to be documented but names no \
                         {missing}. A guess must never look like a measurement.",
                        cb.id
                    ),
                ));
            }
            if let Some(k) = &b.shared_key {
                let sub = cb.sub_windows;
                match shared.get(k.as_str()) {
                    Some((n1, id1, c1, t1, s1))
                        if *c1 != b.count || *t1 != b.time_ms || *s1 != sub =>
                    {
                        out.push(p(
                            "shared-conflict",
                            format!(
                                "`{k}` is declared as {c1} per {t1}ms in node `{n1}` (budget \
                                 `{id1}`) and {} per {}ms in node `{name}` (budget `{}`). They are \
                                 one counter, so one of those declarations is a lie about what it \
                                 enforces. Make them agree or give them different keys.",
                                b.count, b.time_ms, cb.id
                            ),
                        ));
                    }
                    Some(_) => {}
                    None => {
                        shared.insert(
                            k.as_str(),
                            (name.as_str(), cb.id.as_str(), b.count, b.time_ms, sub),
                        );
                    }
                }
            }
        }
    }
}

fn cost_rules(name: &str, cost: &Cost, out: &mut Vec<Problem>) {
    match cost {
        Cost::Fixed(n) => {
            if *n < 1 {
                out.push(p(
                    "cost-integer",
                    format!(
                        "node `{name}`: cost must be a whole number of at least 1. The budget \
                         counter is an integer on this wire, so a fractional cost is not \
                         expressible — express the unit differently (count tenths, and multiply \
                         the budget by ten). Got {n}."
                    ),
                ));
            }
        }
        Cost::Path(c) => {
            if !ok_payload_path(&c.path) {
                out.push(p(
                    "cost-path",
                    format!(
                        "node `{name}`: cost.path `{}` is not a payload path. Write it as \
                         `payload.field` or `payload.a.b`.",
                        c.path
                    ),
                ));
            }
            if c.default < 1 {
                out.push(p(
                    "cost-integer",
                    format!(
                        "node `{name}`: cost.default is {}, and a cost must be a whole number of \
                         at least 1.",
                        c.default
                    ),
                ));
            }
            if let Some(m) = c.max {
                if m < c.default {
                    out.push(p(
                        "cost-max",
                        format!(
                            "node `{name}`: cost.max {m} is below cost.default {}, so the default \
                             cost is itself inadmissible.",
                            c.default
                        ),
                    ));
                }
            }
        }
    }
}

fn shares(doc: &GraphDoc, plan: &Plan, out: &mut Vec<Problem>) {
    for path in &doc.paths {
        if let Some(s) = path.share {
            if !(s > 0.0 && s <= 1.0) {
                out.push(p(
                    "share-range",
                    format!(
                        "path `{}`: share must be in (0, 1]. It is a fraction of the node's \
                         counter, not a rate. Got {s}.",
                        path.name
                    ),
                ));
            }
        }
    }

    for (name, np) in &plan.nodes {
        if np.shares.is_empty() {
            continue;
        }
        // Rank order at THIS node: the paths that actually cross it.
        let mut here: Vec<(&str, u32, f64)> = np
            .shares
            .iter()
            .filter_map(|(pn, s)| doc.path(pn).map(|d| (pn.as_str(), d.priority, *s)))
            .collect();
        here.sort_by_key(|(_, prio, _)| *prio);

        if let Some((top, prio, share)) = here.first().copied() {
            if (share - 1.0).abs() > 1e-9 {
                out.push(p(
                    "share-top",
                    format!(
                        "node `{name}`: the highest-priority path through it is `{top}` at share \
                         {share}. The top priority must be able to reach the whole ceiling, or the \
                         headroom above every other path's share belongs to nobody."
                    ),
                ));
            }
            let _ = prio;
        }
        for i in 0..here.len() {
            for j in (i + 1)..here.len() {
                let (hi, hi_p, hi_s) = here[i];
                let (lo, lo_p, lo_s) = here[j];
                if lo_p > hi_p && lo_s > hi_s + 1e-9 {
                    out.push(p(
                        "share-order",
                        format!(
                            "node `{name}`: path `{lo}` at priority {lo_p} has share {lo_s}, above \
                             path `{hi}` at priority {hi_p} with share {hi_s}. Priority is \
                             expressed as the ceiling, so a lower priority with a larger share is \
                             the opposite of what was asked for."
                        ),
                    ));
                }
            }
        }

        let item_max = np.cost.max();
        for (pn, share) in &np.shares {
            for b in &np.budgets {
                let m = b.max_for(*share);
                if m < item_max {
                    out.push(p(
                        "share-rounds-out",
                        format!(
                            "node `{name}`, path `{pn}`: share {share} of {} per sub-window is {m}, \
                             below the item cost ceiling {item_max}. That path could never admit \
                             its largest item.",
                            b.count_sub
                        ),
                    ));
                }
            }
        }
    }
}

fn ownership(plan: &Plan, facts: &ExternalFacts, out: &mut Vec<Problem>) {
    // One logical node per source queue, including Gate-owned interior queues.
    // Looking only at `node.ingress` misses a named ingress that aliases an
    // interior queue: two consumers then read the same physical stream under
    // different groups even though the document appears to name two queues.
    let mut mine: HashMap<&str, &str> = HashMap::new();
    let mut reported: HashSet<&str> = HashSet::new();
    for stage in &plan.stages {
        let q = stage.source.as_str();
        let name = stage.node.as_str();
        if let Some(other) = mine.insert(q, name) {
            if other == name || !reported.insert(q) {
                continue;
            }
            out.push(p(
                "ingress-owner",
                format!(
                    "`{q}` is the source of both `{other}` and `{name}` in this graph. One queue \
                     cannot be both a declared ingress and a Gate-owned interior stream: their \
                     consumer groups would each receive and forward the same messages."
                ),
            ));
        }
    }

    // The same source ownership rule across replicas. The facts include every
    // source from the local registry; the caller separately checks the durable
    // store for declarations this replica has not reconciled yet.
    for (q, name) in &mine {
        if let Some((_, g, n)) = facts.ingress_owners.iter().find(|(oq, _, _)| oq == q) {
            out.push(p(
                "ingress-owner",
                format!(
                    "`{q}` is already the source of node `{n}` in graph `{g}`. Node `{name}` \
                     would consume the same physical stream under a different group, so both \
                     graphs would forward every message."
                ),
            ));
        }
    }

    // A terminal destination that is also one of this graph's sources is a
    // physical cycle even when the node DAG is acyclic. The simplest case is a
    // one-node target whose ingress and egress names are equal: each admitted
    // message is atomically pushed back into the queue it was just acked from
    // and circulates for ever, paying the budget on every turn.
    // Reachability, not membership. A queue that is both an egress and a source
    // is only a cycle if work put there can come BACK to it: `in -> mid -> out`
    // spread over two paths makes `mid` a terminal destination of one and the
    // source of the other, and that is a chain, not a loop. Walk the queue graph
    // forward from each terminal destination and look for the queue itself.
    let mut forward: HashMap<&str, Vec<&str>> = HashMap::new();
    for stage in &plan.stages {
        forward
            .entry(stage.source.as_str())
            .or_default()
            .extend(stage.destinations.iter().map(|d| d.queue.as_str()));
    }
    let returns_to = |start: &str| -> bool {
        let mut seen: HashSet<&str> = HashSet::new();
        let mut queue: VecDeque<&str> = forward.get(start).into_iter().flatten().copied().collect();
        while let Some(next) = queue.pop_front() {
            if next == start {
                return true;
            }
            if !seen.insert(next) {
                continue;
            }
            queue.extend(forward.get(next).into_iter().flatten().copied());
        }
        false
    };
    let mut feedback: HashSet<&str> = HashSet::new();
    for stage in &plan.stages {
        for destination in &stage.destinations {
            if destination.terminal
                && returns_to(destination.queue.as_str())
                && feedback.insert(destination.queue.as_str())
            {
                out.push(p(
                    "queue-cycle",
                    format!(
                        "`{}` is both an egress and a source in this graph. An admitted message \
                         would be pushed back into a queue this graph consumes and circulate for \
                         ever, paying its budgets on every turn. Use a distinct egress queue.",
                        destination.queue
                    ),
                ));
            }
        }
    }
}

// -------------------------------------------------------------------- warnings

/// The trades, stated out loud. A declare with warnings still succeeds; the
/// caller is told what it bought.
pub fn warnings(doc: &GraphDoc) -> Vec<Problem> {
    warnings_with(doc, &ExternalFacts::default())
}

pub fn warnings_with(doc: &GraphDoc, facts: &ExternalFacts) -> Vec<Problem> {
    let mut out = Vec::new();
    let plan = plan::compile(doc);

    for (name, node) in &doc.nodes {
        let Some(np) = plan.node(name) else { continue };
        for (b, cb) in node.budgets.iter().zip(np.budgets.iter()) {
            // A kv TTL is whole seconds with a minimum of one
            // (`queen-protocol/src/kv.rs`, `Expiry`), so a sub-window that is
            // not a whole number of seconds cannot be expressed. Flooring it
            // makes the window shorter; the one-second minimum makes it longer
            // while the count stays put. **Both directions enforce a tighter
            // RATE than declared**, never a looser one, which is the safe way to
            // be wrong — but neither is what the caller wrote down, so both are
            // said out loud.
            let enforced_ms = cb.window_sub_seconds * 1000 * cb.sub_windows as i64;
            if enforced_ms != b.time_ms {
                out.push(p(
                    "window-sub-second",
                    format!(
                        "budget `{}` of node `{name}` declares {} per {}ms. A kv TTL is whole \
                         seconds, so this is enforced as {} per {}s — tighter than declared, never \
                         looser, but slower. Declare a whole number of seconds per sub-window to \
                         get exactly what you asked for.",
                        cb.id, b.count, b.time_ms, cb.count_sub, cb.window_sub_seconds,
                    ),
                ));
            }
            let total = cb.count_sub * cb.sub_windows as i64;
            if cb.sub_windows > 1 && total < b.count {
                let lost = (b.count - total) as f64 / b.count.max(1) as f64;
                if lost > 0.02 {
                    out.push(p(
                        "subwindow-rounding",
                        format!(
                            "budget `{}` of node `{name}`: {} over {} sub-windows rounds down to \
                             {} each, i.e. {}x{} = {total} against the declared {}. Rounding is \
                             always down, because enforcing tighter than declared is the safe \
                             direction.",
                            cb.id,
                            b.count,
                            cb.sub_windows,
                            cb.count_sub,
                            cb.count_sub,
                            cb.sub_windows,
                            b.count
                        ),
                    ));
                }
            }
            // A FIXED window, whose start is the first admitted request after the
            // previous one expired — not a calendar window and not a sliding one.
            // A sliding observer can therefore see up to 2x count_sub across one
            // boundary. v1 offered a two-bucket rolling window to avoid exactly
            // this; kv cannot express one, and faking it client-side would put
            // back the read-then-write race `incr` exists to remove. So:
            // subdivide, and say the number.
            //
            // Only where the subdivision was CHOSEN FOR the caller. A budget
            // that declares its own `subWindows` has already made this trade
            // knowingly — a weekly per-listing quota has a week-long window and
            // no amount of subdividing changes that — and warning about a
            // decision the document states is how a declare response becomes
            // noise nobody reads.
            if cb.window_sub_seconds > 2 && b.sub_windows.is_none() {
                out.push(p(
                    "window-boundary",
                    format!(
                        "budget `{}` of node `{name}` has a {}s sub-window. This is a fixed \
                         window, so a sliding observer can see up to {} across one boundary. Raise \
                         subWindows to narrow that.",
                        cb.id,
                        cb.window_sub_seconds,
                        cb.count_sub * 2
                    ),
                ));
            }
        }
    }

    for path in &doc.paths {
        for (i, elem) in path.nodes.iter().enumerate() {
            if plan::is_fanout(elem) {
                let names = elem.names();
                out.push(p(
                    "fanout-multiplies",
                    format!(
                        "path `{}` fans out to {}: one message becomes {}, and each branch charges \
                         its own node's budgets. Whatever this node admits, the vendor sees {} \
                         times.",
                        path.name,
                        names.join(", "),
                        names.len(),
                        names.len()
                    ),
                ));
            }
            let _ = i;
        }
    }

    for (name, node) in &doc.nodes {
        let Some(np) = plan.node(name) else { continue };
        if let (Some(q), false) = (np.ingress_queue.as_deref(), np.ingress_owned) {
            match facts.queues.get(q) {
                None => {}
                Some(f) if !f.exists => out.push(p(
                    "ingress-queue",
                    format!(
                        "node `{name}` names ingress queue `{q}`, which does not exist yet. Gate \
                         will consume it from its first message; nothing is created here."
                    ),
                )),
                Some(f) => {
                    if let Some(r) = &f.retention {
                        out.push(p(
                            "ingress-retention",
                            format!(
                                "node `{name}`'s ingress `{q}` retains {r}. Work Gate is holding \
                                 for budget lives on that queue: a retention shorter than the \
                                 drain time deletes it, and a limiter that quietly loses the work \
                                 it is pacing is worse than no limiter."
                            ),
                        ));
                    }
                }
            }
        }
        // A per-key counter with a very long window is a head-of-line block on
        // every OTHER key behind it, for as long as the window lasts.
        //
        // The block itself is not a defect: what gets settled is a true PREFIX
        // of a claim, because the cursor commits positionally and a subset would
        // commit past the gap and DROP what it skipped. So a message that cannot
        // be admitted holds its partition, by design, and order inside a
        // partition is the guarantee the whole passthrough design rests on.
        //
        // What IS a defect is nobody being told. `cost-fits` already refuses the
        // one case where a message can never be admitted at all; this is the
        // case where it can, eventually, and "eventually" is days. The lever is
        // `subWindows`, which turns one week-long block into `N` shorter ones.
        for b in np.budgets.iter().filter(|b| b.is_scoped()) {
            if b.window_sub_seconds > HEAD_OF_LINE_WARN_SECONDS {
                out.push(p(
                    "window-head-of-line",
                    format!(
                        "budget `{}` of node `{name}` counts per `{}` over a window of {} seconds. \
                         A key that fills it blocks the head of its partition until the window \
                         rotates — messages for OTHER keys queued behind it wait too, because a \
                         claim is settled as a prefix and skipping one would commit past it and \
                         drop it. Raise `subWindows` to divide the block, or give this node its \
                         own ingress partitioning so one key cannot sit in front of another.",
                        b.id,
                        b.scope_by.as_deref().unwrap_or("?"),
                        b.window_sub_seconds
                    ),
                ));
            }
        }

        if let Some(q) = np.ingress_queue.as_deref() {
            // The broker's answer first, then the RESOLVED plan — not the raw
            // document. A queue Gate owns has a width whether or not the
            // declaration named one (`DEFAULT_INGRESS_PARTITIONS`), and reading
            // the document meant this rule could never fire for exactly those
            // queues: `ingress: true` carries no number, so it measured zero and
            // said nothing, for the one case where Gate itself decides.
            let partitions = facts
                .queues
                .get(q)
                .map(|f| f.partitions)
                .or_else(|| plan.queue(q).and_then(|qs| qs.partitions))
                .or_else(|| node.ingress.as_ref().and_then(|i| i.partitions()))
                .unwrap_or(0);
            // Keyed on the partition count ALONE. It used to also require a
            // worker count above one, as a proxy for "you asked for parallelism
            // you cannot have" — and a worker count is no longer a statement of
            // intent: it is DERIVED from the budget, so a node whose ceiling is
            // small gets one worker whatever its author meant. The question this
            // warns about was never really about workers anyway.
            if partitions == 1 {
                out.push(p(
                    "single-partition",
                    format!(
                        "`{q}` has one partition, so one claim is in flight at a time and this \
                         stage cannot go faster than one loop, whatever its budget allows. One \
                         partition is one order for the whole node, which is the only way to ask \
                         for strict global FIFO — it must not be a surprise."
                    ),
                ));
            }
        }
        if let Some(q) = np.egress_queue.as_deref() {
            if let Some((_, g)) = facts.egress_owners.iter().find(|(oq, _)| oq == q) {
                out.push(p(
                    "egress-owner",
                    format!(
                        "`{q}` is also the egress of graph `{g}`. That is legal — a queue may have \
                         many producers — and it is named here because the ETA's worker backlog \
                         will count both."
                    ),
                ));
            }
        }
    }

    out
}

// ------------------------------------------------------------- version bumps

/// Changes that re-found a counter or strand a queue, and therefore need a
/// version.
///
/// Far shorter than v1's list, because almost nothing re-founds a counter any
/// more. Everything absent from here — a `count`, a `timeMs`, a `share`, a
/// `priority`, a `cost`, a `concurrency`, a `batch` — is a HOT change: `count`
/// and `share` change the `max` the next `incr` carries and land on the next
/// batch, and `timeMs` changes the TTL the next ROTATION writes, so it takes up
/// to one old window to land.
pub fn needs_version_bump(old: &GraphDoc, new: &GraphDoc) -> bool {
    let op = plan::compile(old);
    let np = plan::compile(new);

    // A budget key that changes: the old key keeps counting until its TTL runs
    // out and the new one starts at zero, which is a window of double-spend the
    // caller must mean.
    let old_keys: HashSet<(String, String)> = op
        .nodes
        .values()
        .flat_map(|n| n.budgets.iter().map(|b| (n.name.clone(), b.key.clone())))
        .collect();
    let new_keys: HashSet<(String, String)> = np
        .nodes
        .values()
        .flat_map(|n| n.budgets.iter().map(|b| (n.name.clone(), b.key.clone())))
        .collect();
    if !old_keys.is_subset(&new_keys) {
        return true;
    }
    for n in op.nodes.values() {
        let Some(m) = np.nodes.get(&n.name) else {
            // A node that has gone takes its interior queue with it, and
            // whatever is waiting there has no consumer in the new plan.
            return true;
        };
        for b in &n.budgets {
            if let Some(c) = m.budgets.iter().find(|c| c.key == b.key) {
                if c.scope_by != b.scope_by {
                    return true;
                }
            }
        }
        if n.ingress_queue != m.ingress_queue {
            return true;
        }
    }
    // A path that has gone: work already in an interior queue under its group
    // has nobody to drain it.
    for path in &old.paths {
        if new.path(&path.name).is_none() {
            return true;
        }
    }
    false
}
