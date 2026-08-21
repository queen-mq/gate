//! v1 documents, read and mapped.
//!
//! The endpoint shapes do not move (design §12.1): a `PUT` of a v1 document is
//! accepted, mapped, and answered **200 with warnings naming every field that
//! was mapped or ignored** — never a silent success, and never a 422 for having
//! been written last year.
//!
//! One document is refused rather than mapped: one carrying `breach[]`. The
//! whole breach machinery hangs off `POST /v1/leases/ack`, which the settled
//! architecture removes — the application consumes the egress queue with its own
//! SDK and Gate never sees the outcome. Silently dropping a bounded-retry policy
//! is the one migration failure that would be discovered by a livelock, so it is
//! a refusal with a pointer instead.

#![allow(deprecated)]

use std::collections::BTreeMap;

use crate::doc::{
    Budget, Confidence, Cost, CostPath, Egress, EgressSpec, GraphDoc, Ingress, IngressSpec, Node,
    Path, PathElem,
};
use crate::v1;
use crate::validate::Problem;

/// What a mapping produced, and everything the caller must be told about it.
#[derive(Debug, Clone)]
pub struct Migrated {
    pub doc: GraphDoc,
    pub warnings: Vec<Problem>,
}

/// The refusal a v1 document earns when it declares something v2 will not
/// silently drop.
#[derive(Debug, Clone)]
pub struct Refused(pub String);

fn w(rule: &'static str, detail: String) -> Problem {
    Problem { rule, detail }
}

/// Whatever v1 named this node's admitted queue — the one the application's
/// consumers are already popping.
///
/// This is the whole of §12.4's stability promise: in v2 a terminal queue is
/// DECLARED rather than derived, so a migrated graph names the string v1 would
/// have computed and the caller's consumers do not move.
fn v1_admitted_queue(app: &str, target: &str, lane: &str) -> String {
    format!("gate.{app}.{target}.admitted.{lane}")
}

fn default_lane_name(lanes: &[v1::Lane]) -> String {
    lanes
        .iter()
        .find(|l| l.default)
        .or_else(|| lanes.first())
        .map(|l| l.name.clone())
        .unwrap_or_else(|| "default".to_string())
}

// ------------------------------------------------------------------ budgets

fn budget(b: &v1::Budget, out: &mut Vec<Problem>, node: &str) -> Budget {
    let time_ms = b.period_seconds.max(1) * 1000;
    let count = (b.cap.floor() as i64).max(1);

    let sub_windows = match b.alignment {
        // v1's two-bucket sliding window is not expressible: `incr` is one
        // atomic add against one row, and faking a slide client-side would put
        // back the read-then-write race the primitive exists to remove.
        // Subdivision is the same trade in a form the primitive can keep — it
        // bounds the boundary exposure to 2 x count/N instead of 2 x count.
        v1::Alignment::Rolling => {
            let n = (time_ms / 1000).clamp(1, count.max(1)) as u32;
            out.push(w(
                "alignment",
                format!(
                    "budget `{}` of node `{node}`: `alignment` is gone — kv owns a fixed window, \
                     and smoothing is expressed as subWindows. `rolling` mapped to subWindows {n}, \
                     which bounds the boundary exposure to {} rather than {}.",
                    b.id,
                    2 * (count / n.max(1) as i64).max(1),
                    2 * count
                ),
            ));
            Some(n)
        }
        v1::Alignment::Calendar => {
            out.push(w(
                "alignment",
                format!(
                    "budget `{}` of node `{node}`: `calendar` mapped to subWindows 1 — a single \
                     fixed window. It is no longer wall-clock aligned; the window starts at the \
                     first admission after the previous one expired.",
                    b.id
                ),
            ));
            Some(1)
        }
    };

    let scope_by = match b.scope.len() {
        0 => None,
        1 => {
            let d = b.scope[0].as_str();
            out.push(w(
                "scope",
                format!(
                    "budget `{}` of node `{node}`: scope `{d}` mapped to scopeBy `payload.{d}`.",
                    b.id
                ),
            ));
            Some(format!("payload.{d}"))
        }
        _ => {
            let d = b.scope[0].as_str();
            out.push(w(
                "scope",
                format!(
                    "budget `{}` of node `{node}` scopes on {} dimensions; v2 keys a counter on \
                     ONE payload path. `payload.{d}` was taken and the rest ({}) were dropped — \
                     check that this is still the limit you meant.",
                    b.id,
                    b.scope.len(),
                    b.scope[1..]
                        .iter()
                        .map(|x| x.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ));
            Some(format!("payload.{d}"))
        }
    };

    if b.max_keys.is_some() {
        out.push(w(
            "maxKeys",
            format!(
                "budget `{}` of node `{node}`: `maxKeys` is a no-op — per-key counters are \
                 Postgres rows with a TTL, not entries in a document Gate re-reads whole every \
                 cycle.",
                b.id
            ),
        ));
    }

    let shared_key = match b.store {
        v1::Store::Kv => {
            out.push(w(
                "store",
                format!(
                    "budget `{}` of node `{node}`: `store: kv` mapped to sharedKey `{}` — same \
                     counter, same scope (the application), and no capacity lease in front of it \
                     any more.",
                    b.id, b.id
                ),
            ));
            Some(b.id.clone())
        }
        v1::Store::Gate => None,
    };

    Budget {
        id: Some(b.id.clone()),
        count,
        time_ms,
        sub_windows,
        scope_by,
        shared_key,
        when_op: b.matcher.as_ref().map(|m| m.op.clone()),
        confidence: b.confidence,
        source: b.source.clone(),
        as_of: b.as_of.clone(),
    }
}

/// The budget a v1 node gets when it declares nothing v2 can measure a rate
/// against.
///
/// Two v1 shapes land here. A **class node** with an out-edge was allowed to
/// declare no budget at all: it existed to isolate a traffic class and carry a
/// priority, and the limit it was checked against lived downstream. And a node
/// with only SCOPED budgets was legal too — v1's ETA read the worst key and its
/// breach ring was per-replica, so neither needed a node-level denominator; v2's
/// ETA and its breaker both do.
///
/// Either way the mapping declares a pass-through — which limits nothing,
/// exactly as before — and says so loudly, rather than inventing a ceiling
/// nobody asked for.
fn passthrough_budget(node: &str, out: &mut Vec<Problem>) -> Budget {
    out.push(w(
        "node-budget",
        format!(
            "node `{node}` declared no budget on the node itself (v1 allowed that for a class \
             node, and for a node carrying only per-key budgets). v2 requires one — it is what \
             the ETA measures a rate against and what the breaker spends when a vendor says 429 \
             — so a pass-through of 1000000 per second has been declared for it: it limits \
             nothing, exactly as before. Replace it with the real limit."
        ),
    ));
    Budget {
        id: Some("passthrough".into()),
        count: 1_000_000,
        time_ms: 1000,
        sub_windows: Some(1),
        scope_by: None,
        shared_key: None,
        when_op: None,
        confidence: Confidence::Inferred,
        source: None,
        as_of: None,
    }
}

fn cost(c: &v1::Cost, node: &str, out: &mut Vec<Problem>) -> Cost {
    let default = c.default.ceil().max(1.0) as i64;
    let max = c.max.ceil().max(default as f64) as i64;
    if c.default.fract() != 0.0 || c.max.fract() != 0.0 {
        out.push(w(
            "cost",
            format!(
                "node `{node}`: cost is an integer in v2 (the counter is an integer on this wire): \
                 default {}->{default}, max {}->{max}.",
                c.default, c.max
            ),
        ));
    }
    Cost::Path(CostPath {
        path: format!("payload.{}", c.field),
        default,
        max: Some(max),
    })
}

fn share_of(cap: &v1::CapPolicy, tightest_rate: f64, out: &mut Vec<Problem>, lane: &str) -> f64 {
    match cap {
        v1::CapPolicy::Ceiling => 1.0,
        v1::CapPolicy::Share(f) => f.clamp(0.01, 1.0),
        v1::CapPolicy::Absolute(n) => {
            let s = if tightest_rate > 0.0 {
                (n / tightest_rate).clamp(0.01, 1.0)
            } else {
                1.0
            };
            out.push(w(
                "lane-cap",
                format!(
                    "lane `{lane}`: `absolute:{n}` mapped to share {s:.2} — a share is a fraction \
                     of the node's own ceiling, so an absolute rate is expressed against it."
                ),
            ));
            s
        }
        v1::CapPolicy::CeilingMinusMeasured => {
            let s = ((1.0 - 0.0f64.max(0.0)).min(1.0) - 0.0).max(0.05);
            // A static share, rounded to the nearest 0.05. There is no meter to
            // derive it from any more, and there does not need to be: one
            // counter with N ceilings cannot oversubscribe, which is what the
            // derived cap existed to prevent (measured: 7131 against a declared
            // 5000 per ten seconds).
            let rounded = ((s * 20.0).round() / 20.0).clamp(0.05, 1.0);
            out.push(w(
                "lane-cap",
                format!(
                    "lane `{lane}`: `ceiling-minus-measured` mapped to a static share of \
                     {rounded:.2}. There is no meter to derive it from any more, and there does \
                     not need to be: one counter with N ceilings cannot oversubscribe, which is \
                     what the derived cap existed to prevent."
                ),
            ));
            rounded
        }
    }
}

// ------------------------------------------------------------ target -> graph

/// A standalone v1 target becomes a one-node graph named for itself.
pub fn from_v1_target(spec: &v1::TargetSpec) -> Result<Migrated, Refused> {
    let mut out = Vec::new();
    let lanes = if spec.lanes.is_empty() {
        vec![v1::Lane {
            name: "default".into(),
            cap: v1::CapPolicy::Ceiling,
            concurrency: 8,
            floor: 0.0,
            default: true,
        }]
    } else {
        spec.lanes.clone()
    };
    let lane0 = default_lane_name(&lanes);
    let node_name = leaf(&spec.name);

    let tightest = spec
        .budgets
        .iter()
        .map(|b| b.cap / b.period_seconds.max(1) as f64)
        .fold(f64::INFINITY, f64::min);

    let mut node = Node {
        budgets: spec
            .budgets
            .iter()
            .map(|b| budget(b, &mut out, &node_name))
            .collect(),
        cost: cost(&spec.cost, &node_name, &mut out),
        ingress: Some(Ingress::Named(IngressSpec {
            queue: None,
            partitions: Some(spec.admitted.partitions.max(1)),
            http: Some(true),
        })),
        egress: Some(Egress::Name(v1_admitted_queue(
            &spec.application,
            &spec.name,
            &lane0,
        ))),
        batch: Some(spec.pacing.batch.clamp(1, 1000)),
        concurrency: lanes.iter().map(|l| l.concurrency).max().filter(|n| *n > 0),
    };
    if node.budgets.iter().all(|b| b.scope_by.is_some()) {
        node.budgets.push(passthrough_budget(&node_name, &mut out));
    }
    pacing_warnings(spec.pacing.lease_seconds, &node_name, &mut out);
    admitted_warnings(&spec.admitted, &node_name, &mut out);
    shard_warnings(spec.shard_by.is_some(), &node_name, &mut out);
    out.push(w(
        "consume",
        format!(
            "node `{node_name}` is now a terminal with egress `{}` — your consumers pop that queue \
             directly with the SDK instead of `GET .../next`.",
            v1_admitted_queue(&spec.application, &spec.name, &lane0)
        ),
    ));

    let paths = lane_paths(
        &lanes,
        &node_name,
        tightest,
        vec![node_name.clone()],
        &mut out,
    );

    let mut nodes = BTreeMap::new();
    nodes.insert(node_name, node);

    Ok(Migrated {
        doc: GraphDoc {
            application: spec.application.clone(),
            graph: leaf(&spec.name),
            version: spec.version,
            nodes,
            paths,
            counters: None,
        },
        warnings: out,
    })
}

/// A v1 graph, mapped chain by chain.
pub fn from_v1_graph(spec: &v1::GraphSpec) -> Result<Migrated, Refused> {
    if !spec.breach.is_empty() {
        return Err(Refused(format!(
            "this graph declares {} breach rule(s), and v2 has nowhere to put them. The whole \
             breach machinery hangs off `POST /v1/leases/ack`, which is gone: the application \
             consumes the egress queue with its own SDK and Gate never sees the outcome. What \
             replaces it is `POST /v1/apps/{}/graphs/{}/nodes/{{node}}/backoff`, which spends the \
             node's window for `retryAfterSeconds` so EVERY path stops — a node-wide backoff \
             rather than a per-item re-entry. Per-item bounded retry is an open question (design \
             §16.6) and is not in this build: to migrate now, remove `breach` and re-push a \
             throttled item to the ingress queue with your own idempotency key.",
            spec.breach.len(),
            spec.application,
            spec.name
        )));
    }

    let mut out = Vec::new();
    let mut nodes: BTreeMap<String, Node> = BTreeMap::new();

    for (name, n) in &spec.nodes {
        let target = format!("{}.{}", spec.name, name);
        let lanes = if n.lanes.is_empty() {
            vec![v1::Lane {
                name: "default".into(),
                cap: v1::CapPolicy::Ceiling,
                concurrency: 8,
                floor: 0.0,
                default: true,
            }]
        } else {
            n.lanes.clone()
        };
        let lane0 = default_lane_name(&lanes);

        let mut node = Node {
            budgets: n
                .budgets
                .iter()
                .map(|b| budget(b, &mut out, name))
                .collect(),
            cost: cost(&n.cost, name, &mut out),
            ingress: n.entry.then(|| {
                Ingress::Named(IngressSpec {
                    queue: None,
                    partitions: Some(n.admitted.partitions.max(1)),
                    http: Some(true),
                })
            }),
            egress: spec.consume.iter().any(|c| c == name).then(|| {
                Egress::Spec(EgressSpec {
                    queue: v1_admitted_queue(&spec.application, &target, &lane0),
                    group: None,
                })
            }),
            batch: Some(n.pacing.batch.clamp(1, 1000)),
            concurrency: lanes.iter().map(|l| l.concurrency).max().filter(|c| *c > 0),
        };
        if node.budgets.iter().all(|b| b.scope_by.is_some()) {
            node.budgets.push(passthrough_budget(name, &mut out));
        }
        if node.egress.is_some() {
            out.push(w(
                "consume",
                format!(
                    "node `{name}` is now a terminal with egress `{}` — your consumers pop that \
                     queue directly with the SDK instead of `GET .../next`.",
                    v1_admitted_queue(&spec.application, &target, &lane0)
                ),
            ));
        }
        pacing_warnings(n.pacing.lease_seconds, name, &mut out);
        admitted_warnings(&n.admitted, name, &mut out);
        shard_warnings(n.shard_by.is_some(), name, &mut out);
        nodes.insert(name.clone(), node);
    }

    // ---- chains. v1 refused a fan-out (`edge-fanout`: two out-edges COPY the
    // stream rather than splitting it), so every node has at most one successor
    // and a chain is a walk, not a search.
    let mut paths: Vec<Path> = Vec::new();
    let entries: Vec<&String> = spec
        .nodes
        .iter()
        .filter(|(_, n)| n.entry)
        .map(|(k, _)| k)
        .collect();
    for entry in entries {
        let mut chain = vec![entry.clone()];
        let mut priority = 0u32;
        let mut cur = entry.clone();
        let mut guard = 0;
        while let Some(e) = spec.edges.iter().find(|e| e.from == cur) {
            if guard == 0 {
                priority = e.priority;
            }
            guard += 1;
            if guard > 32 || chain.contains(&e.to) {
                break;
            }
            chain.push(e.to.clone());
            cur = e.to.clone();
        }
        let node = spec.nodes.get(entry);
        let lanes = node
            .map(|n| n.lanes.clone())
            .filter(|l| !l.is_empty())
            .unwrap_or_else(|| {
                vec![v1::Lane {
                    name: "default".into(),
                    cap: v1::CapPolicy::Ceiling,
                    concurrency: 8,
                    floor: 0.0,
                    default: true,
                }]
            });
        let tightest = node
            .map(|n| {
                n.budgets
                    .iter()
                    .map(|b| b.cap / b.period_seconds.max(1) as f64)
                    .fold(f64::INFINITY, f64::min)
            })
            .unwrap_or(f64::INFINITY);
        let mut made = lane_paths(&lanes, entry, tightest, chain, &mut out);
        for p in &mut made {
            p.priority = priority;
        }
        paths.append(&mut made);
    }

    out.push(w(
        "edges",
        format!(
            "{} edge(s) mapped to {} path(s). An edge was a merge with a strict priority order; a \
             path is a sequence with a share ceiling on one shared counter. Under saturation the \
             high-share path always has its reserve; it no longer OVERTAKES work already queued \
             ahead of it — priority is capacity now, not queue position.",
            spec.edges.len(),
            paths.len()
        ),
    ));

    Ok(Migrated {
        doc: GraphDoc {
            application: spec.application.clone(),
            graph: spec.name.clone(),
            version: spec.version,
            nodes,
            paths,
            counters: None,
        },
        warnings: out,
    })
}

fn lane_paths(
    lanes: &[v1::Lane],
    entry: &str,
    tightest_rate: f64,
    chain: Vec<String>,
    out: &mut Vec<Problem>,
) -> Vec<Path> {
    let elems: Vec<PathElem> = chain.into_iter().map(PathElem::One).collect();
    if lanes.len() <= 1 {
        return vec![Path {
            name: entry.to_string(),
            priority: 0,
            share: Some(1.0),
            nodes: elems,
        }];
    }
    out.push(w(
        "lanes",
        format!(
            "node `{entry}` declared {} lanes, mapped to {} paths. Lanes DIVIDED a ceiling and \
             were addressed by the push URL; paths CAP a shared one and every path on an ingress \
             node receives EVERY message. If your lanes carried different traffic, give each one \
             its own ingress node before applying this — otherwise each message will traverse all \
             {} paths.",
            lanes.len(),
            lanes.len(),
            lanes.len()
        ),
    ));
    let mut ranked: Vec<(usize, &v1::Lane)> = lanes.iter().enumerate().collect();
    // Declaration order IS the priority order, which is what v1's lane list
    // meant in practice.
    ranked.sort_by_key(|(i, _)| *i);
    ranked
        .into_iter()
        .map(|(i, l)| {
            let share = if i == 0 {
                // `share-top`: the highest priority must be able to reach the
                // whole ceiling, or the headroom above every other share belongs
                // to nobody.
                1.0
            } else {
                share_of(&l.cap, tightest_rate, out, &l.name)
            };
            Path {
                name: l.name.clone(),
                priority: i as u32,
                share: Some(share),
                nodes: elems.clone(),
            }
        })
        .collect()
}

fn pacing_warnings(lease_seconds: i64, node: &str, out: &mut Vec<Problem>) {
    let _ = lease_seconds;
    out.push(w(
        "pacing",
        format!(
            "node `{node}`: `pacing.leaseSeconds` is a no-op — the lease is a WORK lease now (30s, \
             renewed), and pacing is the budget window. `pacing.batch` became the stage's batch."
        ),
    ));
}

fn admitted_warnings(a: &v1::Admitted, node: &str, out: &mut Vec<Problem>) {
    out.push(w(
        "admitted",
        format!(
            "node `{node}`: the admitted ring is gone; {} mapped to the ingress queue's partition \
             count. `partitionBy` is ignored — partitioning is the producer's choice now, and Gate \
             passes each message's own partition through, end to end.",
            a.partitions
        ),
    ));
}

fn shard_warnings(sharded: bool, node: &str, out: &mut Vec<Problem>) {
    if sharded {
        out.push(w(
            "shards",
            format!(
                "node `{node}`: `shardBy`/`shards` are a no-op — cardinality is Postgres rows with \
                 a TTL, not shards, so there are no shard runners to allocate. The budget that was \
                 scoped on the shard dimension is now one counter per value."
            ),
        ));
    }
}

/// The last dotted segment of a v1 target name: `airbnb.ip` is the node `ip`.
fn leaf(name: &str) -> String {
    name.rsplit('.').next().unwrap_or(name).to_string()
}
