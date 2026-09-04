//! v1 documents, read and mapped.
//!
//! The endpoint shapes do not move (design §12.1): a `PUT` of a v1 document is
//! accepted, mapped, and answered **200 with warnings naming every field that
//! was mapped or ignored** — never a silent success, and never a 422 for having
//! been written last year.
//!
//! Nothing is refused. The one field that came close is `breach[]`, whose whole
//! machinery hung off `POST /v1/leases/ack` — the application consumes the
//! egress queue with its own SDK now and Gate never sees the outcome — and it
//! was a refusal until re-entry shipped (design §16.6). It maps to
//! `maxAttempts`, with a warning that says the bound survived and the TRIGGER
//! did not: re-entry is something the caller asks for now.

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

/// How many sub-windows a migrated `rolling` budget gets.
///
/// The aim is one-second sub-windows, and three ceilings cut it down. **Every
/// one of them is a rule this document has to pass afterwards** — a migration
/// that produces a document its own validator refuses is not a migration, and
/// §12.1 promises a v1 document is answered 200 with warnings and never a 422
/// for having been written last year.
///
/// * `subwindow-range`: at most 3600. A day at one-second sub-windows is 86400,
///   which was refused outright.
/// * `subwindow-fits`: at most `count`, or each sub-window carries less than one
///   and the budget enforces `N` per window instead of `count`.
/// * `cost-fits`: at most `count / cost.max`, or a sub-window cannot hold the
///   largest item the node declares. Such an item can NEVER be admitted: it
///   parks the head of its partition for ever and never reaches a DLQ, because
///   a lease that expires charges no retry. This is the one that refused every
///   real v1 document, because a daily ceiling divided into seconds admits far
///   fewer than one hundred calls a second while `cost.max: 100` is what a v1
///   caller wrote.
///
/// Then it is shrunk to a DIVISOR of the window in seconds, which buys
/// EXACTNESS and no longer safety. A sub-window is a whole number of seconds, so
/// an `N` that does not divide the period evenly cannot express the declared
/// rate: `subdivide` rounds the window up and the migrated document then
/// enforces slightly less than the caller asked for (20000 per 300s over 150
/// sub-windows is 66.5/s against 66.67/s). A divisor makes it land exactly.
///
/// It used to buy safety too, because `subdivide` floored the window and
/// enforced `100 per 1800ms` as `100 per 1000ms` — nearly twice the declared
/// ceiling. That is fixed at the source now, so a migration that picked a
/// non-divisor would be merely tighter than asked rather than a vendor block.
fn rolling_sub_windows(time_ms: i64, count: i64, cost_max: i64) -> u32 {
    let seconds = (time_ms / 1000).max(1);
    let per_item = (count / cost_max.max(1)).max(1);
    let ceiling = count.clamp(1, 3600).min(per_item).min(seconds);
    let mut n = seconds.min(ceiling).max(1);
    while n > 1 && seconds % n != 0 {
        n -= 1;
    }
    n.clamp(1, u32::MAX as i64) as u32
}

fn budget(b: &v1::Budget, cost_max: i64, out: &mut Vec<Problem>, node: &str) -> Budget {
    let time_ms = b.period_seconds.max(1) * 1000;
    let count = (b.cap.floor() as i64).max(1);

    let sub_windows = match b.alignment {
        // v1's two-bucket sliding window is not expressible: `incr` is one
        // atomic add against one row, and faking a slide client-side would put
        // back the read-then-write race the primitive exists to remove.
        // Subdivision is the same trade in a form the primitive can keep — it
        // bounds the boundary exposure to 2 x count/N instead of 2 x count.
        v1::Alignment::Rolling => {
            let n = rolling_sub_windows(time_ms, count, cost_max);
            out.push(w(
                "alignment",
                format!(
                    "budget `{}` of node `{node}`: `alignment` is gone — kv owns a fixed window, \
                     and smoothing is expressed as subWindows. `rolling` mapped to subWindows {n} \
                     of {} second(s) each, which bounds the boundary exposure to {} rather than \
                     {}. Fewer than one per second where a sub-window would otherwise be too \
                     small to hold this node's largest item ({cost_max}), which can never be \
                     admitted.",
                    b.id,
                    (time_ms / n.max(1) as i64) / 1000,
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
/// with only SCOPED or CONDITIONAL budgets was legal too — v1's ETA read the
/// worst key and its breach ring was per-replica, so neither needed a counter
/// every item meets; v2's ETA and its breaker both do.
///
/// Either way the mapping declares a pass-through — which limits nothing,
/// exactly as before — and says so loudly, rather than inventing a ceiling
/// nobody asked for.
fn passthrough_budget(node: &str, out: &mut Vec<Problem>) -> Budget {
    out.push(w(
        "node-budget",
        format!(
            "node `{node}` declared no unconditional budget on the node itself (v1 allowed that \
             for a class node, and for a node carrying only per-key or conditional budgets). v2 \
             requires one — it is what the ETA measures a rate against and what the breaker \
             spends when a vendor says 429 — so a pass-through of 1000000 per second has been \
             declared for it: it limits nothing, exactly as before. Replace it with the real \
             limit."
        ),
    ));
    Budget {
        // The constant, not the spelling: `plan::fitting_workers` recognises
        // this id to REFUSE to divide by it, and a drift between the two would
        // silently hand a class node a lane per partition.
        id: Some(crate::plan::PASSTHROUGH_BUDGET_ID.into()),
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

fn share_of(
    cap: &v1::CapPolicy,
    floor: f64,
    tightest_rate: f64,
    out: &mut Vec<Problem>,
    lane: &str,
) -> f64 {
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
            // §12.2: `share: 1 - floor`, rounded to the nearest 0.05.
            //
            // The lane's `floor` is the whole point of the mapping and it must
            // be READ. `ceiling-minus-measured` meant "take what the higher
            // lanes are not using, but never less than `floor` of the ceiling",
            // so what it reserved for those higher lanes is `1 - floor`. Losing
            // the floor made every derived lane a share of 1.00 — not
            // oversubscription, since there is one counter, but the exact
            // opposite of §3.6's promise: the top path's headroom stops being a
            // reserve the moment the low path can spend the counter to the
            // ceiling too.
            //
            // A static share, because there is no meter to derive it from any
            // more, and there does not need to be: one counter with N ceilings
            // cannot oversubscribe, which is what the derived cap existed to
            // prevent (measured: 7131 against a declared 5000 per ten seconds).
            let s = (1.0 - floor.clamp(0.0, 1.0)).clamp(0.05, 1.0);
            let rounded = ((s * 20.0).round() / 20.0).clamp(0.05, 1.0);
            out.push(w(
                "lane-cap",
                format!(
                    "lane `{lane}`: `ceiling-minus-measured` with floor {floor:.2} mapped to a \
                     static share of {rounded:.2}. There is no meter to derive it from any more, \
                     and there does not need to be: one counter with N ceilings cannot \
                     oversubscribe, which is what the derived cap existed to prevent."
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

    // The largest an item may cost, which is what decides how finely a budget
    // can be subdivided: a sub-window that cannot hold one item admits nothing,
    // for ever.
    let cost_ceiling = spec.cost.max.ceil().max(1.0) as i64;
    let mut node = Node {
        budgets: spec
            .budgets
            .iter()
            .map(|b| budget(b, cost_ceiling, &mut out, &node_name))
            .collect(),
        cost: cost(&spec.cost, &node_name, &mut out),
        ingress: Some(Ingress::Named(IngressSpec {
            queue: None,
            partitions: Some(spec.admitted.partitions.max(1)),
            http: Some(true),
            // v1's push queued; so does v2's, unless a caller asks otherwise.
            shed: None,
        })),
        egress: Some(Egress::Name(v1_admitted_queue(
            &spec.application,
            &spec.name,
            &lane0,
        ))),
        batch: Some(spec.pacing.batch.clamp(1, 1000)),
        // NOT mapped — see `lane_concurrency_warning`.
        concurrency: None,
    };
    if node
        .budgets
        .iter()
        .all(|b| b.scope_by.is_some() || b.when_op.is_some())
    {
        node.budgets.push(passthrough_budget(&node_name, &mut out));
    }
    lane_concurrency_warning(&lanes, &node_name, &mut out);
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
            max_attempts: None,
            counters: None,
        },
        warnings: out,
    })
}

/// A v1 graph, mapped chain by chain.
pub fn from_v1_graph(spec: &v1::GraphSpec) -> Result<Migrated, Refused> {
    let mut out = Vec::new();
    let max_attempts = breach_attempts(spec, &mut out);
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

        let cost_ceiling = n.cost.max.ceil().max(1.0) as i64;
        let mut node = Node {
            budgets: n
                .budgets
                .iter()
                .map(|b| budget(b, cost_ceiling, &mut out, name))
                .collect(),
            cost: cost(&n.cost, name, &mut out),
            ingress: n.entry.then(|| {
                Ingress::Named(IngressSpec {
                    queue: None,
                    partitions: Some(n.admitted.partitions.max(1)),
                    http: Some(true),
                    shed: None,
                })
            }),
            egress: spec.consume.iter().any(|c| c == name).then(|| {
                Egress::Spec(EgressSpec {
                    queue: v1_admitted_queue(&spec.application, &target, &lane0),
                    group: None,
                })
            }),
            batch: Some(n.pacing.batch.clamp(1, 1000)),
            // NOT mapped — see `lane_concurrency_warning`.
            concurrency: None,
        };
        if node
            .budgets
            .iter()
            .all(|b| b.scope_by.is_some() || b.when_op.is_some())
        {
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
        lane_concurrency_warning(&lanes, name, &mut out);
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
            max_attempts,
            counters: None,
        },
        warnings: out,
    })
}

/// v1's `breach[]`, mapped to `maxAttempts` and a warning that says what moved.
///
/// It used to be a REFUSAL, because the machinery hung off `POST /v1/leases/ack`
/// and there was nowhere to put it. There is now (design §16.6, option 2), and
/// §12.1 is explicit that a v1 document is *"accepted, mapped, and answered 200
/// with warnings naming every field that was mapped or ignored — never a silent
/// success and never a 422 for having been written last year"*.
///
/// What does NOT survive is the trigger. v1 watched the ack and re-entered by
/// itself; v2 never sees an outcome, so `when` has no reader and the application
/// has to ask. The warning has to say so plainly, because a caller who reads
/// "mapped" as "still automatic" gets a graph that silently stops retrying.
fn breach_attempts(spec: &v1::GraphSpec, out: &mut Vec<Problem>) -> Option<u32> {
    if spec.breach.is_empty() {
        return None;
    }
    let max = spec
        .breach
        .iter()
        .map(|b| b.max_attempts)
        .max()
        .unwrap_or(0);
    let foreign: Vec<&str> = spec
        .breach
        .iter()
        .map(|b| b.retry_to.as_str())
        .filter(|t| *t != "origin-entry")
        .collect();
    out.push(w(
        "breach",
        format!(
            "{} breach rule(s) mapped to `maxAttempts: {max}`, and the TRIGGER did not come with \
             them. v1 watched `POST /v1/leases/ack` and re-entered a throttled item by itself; \
             that ack is gone — your consumers pop the egress queue with their own SDK and Gate \
             never sees the outcome — so re-entry is something you ASK for now: \
             `POST /v1/apps/{}/graphs/{}/reenter {{\"payload\": ..., \"txn\": ...}}`. It still \
             goes back to the door the item came in at, still re-pays every budget on its path, \
             still carries the attempt in its transaction id so a double report collapses on \
             dedup, and is still bounded by `maxAttempts`. The aggregate half is \
             `POST .../nodes/{{node}}/backoff`, which spends the node's window so every path \
             stops at once.{}",
            spec.breach.len(),
            spec.application,
            spec.name,
            if foreign.is_empty() {
                String::new()
            } else {
                format!(
                    " `retryTo` is ignored: re-entry is always at the origin entry, never at `{}`.",
                    foreign.join("`, `")
                )
            }
        ),
    ));
    Some(max.max(1))
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
                share_of(&l.cap, l.floor, tightest_rate, out, &l.name)
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

/// v1's `lanes[].concurrency` is dropped rather than carried over, and the
/// caller is told.
///
/// It looks like the same number and it is not. v1's was how many GATE RUNNERS
/// a lane got — a throughput knob for a runtime that pinned a runner per shard
/// per lane, and whose default was eight because eight was a reasonable number
/// of goroutines. v2 derives the worker count from the BUDGET, because a rate
/// limiter's drain never needs to exceed its own cap.
///
/// Carrying it would defeat the change on exactly the documents that motivated
/// it: every migrated node would pin eight workers regardless of its ceiling,
/// which for the three graphs we run is 128 parked consumers for caps between
/// 1.7 and 400 items a second. A caller who genuinely wants a fixed count says
/// so in the v2 field, `nodes[].concurrency`, which still wins.
fn lane_concurrency_warning(lanes: &[v1::Lane], node: &str, out: &mut Vec<Problem>) {
    let declared = lanes.iter().map(|l| l.concurrency).max().unwrap_or(0);
    if declared == 0 {
        return;
    }
    out.push(w(
        "lane-concurrency",
        format!(
            "node `{node}`: `lanes[].concurrency: {declared}` is not carried over. It counted \
             GATE RUNNERS per lane, and there are none — the worker count is DERIVED from this \
             node's own ceiling now (one lane per {} items/s of it, capped by the partition \
             count), because a limiter never needs to drain faster than it admits. Declare \
             `nodes[].concurrency` if you want a fixed number anyway.",
            crate::plan::LANE_CAPACITY
        ),
    ));
}

fn pacing_warnings(lease_seconds: i64, node: &str, out: &mut Vec<Problem>) {
    out.push(w(
        "pacing",
        format!(
            "node `{node}`: `pacing.leaseSeconds: {lease_seconds}` is a no-op — the lease is a \
             WORK lease now (GATE_LEASE_SECONDS, {} by default, renewed while a handler runs), \
             and pacing is the budget window. `pacing.batch` became the stage's batch.",
            crate::plan::DEFAULT_LEASE_SECONDS
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
