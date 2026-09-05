//! When does this go out?
//!
//! The console asks "are we still in control". This answers the question the
//! person waiting on a price update asks, and it is a different one: *how long
//! until my work leaves*. Everything here is read-side — no counter is touched,
//! no decision is made — and the answer is a BOUND, never a promise:
//!
//! > no earlier than this, assuming the backlog that is there right now.
//!
//! # Computed on demand, and nothing standing
//!
//! v1's `Depths` cache with its five read shapes, TTL, stale-serve and 404 memo
//! existed because the RELAY read depths on every cycle. The relay reads no
//! depths at all now, so the cache has no hot-path caller left: it survives for
//! the console, where one page fan-out asks the same question several times, and
//! nothing else in this crate calls it.
//!
//! # Two backlogs, two clocks
//!
//! **Waiting for budget** is paced by a schedule that is DECLARED. The rate to
//! divide by is the compiled `count_sub` per sub-window, plus where the counter
//! already stands — and emphatically not what the node was measured doing: a
//! node whose window is exhausted measures zero per second, and zero per second
//! answers "never" at exactly the moment the question is being asked. The truth
//! then is "nothing until the window rotates, and 150 per second from there",
//! which is a number the declaration knows and the measurement cannot.
//!
//! **Waiting for workers** is the opposite. Nothing declares how fast the
//! caller's own consumers drain the egress queue, so there the measured backlog
//! is all there is.

use std::sync::Arc;

use serde_json::{json, Value};

use gate_core::plan::{CompiledBudget, NodePlan, Stage};

use crate::api::Shared;
use crate::registry::GraphRuntime;

/// What one budget's declared schedule says about a backlog.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Schedule {
    /// Seconds until the last of the wanted cost units could be admitted.
    /// `None` where no schedule ever admits them, which the API spells `null`:
    /// a product can render "we cannot say", and would render an infinity as a
    /// number.
    pub seconds: Option<f64>,
    /// When this budget's sub-window next rotates.
    pub resets_at: i64,
}

/// When the next `want` cost units admit, on the declared schedule alone.
///
/// `cap` is this PATH's slice of the sub-window, not the node's: paths cap a
/// shared counter, so answering off the whole figure would promise capacity this
/// path's own `incr` is going to refuse. `spent` is what the counter holds now,
/// and `resets_in_ms` is what its own TTL says — read from kv, not
/// reconstructed, because the window's start is the first admission after the
/// previous one expired and no arithmetic here can know when that was.
pub fn admits(
    cap: i64,
    window_seconds: i64,
    spent: i64,
    resets_in_ms: i64,
    want: f64,
    now_ms: i64,
) -> Schedule {
    let window_seconds = window_seconds.max(1);
    let resets_at = now_ms.saturating_add(resets_in_ms.max(0));

    // A cap that cannot admit anything never will — no schedule refills it — so
    // "we cannot say" is the only answer that is not a lie.
    if cap <= 0 {
        return Schedule {
            seconds: None,
            resets_at,
        };
    }
    let head = (cap - spent).max(0) as f64;
    if want <= head {
        return Schedule {
            seconds: Some(0.0),
            resets_at,
        };
    }
    let need = want - head;
    // The edge of the current window, plus one full sub-window for every further
    // capful after the first. This is what makes "nothing until it rotates, then
    // 150 per second" expressible at all.
    let windows_after_this = ((need / cap as f64).ceil() as i64 - 1).max(0);
    // Multiply after widening to f64. Both operands are valid i64 values, but
    // a very deep backlog can span enough windows for their integer product to
    // overflow before it ever reaches this display-only estimate.
    let seconds =
        resets_in_ms.max(0) as f64 / 1000.0 + windows_after_this as f64 * window_seconds as f64;
    Schedule {
        seconds: Some(seconds),
        resets_at,
    }
}

/// The answer, for one path through one node.
pub async fn view(
    app: &Shared,
    rt: &Arc<GraphRuntime>,
    node: &str,
    path: &str,
) -> queen_mq::Result<Option<Value>> {
    let Some(stage) = rt.plan.stage(path, node) else {
        return Ok(None);
    };
    let Some(np) = rt.plan.node(node) else {
        return Ok(None);
    };
    let now = crate::now_ms();

    // ---- position.
    //
    // The stage's OWN group, not the queue: queue-level pending is the worst
    // cursor across every reader, and what this needs is the work this stage has
    // not admitted yet.
    let waiting_for_budget: u64 = app
        .depths
        .pending_of_group(&app.queen, &stage.source, &stage.group)
        .await?
        .values()
        .sum();

    // ---- the second backlog: what Gate has already released and the caller's
    // own workers have not picked up.
    let mut waiting_for_workers = 0u64;
    let mut worker_group_known = false;
    if let Some(q) = &np.egress_queue {
        waiting_for_workers = match &np.egress_group {
            Some(g) => {
                worker_group_known = true;
                app.depths
                    .pending_of_group(&app.queen, q, g)
                    .await?
                    .values()
                    .sum()
            }
            // The queue-level number is the WORST cursor across every group, so
            // it is at or above the group being asked about: it can only make
            // the answer later, never earlier, which is the safe direction for a
            // bound.
            None => app.depths.pending(&app.queen, q).await?.values().sum(),
        };
    }

    // ---- weight. Items are what a queue holds; cost units are what a budget
    // spends.
    let measured = measured_cost(app, rt, node, path, now).await;
    // A breaker holding the node is the one caveat that explains the whole
    // answer rather than qualifying it: the window is spent on purpose and the
    // long `etaSeconds` below is a vendor's `Retry-After`, not a backlog.
    let held = crate::breaker::held(&app.budgets, np).await?;
    let cost_per_item = measured.unwrap_or(np.cost.default_value() as f64);
    let want = waiting_for_budget as f64 * cost_per_item;

    // ---- rate.
    let keys: Vec<String> = np.unscoped().map(|b| b.key.clone()).collect();
    let states = app.budgets.read(&keys).await?;

    let mut bound: Option<(&CompiledBudget, Schedule)> = None;
    for b in np.unscoped() {
        let s = states.iter().find(|s| s.key == b.key);
        let sched = admits(
            b.max_for(stage.share),
            b.window_sub_seconds,
            s.map(|s| s.value).unwrap_or(0),
            s.and_then(|s| s.expires_at_ms)
                .map(|e| e.saturating_sub(now))
                .unwrap_or(0),
            want,
            now,
        );
        // The slowest binds, and "never" beats every number.
        let key = |x: &Schedule| x.seconds.unwrap_or(f64::INFINITY);
        bound = match bound {
            Some(a) if key(&a.1) >= key(&sched) => Some(a),
            _ => Some((b, sched)),
        };
    }

    let (bound_by, eta_seconds, resets_at) = match bound {
        Some((b, s)) => (
            Some(b.id.clone()),
            s.seconds.map(|v| v.ceil() as u64),
            Some(s.resets_at),
        ),
        None => (None, None, None),
    };

    let state = if waiting_for_budget > 0 {
        "waiting-budget"
    } else {
        "waiting-workers"
    };

    Ok(Some(json!({
        "at": now,
        "application": rt.doc.application,
        "graph": rt.doc.graph,
        "node": node,
        "path": path,
        // The console's vocabulary, kept: a node IS a target.
        "target": format!("{}.{}", rt.doc.graph, node),
        "state": state,
        "aheadCost": want,
        "etaSeconds": if waiting_for_budget == 0 { Some(0) } else { eta_seconds },
        "boundBy": bound_by,
        "windowResetsAt": resets_at,
        "waitingForBudget": waiting_for_budget,
        "waitingForWorkers": waiting_for_workers,
        "assumes": assumes(rt, np, stage, measured, cost_per_item, worker_group_known, held.as_ref()),
    })))
}

/// What an item was measured costing, over the counters stream when it is on.
async fn measured_cost(
    app: &Shared,
    rt: &Arc<GraphRuntime>,
    node: &str,
    path: &str,
    now: i64,
) -> Option<f64> {
    let h = app.history.as_ref()?;
    h.avg_cost(
        &rt.doc.application,
        &format!("{}.{}", rt.doc.graph, node),
        path,
        now,
    )
    .await
}

/// The caveats that actually apply, so the bound is read as one.
///
/// Every listed caveat is a way work can be put in front of yours **after** the
/// answer is given, which is exactly why the number is a bound and never a
/// promise.
fn assumes(
    rt: &Arc<GraphRuntime>,
    np: &NodePlan,
    stage: &Stage,
    measured: Option<f64>,
    cost_per_item: f64,
    worker_group_known: bool,
    held: Option<&crate::breaker::Record>,
) -> String {
    let mut parts = vec![
        "no earlier than: the backlog that is there right now, at the refill schedule the spec \
         declares"
            .to_string(),
    ];

    parts.push(match measured {
        Some(c) => format!("item cost measured at {c:.3} over the last five minutes"),
        None => format!(
            "item cost taken from the declared default of {cost_per_item} (the counters stream is \
             off for this graph, so nothing has measured one)"
        ),
    });

    let shared: Vec<&str> = np
        .budgets
        .iter()
        .filter_map(|b| b.shared_key.as_deref())
        .collect();
    if !shared.is_empty() {
        parts.push(format!(
            "budget {} is one counter across every node and graph of this application that names \
             it, and the other spenders are not visible here",
            shared.join(", ")
        ));
    }

    let scoped: Vec<&str> = np
        .budgets
        .iter()
        .filter(|b| b.is_scoped())
        .map(|b| b.id.as_str())
        .collect();
    if !scoped.is_empty() {
        parts.push(format!(
            "budget {} is one counter per key and this number does not resolve which key your item \
             will meet",
            scoped.join(", ")
        ));
    }

    // §9 closes the caveat list with this one, and it is the one that changes
    // how the number should be read: a node whose window a breaker has just
    // spent answers a long `etaSeconds` because a vendor said 429, not because
    // anything is queued in front of the caller.
    if let Some(r) = held {
        parts.push(format!(
            "a breaker is holding this node until {} ({}s from {}{}), so the window is spent on              purpose and this number is that deadline rather than a backlog",
            r.until_ms(),
            r.retry_after_seconds,
            r.at,
            match &r.by {
                Some(by) => format!(", reported by {by}"),
                None => String::new(),
            }
        ));
    }

    let crossing = gate_core::plan::paths_through(&rt.plan, &np.name);
    if crossing.len() > 1 {
        let higher: Vec<&str> = crossing
            .iter()
            .filter(|p| {
                rt.plan
                    .stage(p, &np.name)
                    .is_some_and(|s| s.share > stage.share + 1e-9)
            })
            .map(|s| s.as_str())
            .collect();
        if !higher.is_empty() {
            parts.push(format!(
                "`{}` is crossed by {} paths and {} may take the headroom above this path's \
                 ceiling first",
                np.name,
                crossing.len(),
                higher.join(", ")
            ));
        }
    }

    if stage.destinations.len() > 1 {
        parts.push(format!(
            "this hop fans out to {} branches, so one message becomes {} downstream and each one \
             charges its own node's budgets",
            stage.destinations.len(),
            stage.destinations.len()
        ));
    }

    if np.egress_queue.is_some() && !worker_group_known {
        parts.push(
            "the worker backlog is the egress queue's worst cursor across every group, because the \
             declaration names no `egress.group` — it can only make this later, never earlier"
                .to_string(),
        );
    }

    parts.join("; ")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A window that has room answers zero, and does not consult a clock to do
    /// it.
    #[test]
    fn room_now_is_no_wait() {
        let s = admits(150, 1, 20, 400, 100.0, 1_000_000);
        assert_eq!(s.seconds, Some(0.0));
    }

    /// The edge of the current window, and nothing more, when one rotation is
    /// enough.
    #[test]
    fn one_rotation_is_the_edge_of_the_window() {
        let s = admits(150, 1, 150, 400, 100.0, 1_000_000);
        assert_eq!(s.seconds, Some(0.4));
        assert_eq!(s.resets_at, 1_000_400);
    }

    /// The edge, plus one whole sub-window per further capful.
    #[test]
    fn a_backlog_of_several_windows_counts_them() {
        // 150 per second, nothing spent, 400ms to the edge, 460 units wanted:
        // 150 now, then 150 + 150 + 10 over the next three windows.
        let s = admits(150, 1, 150, 400, 460.0, 0);
        assert_eq!(s.seconds, Some(0.4 + 3.0));
    }

    /// A cap that cannot admit anything never will — no schedule refills it — so
    /// `null` rather than an infinity a product would render as a number.
    ///
    /// It is reachable from a stored document: a share that rounds a path out of
    /// existence is refused at declare time, but an older build's document can
    /// still carry one.
    #[test]
    fn a_ceiling_of_zero_answers_we_cannot_say() {
        assert_eq!(admits(0, 1, 0, 0, 1.0, 0).seconds, None);
    }

    /// A spent window measures ZERO per second, and zero per second answers
    /// "never" at exactly the moment somebody asks. The declared schedule is
    /// what makes the answer a number.
    #[test]
    fn a_spent_window_still_answers_from_the_declared_schedule() {
        let s = admits(150, 10, 150, 9_500, 150.0, 0);
        assert_eq!(s.seconds, Some(9.5));
    }

    #[test]
    fn an_extreme_schedule_remains_an_estimate_instead_of_overflowing() {
        let s = admits(1, i64::MAX, 1, 10, 3.0, i64::MAX - 5);
        assert_eq!(s.resets_at, i64::MAX);
        assert!(
            s.seconds.is_some_and(|seconds| seconds > i64::MAX as f64),
            "the two post-edge windows should be represented without integer overflow: {s:?}"
        );
    }
}
