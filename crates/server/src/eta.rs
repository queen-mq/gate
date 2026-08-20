//! When does this go out?
//!
//! The console asks "are we still in control". This answers the question the
//! person waiting on a price update asks, and it is a different one: *how long
//! until my work leaves*. Everything here is read-side — no counter is touched,
//! no decision is made — and the answer is a BOUND, never a promise:
//!
//! > no earlier than this, assuming the backlog that is there right now.
//!
//! # Two backlogs, two clocks
//!
//! The two queues in [`crate::depth`] have different owners, and they need
//! different arithmetic — which is the whole reason this is a module and not a
//! division.
//!
//! **Waiting for budget** is paced by a schedule that is DECLARED. The rate to
//! divide by is `cap / periodSeconds` off the spec, plus where the window
//! already stands, and emphatically not what the lane was measured doing: a lane
//! whose window is exhausted measures zero per second, and zero per second
//! answers "never" at exactly the moment the question is being asked. The truth
//! then is "nothing until :00, and 150/s from there", which is a number the
//! declaration knows and the measurement cannot.
//!
//! **Waiting for workers** is the opposite. Nothing declares how fast the
//! caller's own consumers drain what the gate already admitted, so there the
//! measured rate is the only honest input — and where it is zero, `null` is the
//! honest answer, because stopped workers really do mean "we cannot say".
//!
//! # Why it stays a bound
//!
//! Everything that can put work in front of yours after this answer is given:
//! a higher-priority leg arriving at a merge, a budget on a pool shared with
//! other targets, a breached item re-entering at its entry to be paced again,
//! and — on a sharded target — the fact that the counter your item will meet is
//! one shard's and not the lane's. The response says which of these apply in
//! `assumes` rather than leaving the reader to discover them.

use std::sync::Arc;

use gate_core::{utilisation_max, Alignment, Budget, Store, LANE_BUDGET};
use serde_json::{json, Value};

use crate::api::{eta, Shared};
use crate::registry::{GraphRuntime, TargetRuntime};

/// What one budget's declared schedule says about a backlog.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Schedule {
    /// Seconds until the last of the wanted cost units could be admitted.
    /// `None` where no schedule ever admits them, which the API spells `null`
    /// for the same reason [`crate::api::eta`] does: a product can render "we
    /// cannot say", and would render an infinity as a number.
    pub seconds: Option<f64>,
    /// When this budget's window next rotates.
    pub resets_at: i64,
}

/// When the next `want` cost units admit, on the declared schedule alone.
///
/// `cap` is the lane's SLICE of the declared cap, not the cap: lanes divide a
/// ceiling rather than replicate it (`TargetSpec::lane_share`), so a lane
/// answering with the whole figure would promise capacity its own gate will
/// refuse. `spent` is what that lane's counter already holds in the window.
pub fn admits(
    alignment: Alignment,
    cap: f64,
    period_seconds: i64,
    spent: f64,
    want: f64,
    now_ms: i64,
) -> Schedule {
    let period_seconds = period_seconds.max(1);
    let period_ms = period_seconds * 1000;
    let resets_at = (now_ms / period_ms + 1) * period_ms;

    // A cap that cannot admit anything never will — no schedule refills it — so
    // "we cannot say" is the only answer that is not a lie.
    if cap <= 0.0 {
        return Schedule { seconds: None, resets_at };
    }
    let head = (cap - spent).max(0.0);
    if want <= head {
        return Schedule { seconds: Some(0.0), resets_at };
    }
    let need = want - head;

    let seconds = match alignment {
        // The counter zeroes at the edge, and the WHOLE cap is available the
        // instant it does. So the wait is to that edge, plus one full window for
        // every further cap-worth after the first — which is what makes
        // "nothing until :00, then 150/s" expressible at all.
        Alignment::Calendar => {
            let windows_after_this = ((need / cap).ceil() as i64 - 1).max(0);
            (resets_at - now_ms) as f64 / 1000.0 + (windows_after_this * period_seconds) as f64
        }
        // No edge to wait for. The two-bucket window carries the previous
        // window's spend as a tail that decays continuously, so room comes back
        // continuously too, and over any window-length interval the counter
        // admits `cap` — the declared rate, which is the one to divide by.
        Alignment::Rolling => need / (cap / period_seconds as f64),
    };
    Schedule { seconds: Some(seconds), resets_at }
}

/// The graph and node this target is, if it is one.
fn node_of(app: &Shared, rt: &Arc<TargetRuntime>) -> Option<(Arc<GraphRuntime>, String)> {
    let g = app.registry.graph_by_key(rt.graph.as_ref()?)?;
    let node = g
        .spec
        .nodes
        .keys()
        .find(|n| g.spec.node_target_name(n) == rt.spec.name)?
        .clone();
    Some((g, node))
}

/// Whoever drains this lane's admitted queue — whose backlog IS "waiting for
/// workers".
///
/// For a target and for a node a caller may consume, that is the executor group
/// a `next` pops under. For an INTERIOR node it is the relays on the out-edges,
/// one group per edge because each edge gets the whole stream. Asking about the
/// executor group there would name a group that has never popped anything, and
/// a group with no cursor owes its whole retained range — so a node quietly
/// keeping up would report every message it ever admitted as work its
/// non-existent consumers are behind on.
fn drain_groups(app: &Shared, rt: &Arc<TargetRuntime>, lane: &str) -> Vec<String> {
    match node_of(app, rt) {
        Some((g, node)) if !g.spec.is_consume(&node) => g
            .spec
            .out_edges(&node)
            .iter()
            .map(|e| crate::edge::group_of(&g.spec.application, &g.spec.name, &e.from, &e.to))
            .collect(),
        _ => vec![crate::api::exec_group(lane)],
    }
}

/// The caveats that actually apply, so the bound is read as one.
fn assumes(
    app: &Shared,
    rt: &Arc<TargetRuntime>,
    measured_cost: Option<f64>,
    cost_per_item: f64,
) -> String {
    let mut parts = vec![
        "no earlier than: the backlog that is there right now, at the refill schedule the spec \
         declares"
            .to_string(),
    ];

    parts.push(match measured_cost {
        Some(c) => format!("item cost measured at {c:.3} over the last five minutes"),
        // Every interior node of a graph lands here: it is drained by a relay
        // and a relay acks nothing, so no call event ever carries its cost.
        None => format!("item cost taken from the declared default of {cost_per_item}"),
    });

    let shared: Vec<&str> = rt
        .spec
        .budgets
        .iter()
        .filter(|b| b.store == Store::Kv)
        .map(|b| b.id.as_str())
        .collect();
    if !shared.is_empty() {
        parts.push(format!(
            "budget {} is spent from a pool shared with every target that declares it, and the \
             other spenders are not visible here",
            shared.join(", ")
        ));
    }

    if rt.spec.is_sharded() {
        parts.push(format!(
            "a target sharded by `{}` is answered against its worst shard, and the item you are \
             asking about is in exactly one of them",
            rt.spec.shard_by.map(|d| d.as_str()).unwrap_or("")
        ));
    }

    if let Some((g, node)) = node_of(app, rt) {
        if g.spec.in_edges(&node).len() > 1 {
            parts.push(format!(
                "`{node}` is a merge: a higher-priority leg is drained first and can arrive in \
                 front of this work"
            ));
        }
        if !g.spec.breach.is_empty() {
            parts.push(
                "a call the vendor throttles re-enters at its entry and is paced again from there"
                    .to_string(),
            );
        }
    }

    parts.join("; ")
}

/// The answer, for one lane of one target.
pub async fn view(app: &Shared, rt: &Arc<TargetRuntime>, lane: &str) -> Value {
    let now = crate::now_ms();
    let spec = &rt.spec;

    // ------------------------------------------------------------- position
    //
    // The gate's OWN group, not the queue: queue-level pending is the worst
    // cursor across every reader, and what this needs is the work the gate has
    // not admitted yet.
    let push = app
        .depths
        .pending_of_group(
            &app.queen,
            &spec.push_queue(),
            &crate::gate::consumer_group(spec, lane),
        )
        .await;
    // Only this lane's partitions. A sharded target's are `{lane}:{shard}` and
    // there is one runner on each, so the lane's backlog is their sum.
    let waiting_for_budget: u64 = spec
        .lane_partitions(lane)
        .iter()
        .filter_map(|p| push.get(p).copied())
        .sum();

    let admitted_q = spec.admitted_queue(lane);
    let groups = drain_groups(app, rt, lane);
    let mut waiting_for_workers = 0u64;
    if groups.is_empty() {
        // Nothing drains it at all — an interior node with no out-edge. Whatever
        // is sitting there is sitting there.
        waiting_for_workers = app
            .depths
            .pending(&app.queen, &admitted_q)
            .await
            .values()
            .sum();
    }
    for g in &groups {
        // The worst reader, where there are several: two edges out of one node
        // each get the whole stream, and the work is not gone until the slowest
        // of them has moved it.
        let n: u64 = app
            .depths
            .pending_of_group(&app.queen, &admitted_q, g)
            .await
            .values()
            .sum();
        waiting_for_workers = waiting_for_workers.max(n);
    }

    // --------------------------------------------------------------- weight
    //
    // Items are what a queue holds; cost units are what a budget spends. The
    // table when there is one, this replica's ring only when there is not —
    // both halves of the ratio have to come from the same place to mean
    // anything (see `Meter::avg_cost`).
    let measured_cost = match app.history.as_ref() {
        Some(h) => h.avg_cost(&spec.application, &spec.name, lane, now).await,
        None => app.meter.avg_cost(&spec.key(), lane, now),
    };
    let cost_per_item = measured_cost.unwrap_or(spec.cost.default);

    let state = if waiting_for_budget > 0 {
        "waiting-budget"
    } else {
        "waiting-workers"
    };
    let ahead_items = if waiting_for_budget > 0 {
        waiting_for_budget
    } else {
        waiting_for_workers
    };
    let ahead_cost = ahead_items as f64 * cost_per_item;

    // ----------------------------------------------------------------- rate
    let (eta_seconds, bound_by, resets_at) = if waiting_for_budget == 0 {
        // Nothing is being held back, so no budget is what stands between this
        // caller and their answer: their own consumers are, and only the
        // measured rate can speak for those.
        (eta(waiting_for_workers, drain_rate(app, rt, lane, now).await), None, None)
    } else {
        match binding_budget(rt, lane, ahead_cost, now) {
            Some((id, s)) => (s.seconds.map(|v| v.ceil() as u64), Some(id), Some(s.resets_at)),
            // Nothing declared paces this lane — a class node with no budget of
            // its own, which exists to isolate rather than to limit. The
            // measured rate is then the only thing left to answer with.
            None => (
                eta(waiting_for_budget, drain_rate(app, rt, lane, now).await),
                None,
                None,
            ),
        }
    };

    json!({
        "at": now,
        "application": spec.application,
        "target": spec.name,
        "lane": lane,
        "state": state,
        "aheadCost": ahead_cost,
        "etaSeconds": eta_seconds,
        "boundBy": bound_by,
        "windowResetsAt": resets_at,
        // The two counts the state is chosen between, so `aheadCost` is never a
        // number the reader has to take on trust.
        "waitingForBudget": waiting_for_budget,
        "waitingForWorkers": waiting_for_workers,
        "assumes": assumes(app, rt, measured_cost, cost_per_item),
    })
}

/// The budget that admits `want` last, and when — the one worth naming, because
/// "87% of what" is the question anybody asks the moment they are told to wait.
///
/// `None` where nothing declared paces this lane at all: a class node exists to
/// isolate rather than to limit, and has no budget of its own.
fn binding_budget(
    rt: &Arc<TargetRuntime>,
    lane: &str,
    want: f64,
    now: i64,
) -> Option<(String, Schedule)> {
    let spec = &rt.spec;
    let lane_rt = rt.lanes.get(lane);
    let share = spec.lane_share(lane, lane_rt.and_then(|l| *l.measured_share.read()));
    let states = rt.last_state.read().clone();
    let partitions = spec.lane_partitions(lane);
    // The worst shard and the worst scope key: a budget is as spent as the
    // counter closest to refusing, and an item meets one counter rather than an
    // average of them.
    let spent_of = |b: &Budget| -> f64 {
        partitions
            .iter()
            .filter_map(|p| states.get(p))
            .map(|s| utilisation_max(b, s, now))
            .fold(0.0f64, f64::max)
            * b.cap
    };

    let mut bounds: Vec<(String, Schedule)> = spec
        .budgets
        .iter()
        // A kv budget is settled out of band against a lease this target holds
        // out of a pool shared with other targets, so the gate's own state says
        // nothing about it and reading a zero there would report room that
        // belongs to somebody else. Named in `assumes` instead.
        .filter(|b| b.store != Store::Kv)
        .map(|b| {
            let s = admits(
                b.alignment,
                // The lane's SLICE of the cap. Lanes divide a ceiling rather
                // than replicate it, so answering off the whole figure would
                // promise capacity this lane's own gate is going to refuse.
                b.cap * share,
                b.period_seconds,
                spent_of(b),
                want,
                now,
            );
            (b.id.clone(), s)
        })
        .collect();

    // The lane's own ceiling, which the gate applies as one more rolling budget
    // over a single lease — and which is a plain rate, so it is read as one.
    // Whole and not divided by the shard count: each shard runner enforces
    // `rate / shards` and they drain the lane's partitions at the same time, so
    // the lane's own throughput is the declared figure again. What its counter
    // holds inside the current lease is at most one lease of work, which is a
    // second against an answer measured in windows.
    if let Some(rate) = lane_rt.and_then(|l| *l.effective_cap.read()) {
        let lease_ms = spec.pacing.lease_seconds.max(1) * 1000;
        bounds.push((
            LANE_BUDGET.to_string(),
            Schedule {
                seconds: (rate > 0.0).then(|| want / rate),
                resets_at: (now / lease_ms + 1) * lease_ms,
            },
        ));
    }

    // The slowest binds, and "never" beats every number.
    bounds.into_iter().fold(None, |acc, x| {
        let key = |s: &Schedule| s.seconds.unwrap_or(f64::INFINITY);
        match acc {
            Some(a) if key(&a.1) >= key(&x.1) => Some(a),
            _ => Some(x),
        }
    })
}

/// What this lane was measured sustaining, in items per second.
///
/// Admissions, which is the drain rate of the admitted queue in anything but a
/// transient: work that is admitted and never executed grows that queue without
/// bound, so over any interval that matters the two rates are the same number.
async fn drain_rate(app: &Shared, rt: &Arc<TargetRuntime>, lane: &str, now: i64) -> f64 {
    match app.history.as_ref() {
        Some(h) => {
            h.rate_per_sec(&rt.spec.application, &rt.spec.name, lane, now)
                .await
        }
        None => app.meter.rate_per_sec(&rt.spec.key(), lane, now),
    }
}
