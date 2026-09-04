//! Reading a payload: what an item costs, which counter it belongs to, and
//! whether a budget takes it at all.
//!
//! The one piece of v1's `engine.rs` worth keeping is here — the `whenOp` glob
//! matcher, ported verbatim from `engine::budgets_for`. Everything else in that
//! file was window arithmetic Gate no longer owns, because Postgres owns it.

use serde_json::Value;

use crate::doc::{Cost, PAYLOAD_ROOT};
use crate::plan::CompiledBudget;

/// Walk a dotted payload path. The first segment must be `payload`, which names
/// the message's own `data`; `payload.a.b` is `data["a"]["b"]`.
///
/// `None` for an absent path, a non-object on the way down, or a first segment
/// that is not `payload`. The caller decides what an absence means — a cost
/// falls back to its default, a `scopeBy` refuses the push — and those are
/// different answers to the same silence, which is why this does not choose.
pub fn resolve<'a>(data: &'a Value, path: &str) -> Option<&'a Value> {
    let mut segs = path.split('.');
    if segs.next()? != PAYLOAD_ROOT {
        return None;
    }
    let mut cur = data;
    for s in segs {
        cur = cur.get(s)?;
    }
    Some(cur)
}

/// Whether a string is a usable payload path: `payload` plus at least one
/// segment, each of them non-empty.
pub fn ok_payload_path(path: &str) -> bool {
    let mut segs = path.split('.');
    if segs.next() != Some(PAYLOAD_ROOT) {
        return false;
    }
    let rest: Vec<&str> = segs.collect();
    !rest.is_empty() && rest.iter().all(|s| !s.is_empty())
}

/// The scope value a budget keys on, as it reaches the kv key.
///
/// A number is rendered rather than refused: `listingId: 91372` and
/// `listingId: "91372"` are one listing to everybody except a strict type
/// check, and a limiter that keeps two counters for them enforces the limit
/// twice.
pub fn scope_value(data: &Value, path: &str) -> Option<String> {
    match resolve(data, path)? {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// The first applicable scoped budget whose key cannot be resolved.
///
/// Applicability comes first: a `photo.delete` per-listing budget has no reason
/// to require `listingId` from a `photo.upload`. Both the HTTP door and the
/// relay use this one answer so direct queue ingress cannot enforce a different
/// contract from HTTP ingress.
pub fn missing_scope<'a>(
    budgets: &'a [CompiledBudget],
    data: &Value,
) -> Option<(&'a str, &'a str)> {
    let op = op_of(data);
    budgets.iter().find_map(|budget| {
        if budget
            .when_op
            .as_ref()
            .is_some_and(|patterns| !op_matches(patterns, op))
        {
            return None;
        }
        let path = budget.scope_by.as_deref()?;
        scope_value(data, path)
            .is_none()
            .then_some((budget.id.as_str(), path))
    })
}

/// What this item costs, or the reason it can never be admitted.
///
/// Integers, because `kv.incr`'s delta is `i64` on this wire. A resolved cost
/// that is absent, non-numeric, non-integral or below 1 falls back to the
/// declared default — the same tolerance v1 had. A cost ABOVE `max` is not
/// tolerated: it is refused at the door with a 422, and if it somehow arrives on
/// a user-owned ingress queue it is dead-lettered with the reason. An item
/// costing more than a cap can never be admitted and would otherwise park the
/// head of its partition for ever, never reaching a DLQ, because a lease that
/// expires charges no retry budget.
pub fn cost_of(cost: &Cost, data: &Value) -> Result<i64, TooExpensive> {
    let (resolved, max) = match cost {
        Cost::Fixed(n) => (*n, *n),
        Cost::Path(c) => {
            let max = c.max.unwrap_or(c.default);
            let v = resolve(data, &c.path)
                .and_then(integral)
                .filter(|n| *n >= 1)
                .unwrap_or(c.default);
            (v, max)
        }
    };
    if resolved > max {
        return Err(TooExpensive {
            cost: resolved,
            max,
        });
    }
    Ok(resolved.max(1))
}

/// An item that declares a weight above the ceiling its node accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TooExpensive {
    pub cost: i64,
    pub max: i64,
}

impl std::fmt::Display for TooExpensive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "this item declares a cost of {} and the node admits at most {}: an item that cannot \
             fit a window can never be admitted, and it would park the head of its partition for \
             ever without ever reaching a DLQ",
            self.cost, self.max
        )
    }
}

/// A JSON number that is a whole number. `3.0` is three; `3.5` is not a cost.
fn integral(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => match n.as_i64() {
            Some(i) => Some(i),
            None => n.as_f64().filter(|f| f.fract() == 0.0).map(|f| f as i64),
        },
        _ => None,
    }
}

/// Suffix glob on dot-separated segments: `listing.*` takes `listing.create`.
///
/// No regex — a pattern language in a config file is a failure surface we do not
/// pay for. Ported verbatim from v1's `Match::matches`, which is the one part of
/// the old engine this rewrite kept.
pub fn op_matches(patterns: &[String], op: &str) -> bool {
    patterns.iter().any(|p| match p.strip_suffix(".*") {
        Some(prefix) => op.strip_prefix(prefix).is_some_and(|r| r.starts_with('.')),
        None => p == op || p == "*",
    })
}

/// The `op` a `whenOp` selects on: `payload.op`, as a string. An item with no
/// `op` matches nothing but a bare `*`, which is what "absence takes everything"
/// means at the budget level (a budget with no `whenOp` never asks).
pub fn op_of(data: &Value) -> &str {
    data.get("op").and_then(|v| v.as_str()).unwrap_or("")
}
