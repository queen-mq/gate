//! The admission decision.
//!
//! Two passes, and the split is the whole correctness argument: **evaluate
//! every matching budget first, apply only if all of them admit.** A one-pass
//! loop that spends as it goes leaves a partial charge behind when a later
//! budget refuses, and that charge is never refunded on the SDKs that do not
//! roll back a denied message's state.
//!
//! Everything here is a pure function of `(state, now_ms, item)`. No I/O, no
//! clock of its own, no interior counters. A fully denied cycle discards the
//! state it wrote, so anything kept outside `state` would diverge forever.

use serde_json::{json, Map, Value};

use crate::spec::{Alignment, Budget, Dim, TargetSpec};

/// One unit of outbound work, as the gate sees it.
#[derive(Debug, Clone, Default)]
pub struct Item {
    pub op: String,
    pub cost: f64,
    /// Values for the scope dimensions a budget may key on.
    pub scope: Vec<(Dim, String)>,
}

impl Item {
    pub fn scope_value(&self, dim: Dim) -> Option<&str> {
        self.scope
            .iter()
            .find(|(d, _)| *d == dim)
            .map(|(_, v)| v.as_str())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Decision {
    Admit,
    Deny(Denial),
}

impl Decision {
    pub fn is_admit(&self) -> bool {
        matches!(self, Decision::Admit)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Denial {
    /// The budget that refused. Named, because "87% of what" is the question an
    /// operator asks the moment they see a denial.
    pub budget_id: String,
    pub reason: Reason,
    /// Milliseconds until this budget could admit the item, on current
    /// arithmetic. Advisory: it assumes nobody else spends in the meantime.
    pub retry_after_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    /// No budget left in the window.
    Limit,
    /// A scope dimension the budget keys on is missing from the item. A
    /// rejected push, never a zero — a counter keyed on an absent value is a
    /// counter measuring the wrong thing.
    MissingScope(Dim),
    /// The item costs more than this budget's whole cap, so it can never be
    /// admitted. Validation should have caught it at PUT time.
    Unsatisfiable,
}

/// What one budget's counter looks like in the gate's state document.
///
/// `w` is the window index, `u` the spend inside it, `p` the spend in the
/// window before. Calendar alignment ignores `p`; rolling uses it to weight the
/// tail of the previous window. Three small numbers per counter, because the
/// state document is re-read in full on every cycle and every field is paid for
/// on each one.
/// The lane cap is applied as one more rolling budget, so it composes with the
/// declared ones instead of being a special case. Its cell is swept like any
/// other, which is why the name is a constant rather than a literal in two
/// places.
pub const LANE_BUDGET: &str = "@lane";

#[derive(Clone, Copy)]
struct Cell {

    w: f64,
    u: f64,
    p: f64,
}

fn read_cell(state: &Value, budget: &str, key: &str) -> Option<Cell> {
    let c = state.get("b")?.get(budget)?.get(key)?;
    Some(Cell {
        w: c.get("w")?.as_f64()?,
        u: c.get("u")?.as_f64()?,
        p: c.get("p").and_then(|v| v.as_f64()).unwrap_or(0.0),
    })
}

fn write_cell(state: &mut Value, budget: &str, key: &str, cell: Cell) {
    if !state.is_object() {
        *state = Value::Object(Map::new());
    }
    let root = state.as_object_mut().expect("object");
    let b = root
        .entry("b")
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .expect("object");
    let slot = b
        .entry(budget.to_string())
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .expect("object");
    slot.insert(
        key.to_string(),
        json!({ "w": cell.w, "u": cell.u, "p": cell.p }),
    );
}

/// Where the last cycle's clock is remembered, so the sweep below runs once per
/// cycle rather than once per message.
///
/// A plain name on purpose: the `__` prefix belongs to the stream runtime, and a
/// field this code invents does not go in somebody else's namespace.
const CYCLE_FIELD: &str = "t";

/// How long a counter cell survives after its window closes.
///
/// Nothing else prunes: the state document is rewritten whole every cycle and a
/// scope key written once lives forever, so a budget keyed on an entity grows the
/// document without bound — `maxKeys` is checked at declare time and never
/// again. A cell is readable only in its own window (both alignments) and in the
/// one after it (rolling carries the previous window's spend as a decaying tail),
/// so anything older than that cannot change a decision and is dropped.
fn expire(spec: &TargetSpec, state: &mut Value, now_ms: i64) {
    if !state.is_object() {
        *state = Value::Object(Map::new());
    }
    let root = state.as_object_mut().expect("object");
    root.insert(CYCLE_FIELD.to_string(), json!(now_ms));

    let lease_ms = spec.pacing.lease_seconds.max(1) * 1000;
    let period_of = |budget_id: &str| -> Option<i64> {
        if budget_id == LANE_BUDGET {
            return Some(lease_ms);
        }
        spec.budgets
            .iter()
            .find(|b| b.id == budget_id)
            .map(|b| (b.period_seconds.max(1)) * 1000)
    };

    let Some(budgets) = root.get_mut("b").and_then(|v| v.as_object_mut()) else {
        return;
    };
    budgets.retain(|budget_id, cells| {
        // A budget the spec no longer declares can never be read again — nothing
        // asks for a counter by a name that is not in the spec — so the whole
        // slot goes rather than one cell at a time.
        let Some(period_ms) = period_of(budget_id) else {
            return false;
        };
        let Some(map) = cells.as_object_mut() else {
            return false;
        };
        let current = now_ms / period_ms;
        map.retain(|_, c| {
            c.get("w")
                .and_then(|v| v.as_f64())
                .is_some_and(|w| (w as i64) >= current - 1)
        });
        !map.is_empty()
    });
}

/// The counter key inside a budget: `-` when the budget has no scope, otherwise
/// the scope values joined. Bounded by `maxKeys`, which validation enforces.
fn scope_key(budget: &Budget, item: &Item) -> Result<String, Dim> {
    if budget.scope.is_empty() {
        return Ok("-".to_string());
    }
    let mut parts = Vec::with_capacity(budget.scope.len());
    for dim in &budget.scope {
        match item.scope_value(*dim) {
            Some(v) => parts.push(v.to_string()),
            None => return Err(*dim),
        }
    }
    Ok(parts.join("\u{1f}"))
}

/// What applying this item to one budget would do. Computed in the first pass,
/// committed in the second, never both at once.
struct Pending {
    budget_id: String,
    key: String,
    cell: Cell,
}

/// Evaluate one budget without touching state.
fn evaluate(budget: &Budget, item: &Item, state: &Value, now_ms: i64) -> Result<Pending, Denial> {
    if item.cost > budget.cap {
        return Err(Denial {
            budget_id: budget.id.clone(),
            reason: Reason::Unsatisfiable,
            retry_after_ms: -1,
        });
    }

    let key = scope_key(budget, item).map_err(|dim| Denial {
        budget_id: budget.id.clone(),
        reason: Reason::MissingScope(dim),
        retry_after_ms: -1,
    })?;

    let period_ms = budget.period_seconds * 1000;
    let existing = read_cell(state, &budget.id, &key);

    match budget.alignment {
        Alignment::Calendar => {
            let window = (now_ms / period_ms) as f64;
            let used = match existing {
                Some(c) if c.w == window => c.u,
                // A rotated (or absent) window starts empty. This is what makes
                // a fixed window a single call rather than a read-then-write.
                _ => 0.0,
            };
            if used + item.cost > budget.cap {
                let next_edge = ((now_ms / period_ms) + 1) * period_ms;
                return Err(Denial {
                    budget_id: budget.id.clone(),
                    reason: Reason::Limit,
                    retry_after_ms: next_edge - now_ms,
                });
            }
            Ok(Pending {
                budget_id: budget.id.clone(),
                key,
                cell: Cell { w: window, u: used + item.cost, p: 0.0 },
            })
        }
        Alignment::Rolling => {
            // A two-bucket sliding-window counter, NOT a token bucket.
            //
            // The obvious implementation — a bucket of `cap` tokens refilling at
            // `cap/period` — is wrong for this product, and the property test
            // catches it: a full bucket admits `cap` instantly and then refills
            // another `cap` over the period, so a window of `period` can carry
            // twice the ceiling. Against "no more than 2000 from an IP in any
            // ten seconds", that is exactly the breach the limiter exists to
            // prevent, and the penalty is a fleet-wide block.
            //
            // So the previous window's spend is carried, weighted by how much of
            // the current window is still ahead. It assumes the previous window
            // was spent uniformly, which is the standard approximation; the
            // residual error is small and, unlike the token bucket's, bounded
            // well under a factor of two.
            let window = (now_ms / period_ms) as f64;
            let elapsed_frac = (now_ms % period_ms) as f64 / period_ms as f64;
            let (cur, prev) = match existing {
                Some(c) if c.w == window => (c.u, c.p),
                Some(c) if c.w == window - 1.0 => (0.0, c.u),
                _ => (0.0, 0.0),
            };
            let estimated = prev * (1.0 - elapsed_frac) + cur;
            if estimated + item.cost > budget.cap {
                // Time until the decaying tail frees enough room, capped at the
                // window edge where `prev` drops out entirely.
                let need = estimated + item.cost - budget.cap;
                let retry = if prev > 0.0 {
                    ((need / prev) * period_ms as f64).ceil() as i64
                } else {
                    period_ms - (now_ms % period_ms)
                };
                return Err(Denial {
                    budget_id: budget.id.clone(),
                    reason: Reason::Limit,
                    retry_after_ms: retry.clamp(1, period_ms),
                });
            }
            Ok(Pending {
                budget_id: budget.id.clone(),
                key,
                cell: Cell { w: window, u: cur + item.cost, p: prev },
            })
        }
    }
}

/// Decide whether one item may go out now, and charge every budget it consumes.
///
/// `lane_cap_per_sec` is the lane's own effective ceiling, injected by the
/// server because `ceiling-minus-measured` is a number the meter produces. It
/// is applied as one more rolling budget, so a lane cap composes with the
/// target's budgets instead of being a special case.
pub fn decide(
    spec: &TargetSpec,
    lane_cap_per_sec: Option<f64>,
    state: &mut Value,
    now_ms: i64,
    item: &Item,
) -> Decision {
    decide_with_share(spec, 1.0, lane_cap_per_sec, state, now_ms, item)
}

/// The real entry point. `share` is the fraction of every target budget this
/// lane may spend — see [`TargetSpec::lane_share`] for why it cannot be 1.0 on
/// every lane at once.
pub fn decide_with_share(
    spec: &TargetSpec,
    share: f64,
    lane_cap_per_sec: Option<f64>,
    state: &mut Value,
    now_ms: i64,
    item: &Item,
) -> Decision {
    let mut pending: Vec<Pending> = Vec::new();

    // Once per cycle, before anything is charged. The gate fn runs per message
    // but its clock is sampled once per cycle, so a changed clock IS a new cycle
    // — and a cycle that ends up denying everything discards this write along
    // with the rest, which costs one repeated sweep and nothing else.
    if state.get(CYCLE_FIELD).and_then(|v| v.as_i64()) != Some(now_ms) {
        expire(spec, state, now_ms);
    }

    // Pass one: evaluate everything, mutate nothing.

    for budget in spec.budgets_for(&item.op) {
        // A kv-backed budget is not this function's business: it crosses
        // partitions, so the server settles it out of band before we are called.
        if budget.store == crate::spec::Store::Kv {
            continue;
        }
        let scaled = scale(budget, share, item.cost);
        match evaluate(&scaled, item, state, now_ms) {
            Ok(p) => pending.push(p),
            Err(d) => return Decision::Deny(d),
        }
    }

    if let Some(rate) = lane_cap_per_sec {
        let synthetic = Budget {
            id: LANE_BUDGET.to_string(),

            cap: (rate * spec.pacing.lease_seconds as f64).max(item.cost),
            period_seconds: spec.pacing.lease_seconds.max(1),
            alignment: Alignment::Rolling,
            matcher: None,
            scope: vec![],
            max_keys: None,
            store: crate::spec::Store::Gate,
            confidence: crate::spec::Confidence::Inferred,
            source: None,
            as_of: None,
        };
        match evaluate(&synthetic, item, state, now_ms) {
            Ok(p) => pending.push(p),
            Err(d) => return Decision::Deny(d),
        }
    }

    // Pass two: everything admitted, so charge everything.
    for p in pending {
        write_cell(state, &p.budget_id, &p.key, p.cell);
    }
    Decision::Admit
}

/// A lane sees its slice of a budget. The cap never falls below the item's own
/// cost, because a share that rounds an item out of existence would block the
/// lane forever — the same failure `cost-fits` rejects at declare time.
fn scale(budget: &Budget, share: f64, cost: f64) -> Budget {
    if (share - 1.0).abs() < f64::EPSILON {
        return budget.clone();
    }
    let mut b = budget.clone();
    b.cap = (budget.cap * share).max(cost);
    b
}

/// The busiest counter this budget holds, across every scope key in the document.
///
/// A scoped budget has no `-` cell, so asking for one reports a per-listing limit
/// at 0% while it is refusing work. The worst key is the honest number: a budget
/// is as spent as the key closest to being refused.
pub fn utilisation_max(budget: &Budget, state: &Value, now_ms: i64) -> f64 {
    let Some(cells) = state
        .get("b")
        .and_then(|b| b.get(&budget.id))
        .and_then(|v| v.as_object())
    else {
        return 0.0;
    };
    cells
        .keys()
        .map(|k| utilisation(budget, state, k, now_ms))
        .fold(0.0f64, f64::max)
}

/// How many scope keys this budget's counter holds right now — the number
/// `maxKeys` was a promise about, and the one the expiry sweep bounds.
pub fn key_count(budget: &Budget, state: &Value) -> usize {
    state
        .get("b")
        .and_then(|b| b.get(&budget.id))
        .and_then(|v| v.as_object())
        .map(|m| m.len())
        .unwrap_or(0)
}

/// Live utilisation of one budget's counter, for the console and the meter.
/// Returns spend as a fraction of cap, which is the same number in both
/// alignments even though the stored field means different things.
pub fn utilisation(budget: &Budget, state: &Value, key: &str, now_ms: i64) -> f64 {
    let Some(c) = read_cell(state, &budget.id, key) else {
        return 0.0;
    };
    let period_ms = budget.period_seconds * 1000;
    match budget.alignment {
        Alignment::Calendar => {
            if c.w == (now_ms / period_ms) as f64 {
                c.u / budget.cap
            } else {
                0.0
            }
        }
        Alignment::Rolling => {
            let window = (now_ms / period_ms) as f64;
            let elapsed_frac = (now_ms % period_ms) as f64 / period_ms as f64;
            let (cur, prev) = if c.w == window {
                (c.u, c.p)
            } else if c.w == window - 1.0 {
                (0.0, c.u)
            } else {
                (0.0, 0.0)
            };
            (prev * (1.0 - elapsed_frac) + cur) / budget.cap
        }
    }
}
