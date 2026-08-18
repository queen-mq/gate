//! The declared targets and their live state.
//!
//! One lock over a small map. Everything expensive — the gate state, the work —
//! lives in Postgres; this holds only what a process needs to route a request
//! and what the console needs to answer "how close are we".

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use gate_core::TargetSpec;
use serde_json::Value;

/// Counters a lane accumulates between meter windows. Not the budget — the
/// budget is in the gate's own state, in Postgres, where the single writer is.
#[derive(Debug, Default, Clone)]
pub struct LaneStats {
    pub admitted: u64,
    pub denied: u64,
    pub calls: u64,
    pub throttled: u64,
    pub last_denial_budget: Option<String>,
}

pub struct LaneRuntime {
    pub name: String,
    /// The lane's own ceiling in cost units per second, or `None` for
    /// `ceiling`. The meter rewrites this; the gate reads it every cycle, which
    /// is why a cap retune needs no restart.
    pub effective_cap: RwLock<Option<f64>>,
    /// What the meter last observed this lane's siblings actually spending, as
    /// a fraction of the target ceiling. `None` until a meter has run, which is
    /// why `ceiling-minus-measured` degrades to its declared floor.
    pub measured_share: RwLock<Option<f64>>,
    pub stats: RwLock<LaneStats>,
    pub cancel: queen_mq::Cancel,
}

pub struct TargetRuntime {
    pub spec: TargetSpec,
    pub lanes: HashMap<String, Arc<LaneRuntime>>,
    /// Last state document seen by the gate, per lane. A copy, for reading
    /// utilisation without a round trip; the authority is always Postgres.
    pub last_state: RwLock<HashMap<String, Value>>,
    pub last_breach: RwLock<Option<(String, i64)>>,
    /// Kept alive for the process's lifetime: a dropped handle does not stop a
    /// runner, and stopping is what the cancel token is for.
    pub handles: RwLock<Vec<queen_mq::streams::StreamHandle>>,
    pub meter_cancel: RwLock<Option<queen_mq::Cancel>>,
    /// One local lease per cross-target budget. Shared by every lane of this
    /// target, because the budget is shared by every lane of this target.
    pub pools: Vec<Arc<crate::shared::Pool>>,
}

#[derive(Default)]
pub struct Registry {
    targets: RwLock<HashMap<String, Arc<TargetRuntime>>>,
}

impl Registry {
    /// Keyed on `application/name`: two teams may both own something they call
    /// `airbnb`, and without the pair the second declare would quietly take over
    /// the first's queues, gate state and stored spec.
    pub fn get(&self, app: &str, name: &str) -> Option<Arc<TargetRuntime>> {
        self.targets.read().get(&format!("{app}/{name}")).cloned()
    }

    pub fn put(&self, rt: Arc<TargetRuntime>) -> Option<Arc<TargetRuntime>> {
        self.targets.write().insert(rt.spec.key(), rt)
    }

    pub fn remove(&self, app: &str, name: &str) -> Option<Arc<TargetRuntime>> {
        self.targets.write().remove(&format!("{app}/{name}"))
    }

    /// Only this application's targets. A sync reaps within its own envelope
    /// and никогда outside it.
    pub fn of_app(&self, app: &str) -> Vec<Arc<TargetRuntime>> {
        self.targets
            .read()
            .values()
            .filter(|rt| rt.spec.application == app)
            .cloned()
            .collect()
    }

    pub fn applications(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .targets
            .read()
            .values()
            .map(|rt| rt.spec.application.clone())
            .collect();
        v.sort();
        v.dedup();
        v
    }

    pub fn all(&self) -> Vec<Arc<TargetRuntime>> {
        self.targets.read().values().cloned().collect()
    }
}
