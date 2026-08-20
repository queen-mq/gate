//! The declared targets and their live state.
//!
//! One lock over a small map. Everything expensive — the gate state, the work —
//! lives in Postgres; this holds only what a process needs to route a request
//! and what the console needs to answer "how close are we".

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use gate_core::{GraphSpec, TargetSpec};
use serde_json::Value;


/// Counters a lane accumulates between meter windows. Not the budget — the
/// budget is in the gate's own state, in Postgres, where the single writer is.
#[derive(Debug, Default, Clone)]
pub struct LaneStats {
    pub admitted: u64,
    pub denied: u64,
    pub calls: u64,
    pub throttled: u64,
    /// Items a breach rule sent back to an entry node to be paced again.
    pub retried: u64,
    /// Items that had used up `maxAttempts` and were settled instead.
    pub exhausted: u64,
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
    /// `application/graph` when this target is a graph node rather than a
    /// standalone target.
    ///
    /// Two things hang off it. A node is never reaped by a target sync — the
    /// caller's target list does not name it, and reaping it would tear down half
    /// a graph. And a node's spec is not in the target store: the graph document
    /// is the authority, so the reconcile loop restores the graph and the graph
    /// provisions the node.
    pub graph: Option<String>,
    /// Whether this spec is known to be in the durable store.
    ///
    /// The reconcile loop removes a target the store no longer holds, which is
    /// how a delete on one replica reaches the others. A declare whose persist
    /// failed would look exactly like that, so it is marked instead and the
    /// reconcile retries the save rather than tearing down a live target.
    pub persisted: AtomicBool,
    /// Set the moment this runtime's runners are cancelled.
    ///
    /// A target that is registered but stopped is the one unrecoverable state in this
    /// server: it accepts pushes and admits nothing, for ever, and every route that
    /// gates on "is it in the registry" would serve it. Provisioning is stop-then-start,
    /// so the state exists for as long as a swap takes and outlives it whenever a
    /// restore fails — this is what makes it visible instead of implied.
    pub stopped: AtomicBool,
    pub lanes: HashMap<String, Arc<LaneRuntime>>,


    /// Last state document seen by the gate, per lane. A copy, for reading
    /// utilisation without a round trip; the authority is always Postgres.
    pub last_state: RwLock<HashMap<String, Value>>,
    pub last_breach: RwLock<Option<(String, i64)>>,
    /// Kept alive for the process's lifetime: a dropped handle does not stop a
    /// runner, and stopping is what the cancel token is for.
    pub handles: RwLock<Vec<queen_mq::streams::StreamHandle>>,
    pub meter_cancel: RwLock<Option<queen_mq::Cancel>>,
    /// The meter loop's task, so stop() can wait for it the way it waits for
    /// the runners. The meter is the one caller of Pool::top_up: released
    /// pools must not be observable by a still-parked meter, or the top-up
    /// re-reserves a chunk of the shared window that nobody will ever spend.
    pub meter_task: RwLock<Option<tokio::task::JoinHandle<()>>>,
    /// One local lease per cross-target budget. Shared by every lane of this
    /// target, because the budget is shared by every lane of this target.
    pub pools: Vec<Arc<crate::shared::Pool>>,
}

/// One merge relay: the tasks that move work INTO one node, from every node that
/// has an edge to it.
///
/// One per destination rather than one per edge, because priority is only real
/// where the streams meet. Two independent relays into one queue would each
/// forward as fast as they could and the destination's FIFO would decide the
/// order by arrival — which is exactly the thing priority is supposed to
/// override.
///
/// One RUNTIME per destination, not one task: inside it a leg is drained by one
/// runner per partition of its source's admitted queue. The legs are still taken
/// in strict priority order, one at a time — see `edge.rs` for why the
/// parallelism goes inside a leg and never across two.
pub struct RelayRuntime {
    /// The node work is relayed into, as declared (not the target name).
    pub dest: String,
    /// Lowest priority first — the order it drains in.
    pub sources: Vec<RelaySource>,
    /// How many items the destination's push queue may hold before this relay
    /// stops forwarding. The bottleneck queue stays shallow, so priority at the
    /// entrance is priority in fact.
    pub window: u64,
    pub forwarded: AtomicU64,
    /// Transactions this relay committed. Divided into `forwarded` it is the
    /// average batch a relay transaction carried, which is the number that
    /// explains an edge's throughput: the destination's push partition takes one
    /// row lock per transaction whoever holds it, so items-per-transaction is the
    /// multiplier on everything the runners do in parallel.
    pub commits: AtomicU64,
    /// Items a relay could not route: a destination sharded by a dimension the
    /// item does not carry. Nacked with a reason rather than dropped.
    pub unroutable: AtomicU64,
    /// Batches this relay found already partly forwarded, and settled one item at a
    /// time instead. Should be zero; it is here because "should be" is not a
    /// measurement, and because a recovery path nobody can see is a recovery path
    /// nobody knows ran.
    pub duplicates: AtomicU64,

    pub cancel: queen_mq::Cancel,
}

/// One in-edge of a merge relay, and how wide it runs.
pub struct RelaySource {
    pub node: String,
    pub priority: u32,
    /// How many runners drain this leg: one per partition of the source's
    /// admitted queue, per lane of it. This is the number the throughput of the
    /// edge scales with, and it is derived — a caller raises it by declaring more
    /// `admitted.partitions` on the source, never by asking for more runners on
    /// the partitions it has.
    pub runners: u32,
}

impl RelayRuntime {
    pub fn forwarded(&self) -> u64 {
        self.forwarded.load(Ordering::Relaxed)
    }
    pub fn commits(&self) -> u64 {
        self.commits.load(Ordering::Relaxed)
    }
    pub fn unroutable(&self) -> u64 {
        self.unroutable.load(Ordering::Relaxed)
    }
    pub fn duplicates(&self) -> u64 {
        self.duplicates.load(Ordering::Relaxed)
    }

}

/// A declared graph and the relays that make its edges real.
///
/// The nodes are NOT here: they are targets in the target map, because a node is
/// a target and every route, queue and gate runner they need already exists. This
/// holds the document and the tasks that no target has.
pub struct GraphRuntime {
    pub spec: GraphSpec,
    pub relays: Vec<Arc<RelayRuntime>>,
    /// `application/name` of every node target, in declared order.
    pub node_keys: Vec<String>,
    pub persisted: AtomicBool,
}

impl TargetRuntime {
    /// Whether this runtime is still draining its queues. A caller must never be
    /// pointed at one that is not.
    pub fn is_running(&self) -> bool {
        !self.stopped.load(Ordering::Relaxed)
    }
}

#[derive(Default)]
pub struct Registry {

    targets: RwLock<HashMap<String, Arc<TargetRuntime>>>,
    graphs: RwLock<HashMap<String, Arc<GraphRuntime>>>,
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

    /// Standalone targets only — what a target sync owns and may reap. A graph
    /// node is owned by its graph document and reaping it would tear down half a
    /// topology.
    pub fn standalone_of_app(&self, app: &str) -> Vec<Arc<TargetRuntime>> {
        self.of_app(app)
            .into_iter()
            .filter(|rt| rt.graph.is_none())
            .collect()
    }

    // ------------------------------------------------------------------ graphs

    pub fn graph(&self, app: &str, name: &str) -> Option<Arc<GraphRuntime>> {
        self.graphs.read().get(&format!("{app}/{name}")).cloned()
    }

    pub fn put_graph(&self, g: Arc<GraphRuntime>) -> Option<Arc<GraphRuntime>> {
        self.graphs.write().insert(g.spec.key(), g)
    }

    pub fn remove_graph(&self, app: &str, name: &str) -> Option<Arc<GraphRuntime>> {
        self.graphs.write().remove(&format!("{app}/{name}"))
    }

    /// By `application/name`, which is what a target runtime carries as its owner.
    pub fn graph_by_key(&self, key: &str) -> Option<Arc<GraphRuntime>> {
        self.graphs.read().get(key).cloned()
    }

    pub fn graphs(&self) -> Vec<Arc<GraphRuntime>> {

        let mut v: Vec<Arc<GraphRuntime>> = self.graphs.read().values().cloned().collect();
        v.sort_by_key(|g| g.spec.key());
        v
    }

    pub fn graphs_of_app(&self, app: &str) -> Vec<Arc<GraphRuntime>> {
        self.graphs()
            .into_iter()
            .filter(|g| g.spec.application == app)
            .collect()
    }

    /// The graph that owns a target name in this application, if one does.
    ///
    /// G10: a node is the target `{graph}.{node}`, so a standalone target of that
    /// name would be a second owner of one queue family. Asked at both declares.
    pub fn graph_owning_target(&self, app: &str, name: &str) -> Option<Arc<GraphRuntime>> {
        self.graphs_of_app(app).into_iter().find(|g| {
            g.spec
                .nodes
                .keys()
                .any(|n| g.spec.node_target_name(n) == name)
        })
    }
}

