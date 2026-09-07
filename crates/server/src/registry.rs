//! The declared graphs and their live state.
//!
//! One lock over a small map. Everything expensive — the counters, the work —
//! lives in Postgres; this holds only what a process needs to route a request
//! and what the console needs to answer "how close are we".
//!
//! v1's `TargetRuntime`, `LaneRuntime`, `RelayRuntime` and `RelaySource` are all
//! one type now, because a node is not a target, a lane is not a partition and
//! an edge is not a relay: there is one document, and the only live object is a
//! stage.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use gate_core::plan::Plan;
use gate_core::GraphDoc;

use crate::relay::StageRuntime;

pub struct GraphRuntime {
    pub doc: GraphDoc,
    pub plan: Plan,
    pub stages: Vec<Arc<StageRuntime>>,
    /// Kept for the whole runtime's life: a dropped handle does not stop a task,
    /// and stopping is what the cancel token is for.
    pub handles: RwLock<Vec<tokio::task::JoinHandle<()>>>,
    /// Whether this document is known to be in the durable store.
    ///
    /// The reconcile loop removes a graph the store no longer holds, which is
    /// how a delete on one replica reaches the others. A declare whose persist
    /// FAILED would look exactly like that, so it is marked instead and the
    /// reconcile retries the save rather than tearing down a live graph.
    pub persisted: AtomicBool,
    /// Set the moment this runtime's stages are cancelled.
    ///
    /// A graph that is registered but stopped is the one unrecoverable state in
    /// this server: it accepts pushes and admits nothing, for ever, and every
    /// route that gates on "is it in the registry" would serve it. Provisioning
    /// is stop-then-start, so the state exists for as long as a swap takes and
    /// outlives it whenever a restore fails — this is what makes it visible
    /// instead of implied.
    pub stopped: Arc<AtomicBool>,
    /// One token per graph, cloned into every stage.
    pub cancel: queen_mq::Cancel,
}

impl GraphRuntime {
    pub fn key(&self) -> String {
        self.doc.key()
    }

    /// Whether this runtime is still draining its queues. A caller must never be
    /// pointed at one that is not.
    pub fn is_running(&self) -> bool {
        !self.stopped.load(Ordering::Relaxed)
    }

    pub fn stage(&self, path: &str, node: &str) -> Option<&Arc<StageRuntime>> {
        self.stages
            .iter()
            .find(|s| s.stage.path == path && s.stage.node == node)
    }

    pub fn stages_of_node<'a>(
        &'a self,
        node: &'a str,
    ) -> impl Iterator<Item = &'a Arc<StageRuntime>> {
        self.stages.iter().filter(move |s| s.stage.node == node)
    }
}

#[derive(Default)]
pub struct Registry {
    graphs: RwLock<HashMap<String, Arc<GraphRuntime>>>,
}

impl Registry {
    /// Keyed on `application/graph`: two teams may both own something they call
    /// `airbnb`, and without the pair the second declare would quietly take over
    /// the first's queues, counters and stored document.
    pub fn get(&self, app: &str, graph: &str) -> Option<Arc<GraphRuntime>> {
        self.graphs.read().get(&format!("{app}/{graph}")).cloned()
    }

    pub fn by_key(&self, key: &str) -> Option<Arc<GraphRuntime>> {
        self.graphs.read().get(key).cloned()
    }

    pub fn put(&self, rt: Arc<GraphRuntime>) -> Option<Arc<GraphRuntime>> {
        self.graphs.write().insert(rt.key(), rt)
    }

    pub fn remove(&self, app: &str, graph: &str) -> Option<Arc<GraphRuntime>> {
        self.graphs.write().remove(&format!("{app}/{graph}"))
    }

    pub fn all(&self) -> Vec<Arc<GraphRuntime>> {
        let mut v: Vec<Arc<GraphRuntime>> = self.graphs.read().values().cloned().collect();
        v.sort_by_key(|g| g.key());
        v
    }

    /// Only this application's graphs. A sync reaps within its own envelope and
    /// never outside it — the flat version of that rule let two teams sharing a
    /// deployment delete each other's configuration, including from the store.
    pub fn of_app(&self, app: &str) -> Vec<Arc<GraphRuntime>> {
        self.all()
            .into_iter()
            .filter(|g| g.doc.application == app)
            .collect()
    }

    pub fn applications(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .graphs
            .read()
            .values()
            .map(|g| g.doc.application.clone())
            .collect();
        v.sort();
        v.dedup();
        v
    }

    /// Resolve a bare graph name across applications.
    ///
    /// The one case the server refuses to guess: two applications with a graph
    /// of one name, asked for without an application. Picking either would run
    /// somebody else's declaration.
    pub fn resolve(&self, name: &str) -> Resolved {
        let hits: Vec<Arc<GraphRuntime>> = self
            .all()
            .into_iter()
            .filter(|g| g.doc.graph == name)
            .collect();
        match hits.len() {
            0 => Resolved::None,
            1 => Resolved::One(hits.into_iter().next().unwrap()),
            _ => Resolved::Ambiguous(hits.iter().map(|g| g.doc.application.clone()).collect()),
        }
    }

    /// Ingress queues already claimed, excluding one graph — what the
    /// `ingress-owner` rule is asked against on a redeclare.
    pub fn ingress_owners(&self, except: &str) -> Vec<(String, String, String)> {
        let mut out = Vec::new();
        for g in self.all() {
            if g.key() == except {
                continue;
            }
            for (name, np) in &g.plan.nodes {
                if let Some(q) = &np.ingress_queue {
                    out.push((q.clone(), g.key(), name.clone()));
                }
            }
        }
        out
    }

    pub fn egress_owners(&self, except: &str) -> Vec<(String, String)> {
        let mut out = Vec::new();
        for g in self.all() {
            if g.key() == except {
                continue;
            }
            for np in g.plan.nodes.values() {
                if let Some(q) = &np.egress_queue {
                    out.push((q.clone(), g.key()));
                }
            }
        }
        out
    }
}

pub enum Resolved {
    None,
    One(Arc<GraphRuntime>),
    Ambiguous(Vec<String>),
}
