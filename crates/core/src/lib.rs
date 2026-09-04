//! The pure half of Gate: a declaration, its validation, and the plan it
//! compiles to.
//!
//! No I/O, no clock, no network — which is what lets the entire topology of a
//! graph be tested without a broker, and what makes `compile` the one place a
//! queue name, a consumer group or a kv key is minted.
//!
//! v1's `engine.rs` used to live here: two-pass admission, a two-bucket rolling
//! window, a calendar window, cell expiry, a synthetic lane budget. All of it is
//! gone, and none of it because it was wrong. **Postgres does the counting now**
//! — `kv.incr` with `max` is the admission decision, `applied` IS the verdict,
//! and the create-only TTL rotates the window — so an arithmetic Gate no longer
//! owns is an arithmetic Gate must not keep a second copy of.

pub mod cost;
pub mod doc;
pub mod ids;
pub mod migrate;
pub mod plan;
pub mod v1;
pub mod validate;

pub use cost::{
    cost_of, missing_scope, ok_payload_path, op_matches, op_of, resolve, scope_value, TooExpensive,
};
pub use doc::{
    default_application, ok_name, ok_target_name, Budget, Confidence, Cost, CostPath, Counters,
    Egress, EgressSpec, GraphDoc, Ingress, IngressSpec, Node, Path, PathElem, GATE_META,
    PAYLOAD_ROOT,
};
pub use ids::derive;
pub use plan::{
    breaker_key, budget_key, compile, compile_with, default_shares, default_sub_windows,
    interior_queue, namespace, owned_ingress_queue, shared_budget_key, stage_group, subdivide,
    CompiledBudget, Destination, NodePlan, Plan, PlanOpts, QueueKind, QueueSpec, Stage,
    ASSUMED_FACTOR, DEFAULT_BATCH, DEFAULT_INGRESS_PARTITIONS,
};
pub use validate::{
    needs_version_bump, validate, validate_with, warnings, warnings_with, ExternalFacts, Problem,
    QueueFacts,
};
