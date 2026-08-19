//! The admission engine for an egress rate limiter.
//!
//! Pure: no I/O, no clock, no network. Everything is a function of the state
//! document, the instant, and the item — which is what lets the same code run
//! inside a streaming gate whose denied cycles discard their writes.

pub mod engine;
pub mod graph;
pub mod spec;
pub mod validate;

pub use engine::{
    decide, decide_with_share, key_count, utilisation, utilisation_max, Decision, Denial, Item,
    Reason, LANE_BUDGET,
};
pub use graph::{
    graph_warnings, needs_graph_version_bump, validate_graph, BreachRule, BreachWhen, Edge,
    GraphSpec, Node, GATE_META, MAX_HOPS, ORIGIN_ENTRY, THROTTLED,
};
pub use spec::{
    default_application, shard_index, Admitted, Alignment, Budget, CapPolicy, Confidence, Cost, Dim,
    Lane, Match, Pacing, PartitionBy, Store, TargetSpec, DEFAULT_CONCURRENCY,
};
pub use validate::{
    effective_cap, needs_version_bump, ok_name, ok_target_name, pacing_warnings, validate,
    validate_with, warnings, Problem, ValidateOpts, GATE_MAX_KEYS, GATE_MAX_SHARDS,
};

