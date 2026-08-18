//! The admission engine for an egress rate limiter.
//!
//! Pure: no I/O, no clock, no network. Everything is a function of the state
//! document, the instant, and the item — which is what lets the same code run
//! inside a streaming gate whose denied cycles discard their writes.

pub mod engine;
pub mod spec;
pub mod validate;

pub use engine::{decide, decide_with_share, utilisation, Decision, Denial, Item, Reason};
pub use spec::{
    default_application, Admitted, Alignment, Budget, CapPolicy, Confidence, Cost, Dim, Lane, Match, Pacing,
    PartitionBy, Store, TargetSpec,
};
pub use validate::{
    effective_cap, needs_version_bump, pacing_warnings, validate, warnings, Problem,
};
