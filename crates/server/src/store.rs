//! Where the declared targets live between restarts.
//!
//! `gate` has no database of its own, and that is a deliberate position:
//! the work is in queen's log, the budget counters are in `queen_streams.state`
//! keyed by partition, the cross-target ceilings are in `queen.kv`. Everything
//! that matters is already durable in the Postgres queen owns, and adding a
//! second data system to hold one more thing would be a second data system to
//! run, size, back up and lose.
//!
//! But the target SPECS were the exception, and not on purpose: they lived in a
//! HashMap in the process. A restart dropped every one of them while their
//! queues stayed in the broker with nobody draining — 112 orphans after an
//! afternoon of testing. So they go where they always belonged, which is the
//! store queen already gives us for exactly this: a small durable value under a
//! name we choose.
//!
//! `forever` rather than a TTL, and it is the one place in this codebase that
//! asks for it: a configuration that expires is a configuration that vanishes
//! at 3am for no reason anybody can reconstruct.

use queen_mq::{Expiry, Queen, Result};
use gate_core::TargetSpec;

const NS: &str = "gate";
const PREFIX: &str = "spec:";

fn key(app: &str, name: &str) -> String {
    format!("{PREFIX}{app}:{name}")
}

pub async fn save(queen: &Queen, spec: &TargetSpec) -> Result<()> {
    let value = serde_json::to_value(spec).map_err(|e| queen_mq::Error::Decode(e.to_string()))?;
    queen
        .kv()
        .put(NS, &key(&spec.application, &spec.name), value, Expiry::forever())
        .send()
        .await?;
    Ok(())
}

pub async fn forget(queen: &Queen, app: &str, name: &str) -> Result<()> {
    queen.kv().delete(NS, &key(app, name)).send().await?;
    Ok(())
}

/// Every spec this cell has been told about.
///
/// A prefix is mandatory — a namespace is not a table to enumerate — which is
/// why the specs are keyed under one rather than at the root.
pub async fn load_all(queen: &Queen) -> Vec<TargetSpec> {
    let res = match queen.kv().get_prefix(NS, PREFIX).limit(1000).send().await {
        Ok(r) => r,
        Err(_) => return vec![],
    };
    res.rows
        .unwrap_or_default()
        .iter()
        .filter_map(|row| row.value.clone())
        .filter_map(|v| serde_json::from_value::<TargetSpec>(v).ok())
        .collect()
}
