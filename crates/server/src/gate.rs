//! One streaming gate per lane, pinned to that lane's partition.
//!
//! The whole limiter is this function plus `gate_core::decide`. Everything the
//! design argues about — nested windows, weighted cost, deferral, fleet
//! correctness — is either in the pure engine or is a property the broker
//! already gives us here:
//!
//! * the exclusive partition lease makes the counter single-writer, so nested
//!   windows are evaluated in memory and need no distributed atomicity;
//! * a denial stops the batch, acks the allowed prefix and keeps the lease, so
//!   deferral costs nothing and cannot reorder a lane;
//! * the lease is the pacing quantum.

use std::sync::Arc;

use queen_mq::streams::{RunOptions, Stream};
use queen_mq::{Queen, Result};
use gate_core::{Decision, Dim, Item, TargetSpec};

use crate::meter::Meter;
use crate::registry::{LaneRuntime, TargetRuntime};

/// Read the scope dimensions a budget may key on off the pushed payload.
fn scope_of(data: &serde_json::Value) -> Vec<(Dim, String)> {
    const DIMS: [(Dim, &str); 5] = [
        (Dim::Host, "host"),
        (Dim::Entity, "entity"),
        (Dim::Account, "account"),
        (Dim::Connection, "connection"),
        (Dim::Tenant, "tenant"),
    ];
    DIMS.iter()
        .filter_map(|(d, k)| data.get(*k)?.as_str().map(|v| (*d, v.to_string())))
        .collect()
}

pub async fn spawn(
    queen: &Queen,
    meter: Arc<Meter>,
    target: Arc<TargetRuntime>,
    lane: Arc<LaneRuntime>,
) -> Result<()> {
    let spec: TargetSpec = target.spec.clone();
    let lane_name = lane.name.clone();
    let cost_field = spec.cost.field.clone();
    let cost_default = spec.cost.default;

    let admitted = spec.admitted_queue(&lane_name);
    let partition_by = spec.admitted.partition_by;
    let partitions = spec.admitted.partitions as u64;

    let gate_spec = spec.clone();
    let gate_lane = lane.clone();
    let gate_target = target.clone();
    let handle_target = target.clone();
    let gate_lane_name = lane_name.clone();
    let gate_meter = meter.clone();
    let gate_pools = target.pools.clone();
    let gate_target_name = spec.key();

    let source = queen
        .queue(spec.push_queue())
        // Pinned: this runner sees one lane and nothing else, so its budget,
        // its batch, its deny parking and its lease are all its own.
        .partition(lane_name.clone())
        .lease_seconds(spec.pacing.lease_seconds as i32);

    let stream = Stream::from(source)
        .gate(move |rec, ctx| {
            let op = rec.text("op").unwrap_or("").to_string();
            let cost = rec.number(&cost_field).unwrap_or(cost_default);
            let item = Item { op, cost, scope: scope_of(&rec.data) };
            let cap = *gate_lane.effective_cap.read();

            // The lane's slice of every target budget. Each lane is its own
            // partition with its own copy of the counters, so a ceiling handed
            // to every lane is a ceiling enforced N times.
            let share = gate_spec.lane_share(&gate_lane_name, *gate_lane.measured_share.read());
            // Cross-target budgets first, and from the local lease: they cross
            // partitions, so no gate can see them, and this one cannot await.
            for pool in &gate_pools {
                if !pool.try_spend(item.cost.ceil() as i64, ctx.stream_time_ms) {
                    {
                        let mut s = gate_lane.stats.write();
                        s.denied += 1;
                        s.last_denial_budget = Some(pool.budget.id.clone());
                    }
                    gate_meter.record_denial(
                        &gate_target_name,
                        &gate_lane_name,
                        rec.text("op").unwrap_or(""),
                        &pool.budget.id,
                        ctx.stream_time_ms,
                    );
                    return false;
                }
            }
            match gate_core::decide_with_share(
                &gate_spec,
                share,
                cap,
                ctx.state,
                ctx.stream_time_ms,
                &item,
            ) {
                Decision::Admit => {
                    gate_lane.stats.write().admitted += 1;
                    // Mirror the counters out so the console can read
                    // utilisation without a round trip to Postgres. The
                    // authority is always the gate's own state; this is a copy,
                    // and a stale one between cycles is fine for a gauge.
                    *gate_target.last_state.write().entry(gate_lane_name.clone()).or_default() =
                        ctx.state.clone();
                    true
                }
                Decision::Deny(d) => {
                    {
                        let mut s = gate_lane.stats.write();
                        s.denied += 1;
                        s.last_denial_budget = Some(d.budget_id.clone());
                    }
                    gate_meter.record_denial(
                        &gate_target_name,
                        &gate_lane_name,
                        rec.text("op").unwrap_or(""),
                        &d.budget_id,
                        ctx.stream_time_ms,
                    );
                    false
                }
            }
        })
        .to_partitioned(queen.queue(admitted), move |v| match partition_by {
            gate_core::PartitionBy::None => "default".to_string(),
            gate_core::PartitionBy::Entity => bucket(v.get("entity"), partitions),
            gate_core::PartitionBy::Connection => bucket(v.get("connection"), partitions),
        });

    let mut opts = RunOptions::new(spec.query_id(&lane_name));
    opts.batch_size = spec.pacing.batch as i32;
    // One partition each, because the source is pinned.
    opts.max_partitions = 1;
    opts.max_wait = std::time::Duration::from_millis(250);
    opts.cancel = Some(lane.cancel.clone());
    opts.reset = true;

    // The handle owns the loop task; dropping it would not stop the runner, and
    // stopping is the cancel token's job (supervisor::stop). Parked here so the
    // task outlives this call.
    let handle = stream.run(queen, opts).await?;
    handle_target.handles.write().push(handle);
    Ok(())
}

/// A fixed hash ring, so an unmeasured cardinality cannot become an unbounded
/// partition count. Collisions serialise more than strictly necessary, never
/// less, which is the only safe direction to be wrong in.
fn bucket(v: Option<&serde_json::Value>, n: u64) -> String {
    let s = v.and_then(|v| v.as_str()).unwrap_or("default");
    let mut h: u64 = 1469598103934665603;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    format!("p{}", h % n.max(1))
}
