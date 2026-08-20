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

/// How long an empty poll parks on the broker before coming home.
///
/// A push wakes a parked poll at once — broker-side, through the queue's wake
/// gate — so this number buys no latency on the admit path; it is purely the
/// cadence at which an IDLE lane re-asks a question whose answer has not
/// changed. It used to be 250ms, which read as responsiveness and was in fact
/// the opposite: each empty poll runs the pop SP against Postgres at least
/// once, and a window that short returns before the broker's own empty-poll
/// backoff (100ms initial, escalating past three parks) ever escalates — the
/// re-poll counter is per REQUEST, so short requests pinned it at the floor.
/// Measured on the fleet that motivated the change: a 64-shard node is 64
/// runners asking 3 times per 250ms window each, ~8 SP calls/s per runner of
/// pure silence, and the dashboard's fill ratio read the silence as collapse.
///
/// The ceiling on this value is shutdown, not latency: the runner notices its
/// cancel token BETWEEN polls, so a spec swap waits out whatever poll is in
/// flight (supervisor::stop awaits the handles for exactly this long). Five
/// seconds keeps a redeclare humane while cutting the idle chatter ~15x.
pub(crate) const STREAM_MAX_WAIT: std::time::Duration = std::time::Duration::from_millis(5_000);

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

/// One runner for one lane, or one shard of one lane.
///
/// `shard` names a partition of the push queue, and the partition lease is the
/// entire correctness argument: it makes each counter single-writer. Two runners
/// on one partition would lose updates in silence, which is why the source is
/// pinned and `max_partitions` is one — and why sharding is expressed as more
/// partitions rather than as more runners on the same one.
pub async fn spawn(
    queen: &Queen,
    meter: Arc<Meter>,
    target: Arc<TargetRuntime>,
    lane: Arc<LaneRuntime>,
    shard: u32,
) -> Result<()> {
    let spec: TargetSpec = target.spec.clone();
    let lane_name = lane.name.clone();
    // The push partition, which IS the lane name when the target is not sharded.
    let partition = spec.push_partition(&lane_name, shard);
    let shards_of_lane = spec.shard_count().max(1);


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
    // The gate mirrors its state out per partition, not per lane: a shard is its
    // own document and folding them together would report one shard's spend as the
    // node's.
    let gate_state_key = partition.clone();
    let gate_meter = meter.clone();
    let gate_pools = target.pools.clone();
    let gate_target_name = spec.key();

    let source = queen
        .queue(spec.push_queue())
        // Pinned: this runner sees one lane (one shard of one lane, when the
        // target is sharded) and nothing else, so its budget, its batch, its deny
        // parking and its lease are all its own.
        .partition(partition.clone())
        .lease_seconds(spec.pacing.lease_seconds as i32);


    let stream = Stream::from(source)
        .gate(move |rec, ctx| {
            let op = rec.text("op").unwrap_or("").to_string();
            let cost = rec.number(&cost_field).unwrap_or(cost_default);
            let item = Item { op, cost, scope: scope_of(&rec.data) };
            // The lane's rate cap, divided by the shards it is spread over.
            //
            // `effective_cap` is a RATE (cost per second) and every shard runner holds
            // its own copy of the counter it is applied to, so handing the whole
            // figure to each of 64 shards would enforce it 64 times — the same
            // multiplication that makes lanes divide a ceiling instead of replicating
            // it. Declared budgets need no such division: a sharded target's budgets
            // are all keyed by the shard dimension (`shard-scope`), so each cap is per
            // key and a key lives in exactly one shard.
            let cap = gate_lane
                .effective_cap
                .read()
                .map(|rate| rate / shards_of_lane as f64);


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
                    *gate_target.last_state.write().entry(gate_state_key.clone()).or_default() =
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

    // One query id per lane, shared by that lane's shards: the stream's state is
    // keyed `(query_id, partition_id)`, so pinned runners on different partitions
    // already have different rows, and the registration is idempotent under the
    // same operator chain.
    let mut opts = RunOptions::new(spec.query_id(&lane_name));
    // Explicit, and identical to what the SDK would derive from the query id on
    // its own. Named here because the ETA read has to ask the broker for THIS
    // group's backlog, and an unset option makes that string an SDK detail
    // rather than something this crate states once.
    opts.consumer_group = Some(consumer_group(&spec, &lane_name));

    opts.batch_size = spec.pacing.batch as i32;
    // `all`, and this one is not a nicety: a stream's consumer group is created at
    // its first poll, and the broker's default puts a new cursor at the TAIL. Every
    // message pushed between a target being declared and its gate's first poll was
    // silently skipped — the push queue showed it settled, the budget never saw it,
    // and the item never reached a consumer. Measured on a single push into a
    // freshly declared target: one message in, nothing admitted, nothing pending,
    // no error anywhere.
    opts.subscription_mode = Some(queen_mq::SubscriptionMode::All);

    // One partition each, because the source is pinned.
    opts.max_partitions = 1;
    opts.max_wait = STREAM_MAX_WAIT;
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
    // The same hash the push route shards on, from the same place, so a value
    // cannot land in one bucket here and another there.
    format!("p{}", gate_core::shard_index(s, n.min(u32::MAX as u64) as u32))
}


/// The consumer group this runner reads its push queue under.
///
/// Not the query id, though it is derived from one: `RunOptions` leaves
/// `consumer_group` unset and the SDK then defaults it to `streams.{query_id}`.
/// It is set explicitly here, and read from here by anything that needs to name
/// it, because a near-miss does not fail loudly — the broker's depth route
/// answers a group that has no cursor with the queue's WHOLE retained range, so
/// an ETA built on the wrong string would report every message ever pushed as
/// waiting for budget and look entirely plausible doing it.
pub fn consumer_group(spec: &TargetSpec, lane: &str) -> String {
    format!("streams.{}", spec.query_id(lane))
}
