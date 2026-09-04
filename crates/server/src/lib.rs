//! gate: an egress rate limiter built out of plain queen primitives.
//!
//! A library with a thin binary on top, so the graph and the reconcile loop can
//! be driven from a test against a real broker rather than only from a running
//! deployment.
//!
//! Three primitives, used directly, are the whole data plane:
//!
//! * **`kv.incr` with `max` is the admission decision.** `applied` IS the
//!   verdict; a refusal consumes nothing and returns the current value.
//! * **The transaction wire is the relay.** `ack + push`, atomic, one source
//!   partition per claim, same-named destination partition.
//! * **A wildcard long-poll is the scheduler.** The broker picks candidates in
//!   randomised order under `FOR UPDATE SKIP LOCKED`, so N workers spread across
//!   partitions with no coordination at all.
//!
//! What is left of Gate at runtime is one consumer per DAG edge set. The control
//! plane — declarations, validation, the console, sign-in, the document store —
//! is unchanged in shape.

pub mod api;
pub mod auth;
pub mod breaker;
pub mod budget;
pub mod depth;
pub mod eta;
pub mod graph;
pub mod history;
pub mod knobs;
pub mod obs;
pub mod registry;
pub mod relay;
pub mod store;
pub mod supervisor;
pub mod webapp;

/// One clock for the whole process.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use queen_mq::{Config, Queen};

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "gate_server=info,queen_mq=warn".into()),
        )
        .init();

    let queen_url = std::env::var("QUEEN_URL").unwrap_or_else(|_| "http://localhost:6632".into());
    let bind = std::env::var("GATE_BIND").unwrap_or_else(|_| "0.0.0.0:8788".into());
    let public_bind = std::env::var("GATE_PUBLIC_BIND").ok();

    // Configured only when the deployment actually exposes a console. A cell
    // that only serves its cluster needs no client id, and refusing to boot for
    // want of one it does not use would be a bad trade.
    let auth = match auth::AuthConfig::from_env() {
        Some(Ok(cfg)) => Some(Arc::new(auth::Auth::new(cfg))),
        Some(Err(e)) => return Err(e.into()),
        None => None,
    };
    let dev_email = std::env::var("GATE_DEV_EMAIL").ok();
    if let Some(email) = &dev_email {
        let public_url = std::env::var("GATE_PUBLIC_URL").unwrap_or_default();
        if public_url.starts_with("https://") {
            return Err(format!(
                "GATE_DEV_EMAIL is set on an https deployment ({public_url}): the sign-in bypass \
                 exists for a laptop, and this is not one"
            )
            .into());
        }
        tracing::warn!(
            %email,
            "SIGN-IN BYPASSED: every request to the console is treated as this identity. \
             For local development only."
        );
    }
    if public_bind.is_some() && auth.is_none() && dev_email.is_none() {
        return Err(
            "GATE_PUBLIC_BIND is set but GOOGLE_CLIENT_ID is not: the public listener \
                    exists to be authenticated, and starting it open would put the control \
                    plane on the internet. For local work set GATE_DEV_EMAIL instead."
                .into(),
        );
    }

    // History is optional: the data plane needs nothing from it, so a cell
    // without a database limits traffic exactly as well and simply cannot answer
    // "how were we doing last Tuesday".
    let history = match history::History::connect().await {
        Some(Ok(h)) => Some(Arc::new(h)),
        Some(Err(e)) => return Err(e.into()),
        None => {
            tracing::warn!("PG_HOST is not set: rollups and traces are not being kept");
            None
        }
    };

    let queen = Queen::connect(Config::new(&queen_url))?;

    let app = Arc::new(api::App {
        auth,
        budgets: budget::Budgets::new(queen.clone()),
        queen,
        registry: Default::default(),
        depths: Arc::new(depth::Depths::default()),
        traces: Arc::new(obs::Traces::default()),
        history: history.clone(),
        queen_url: queen_url.clone(),
        started_ms: now_ms(),
        broker: parking_lot::RwLock::new(None),
        declare_lock: tokio::sync::Mutex::new(()),
    });

    // Everything queen owns came back on its own; the declarations are the one
    // thing this process invented, so they are the one thing it has to go and
    // fetch. Without this a restart leaves the queues in the broker with nobody
    // draining them, which looks from the outside exactly like a limiter that
    // has decided to refuse everything.
    restore(&app).await;

    // And keep it that way. A declare lands on ONE replica, and without this the
    // fleet enforces whichever document each pod happens to hold —
    // indefinitely, and with the looser one winning, because the tighter pod
    // simply admits less of the same traffic. There is no reconcile in the
    // broker to lean on: the declarations are the one thing gate owns.
    spawn_reconcile(
        app.clone(),
        std::time::Duration::from_secs(
            std::env::var("GATE_RECONCILE_SECONDS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(15),
        ),
    );
    spawn_counters(app.clone());

    // Two listeners, the SAME router. The internal one has no authentication and
    // is reachable only from inside the cluster; the public one requires a
    // session on every route. Not "every route except the control plane" — every
    // route, so there is no path table for an ingress rule to get wrong.
    let internal = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(%bind, %queen_url, "gate listening (internal, no auth)");

    let public = match public_bind {
        Some(addr) => {
            let l = tokio::net::TcpListener::bind(&addr).await?;
            tracing::info!(bind = %addr, "gate console listening (google sign-in required)");
            Some(l)
        }
        None => None,
    };

    // Retention on its own slow clock: O(space) work does not belong on the same
    // tick as O(work) work.
    if let Some(h) = history.clone() {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                h.prune(90, 7).await;
            }
        });
    }

    let internal_app = api::router(app.clone());
    match public {
        Some(l) => {
            let public_app = api::public_router(app.clone());
            tokio::try_join!(async { axum::serve(internal, internal_app).await }, async {
                axum::serve(l, public_app).await
            },)?;
        }
        None => axum::serve(internal, internal_app).await?,
    }
    Ok(())
}

/// The loop itself, with its interval passed rather than read from the
/// environment — so a test can drive the wiring (the task, the period, its turn
/// at the declare lock) and not just the pass it performs.
pub fn spawn_reconcile(
    app: api::Shared,
    every: std::time::Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(every).await;
            reconcile(&app).await;
        }
    })
}

/// The opt-in counters flush.
///
/// **Off unless a graph declares `counters`.** That is the point of the
/// architecture: observability is a thing you switch on, not a thing that runs
/// whether or not anyone is looking. v1's meter ran at 1Hz per target whatever
/// the console was doing, and it was 9,949 of the 275,000 idle calls an hour.
///
/// It reads the stages' own `AtomicU64`s and writes the DELTA since the last
/// pass, so two replicas writing the same minute is the normal case rather than
/// a race: they saw different halves of the traffic and the row is the sum.
type CounterSnapshot = (u64, u64, u64);
type CounterCheckpoint = (String, CounterSnapshot);
const MINUTE_MS: i64 = 60_000;

pub fn spawn_counters(app: api::Shared) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last: HashMap<String, CounterSnapshot> = HashMap::new();
        loop {
            // Anchor samples to wall-clock minute boundaries. Sleeping a fixed
            // minute from process start makes every bucket span, for example,
            // 12:00:37..12:01:37 while labelling it 12:01:00.
            tokio::time::sleep(until_next_minute(now_ms())).await;
            // The delta ends at this boundary, so it belongs to the minute that
            // just completed, not to the empty minute that has just begun.
            let minute = completed_minute(now_ms());

            if let Some(h) = app.history.as_ref() {
                let mut active = HashSet::new();
                for g in app.registry.all() {
                    if g.plan.counters_window_seconds.is_none() {
                        continue;
                    }
                    let mut per_target: HashMap<String, HashMap<String, history::Bucket>> =
                        HashMap::new();
                    let mut checkpoints: HashMap<String, Vec<CounterCheckpoint>> = HashMap::new();
                    for s in &g.stages {
                        let key = format!("{}/{}", g.key(), s.key());
                        active.insert(key.clone());
                        let target = format!("{}.{}", g.doc.graph, s.stage.node);
                        let c = &s.counters;
                        let o = std::sync::atomic::Ordering::Relaxed;
                        let now3 = (
                            c.admitted.load(o),
                            c.deferred.load(o).saturating_add(c.released.load(o)),
                            c.cost.load(o),
                        );
                        let d = counter_delta(now3, last.get(&key).copied());
                        checkpoints
                            .entry(target.clone())
                            .or_default()
                            .push((key, now3));
                        if d == (0, 0, 0) {
                            continue;
                        }
                        per_target.entry(target).or_default().insert(
                            s.stage.path.clone(),
                            history::Bucket {
                                admitted: d.0,
                                denied: d.1,
                                cost_estimated: d.2 as f64,
                                ..Default::default()
                            },
                        );
                    }
                    for (target, samples) in checkpoints {
                        let written = match per_target.remove(&target) {
                            Some(paths) => h.add(&g.doc.application, &target, minute, &paths).await,
                            None => true,
                        };
                        if written {
                            for (key, value) in samples {
                                last.insert(key, value);
                            }
                        } else {
                            tracing::warn!(
                                application = %g.doc.application,
                                %target,
                                "could not persist counter rollup; retaining the previous checkpoint"
                            );
                        }
                    }
                }
                // A delete, or a redeclare that removes a path, must also
                // remove its lifetime-counter baseline. Otherwise this map
                // grows for the life of the process and a later stage reusing
                // the same identity inherits a checkpoint from a runtime that
                // no longer exists. Failed writes for ACTIVE stages remain in
                // the set and deliberately retain their previous checkpoint.
                retain_active_checkpoints(&mut last, &active);
                // The refusal ring, on the same cadence. Bounded and
                // drop-oldest, so a flush that misses a pass loses the oldest
                // denials and never blocks the hot path.
                let traces = app.traces.drain();
                if !traces.is_empty() && !h.add_traces(&traces).await {
                    tracing::warn!(
                        count = traces.len(),
                        "could not persist traces; returning them to the ring"
                    );
                    app.traces.restore(traces);
                }
            }
        }
    })
}

fn retain_active_checkpoints(
    checkpoints: &mut HashMap<String, CounterSnapshot>,
    active: &HashSet<String>,
) {
    checkpoints.retain(|key, _| active.contains(key));
}

/// Delta of a lifetime counter tuple. A lower value means the stage runtime was
/// replaced and its atomics restarted at zero, so the new value is itself the
/// entire increment since that reset.
fn counter_delta(now: CounterSnapshot, previous: Option<CounterSnapshot>) -> CounterSnapshot {
    let Some(was) = previous else {
        return now;
    };
    (
        now.0.checked_sub(was.0).unwrap_or(now.0),
        now.1.checked_sub(was.1).unwrap_or(now.1),
        now.2.checked_sub(was.2).unwrap_or(now.2),
    )
}

fn until_next_minute(now_ms: i64) -> std::time::Duration {
    let elapsed = now_ms.rem_euclid(MINUTE_MS);
    std::time::Duration::from_millis((MINUTE_MS - elapsed) as u64)
}

fn completed_minute(now_ms: i64) -> i64 {
    now_ms.div_euclid(MINUTE_MS) * MINUTE_MS - MINUTE_MS
}

/// Bring back everything that was declared, at boot.
pub async fn restore(app: &api::Shared) {
    let _guard = app.declare_lock.lock().await;
    let stored = store::load_all(&app.queen).await;
    for doc in stored.items {
        let key = doc.key();
        let migrated = stored.migrated.contains(&key);
        match graph::declare_from_store(app, doc.clone()).await {
            Ok(_) => {
                tracing::info!(graph = %key, migrated, "restored");
                // Re-save in the new shape so the mapping happens once per
                // upgrade rather than on every boot — and so the next reconcile
                // pass diffs a v2 document against a v2 document rather than
                // re-mapping and re-declaring for ever.
                if migrated {
                    match store::save(&app.queen, &doc).await {
                        Ok(()) => {
                            tracing::info!(graph = %key, "migrated document stored in the v2 shape")
                        }
                        Err(e) => {
                            tracing::warn!(graph = %key, error = %e, "could not store the migrated document")
                        }
                    }
                }
            }
            Err(e) => tracing::warn!(graph = %key, error = %e.message(), "could not restore"),
        }
    }
}

/// One pass of the reconcile loop: make this replica's runtimes match the store.
///
/// The store is the authority and the diff is by value — `GraphDoc` derives
/// `PartialEq`, so "changed" is exact rather than a heuristic on a version
/// number that a hot change does not bump.
///
/// Two asymmetries are deliberate. A read that FAILS ends the pass: reading an
/// error as an empty store would reap the whole fleet's configuration on one
/// transient failure. And a runtime whose document was never persisted is not
/// removed — it is re-saved — because a declare whose store write failed looks
/// exactly like a delete from here, and one of those two readings is
/// unrecoverable.
pub async fn reconcile(app: &api::Shared) {
    let _guard = app.declare_lock.lock().await;

    let stored = match store::try_load_all(&app.queen).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "reconcile: could not read the store; keeping what is running");
            return;
        }
    };
    let complete = stored.complete;
    let mut want: HashMap<String, gate_core::GraphDoc> =
        stored.items.into_iter().map(|d| (d.key(), d)).collect();

    for rt in app.registry.all() {
        match want.remove(&rt.key()) {
            // Unchanged document, and it is actually running. The second half
            // matters: a graph whose provisioning failed is registered-and-
            // stopped, and comparing documents alone would leave it down for
            // ever while its ingress queue kept filling.
            Some(doc) if doc == rt.doc && rt.is_running() => {}
            Some(doc) => {
                tracing::info!(graph = %doc.key(), "reconcile: re-declaring a graph that is changed or not fully up");
                if let Err(e) = graph::declare_from_store(app, doc).await {
                    tracing::warn!(graph = %rt.key(), error = %e.message(),
                                   "reconcile: could not apply the stored document");
                }
            }
            // Absent from the store. A removal only if we are sure we SAW the
            // whole store: a clamped page, or a document this build cannot
            // parse, must never read as a delete.
            None if !complete => {}
            None => {
                if rt.persisted.load(std::sync::atomic::Ordering::Relaxed) {
                    tracing::info!(graph = %rt.key(), "reconcile: removing a graph the store no longer holds");
                    supervisor::stop(&rt).await;
                    app.registry.remove(&rt.doc.application, &rt.doc.graph);
                } else if store::save(&app.queen, &rt.doc).await.is_ok() {
                    rt.persisted
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    }

    for (key, doc) in want {
        tracing::info!(graph = %key, "reconcile: declaring a graph from the store");
        if let Err(e) = graph::declare_from_store(app, doc).await {
            tracing::warn!(graph = %key, error = %e.message(), "reconcile: could not declare");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::{completed_minute, counter_delta, retain_active_checkpoints, until_next_minute};

    #[test]
    fn a_restarted_counter_counts_from_its_new_zero() {
        assert_eq!(counter_delta((7, 3, 11), Some((100, 80, 900))), (7, 3, 11));
    }

    #[test]
    fn a_running_counter_reports_only_its_increment() {
        assert_eq!(
            counter_delta((107, 83, 911), Some((100, 80, 900))),
            (7, 3, 11)
        );
    }

    #[test]
    fn counter_flush_waits_for_the_next_wall_clock_boundary() {
        assert_eq!(until_next_minute(120_000).as_millis(), 60_000);
        assert_eq!(until_next_minute(120_001).as_millis(), 59_999);
        assert_eq!(until_next_minute(179_999).as_millis(), 1);
    }

    #[test]
    fn counter_flush_labels_the_minute_that_just_completed() {
        assert_eq!(completed_minute(120_000), 60_000);
        assert_eq!(completed_minute(120_001), 60_000);
        assert_eq!(completed_minute(179_999), 60_000);
        assert_eq!(completed_minute(180_000), 120_000);
    }

    #[test]
    fn deleted_stages_do_not_leave_lifetime_checkpoints_behind() {
        let mut checkpoints = HashMap::from([
            ("app/graph/path/live".to_string(), (10, 20, 30)),
            ("app/graph/path/deleted".to_string(), (40, 50, 60)),
        ]);
        let active = HashSet::from(["app/graph/path/live".to_string()]);

        retain_active_checkpoints(&mut checkpoints, &active);

        assert_eq!(checkpoints.len(), 1);
        assert_eq!(checkpoints["app/graph/path/live"], (10, 20, 30));
    }
}
