//! gate: an egress rate limiter that runs as a streaming gate on QueenMQ.
//!
//! A library with a thin binary on top, so the graph and the reconcile loop can
//! be driven from a test against a real broker rather than only from a running
//! deployment.

pub mod api;
pub mod auth;
pub mod depth;
pub mod edge;
pub mod eta;
pub mod gate;
pub mod graph;
pub mod history;
pub mod meter;
pub mod registry;
pub mod shared;
pub mod store;
pub mod supervisor;
pub mod webapp;

/// One clock for the whole process. The gate has its own — the stream runtime's
/// `stream_time_ms` — and the two must not be confused: that one is sampled
/// once per cycle and is what the budget arithmetic runs on.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

use std::collections::HashMap;
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
        return Err("GATE_PUBLIC_BIND is set but GOOGLE_CLIENT_ID is not: the public listener \
                    exists to be authenticated, and starting it open would put the control \
                    plane on the internet. For local work set GATE_DEV_EMAIL instead."
            .into());
    }

    // History is optional: the gate needs nothing from it, so a cell without a
    // database limits traffic exactly as well and simply cannot answer "how
    // were we doing last Tuesday".
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
        queen,
        registry: Default::default(),
        meter: Arc::new(meter::Meter::default()),
        depths: Arc::new(depth::Depths::default()),
        history: history.clone(),
        queen_url: queen_url.clone(),
        started_ms: now_ms(),
        declare_lock: tokio::sync::Mutex::new(()),
    });

    // Everything queen owns came back on its own; the specs are the one thing
    // this process invented, so it is the one thing it has to go and fetch.
    // Without this a restart leaves the queues in the broker with nobody
    // draining them, which looks from the outside exactly like a limiter that
    // has decided to refuse everything.
    restore(&app).await;

    // And keep it that way. A declare lands on ONE replica, and without this the
    // fleet enforces whichever spec each pod happens to hold — indefinitely, and
    // with the looser one winning, because the tighter pod simply admits less of
    // the same traffic. There is no reconcile in the broker to lean on: the specs
    // are the one thing gate owns.
    spawn_reconcile(
        app.clone(),
        std::time::Duration::from_secs(
            std::env::var("GATE_RECONCILE_SECONDS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(15),
        ),
    );



    // Two listeners, the SAME router. The internal one has no authentication
    // and is reachable only from inside the cluster; the public one requires a
    // session on every route. Not "every route except the control plane" —
    // every route, so there is no path table for an ingress rule to get wrong.
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

    // Retention on its own slow clock: O(space) work does not belong on the
    // same tick as O(work) work.
    if let Some(h) = history.clone() {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                h.prune(90, 7).await;
            }
        });
    }

    let internal_app = api::router(app.clone());
    let _ = &internal_app;

    match public {
        Some(l) => {
            let public_app = api::public_router(app.clone());
            tokio::try_join!(
                async { axum::serve(internal, internal_app).await },
                async { axum::serve(l, public_app).await },
            )?;
        }
        None => axum::serve(internal, internal_app).await?,
    }
    Ok(())
}

/// The loop itself, with its interval passed rather than read from the environment —
/// so a test can drive the wiring (the task, the period, its turn at the declare lock)
/// and not just the pass it performs.
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

/// Bring back everything that was declared, at boot.
///
/// Graphs first, then standalone targets: a graph owns the targets `{graph}.{node}`
/// and a stale spec of that name in the target store must not be allowed to take
/// over a node's queues — so if both exist, the graph wins and the stray is skipped.
pub async fn restore(app: &api::Shared) {
    let _guard = app.declare_lock.lock().await;
    for spec in store::load_graphs(&app.queen).await {
        let key = spec.key();
        match graph::declare_from_store(app, spec).await {

            Ok(_) => tracing::info!(graph = %key, "restored"),
            Err(e) => tracing::warn!(graph = %key, error = %refusal(&e), "could not restore"),
        }
    }
    for spec in store::load_all(&app.queen).await {
        let name = spec.key();
        if let Some(g) = app
            .registry
            .graph_owning_target(&spec.application, &spec.name)
        {
            tracing::warn!(
                target_name = %name, graph = %g.spec.key(),
                "a stored target has the name of a graph node; the graph owns it, so the target is \
                 not restored"
            );
            continue;
        }
        match supervisor::start(&app.queen, app.meter.clone(), app.history.clone(), spec, None).await
        {
            Ok(rt) => {
                rt.persisted.store(true, std::sync::atomic::Ordering::Relaxed);
                app.registry.put(rt);
                tracing::info!(target_name = %name, "restored");
            }
            Err(e) => tracing::warn!(target_name = %name, error = %e, "could not restore"),
        }
    }
}

fn refusal(r: &graph::Refusal) -> String {
    match r {
        graph::Refusal::Invalid(m) | graph::Refusal::Conflict(m) | graph::Refusal::Gateway(m) => {
            m.clone()
        }
    }
}

/// One pass of the reconcile loop: make this replica's runtimes match the store.
///
/// The store is the authority and the diff is by value — `TargetSpec` and
/// `GraphSpec` both derive `PartialEq`, so "changed" is exact rather than a
/// heuristic on a version number that a hot change does not bump.
///
/// Two asymmetries are deliberate. A read that FAILS ends the pass: reading an
/// error as an empty store would reap the whole fleet's configuration on one
/// transient failure. And a runtime whose spec was never persisted is not removed —
/// it is re-saved — because a declare whose store write failed looks exactly like a
/// delete from here, and one of those two readings is unrecoverable.
pub async fn reconcile(app: &api::Shared) {
    let _guard = app.declare_lock.lock().await;

    // ---- graphs. Before targets, because a graph owns node targets and the target
    // pass must see the ownership that this pass establishes.
    match store::try_load_graphs(&app.queen).await {
        Err(e) => {
            tracing::warn!(error = %e, "reconcile: could not read the graph store; keeping what is running");
            return;
        }
        Ok(stored) => {
            let complete = stored.complete;
            let mut want: HashMap<String, gate_core::GraphSpec> =
                stored.items.into_iter().map(|s| (s.key(), s)).collect();

            for g in app.registry.graphs() {
                match want.remove(&g.spec.key()) {
                    // Unchanged document, and every node it names is running. The
                    // second half matters: a node that could not be provisioned and
                    // could not be restored is unregistered, and comparing documents
                    // alone would leave it down for ever while its relay kept feeding
                    // a queue with no gate on it.
                    Some(spec)
                        if spec == g.spec
                            // Running, not merely registered: a node whose restore
                            // failed is registered-and-stopped or gone, and either way
                            // this graph needs provisioning again.
                            && spec.node_specs().iter().all(|(_, ns)| {
                                app.registry
                                    .get(&ns.application, &ns.name)
                                    .is_some_and(|rt| rt.is_running())
                            }) => {}


                    Some(spec) => {
                        tracing::info!(graph = %spec.key(), "reconcile: re-declaring a graph that is changed or not fully up");

                        if let Err(e) = graph::declare_from_store(app, spec).await {
                            tracing::warn!(graph = %g.spec.key(), error = %refusal(&e),
                                           "reconcile: could not apply the stored graph");
                        }
                    }
                    // Absent from the store. A removal only if we are sure we SAW the
                    // whole store: a clamped page or a document this build cannot
                    // parse must never read as a delete.
                    None if !complete => {}
                    None => {
                        if g.persisted.load(std::sync::atomic::Ordering::Relaxed) {
                            tracing::info!(graph = %g.spec.key(), "reconcile: removing a graph the store no longer holds");
                            graph::stop(app, &g).await;
                            app.registry
                                .remove_graph(&g.spec.application, &g.spec.name);
                        } else if store::save_graph(&app.queen, &g.spec).await.is_ok() {
                            g.persisted.store(true, std::sync::atomic::Ordering::Relaxed);
                        }
                    }

                }
            }
            for (key, spec) in want {
                tracing::info!(graph = %key, "reconcile: declaring a graph from the store");
                if let Err(e) = graph::declare_from_store(app, spec).await {

                    tracing::warn!(graph = %key, error = %refusal(&e), "reconcile: could not declare");
                }
            }
        }
    }

    // ---- standalone targets.
    let stored = match store::try_load_all(&app.queen).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "reconcile: could not read the target store; keeping what is running");
            return;
        }
    };
    let complete = stored.complete;
    let mut want: HashMap<String, gate_core::TargetSpec> =
        stored.items.into_iter().map(|s| (s.key(), s)).collect();


    for rt in app.registry.all() {
        // A node is not in the target store by design: its graph document is the
        // authority, and the graph pass above has already applied it.
        if rt.graph.is_some() {
            want.remove(&rt.spec.key());
            continue;
        }
        match want.remove(&rt.spec.key()) {
            Some(spec) if spec == rt.spec => {}
            Some(spec) => {
                tracing::info!(target_name = %spec.key(), "reconcile: restarting on the stored spec");
                swap_in(app, Some(&rt), spec).await;
            }
            // Same rule as for graphs: an incomplete read may add and change, never
            // remove.
            None if !complete => {}
            None if rt.persisted.load(std::sync::atomic::Ordering::Relaxed) => {

                tracing::info!(target_name = %rt.spec.key(), "reconcile: removing a target the store no longer holds");
                supervisor::stop_with(&app.queen, &rt).await;
                app.registry.remove(&rt.spec.application, &rt.spec.name);
            }
            // Declared here, never persisted. Retry the save rather than tear down a
            // live target on the strength of a write that failed.
            None => {
                if store::save(&app.queen, &rt.spec).await.is_ok() {
                    rt.persisted.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    }

    for (key, spec) in want {
        if app
            .registry
            .graph_owning_target(&spec.application, &spec.name)
            .is_some()
        {
            continue;
        }
        tracing::info!(target_name = %key, "reconcile: starting a target from the store");
        swap_in(app, None, spec).await;
    }
}

async fn swap_in(
    app: &api::Shared,
    old: Option<&Arc<registry::TargetRuntime>>,
    spec: gate_core::TargetSpec,
) {
    let key = spec.key();
    match supervisor::swap(
        &app.queen,
        app.meter.clone(),
        app.history.clone(),
        old,
        spec,
        None,
    )
    .await
    {
        Ok(rt) => {
            rt.persisted.store(true, std::sync::atomic::Ordering::Relaxed);
            app.registry.put(rt);
        }
        Err(f) => match f.restored {
            Some(rt) => {
                app.registry.put(rt);
                tracing::warn!(target_name = %key, error = %f.error,
                               "reconcile: could not apply the stored spec; still serving the old one");
            }
            None => {
                if let Some((a, n)) = key.split_once('/') {
                    app.registry.remove(a, n);
                }
                tracing::error!(target_name = %key, error = %f.error,
                                "reconcile: could not provision, and nothing is serving it");
            }
        },
    }
}
