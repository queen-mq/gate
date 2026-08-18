//! gate: an egress rate limiter that runs as a streaming gate on QueenMQ.

mod api;
mod auth;
mod depth;
mod gate;
mod history;
mod webapp;
mod meter;
mod registry;
mod shared;
mod store;
mod supervisor;

/// One clock for the whole process. The gate has its own — the stream runtime's
/// `stream_time_ms` — and the two must not be confused: that one is sampled
/// once per cycle and is what the budget arithmetic runs on.
pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

use std::sync::Arc;

use queen_mq::{Config, Queen};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
    });

    // Everything queen owns came back on its own; the specs are the one thing
    // this process invented, so it is the one thing it has to go and fetch.
    // Without this a restart leaves the queues in the broker with nobody
    // draining them, which looks from the outside exactly like a limiter that
    // has decided to refuse everything.
    for spec in store::load_all(&app.queen).await {
        let name = spec.key();
        match supervisor::start(&app.queen, app.meter.clone(), app.history.clone(), spec).await {
            Ok(rt) => {
                app.registry.put(rt);
                tracing::info!(target_name = %name, "restored");
            }
            Err(e) => tracing::warn!(target_name = %name, error = %e, "could not restore"),
        }
    }

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
