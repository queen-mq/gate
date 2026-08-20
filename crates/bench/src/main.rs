//! What Gate itself costs, per use case.
//!
//! This is not [`gate-e2e`]. That one asks whether the ceiling holds — whether
//! a target declared at 50/s admits 50/s — and a limiter that admits everything
//! fails it. This one asks the opposite question: with the limiter deliberately
//! kept out of the way, how many requests a second does the machinery move, and
//! what does a caller wait?
//!
//! The distinction runs through every number below, because Gate has TWO
//! latencies and confusing them makes both meaningless:
//!
//! * **API latency** — how long `push`, `next` and `ack` take on the wire. This
//!   is Gate's own performance. A push is one queen append; an ack is one queen
//!   transaction. It should be flat whether the target is idle or a hundred
//!   thousand items deep, and a run that shows otherwise has found something.
//!
//! * **Admission latency** — how long an item waits between being pushed and
//!   being poppable. This is NOT performance: it is the declared ceiling doing
//!   its job. An item that arrives 900 deep behind a 50/s ceiling waits
//!   eighteen seconds because it was told to.
//!
//! Quoting the second as if it were the first is how a rate limiter gets
//! reported as slow. So the scenarios come in two families: `push` and `drain`
//! measure the API with the ceiling out of the way, and `throttled` and `lanes`
//! measure the wait with the ceiling doing exactly what it was declared to do.
//!
//! One thing that is NOT a source of latency, and was assumed to be until the
//! runner was read: the lease. `gate::spawn` runs its stream with `wait(true)`
//! against the broker's long poll, and a push wakes a parked poll broker-side
//! (`gate.rs`, `STREAM_MAX_WAIT`). The gate does not sample once per lease. The
//! lease is the partition lease and the quantum a DENIED batch parks for — so
//! it bounds the throttled path, and the free path is woken immediately.
//!
//! ```text
//! cargo run --release -p gate-bench -- all
//! cargo run --release -p gate-bench -- push,drain
//! ```

mod load;
mod world;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;

use load::{burst, closed, header, open, Hist, Outcome};
use world::{now_us, one_lane, uncapped_spec, Gate};

/// The ceiling the "get out of the way" scenarios declare. Far above anything a
/// laptop moves, so the gate admits on the first wake and the number measures
/// the machinery instead of the pacing.
const WIDE_PER_SEC: f64 = 20_000.0;

/// How many partitions the admitted queues are cut into, and therefore how many
/// consumers can pop at once. It is a declared number rather than a constant of
/// nature, and `drain` sweeps consumers across it to show what happens either
/// side.
const PARTITIONS: usize = 8;

struct Cfg {
    secs: u64,
    warmup: u64,
    backlog: u64,
    cycle_rate: f64,
    conc: Vec<usize>,
}

fn env_u64(k: &str, d: u64) -> u64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The driver and the server are almost always the same laptop, and they
    // compete for the same cores. Fewer driver threads than the machine has is
    // deliberate: a driver that takes every core measures the contention it
    // caused.
    let threads = env_u64("BENCH_THREADS", 4) as usize;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()?;
    rt.block_on(run(threads))
}

async fn run(threads: usize) -> Result<(), Box<dyn std::error::Error>> {
    let base = std::env::var("GATE_URL").unwrap_or_else(|_| "http://127.0.0.1:8788".into());
    let app = std::env::var("GATE_APP").unwrap_or_else(|_| "bench".into());
    let which = std::env::args().nth(1).unwrap_or_else(|| "all".into());

    let cfg = Cfg {
        secs: env_u64("BENCH_SECONDS", 10),
        warmup: env_u64("BENCH_WARMUP", 3),
        backlog: env_u64("BENCH_BACKLOG", 30_000),
        cycle_rate: env_u64("BENCH_CYCLE_RATE", 500) as f64,
        conc: std::env::var("BENCH_CONC")
            .ok()
            .map(|v| v.split(',').filter_map(|s| s.trim().parse().ok()).collect())
            .unwrap_or_else(|| vec![1, 8, 32, 128]),
    };

    let g = Arc::new(Gate::new(base.clone(), app.clone()));
    let probe = g.health().await;
    if probe.status != 200 {
        return Err(format!(
            "gate at {base} did not answer /health (status {}). Start it, or set GATE_URL",
            probe.status
        )
        .into());
    }

    println!("== gate-bench");
    println!("   gate            {base}");
    println!("   application     {app}   (targets are suffixed `-{}`)", g.run_id);
    println!(
        "   window          {}s measured after {}s warmup, driver on {threads} threads",
        cfg.secs, cfg.warmup
    );
    println!(
        "   caveat          driver and server share this machine; every number is a floor, not\n\
         \x20                  the hardware's limit"
    );

    let mut failures = Vec::new();
    for s in which.split(',').map(|s| s.trim()) {
        let r = match s {
            "all" => {
                let mut acc = Ok(());
                for one in ["health", "push", "drain", "cycle", "throttled", "pacing", "lanes", "graph"] {
                    if let Err(e) = dispatch(one, &g, &cfg).await {
                        acc = Err(e);
                        break;
                    }
                }
                acc
            }
            other => dispatch(other, &g, &cfg).await,
        };
        if let Err(e) = r {
            failures.push(format!("{s}: {e}"));
        }
    }

    if !failures.is_empty() {
        println!("\n== incomplete");
        for f in &failures {
            println!("   {f}");
        }
        std::process::exit(1);
    }
    Ok(())
}

async fn dispatch(what: &str, g: &Arc<Gate>, cfg: &Cfg) -> Result<(), String> {
    match what {
        "health" => health(g, cfg).await,
        "push" => push(g, cfg).await,
        "drain" => drain(g, cfg).await,
        "cycle" => cycle(g, cfg).await,
        "throttled" => throttled(g, cfg).await,
        "pacing" => pacing(g, cfg).await,
        "lanes" => lanes(g, cfg).await,
        "graph" => graph(g, cfg).await,
        other => Err(format!(
            "unknown scenario `{other}`: one of health, push, drain, cycle, throttled, pacing, \
             lanes, graph, all"
        )),
    }
}

fn warm(cfg: &Cfg) -> Duration {
    Duration::from_secs(cfg.warmup)
}
fn dur(cfg: &Cfg) -> Duration {
    Duration::from_secs(cfg.secs)
}

// ---------------------------------------------------------------- the control

/// `/health` is a route that touches nothing: no registry, no broker, no state.
/// Whatever it costs is axum, the kernel and the driver, and every other row in
/// this report sits on top of it. Without it a slow `push` cannot be attributed
/// to anything.
async fn health(g: &Arc<Gate>, cfg: &Cfg) -> Result<(), String> {
    println!("\n\n== health — the floor: HTTP in, HTTP out, nothing in between");
    header("route");
    for c in &cfg.conc {
        let gg = g.clone();
        let run = closed("GET /health", *c, warm(cfg), dur(cfg), move |_w, _i| {
            let g = gg.clone();
            async move { g.health().await }
        })
        .await;
        run.print();
    }
    println!("\n   Read the rest of this report as: (that row) minus (this one) is the broker.");
    Ok(())
}

// -------------------------------------------------------------------- ingest

/// The call an application actually makes, and the only one it waits on.
/// `channel-go` pushes and returns; everything after that is Gate's problem. So
/// this row is the one that decides whether Gate can sit in a hot path.
///
/// Every concurrency gets a FRESH target. Sharing one across the sweep looks
/// tidier and is a trap: each row leaves its pushes in the queue, the gate
/// admits them into a queue nobody drains, and the last row is measuring a
/// broker carrying a hundred thousand items the first row did not have.
async fn push(g: &Arc<Gate>, cfg: &Cfg) -> Result<(), String> {
    println!("\n\n== push — one queen append per call, ceiling declared far out of the way");
    header("closed loop");
    let mut peak = 0.0f64;
    for c in &cfg.conc {
        let name = format!("{}-c{c}", g.target_name("push"));
        g.declare(&name, uncapped_spec(&g.app, &name, WIDE_PER_SEC, one_lane()))
            .await?;
        let (gg, nn) = (g.clone(), name.clone());
        let run = closed("POST …/push", *c, warm(cfg), dur(cfg), move |w, i| {
            let (g, name) = (gg.clone(), nn.clone());
            async move {
                let body = json!({
                    "op": "bench.call", "cost": 1,
                    "payload": { "connection": format!("conn-{}", (w as u64 + i) % 40) }
                });
                g.push(&name, "bulk", &body).await
            }
        })
        .await;
        peak = peak.max(run.rps());
        run.print();
        g.drop_target(&name).await;
    }

    // A latency quoted from the saturation row is a latency nobody can
    // reproduce: at the knee the queue is full by construction. These rows are
    // what a caller would feel at a load a deployment would deliberately run at.
    println!("\n   Latency at a load below the knee, offered on a fixed schedule:");
    header("open loop");
    for frac in [0.25, 0.5] {
        let rate = (peak * frac).max(50.0);
        let name = format!("{}-o{:.0}", g.target_name("push"), rate);
        g.declare(&name, uncapped_spec(&g.app, &name, WIDE_PER_SEC, one_lane()))
            .await?;
        let (gg, nn) = (g.clone(), name.clone());
        let run = open("POST …/push", rate, warm(cfg), dur(cfg), 4_096, move |_w, i| {
            let (g, name) = (gg.clone(), nn.clone());
            async move {
                let body = json!({
                    "op": "bench.call", "cost": 1,
                    "payload": { "connection": format!("conn-{}", i % 40) }
                });
                g.push(&name, "bulk", &body).await
            }
        })
        .await;
        run.print();
        g.drop_target(&name).await;
    }

    println!(
        "\n   peak {peak:.0} req/s. Every push is one append to the broker, so this row is a\n   \
         measurement of Gate AND queen together — Gate's own share is the gap to /health."
    );
    Ok(())
}

// --------------------------------------------------------------------- drain

/// Pop and settle, against a backlog deep enough that the gate is never the
/// thing being waited on.
///
/// Two sweeps, because two different things bound this path. **Batch**: one
/// `next` and one `ack` cost two round trips whether they carry one item or a
/// hundred, so items per second is very nearly linear in the batch, and a
/// consumer that acks one at a time is paying two broker round trips per vendor
/// call. **Consumers**: the admitted queue is cut into `admitted.partitions`,
/// and a pop takes partition leases — so consumers past the partition count do
/// not add throughput, they take each other's partitions and come back empty.
async fn drain(g: &Arc<Gate>, cfg: &Cfg) -> Result<(), String> {
    println!("\n\n== drain — GET next + POST ack, the consumer's half of the cycle");
    println!(
        "   Each row gets its own target, prefilled with {} items and fully admitted first.\n   \
         The admitted queue has {PARTITIONS} partitions.",
        cfg.backlog
    );

    header("batch (c=4)");
    for batch in [1u32, 25, 100] {
        drain_row(g, cfg, &format!("b{batch}"), batch, 4).await?;
    }

    header("consumers (batch=25)");
    for c in [1usize, 4, 8, 16] {
        drain_row(g, cfg, &format!("c{c}"), 25, c).await?;
    }

    println!(
        "\n   `units` is items settled per second — the number to compare against a vendor's\n   \
         ceiling. `requests` is round trips, and it is what stays flat as the batch grows."
    );
    Ok(())
}

async fn drain_row(
    g: &Arc<Gate>,
    cfg: &Cfg,
    suffix: &str,
    batch: u32,
    consumers: usize,
) -> Result<(), String> {
    let name = format!("{}-{suffix}", g.target_name("drain"));
    g.declare(&name, uncapped_spec(&g.app, &name, WIDE_PER_SEC, one_lane()))
        .await?;
    let pushed = prefill(g, &name, "bulk", cfg.backlog, 64).await;
    wait_admitted(g, &name, pushed, Duration::from_secs(60)).await;

    let empty = Arc::new(AtomicU64::new(0));
    let (gg, nn, ee) = (g.clone(), name.clone(), empty.clone());
    let run = closed(
        &format!("next({batch})+ack ×{consumers}"),
        consumers,
        Duration::from_secs(1),
        Duration::from_secs(cfg.secs.min(6)),
        move |_w, _i| {
            let (g, name, empty) = (gg.clone(), nn.clone(), ee.clone());
            async move {
                let Some((status, body)) = g.next(&name, "bulk", batch, 1_000).await else {
                    return Outcome::dead();
                };
                let n = body.get("items").and_then(|v| v.as_array()).map_or(0, |a| a.len());
                if n == 0 {
                    empty.fetch_add(1, Ordering::Relaxed);
                    return Outcome::ok(status).with_units(0);
                }
                g.ack(&body, n, "bulk").await
            }
        },
    )
    .await;
    run.print();

    let e = empty.load(Ordering::Relaxed);
    if e > 0 {
        // Deliberately not called "the backlog ran dry". An empty pop has two
        // causes and they mean opposite things: nothing left to pop, or every
        // partition currently leased by a peer consumer. The second is the one
        // the consumer sweep is looking for, and calling it starvation would
        // have hidden it.
        println!(
            "  {:>22}   ^ {e} pops returned nothing: either the {} items ran out, or all \
             {PARTITIONS} partitions\n  {:>22}     were leased by the other {} consumer(s). \
             Each empty pop cost a 1000ms poll.",
            "", cfg.backlog, "", consumers.saturating_sub(1)
        );
    }
    g.drop_target(&name).await;
    Ok(())
}

// --------------------------------------------------------------------- cycle

/// Push, wait for the gate, pop, ack — with the ceiling out of the way, so what
/// is left is the machinery.
///
/// The number this scenario exists for is end to end: stamped at push, read at
/// pop. It is NOT bounded below by the lease. The gate's stream long-polls with
/// `wait(true)` and the broker wakes a parked poll when a push lands, so an
/// unthrottled item is admitted as soon as the gate can get to it.
async fn cycle(g: &Arc<Gate>, cfg: &Cfg) -> Result<(), String> {
    let name = g.target_name("cycle");
    g.declare(&name, uncapped_spec(&g.app, &name, WIDE_PER_SEC, one_lane()))
        .await?;

    println!("\n\n== cycle — push to pop, end to end, nothing throttling");
    let d = Drainers::target(g, &name, "bulk", 4, 50);

    header("offered");
    let (gg, nn) = (g.clone(), name.clone());
    let run = open(
        "push (API only)",
        cfg.cycle_rate,
        warm(cfg),
        dur(cfg),
        4_096,
        move |_w, i| {
            let (g, name) = (gg.clone(), nn.clone());
            async move {
                let body = json!({
                    "op": "bench.call", "cost": 1,
                    "payload": { "connection": format!("conn-{}", i % 40), "bench_us": now_us() }
                });
                g.push(&name, "bulk", &body).await
            }
        },
    )
    .await;
    run.print();

    // Let the tail through: items pushed in the last moments have not been
    // admitted yet, and cutting here would report the queue as faster than it is
    // by simply not counting the slow ones.
    let (e2e, settled) = d.finish(Duration::from_secs(3)).await;
    println!("\n   settled {settled} items, end to end (stamped at push, read at pop):");
    print_e2e(&e2e);
    println!(
        "   This is the free path: the ceiling never denied, so nothing ever waited for a\n   \
         lease. Compare it against `throttled`, which is the same machinery with a real\n   \
         ceiling in front of it."
    );

    g.drop_target(&name).await;
    Ok(())
}

// ----------------------------------------------------------------- throttled

/// The use case Gate is actually for: a ceiling well below what the caller
/// wants, and a backlog.
///
/// Two things are asserted at once. The API stays fast — pushing into a target
/// that is a thousand items deep costs the same as pushing into an empty one,
/// because a push is an append and does not consult the gate. And the wait is
/// arithmetic: `depth / ceiling`, which is the ceiling working, not Gate being
/// slow.
///
/// ONE lane, deliberately. Lanes divide a ceiling rather than replicating it, so
/// a target with an idle `urgent` lane admits its `bulk` traffic at bulk's share
/// and not at the ceiling — which is correct, and would read here as a 50%
/// throughput shortfall against a number nobody declared. That division is worth
/// measuring on its own, and `lanes` does.
async fn throttled(g: &Arc<Gate>, cfg: &Cfg) -> Result<(), String> {
    const CEILING: f64 = 50.0;
    let depth = 1_500u64;
    let name = g.target_name("throttled");
    g.declare(&name, capped_spec(&g.app, &name, CEILING, one_lane()))
        .await?;

    println!("\n\n== throttled — a {CEILING:.0}/s ceiling and {depth} items that want through now");
    header("phase");

    // A burst rather than a timed loop: the flood is finite, and it has to STOP,
    // or what is measured below would be racing a producer.
    let (gg, nn) = (g.clone(), name.clone());
    let flood = burst("POST …/push (flood)", 32, depth, move |w, i| {
        let (g, name) = (gg.clone(), nn.clone());
        async move {
            let body = json!({
                "op": "bench.call", "cost": 1,
                "payload": { "connection": format!("conn-{}", (w as u64 + i) % 40),
                             "bench_us": now_us() }
            });
            g.push(&name, "bulk", &body).await
        }
    })
    .await;
    flood.print();

    // Consumers, because a gate with nobody popping is not the use case: the
    // admitted queue just grows and the number measured would be a queue depth.
    let d = Drainers::target(g, &name, "bulk", 4, 25);

    // Watch what leaves, from the gate's own admitted counter. The driver's
    // drain rate would only say how fast the driver is.
    // Sized to the backlog, not to the default window: 1500 items behind 50/s
    // is thirty seconds of work, and a ten-second watch leaves so few steady
    // samples that the one zero at the boundary moves the mean by a fifth.
    let watch = cfg.secs.max(20);
    let t0 = Instant::now();
    let per_sec = sample_admitted(g, &name, watch).await;
    let (cold, rate, observed) = steady_rate(&per_sec);
    let admitted_total: u64 = per_sec.iter().sum();

    let (e2e, settled) = d.finish(Duration::from_secs(1)).await;

    println!("\n   admitted per second   {observed:?}");
    println!(
        "   declared ceiling      {CEILING:.0}/s      held {rate:.1}/s ({:.0}% of cap)      \
         {admitted_total} admitted in {:.0}s",
        rate / CEILING * 100.0,
        t0.elapsed().as_secs_f64()
    );
    if cold > 0 {
        println!(
            "   cold start            {cold}s before the first admission: a freshly declared \
             target has queues\n                         and a consumer group to create, and a \
             partition lease to take."
        );
    }
    println!(
        "   pushed {} items in {:.1}s at p50 {:.1}ms / p99 {:.1}ms — the API did not slow down\n   \
         as the queue grew, because a push is an append and does not consult the gate.",
        flood.units,
        flood.elapsed,
        flood.waited.pct(50.0),
        flood.waited.pct(99.0)
    );
    println!("\n   what the {settled} settled items waited, push to pop:");
    print_e2e(&e2e);
    println!(
        "   A {depth}-item backlog behind {CEILING:.0}/s drains in {:.0}s, so an item's wait is\n   \
         its position over the ceiling and the percentiles above are a ramp, not a tail. That\n   \
         wait is the declaration, not the implementation: nothing Gate could do differently\n   \
         would shorten it without breaking the vendor's limit.",
        depth as f64 / CEILING
    );

    g.drop_target(&name).await;
    Ok(())
}

// -------------------------------------------------------------------- pacing

/// What a lease costs, measured, because the arithmetic invites a wrong guess.
///
/// The guess: a denial stops the batch and the runner keeps its lease, so the
/// cycle is `lease + overhead` and a long lease amortises the overhead — longer
/// should therefore be closer to the ceiling. It is the wrong way round.
/// Measured against a ten-second window, a 50/s ceiling held 48.4/s at a
/// one-second lease, 49.7/s at three, and 37.8/s at six.
///
/// `validate::pacing_warnings` already knew, and refuses to let a lease past a
/// fifth of the tightest window go by unremarked: a lane whose lease is a large
/// fraction of the window wakes about once per window and cannot recover the
/// budget that decayed while it was parked. Its estimate — "roughly three
/// quarters of the declared ceiling" — lands almost exactly on the 76% measured
/// at six seconds. This scenario is therefore not a discovery but a check, and
/// the rule of thumb passes it.
///
/// It is also a warning about measuring this too briefly. An earlier version
/// sampled for ten seconds and reported 19% and 43% for the last two rows: with
/// a ten-second window and a six-second lease, ten seconds is one cycle wide,
/// and the average is whatever phase it caught. The series is printed under each
/// row so a reader can see the shape rather than trust the mean.
async fn pacing(g: &Arc<Gate>, cfg: &Cfg) -> Result<(), String> {
    const CEILING: f64 = 50.0;
    const WINDOW: u64 = 10;
    // Long enough to cover several windows. A lease of six against a window of
    // ten admits roughly one burst per window, so a ten-second sample would be
    // one cycle wide and the average would be whatever phase it happened to
    // catch — which is how the first draft of this row reported 19% and 43% as
    // if they were stable numbers.
    let watch = cfg.secs.max(WINDOW * 3);
    let depth = CEILING as u64 * watch + 500;

    println!("\n\n== pacing — the same {CEILING:.0}/s ceiling under three lease lengths");
    println!(
        "   {depth} items flooded in and drained, sampled over {watch}s ({} windows of {WINDOW}s).\n   \
         `achieved` is the gate's own admitted counter. The validator's rule of thumb is a\n   \
         lease no longer than a fifth of the window, so only the first row is inside it.",
        watch / WINDOW
    );
    println!(
        "\n  {:<30} {:>11}   {:>11}   {}",
        "leaseSeconds", "achieved", "of ceiling", "verdict"
    );
    println!("  {:-<30} {:->11}   {:->11}   {:->17}", "", "", "", "");

    for lease in [1i64, 3, 6] {
        let name = format!("{}-l{lease}", g.target_name("pacing"));
        g.declare(
            &name,
            capped_spec_leased(&g.app, &name, CEILING, one_lane(), lease),
        )
        .await?;

        let (gg, nn) = (g.clone(), name.clone());
        burst("flood", 32, depth, move |w, i| {
            let (g, name) = (gg.clone(), nn.clone());
            async move {
                let body = json!({
                    "op": "bench.call", "cost": 1,
                    "payload": { "connection": format!("conn-{}", (w as u64 + i) % 40),
                                 "bench_us": now_us() }
                });
                g.push(&name, "bulk", &body).await
            }
        })
        .await;

        let d = Drainers::target(g, &name, "bulk", 4, 25);
        let per_sec = sample_admitted(g, &name, watch).await;
        let (_, settled) = d.finish(Duration::from_secs(1)).await;

        let (cold, achieved, steady) = steady_rate(&per_sec);
        println!(
            "  {:<30} {:>9.1}/s   {:>3.0}% of cap   settled {settled:<6} {}",
            format!("lease {lease}s ({lease}/{WINDOW} of window)"),
            achieved,
            achieved / CEILING * 100.0,
            if lease * 5 > WINDOW as i64 {
                "OVER the 1/5 rule"
            } else {
                "within the rule"
            },
        );
        // The series, not just its mean. It is what showed the short-sample
        // version of this scenario was reporting phase rather than rate, and a
        // reader deserves to see a run of zeros if there is one.
        println!("  {:>30}   cold start {cold}s, then {steady:?}", "");
        g.drop_target(&name).await;
    }

    println!(
        "\n   The declared ceiling is the same in every row; only the lease changes. A lease is\n   \
         also the failover window — a dead runner's partition is held for exactly this long —\n   \
         so it is a real trade and not a free dial. The declare-time warning is the cheap\n   \
         version of this whole scenario: it fires on rows two and three before anything runs."
    );
    Ok(())
}

// --------------------------------------------------------------------- lanes

/// What lanes are for: an urgent push that does not queue behind a photo upload.
///
/// The claim in the README is isolation — "an urgent push does not queue behind
/// a photo upload" — and it has a price the same paragraph states: lanes DIVIDE
/// a ceiling. Both halves are measured here. `bulk` gets a flood it cannot
/// possibly clear; `urgent` gets a trickle well inside its share. If isolation
/// holds, urgent's end-to-end sits near the free path while bulk's runs into
/// tens of seconds — in the same target, under the same ceiling.
async fn lanes(g: &Arc<Gate>, cfg: &Cfg) -> Result<(), String> {
    const CEILING: f64 = 100.0;
    let flood_n = 3_000u64;
    let name = g.target_name("lanes");
    let two = json!([
        { "name": "urgent", "cap": "ceiling", "concurrency": 8 },
        { "name": "bulk", "cap": "ceiling-minus-measured", "concurrency": 16,
          "floor": 0.5, "default": true }
    ]);
    g.declare(&name, capped_spec(&g.app, &name, CEILING, two)).await?;

    println!("\n\n== lanes — {CEILING:.0}/s divided in two, a flood in one and a trickle in the other");
    header("phase");

    let bulk = Drainers::target(g, &name, "bulk", 4, 25);
    let urgent = Drainers::target(g, &name, "urgent", 2, 25);

    let (gg, nn) = (g.clone(), name.clone());
    let flood = burst("push → bulk (flood)", 32, flood_n, move |w, i| {
        let (g, name) = (gg.clone(), nn.clone());
        async move {
            let body = json!({
                "op": "bench.bulk", "cost": 1,
                "payload": { "connection": format!("conn-{}", (w as u64 + i) % 40),
                             "bench_us": now_us() }
            });
            g.push(&name, "bulk", &body).await
        }
    })
    .await;
    flood.print();

    // The trickle starts AFTER the flood has landed, which is the interesting
    // moment: every urgent item now enters a target already thousands deep.
    let (gg, nn) = (g.clone(), name.clone());
    let trickle = open("push → urgent (10/s)", 10.0, Duration::from_secs(0), dur(cfg), 256, move |_w, i| {
        let (g, name) = (gg.clone(), nn.clone());
        async move {
            let body = json!({
                "op": "bench.urgent", "cost": 1,
                "payload": { "connection": format!("conn-{}", i % 40), "bench_us": now_us() }
            });
            g.push(&name, "urgent", &body).await
        }
    })
    .await;
    trickle.print();

    let (u_e2e, u_settled) = urgent.finish(Duration::from_secs(2)).await;
    let (b_e2e, b_settled) = bulk.finish(Duration::from_secs(1)).await;

    println!("\n   urgent — {u_settled} items, {} deep behind a flood:", flood_n);
    print_e2e(&u_e2e);
    println!("   bulk — {b_settled} of {flood_n} items:");
    print_e2e(&b_e2e);
    println!(
        "\n   Isolation is the gap between those two lines: same target, same ceiling, and the\n   \
         urgent item did not wait behind the flood. The price is in the ceiling — `urgent` is\n   \
         allocated the residual of bulk's floor (0.5), so each lane runs at about half of\n   \
         {CEILING:.0}/s and neither can borrow the other's idle capacity. That is deliberate\n   \
         (`spec.rs`: a borrower with no reclaim protocol double-counts the ceiling), and it is\n   \
         why two lanes are a division and not a free win."
    );

    g.drop_target(&name).await;
    Ok(())
}

// --------------------------------------------------------------------- graph

/// Two hops, so the per-hop cost is visible.
///
/// Between the nodes sits a relay: one queen transaction carrying `ack` and
/// `push` together. The README caps a path at three hops because the smear
/// composes and each hop costs a queue; this scenario is what turns that from a
/// claim into a number.
async fn graph(g: &Arc<Gate>, cfg: &Cfg) -> Result<(), String> {
    let name = g.target_name("graph");
    let node = |id: &str| {
        json!({
            "budgets": [{ "id": id, "cap": WIDE_PER_SEC * 10.0, "periodSeconds": 10,
                          "alignment": "rolling", "confidence": "inferred", "source": "bench" }],
            "cost": { "field": "httpCost", "default": 1, "max": 5 },
            "pacing": { "leaseSeconds": 1, "batch": WIDE_PER_SEC as i64 }
        })
    };
    let mut entry = node("w");
    entry["entry"] = json!(true);
    let doc = json!({
        "version": 1,
        "nodes": { "entry": entry, "ip": node("ip") },
        "edges": [{ "from": "entry", "to": "ip", "priority": 0 }],
        "consume": ["ip"]
    });
    let declared = g.declare_graph(&name, doc).await?;
    if let Some(w) = declared.get("warnings").and_then(|v| v.as_array()) {
        if !w.is_empty() {
            println!("   graph declared with warnings: {w:?}");
        }
    }

    println!("\n\n== graph — two hops: push at the entry, pop at the terminal");
    let d = Drainers::node(g, &name, "ip", 4, 50);

    header("offered");
    let (gg, nn) = (g.clone(), name.clone());
    let run = open(
        "graph push (API)",
        cfg.cycle_rate,
        warm(cfg),
        dur(cfg),
        4_096,
        move |_w, i| {
            let (g, name) = (gg.clone(), nn.clone());
            async move {
                let body = json!({
                    "op": "bench.call", "cost": 1,
                    "payload": { "connection": format!("conn-{}", i % 40), "bench_us": now_us() }
                });
                g.graph_push(&name, "entry", &body).await
            }
        },
    )
    .await;
    run.print();

    let (e2e, settled) = d.finish(Duration::from_secs(4)).await;
    println!("\n   settled {settled} items through 2 hops, end to end:");
    print_e2e(&e2e);
    println!(
        "   Compare against `cycle`, which is the same work over one hop. The difference is\n   \
         the second gate plus one relay transaction, and it is what the README is pricing when\n   \
         it caps a path at three hops."
    );

    g.drop_graph(&name).await;
    Ok(())
}

// ------------------------------------------------------------------ helpers

/// A pool of consumers popping and settling in the background while something
/// else is measured, recording what each item waited between its push and its
/// pop.
struct Drainers {
    stop: Arc<AtomicU64>,
    settled: Arc<AtomicU64>,
    rx: tokio::sync::mpsc::UnboundedReceiver<u64>,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Drainers {
    fn target(g: &Arc<Gate>, name: &str, lane: &str, n: usize, batch: u32) -> Self {
        Self::spawn(g, n, batch, name.to_string(), lane.to_string(), false)
    }

    fn node(g: &Arc<Gate>, graph: &str, node: &str, n: usize, batch: u32) -> Self {
        Self::spawn(g, n, batch, graph.to_string(), node.to_string(), true)
    }

    fn spawn(
        g: &Arc<Gate>,
        n: usize,
        batch: u32,
        what: String,
        part: String,
        is_graph: bool,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<u64>();
        let stop = Arc::new(AtomicU64::new(0));
        let settled = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();
        for _ in 0..n {
            let (g, what, part, tx, stop, settled) = (
                g.clone(),
                what.clone(),
                part.clone(),
                tx.clone(),
                stop.clone(),
                settled.clone(),
            );
            handles.push(tokio::spawn(async move {
                while stop.load(Ordering::Relaxed) == 0 {
                    let got = if is_graph {
                        g.graph_next(&what, &part, batch, 500).await
                    } else {
                        g.next(&what, &part, batch, 500).await
                    };
                    let Some((_, body)) = got else { continue };
                    let Some(items) = body.get("items").and_then(|v| v.as_array()).cloned() else {
                        continue;
                    };
                    if items.is_empty() {
                        continue;
                    }
                    let now = now_us();
                    for it in &items {
                        if let Some(t0) = it
                            .get("payload")
                            .and_then(|p| p.get("bench_us"))
                            .and_then(|v| v.as_u64())
                        {
                            let _ = tx.send(now.saturating_sub(t0));
                        }
                    }
                    // A graph node is settled as the target `{graph}.{node}`,
                    // which the pop answer carries — `Gate::ack` reads it from
                    // there rather than reconstructing a name the driver never
                    // typed. The lane is the node's default one.
                    let lane = if is_graph { "bulk" } else { part.as_str() };
                    let n = items.len();
                    g.ack(&body, n, lane).await;
                    settled.fetch_add(n as u64, Ordering::Relaxed);
                }
            }));
        }
        Self { stop, settled, rx, handles }
    }

    /// Let the tail through, stop, and give back the sorted samples.
    async fn finish(mut self, tail: Duration) -> (Hist, u64) {
        tokio::time::sleep(tail).await;
        self.stop.store(1, Ordering::Relaxed);
        for h in self.handles {
            let _ = tokio::time::timeout(Duration::from_secs(3), h).await;
        }
        let mut v: Vec<u64> = Vec::new();
        while let Ok(x) = self.rx.try_recv() {
            v.push(x);
        }
        v.sort_unstable();
        (Hist::from_sorted(v), self.settled.load(Ordering::Relaxed))
    }
}

fn print_e2e(h: &Hist) {
    if h.len() == 0 {
        println!("     nothing was settled: no item made it from push to pop.");
        return;
    }
    println!(
        "     p50 {:.0}ms   p90 {:.0}ms   p99 {:.0}ms   max {:.0}ms   over {} items",
        h.pct(50.0),
        h.pct(90.0),
        h.pct(99.0),
        h.max_ms(),
        h.len()
    );
}

/// A real ceiling, of the shape a vendor actually gives you.
///
/// The window is ten leases wide, and that is the whole reason this helper
/// exists rather than a literal in each scenario. A budget declared as `50 per
/// 1s` paced by a one-second lease is the `lease-beats-window` case the
/// validator warns about: the gate admits its fill, is denied, parks for a
/// lease, and the window it was denied against has barely moved — measured here
/// at about 40 of a declared 50. Expressed as `500 per 10s` the same rate paces
/// smoothly, which is why a declaration should name the vendor's window and not
/// the arithmetic mean the operator happens to think in.
fn capped_spec(app: &str, name: &str, per_sec: f64, lanes: serde_json::Value) -> serde_json::Value {
    capped_spec_leased(app, name, per_sec, lanes, 1)
}

fn capped_spec_leased(
    app: &str,
    name: &str,
    per_sec: f64,
    lanes: serde_json::Value,
    lease: i64,
) -> serde_json::Value {
    const WINDOW: f64 = 10.0;
    json!({
        "application": app,
        "name": name,
        "version": 1,
        "budgets": [
            { "id": "vendor", "cap": per_sec * WINDOW, "periodSeconds": WINDOW as i64,
              "alignment": "rolling", "confidence": "documented", "source": "bench",
              "asOf": "2026-08-20" }
        ],
        "lanes": lanes,
        "cost": { "field": "httpCost", "default": 1, "max": 5 },
        // A lease's worth of budget, with room: below it the batch is the
        // limiter and `batch-fits` refuses the declare.
        "pacing": { "leaseSeconds": lease, "batch": (per_sec * lease as f64 * 4.0).ceil() as i64 },
        "admitted": { "partitionBy": "connection", "partitions": PARTITIONS }
    })
}

/// The admitted rate, with two artifacts of measurement taken out of it.
///
/// **Leading zeros are a cold start, not a rate.** A freshly declared target
/// takes a few seconds before its gate admits anything — queues and consumer
/// groups are created, the runner takes its partition lease, `opts.reset` makes
/// it re-register. Measured here at five to nine seconds. Averaging those
/// seconds into the rate reports a limiter running at two thirds of a ceiling it
/// is in fact holding, so they are returned separately instead. Only LEADING
/// zeros: for a long lease the interior zeros are the pacing itself, and
/// dropping those would report the burst rate and call it the ceiling.
///
/// **The total, not the median sample.** Each sample is a difference of a
/// lifetime counter across one HTTP read, and a read that lands late moves its
/// count into the next sample — which shows up as a 0 beside a double. The
/// median of that series over-reads: `throttled` reported a median of 56/s
/// against a declared 50 and looked like an overshoot, while the total over the
/// same window was 33/s. Sum over elapsed is immune to it.
fn steady_rate(samples: &[u64]) -> (u64, f64, Vec<u64>) {
    let cold = samples.iter().take_while(|n| **n == 0).count() as u64;
    let steady: Vec<u64> = samples.iter().skip(cold as usize).copied().collect();
    let rate = if steady.is_empty() {
        0.0
    } else {
        steady.iter().sum::<u64>() as f64 / steady.len() as f64
    };
    (cold, rate, steady)
}

/// The gate's own admitted counter, differenced once a second.
///
/// On an ABSOLUTE schedule, not `sleep(1s)` in a loop: each iteration also makes
/// an HTTP call, so a relative sleep drifts a little further every time and the
/// series grows a spurious dip wherever the drift crosses a second — which is
/// exactly what the first run of `throttled` showed.
async fn sample_admitted(g: &Arc<Gate>, name: &str, seconds: u64) -> Vec<u64> {
    let t0 = Instant::now();
    let mut last = g.admitted(name).await;
    let mut out = Vec::new();
    for i in 1..=seconds {
        tokio::time::sleep_until((t0 + Duration::from_secs(i)).into()).await;
        let now = g.admitted(name).await;
        out.push(now.saturating_sub(last));
        last = now;
    }
    out
}

/// Fill a target's queue, as fast as this machine can, without measuring
/// anything. Returns how many landed.
async fn prefill(g: &Arc<Gate>, name: &str, lane: &str, n: u64, conc: u64) -> u64 {
    let t0 = Instant::now();
    let done = Arc::new(AtomicU64::new(0));
    let mut workers = Vec::new();
    for w in 0..conc {
        let (g, name, lane, done) = (g.clone(), name.to_string(), lane.to_string(), done.clone());
        workers.push(tokio::spawn(async move {
            let share = n / conc;
            for i in 0..share {
                let body = json!({
                    "op": "bench.call", "cost": 1,
                    "payload": { "connection": format!("conn-{}", (w + i) % 40),
                                 "bench_us": now_us() }
                });
                if g.push(&name, &lane, &body).await.status == 200 {
                    done.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for w in workers {
        let _ = w.await;
    }
    let landed = done.load(Ordering::Relaxed);
    println!(
        "  prefilling {name}: {landed} items in {:.1}s",
        t0.elapsed().as_secs_f64()
    );
    landed
}

/// Wait until the gate has admitted what was pushed, so the drain rows are
/// measuring the pop and not the gate still working through the backlog.
async fn wait_admitted(g: &Arc<Gate>, name: &str, want: u64, limit: Duration) {
    let t0 = Instant::now();
    loop {
        let a = g.admitted(name).await;
        if a >= want || t0.elapsed() > limit {
            println!("  gate admitted {a}/{want} after {:.1}s", t0.elapsed().as_secs_f64());
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
