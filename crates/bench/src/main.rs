//! What Gate itself costs, per use case.
//!
//! This is not [`gate-e2e`]. That one asks whether the ceiling holds — whether a
//! graph declared at 50/s admits 50/s — and a limiter that admits everything
//! fails it. This one asks the opposite question: with the limiter deliberately
//! kept out of the way, how much does the machinery move, and what does a caller
//! wait?
//!
//! The distinction runs through every number below, because Gate has TWO
//! latencies and confusing them makes both meaningless:
//!
//! * **API latency** — how long a `push` takes on the wire. This is Gate's own
//!   performance. A push is one queen append. It should be flat whether the
//!   graph is idle or a hundred thousand items deep, and a run that shows
//!   otherwise has found something.
//!
//! * **Admission latency** — how long an item waits between being pushed and
//!   arriving on the egress queue. This is NOT performance: it is the declared
//!   ceiling doing its job. An item that arrives nine hundred deep behind a 50/s
//!   ceiling waits eighteen seconds because it was told to.
//!
//! Quoting the second as if it were the first is how a rate limiter gets
//! reported as slow. So `push` and `drain` measure the API with the ceiling out
//! of the way, and `throttled` measures the wait with the ceiling doing exactly
//! what it was declared to do.
//!
//! Three scenarios are new in v2 and each one answers an acceptance criterion:
//!
//! * `throughput` — the reason the rewrite is worth doing. The floor is **2.8k
//!   items/s**, which is what the old counter-funnel relay reached on a 32-core
//!   VM with tuple lock waits at 96–100%; the target is **10k at batch 200**.
//!   The batch sweep is in the output so the batching effect is visible and not
//!   asserted.
//! * `contention` — N stages charging ONE shared counter. `kv.incr` on one key
//!   was measured at 33k/s, and the budget is charged once per batch, so this
//!   should be flat.
//! * `idle` — the reason the rewrite is worth doing, from the other side. In
//!   prod the old design made ~275,000 "is there work?" calls an hour to move
//!   messages 963 times. An idle graph here should cost parked timers and
//!   approximately nothing else.
//!
//! ```text
//! cargo run --release -p gate-bench -- all
//! cargo run --release -p gate-bench -- throughput,idle
//! ```

mod load;
mod world;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;

use load::{burst, closed, header, open, Hist};
use world::{capped_doc, now_us, shared_doc, wide_doc, Gate};

/// The ceiling the "get out of the way" scenarios declare. Far above anything a
/// laptop moves, so the counter admits on the first ask and the number measures
/// the machinery instead of the pacing.
const WIDE_PER_SEC: i64 = 200_000;

struct Cfg {
    secs: u64,
    warmup: u64,
    backlog: u64,
    idle_secs: u64,
    conc: Vec<usize>,
    batches: Vec<u32>,
}

fn env_u64(k: &str, d: u64) -> u64 {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(d)
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
    let queen_url = std::env::var("QUEEN_URL").unwrap_or_else(|_| "http://127.0.0.1:6632".into());
    let which = std::env::args().nth(1).unwrap_or_else(|| "all".into());

    let cfg = Cfg {
        secs: env_u64("BENCH_SECONDS", 10),
        warmup: env_u64("BENCH_WARMUP", 3),
        backlog: env_u64("BENCH_BACKLOG", 30_000),
        idle_secs: env_u64("BENCH_IDLE_SECONDS", 60),
        conc: std::env::var("BENCH_CONC")
            .ok()
            .map(|v| v.split(',').filter_map(|s| s.trim().parse().ok()).collect())
            .unwrap_or_else(|| vec![1, 8, 32, 128]),
        batches: std::env::var("BENCH_BATCHES")
            .ok()
            .map(|v| v.split(',').filter_map(|s| s.trim().parse().ok()).collect())
            .unwrap_or_else(|| vec![1, 25, 200, 500]),
    };

    let g = Arc::new(Gate::new(base.clone(), app.clone(), queen_url.clone()));
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
    println!("   queen           {queen_url}");
    println!(
        "   application     {app}   (graphs are suffixed `-{}`)",
        g.run_id
    );
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
                for one in [
                    "health",
                    "push",
                    "drain",
                    "throughput",
                    "contention",
                    "throttled",
                    "idle",
                ] {
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
        "throughput" => throughput(g, cfg).await,
        "contention" => contention(g, cfg).await,
        "throttled" => throttled(g, cfg).await,
        "idle" => idle(g, cfg).await,
        other => Err(format!(
            "unknown scenario `{other}`: one of health, push, drain, throughput, contention, \
             throttled, idle, all"
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
/// this report sits on top of it.
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

// ------------------------------------------------------------------- the API

/// The front door: one queen append, with the ceiling far out of the way.
async fn push(g: &Arc<Gate>, cfg: &Cfg) -> Result<(), String> {
    let name = g.graph_name("push");
    let out = g.egress("push");
    g.declare(&name, wide_doc(&out, WIDE_PER_SEC, 200)).await?;

    println!("\n\n== push — what a caller waits for the door, with the limiter out of the way");
    header("closed loop");
    let mut best = 0.0f64;
    for c in &cfg.conc {
        let (gg, n) = (g.clone(), name.clone());
        let run = closed("POST …/push", *c, warm(cfg), dur(cfg), move |w, i| {
            let (g, n) = (gg.clone(), n.clone());
            async move {
                g.push(
                    &n,
                    "n",
                    &json!({ "op": "bench.call",
                                 "partition": format!("c{}", (w as u64 + i) % 64),
                                 "payload": { "at": now_us() } }),
                )
                .await
            }
        })
        .await;
        best = best.max(run.rps());
        run.print();
    }

    // Half the saturation rate, on a schedule. Quoting a latency from the
    // saturation run is the classic way to publish a number nobody can
    // reproduce.
    println!();
    header("open loop");
    for share in [0.25f64, 0.5] {
        let rate = (best * share).max(1.0);
        let (gg, n) = (g.clone(), name.clone());
        let run = open(
            &format!("POST …/push @{:.0}%", share * 100.0),
            rate,
            warm(cfg),
            dur(cfg),
            4096,
            move |w, i| {
                let (g, n) = (gg.clone(), n.clone());
                async move {
                    g.push(
                        &n,
                        "n",
                        &json!({ "op": "bench.call",
                                 "partition": format!("c{}", (w as u64 + i) % 64),
                                 "payload": { "at": now_us() } }),
                    )
                    .await
                }
            },
        )
        .await;
        run.print();
    }

    g.drop_graph(&name).await;
    Ok(())
}

/// The other end: the application's own SDK popping the egress queue. Gate is
/// not in this loop at all, which is the point — and the row is here so a slow
/// end-to-end number can be attributed.
async fn drain(g: &Arc<Gate>, cfg: &Cfg) -> Result<(), String> {
    let name = g.graph_name("drain");
    let out = g.egress("drain");
    g.declare(&name, wide_doc(&out, WIDE_PER_SEC, 200)).await?;

    println!("\n\n== drain — the caller's own SDK against the egress queue. No Gate in this loop");
    let filled = prefill(g, &name, cfg.backlog).await;
    println!("   prefilled {filled} items and waited for them to be admitted");

    header("consumers");
    for c in &cfg.conc {
        let (gg, q) = (g.clone(), out.clone());
        let group = format!("bench-drain-{}", c);
        let run = closed(
            "pop(batch 100)",
            *c,
            Duration::from_secs(1),
            dur(cfg),
            move |_w, _i| {
                let (g, q, group) = (gg.clone(), q.clone(), group.clone());
                async move { g.drain(&q, &group, 100, 500).await }
            },
        )
        .await;
        run.print();
    }

    g.drop_graph(&name).await;
    Ok(())
}

// ------------------------------------------------------------ the throughput

/// One stage, one node, one budget large enough not to bind — and the batch
/// swept, because the budget is charged ONCE per batch and that is the whole
/// arithmetic.
///
/// Two DB round trips per batch: one KV call and one transaction commit. At
/// batch 200 that is one of each per two hundred items, which is why 10k
/// items/s is a target and not a hope.
async fn throughput(g: &Arc<Gate>, cfg: &Cfg) -> Result<(), String> {
    println!("\n\n== throughput — items/s end to end, swept over the batch");
    println!(
        "   floor 2.8k items/s (the old counter-funnel relay's ceiling on a 32-core VM, with\n   \
         tuple lock waits at 96-100%); target 10k at batch 200."
    );
    println!(
        "\n  {:<12} {:>12} {:>12} {:>14} {:>16}",
        "batch", "pushed/s", "admitted/s", "items/commit", "verdict"
    );
    println!(
        "  {:-<12} {:->12} {:->12} {:->14} {:->16}",
        "", "", "", "", ""
    );

    for batch in cfg.batches.clone() {
        let name = format!("{}-b{batch}", g.graph_name("thr"));
        let out = format!("{}.b{batch}", g.egress("thr"));
        g.declare(&name, wide_doc(&out, WIDE_PER_SEC, batch))
            .await?;

        // Flood first, then measure what came out. A closed-loop push and a
        // closed-loop drain competing for the same cores would measure the
        // driver, so the two halves are separated in time.
        let pushed = burst("flood", 64, cfg.backlog, {
            let (gg, n) = (g.clone(), name.clone());
            move |w, i| {
                let (g, n) = (gg.clone(), n.clone());
                async move {
                    g.push(
                        &n,
                        "n",
                        &json!({ "op": "bench.call",
                                 "partition": format!("c{}", (w as u64 + i) % 16),
                                 "payload": { "at": now_us() } }),
                    )
                    .await
                }
            }
        })
        .await;

        let (a0, f0, c0) = g.stages(&name).await;
        let t0 = Instant::now();
        // Drain while the stage works, so the egress queue does not become the
        // thing that is full.
        let stop = Arc::new(AtomicU64::new(0));
        let mut drains = Vec::new();
        for i in 0..8 {
            let (gg, q, stop) = (g.clone(), out.clone(), stop.clone());
            let group = format!("bench-thr-{i}");
            drains.push(tokio::spawn(async move {
                while stop.load(Ordering::Relaxed) == 0 {
                    let _ = gg.drain(&q, &group, 200, 300).await;
                }
            }));
        }
        tokio::time::sleep(dur(cfg)).await;
        stop.store(1, Ordering::Relaxed);
        for d in drains {
            let _ = tokio::time::timeout(Duration::from_secs(3), d).await;
        }

        let (a1, f1, c1) = g.stages(&name).await;
        let elapsed = t0.elapsed().as_secs_f64();
        let admitted = (a1 - a0) as f64 / elapsed;
        let per_commit = if c1 > c0 {
            (f1 - f0) as f64 / (c1 - c0) as f64
        } else {
            0.0
        };
        // `forwarded / commits` within 10% of the batch is the direct evidence
        // the batching is real. It is not asserted where the budget is not the
        // constraint and the queue simply runs dry — a stage that finds three
        // items commits three.
        let verdict = if admitted >= 10_000.0 {
            "TARGET"
        } else if admitted >= 2_800.0 {
            "above floor"
        } else {
            "BELOW FLOOR"
        };
        println!(
            "  {:<12} {:>12.0} {:>12.0} {:>14.1} {:>16}",
            batch,
            pushed.rps(),
            admitted,
            per_commit,
            verdict
        );
        g.drop_graph(&name).await;
    }
    Ok(())
}

/// N stages, one shared counter.
///
/// `kv.incr` on one key was measured at 33k/s, and the budget is charged once
/// per BATCH — at batch 200 and 34k items/s the counter sees 170 incr/s. So this
/// table should be flat, and a row that falls away with the stage count has
/// found the serialization point the old counter-funnel had and this design
/// claims not to.
async fn contention(g: &Arc<Gate>, cfg: &Cfg) -> Result<(), String> {
    println!("\n\n== contention — N stages charging ONE shared kv counter");
    println!(
        "\n  {:<12} {:>12} {:>14} {:>12}",
        "stages", "admitted/s", "per stage/s", "incr/s"
    );
    println!("  {:-<12} {:->12} {:->14} {:->12}", "", "", "", "");

    for stages in [1usize, 2, 4, 8] {
        let name = format!("{}-s{stages}", g.graph_name("cont"));
        let out = format!("{}.s{stages}", g.egress("cont"));
        g.declare(&name, shared_doc(&out, stages, WIDE_PER_SEC, 200))
            .await?;

        let per_node = cfg.backlog / stages as u64;
        for i in 0..stages {
            let node = format!("n{i}");
            let _ = burst("flood", 32, per_node, {
                let (gg, n, node) = (g.clone(), name.clone(), node.clone());
                move |w, j| {
                    let (g, n, node) = (gg.clone(), n.clone(), node.clone());
                    async move {
                        g.push(
                            &n,
                            &node,
                            &json!({ "op": "bench.call",
                                     "partition": format!("c{}", (w as u64 + j) % 8),
                                     "payload": { "at": now_us() } }),
                        )
                        .await
                    }
                }
            })
            .await;
        }

        let (a0, _, c0) = g.stages(&name).await;
        let t0 = Instant::now();
        let stop = Arc::new(AtomicU64::new(0));
        let mut drains = Vec::new();
        for i in 0..8 {
            let (gg, q, stop) = (g.clone(), out.clone(), stop.clone());
            let group = format!("bench-cont-{i}");
            drains.push(tokio::spawn(async move {
                while stop.load(Ordering::Relaxed) == 0 {
                    let _ = gg.drain(&q, &group, 200, 300).await;
                }
            }));
        }
        tokio::time::sleep(dur(cfg)).await;
        stop.store(1, Ordering::Relaxed);
        for d in drains {
            let _ = tokio::time::timeout(Duration::from_secs(3), d).await;
        }

        let (a1, _, c1) = g.stages(&name).await;
        let elapsed = t0.elapsed().as_secs_f64();
        let admitted = (a1 - a0) as f64 / elapsed;
        // One charge per commit, so commits/s IS the rate the shared row sees.
        let incrs = (c1 - c0) as f64 / elapsed;
        println!(
            "  {:<12} {:>12.0} {:>14.0} {:>12.0}",
            stages,
            admitted,
            admitted / stages as f64,
            incrs
        );
        g.drop_graph(&name).await;
    }
    Ok(())
}

// ------------------------------------------------------------- the admission

/// The ceiling doing its job. This row is NOT performance.
async fn throttled(g: &Arc<Gate>, cfg: &Cfg) -> Result<(), String> {
    const PER_SEC: i64 = 200;
    let name = g.graph_name("throttled");
    let out = g.egress("throttled");
    g.declare(&name, capped_doc(&out, PER_SEC * 10, 10_000, 200))
        .await?;

    println!("\n\n== throttled — the WAIT, at a ceiling of {PER_SEC}/s over a ten-second window");
    println!(
        "   This is admission latency, not API latency: an item nine hundred deep behind a\n   \
         {PER_SEC}/s ceiling waits four and a half seconds because it was told to."
    );

    let n = cfg.backlog.min(4_000);
    let pushed = burst("flood", 32, n, {
        let (gg, nm) = (g.clone(), name.clone());
        move |w, i| {
            let (g, nm) = (gg.clone(), nm.clone());
            async move {
                g.push(
                    &nm,
                    "n",
                    &json!({ "op": "bench.call",
                             "partition": format!("c{}", (w as u64 + i) % 16),
                             "payload": { "at": now_us() } }),
                )
                .await
            }
        }
    })
    .await;
    header("the flood itself");
    pushed.print();

    // Read the wait off the payload: the driver stamped the clock at push and
    // reads it at pop, and it is the same clock because it is the same process.
    let mut waits = Hist::default();
    let deadline = Instant::now() + Duration::from_secs(cfg.secs.max(20));
    let mut got = 0u64;
    while got < n && Instant::now() < deadline {
        let msgs = g
            .queen
            .queue(&out)
            .group("bench-throttled")
            .subscription_mode(queen_mq::SubscriptionMode::All)
            .batch(200)
            .partitions(32)
            .wait(true)
            .poll_timeout(Duration::from_millis(500))
            .pop_auto_ack()
            .await
            .unwrap_or_default();
        for m in &msgs {
            if let Some(at) = m.data.get("at").and_then(|v| v.as_u64()) {
                waits.push(now_us().saturating_sub(at));
            }
            got += 1;
        }
    }
    print_e2e(&waits, got, PER_SEC);

    g.drop_graph(&name).await;
    Ok(())
}

fn print_e2e(h: &Hist, got: u64, per_sec: i64) {
    let mut h = h.clone();
    h.sorted();
    println!(
        "\n  {:<22} {:>10} {:>10} {:>10} {:>10}",
        "admission latency", "p50", "p90", "p99", "max"
    );
    println!(
        "  {:-<22} {:->10} {:->10} {:->10} {:->10}",
        "", "", "", "", ""
    );
    println!(
        "  {:<22} {:>9.0}ms {:>9.0}ms {:>9.0}ms {:>9.0}ms",
        format!("{got} items"),
        h.pct(50.0),
        h.pct(90.0),
        h.pct(99.0),
        h.max_ms()
    );
    println!(
        "\n   Expected, not a defect: {got} items at {per_sec}/s is about {:.0}s of queue, and the\n   \
         last item waits all of it.",
        got as f64 / per_sec as f64
    );
}

// ------------------------------------------------------------------- the idle

/// What a declared graph costs while nothing is happening.
///
/// The number this exists to replace: **~275,000 "is there work?" calls an hour**
/// in prod, to move messages 963 times — 285 polls per relay. Here an idle stage
/// is a parked long-poll that releases its pooled PG connection before parking
/// and is woken by the push notifier, so the expected answer is roughly one
/// re-park per stage per poll timeout and nothing else.
///
/// Measured off the BROKER's own lifetime request counter, which counts every
/// client. With nothing else pointed at it, the delta is what Gate asked — and
/// the caveat is printed rather than assumed away.
async fn idle(g: &Arc<Gate>, cfg: &Cfg) -> Result<(), String> {
    let name = g.graph_name("idle");
    let out = g.egress("idle");

    // Seven stages, the size of the flagship graph, so the number is comparable
    // to the one it replaces.
    let mut doc = shared_doc(&out, 7, WIDE_PER_SEC, 200);
    doc["version"] = json!(1);
    g.declare(&name, doc).await?;

    println!("\n\n== idle — a declared seven-stage graph, with nothing pushed");
    let Some(before) = g.broker_requests().await else {
        println!("   the broker does not report a request total at /metrics; this scenario cannot");
        println!("   answer, and pg_stat_statements around a quiet window is the way to ask.");
        g.drop_graph(&name).await;
        return Ok(());
    };

    // Let the stages take their first claims, which are real work and not idle
    // cost, then start counting.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let start = g.broker_requests().await.unwrap_or(before);

    println!(
        "   waiting {}s with the graph up and no traffic…",
        cfg.idle_secs
    );
    tokio::time::sleep(Duration::from_secs(cfg.idle_secs)).await;
    let end = g.broker_requests().await.unwrap_or(start);

    let delta = end.saturating_sub(start);
    let per_hour = delta as f64 * 3600.0 / cfg.idle_secs as f64;
    println!(
        "\n  {:<34} {:>12}",
        "broker requests during the window", delta
    );
    println!("  {:<34} {:>12.0}", "extrapolated per hour", per_hour);
    println!("  {:<34} {:>12}", "v1, measured in prod, per hour", 275_000);
    println!(
        "\n   Caveat: this counts every client of the broker, not only Gate. Run it against a\n   \
         broker nothing else is using, or read pg_stat_statements instead."
    );
    println!(
        "   Expected shape: one re-park per stage per poll timeout ({}s), so roughly {:.0} an\n   \
         hour for seven stages — plus one reconcile pass every GATE_RECONCILE_SECONDS.",
        30,
        7.0 * 3600.0 / 30.0
    );

    g.drop_graph(&name).await;
    Ok(())
}

// ------------------------------------------------------------------- helpers

/// Push `n` items and wait for the stage to have admitted them, so a drain
/// scenario measures the pop and not the fill.
async fn prefill(g: &Arc<Gate>, graph: &str, n: u64) -> u64 {
    let run = burst("prefill", 64, n, {
        let (gg, nm) = (g.clone(), graph.to_string());
        move |w, i| {
            let (g, nm) = (gg.clone(), nm.clone());
            async move {
                g.push(
                    &nm,
                    "n",
                    &json!({ "op": "bench.call",
                             "partition": format!("c{}", (w as u64 + i) % 16),
                             "payload": { "at": now_us() } }),
                )
                .await
            }
        }
    })
    .await;

    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        let (admitted, _, _) = g.stages(graph).await;
        if admitted >= run.requests {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    run.requests
}
