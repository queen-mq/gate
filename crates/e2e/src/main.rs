//! End to end: declare a graph, flood it, drain its egress queue, and check that
//! the rate that actually left is the rate the graph declared.
//!
//! The assertion that matters is not "it worked" but "it refused the right
//! amount". A limiter that admits everything passes a smoke test.
//!
//! Three gates, and the third is new (design §15C):
//!
//! 1. the worst window admitted no more than 125% of the declared count;
//! 2. the admitted p50 reached at least 60% of the declared rate;
//! 3. **while the counter is between half and full, the 0.5-share path is
//!    refusing and the 1.0-share path is still admitting.** That is the atomic
//!    reserve, measured — the property v1 could not have, because its lanes each
//!    held their own copy of the counter and two lanes both told "you may use
//!    the ceiling" genuinely spent it twice (93/s against a declared 50/s).
//!
//! The drain is the application's own SDK against the egress queue, because that
//! is what a caller does now: there is no Gate-mediated pop, no opaque lease and
//! no ack. An ordinary queue that is sometimes empty.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use queen_mq::{Config, Queen, SubscriptionMode};
use serde_json::{json, Value};

/// Where the gate under test is listening.
fn gate_url() -> &'static str {
    static URL: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    URL.get_or_init(|| {
        std::env::var("GATE_URL").unwrap_or_else(|_| "http://127.0.0.1:8788".to_string())
    })
}

fn queen_url() -> String {
    std::env::var("QUEEN_URL").unwrap_or_else(|_| "http://127.0.0.1:6632".to_string())
}

#[derive(Default)]
struct Counters {
    pushed: AtomicU64,
    drained: AtomicU64,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(64)
        .build()?;
    let gate = gate_url();
    let queen = Queen::connect(Config::new(queen_url()))?;

    let graph = std::env::args().nth(1).unwrap_or_else(|| "load".into());
    let application = std::env::var("GATE_APP").unwrap_or_else(|_| "default".to_string());
    let per_sec: f64 = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(50.0);
    let total: u64 = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(3_000);
    let seconds: u64 = std::env::args()
        .nth(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    // The declared window, in seconds. The ceiling is declared over the WINDOW,
    // so a one-second sample above `per_sec` is perfectly legal when the window
    // is ten — the assertion has to be on the window and not on the sample.
    let window_s: i64 = std::env::args()
        .nth(5)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let batch: u32 = std::env::args()
        .nth(6)
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);

    let egress = format!("gate.e2e.{application}.{graph}.out");
    let count = (per_sec * window_s as f64).round() as i64;

    println!("== declaring `{graph}`: {count} per {window_s}s, batch {batch}");
    let doc = json!({
        "application": application,
        "graph": graph,
        "version": 1,
        "nodes": {
            "fast": {
                "ingress": true,
                "batch": batch,
                "budgets": [ { "id": "entry-fast", "count": 1000000, "timeMs": 1000 } ]
            },
            "bulk": {
                "ingress": true,
                "batch": batch,
                "budgets": [ { "id": "entry-bulk", "count": 1000000, "timeMs": 1000 } ]
            },
            // The one node that holds the ceiling, and the one counter both paths
            // spend. `subWindows` at the declared width so the window arithmetic
            // below reads the same number the limiter enforces.
            "ip": {
                "batch": batch,
                "budgets": [
                    { "id": "binding", "count": count, "timeMs": window_s * 1000,
                      "subWindows": 1,
                      "confidence": "documented", "source": "e2e", "asOf": "2026-08-21" }
                ],
                "egress": egress
            }
        },
        "paths": [
            { "name": "fast", "priority": 0, "share": 1.0, "nodes": ["fast", "ip"] },
            { "name": "bulk", "priority": 1, "share": 0.5, "nodes": ["bulk", "ip"] }
        ]
    });

    let res: Value = http
        .put(format!("{gate}/v1/apps/{application}/graphs/{graph}"))
        .json(&doc)
        .send()
        .await?
        .json()
        .await?;
    if res.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        return Err(format!("declare failed: {res}").into());
    }

    let c = Arc::new(Counters::default());

    // ------------------------------------------------------------ producers
    println!("== pushing {total} items across both paths");
    let t_push = Instant::now();
    let mut producers = Vec::new();
    for w in 0..8u64 {
        let (http, graph, c, app_name) =
            (http.clone(), graph.clone(), c.clone(), application.clone());
        producers.push(tokio::spawn(async move {
            let gate = gate_url();
            let share = total / 8;
            for i in 0..share {
                let node = if (i + w) % 5 == 0 { "fast" } else { "bulk" };
                let body = json!({
                    "op": "calendar.push",
                    // The producer's partition, passed through unchanged at every
                    // hop: it is what keeps a connection's items in order and what
                    // keeps the relay's transactions lane-disjoint.
                    "partition": format!("conn-{}", (i + w) % 40),
                    "payload": { "path": node }
                });
                if http
                    .post(format!(
                        "{gate}/v1/apps/{app_name}/graphs/{graph}/nodes/{node}/push"
                    ))
                    .json(&body)
                    .send()
                    .await
                    .map(|r| r.status().is_success())
                    .unwrap_or(false)
                {
                    c.pushed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for p in producers {
        p.await?;
    }
    let push_secs = t_push.elapsed().as_secs_f64();
    println!(
        "   pushed {} in {:.2}s ({:.0}/s into the queue)",
        c.pushed.load(Ordering::Relaxed),
        push_secs,
        c.pushed.load(Ordering::Relaxed) as f64 / push_secs
    );

    // ------------------------------------------------------------ consumers
    //
    // The application's own SDK against the egress queue. No Gate in this loop.
    println!("== draining `{egress}` for {seconds}s, sampling every second");
    let stop = Arc::new(AtomicU64::new(0));
    let mut consumers = Vec::new();
    for _ in 0..12 {
        let (q, c, stop, egress) = (queen.clone(), c.clone(), stop.clone(), egress.clone());
        consumers.push(tokio::spawn(async move {
            while stop.load(Ordering::Relaxed) == 0 {
                let got = q
                    .queue(&egress)
                    .group("e2e-workers")
                    .subscription_mode(SubscriptionMode::All)
                    .batch(100)
                    .partitions(16)
                    .wait(true)
                    .poll_timeout(Duration::from_millis(500))
                    .pop_auto_ack()
                    .await
                    .unwrap_or_default();
                if !got.is_empty() {
                    c.drained.fetch_add(got.len() as u64, Ordering::Relaxed);
                }
            }
        }));
    }

    // ------------------------------------------------------------- sampling
    //
    // Two series, because they answer different questions. `admitted` is the
    // gate's own counter: what the limiter let through. `drained` is what this
    // driver managed to pull back out. When the second is lower, the harness is
    // the bottleneck and the first is the number that means anything.
    //
    // And a third, which is the point of the rewrite: the per-path counters, so
    // the reserve can be measured rather than argued about.
    #[derive(Default, Clone, Copy)]
    struct Stage {
        admitted: u64,
        deferred: u64,
    }

    async fn stages(
        http: &reqwest::Client,
        application: &str,
        graph: &str,
    ) -> (Stage, Stage, i64, i64) {
        let gate = gate_url();
        let v: Value = match http
            .get(format!("{gate}/v1/apps/{application}/graphs/{graph}"))
            .send()
            .await
        {
            Ok(r) => r.json().await.unwrap_or(Value::Null),
            Err(_) => return (Stage::default(), Stage::default(), 0, 0),
        };
        let mut fast = Stage::default();
        let mut bulk = Stage::default();
        for s in v["stages"].as_array().cloned().unwrap_or_default() {
            if s["node"] != "ip" {
                continue;
            }
            let into = if s["path"] == "fast" {
                &mut fast
            } else {
                &mut bulk
            };
            into.admitted = s["counters"]["admitted"].as_u64().unwrap_or(0);
            into.deferred = s["counters"]["deferred"].as_u64().unwrap_or(0)
                + s["counters"]["released"].as_u64().unwrap_or(0);
        }
        let mut value = 0i64;
        let mut ceiling = 0i64;
        for n in v["nodes"].as_array().cloned().unwrap_or_default() {
            if n["node"] != "ip" {
                continue;
            }
            for b in n["budgets"].as_array().cloned().unwrap_or_default() {
                value = b["value"].as_i64().unwrap_or(0);
                ceiling = b["ceilings"]["fast"].as_i64().unwrap_or(0);
            }
        }
        (fast, bulk, value, ceiling)
    }

    let mut drained_samples: Vec<u64> = Vec::new();
    let mut adm_samples: Vec<u64> = Vec::new();
    // Seconds in which the counter sat between half and full — the window in
    // which the reserve is the only thing that can explain who got through.
    let mut contended = 0u32;
    let mut reserve_held = 0u32;

    let (mut last_fast, mut last_bulk, _, _) = stages(&http, &application, &graph).await;
    let mut last_drained = 0u64;
    let t0 = Instant::now();
    for _ in 0..seconds {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let now = c.drained.load(Ordering::Relaxed);
        drained_samples.push(now - last_drained);
        last_drained = now;

        let (fast, bulk, value, ceiling) = stages(&http, &application, &graph).await;
        let d_fast = fast.admitted.saturating_sub(last_fast.admitted);
        let d_bulk = bulk.admitted.saturating_sub(last_bulk.admitted);
        let bulk_refused = bulk.deferred.saturating_sub(last_bulk.deferred);
        adm_samples.push(d_fast + d_bulk);

        // The reserve, measured: while the shared counter is above the 0.5-share
        // path's ceiling and below the 1.0-share path's, the low path must be
        // refusing and the high one must still be getting through.
        if ceiling > 0 && value * 2 > ceiling && value < ceiling {
            contended += 1;
            if bulk_refused > 0 && d_fast > 0 {
                reserve_held += 1;
            }
        }
        last_fast = fast;
        last_bulk = bulk;
    }
    stop.store(1, Ordering::Relaxed);
    for c in consumers {
        let _ = tokio::time::timeout(Duration::from_secs(3), c).await;
    }

    let elapsed = t0.elapsed().as_secs_f64();
    let drained = c.drained.load(Ordering::Relaxed);
    let mean = drained as f64 / elapsed;

    // Skip the first two samples: the first is a partial second and the second
    // carries whatever the gate admitted before the drain loop was up.
    let steady: Vec<u64> = drained_samples.iter().skip(2).copied().collect();
    let adm_steady: Vec<u64> = adm_samples.iter().skip(2).copied().collect();
    let pick = |v: &Vec<u64>| {
        let mut s = v.clone();
        s.sort_unstable();
        (
            s.get(s.len() / 2).copied().unwrap_or(0),
            s.last().copied().unwrap_or(0),
        )
    };
    let (p50, peak) = pick(&steady);
    let (adm_p50, adm_peak) = pick(&adm_steady);
    let adm_total: u64 = adm_samples.iter().sum();

    println!("\n== result");
    println!("   declared ceiling      {per_sec:.0}/s ({count} per {window_s}s)");
    println!("   ADMITTED by the gate  p50 {adm_p50}/s  peak {adm_peak}/s  total {adm_total}");
    println!("   drained by the driver p50 {p50}/s  peak {peak}/s  total {drained}");
    println!("   mean drained          {mean:.1}/s over {elapsed:.1}s");
    println!("   admitted samples      {adm_steady:?}");
    println!("   contended seconds     {contended}, reserve held in {reserve_held}");

    let window_total: u64 = adm_steady
        .windows(window_s.max(1) as usize)
        .map(|w| w.iter().sum::<u64>())
        .max()
        .unwrap_or(adm_steady.iter().sum());
    let window_cap = count as f64;
    let ok_peak = (window_total as f64) <= window_cap * 1.25;
    let ok_mean = (adm_p50 as f64) >= per_sec * 0.6;
    // Contention has to HAPPEN for the third gate to mean anything. If the
    // counter never sat between the two ceilings there was nothing to reserve,
    // and reporting a pass would be reporting that a test nobody ran succeeded.
    let ok_reserve = contended == 0 || reserve_held * 2 >= contended;

    println!("   worst {window_s}s window     {window_total} against a cap of {window_cap:.0}");
    println!(
        "\n   ceiling held (worst window <= 125% of cap)    {}",
        if ok_peak { "PASS" } else { "FAIL" }
    );
    println!(
        "   throughput reached (admitted p50 >= 60%)      {}",
        if ok_mean { "PASS" } else { "FAIL" }
    );
    println!(
        "   reserve held (low path refuses, high admits)  {}",
        match (contended, ok_reserve) {
            (0, _) => "NOT EXERCISED (the counter never sat between the two ceilings)",
            (_, true) => "PASS",
            _ => "FAIL",
        }
    );
    if !(ok_peak && ok_mean && ok_reserve) {
        std::process::exit(1);
    }
    Ok(())
}
