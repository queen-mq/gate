//! End to end: declare a target, flood it, drain it, and check that the rate
//! that actually left is the rate the target declared.
//!
//! The assertion that matters is not "it worked" but "it refused the right
//! amount". A limiter that admits everything passes a smoke test.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

const GATE: &str = "http://127.0.0.1:8788";

#[derive(Default)]
struct Counters {
    pushed: AtomicU64,
    drained: AtomicU64,
    acked: AtomicU64,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(64)
        .build()?;

    let target = std::env::args().nth(1).unwrap_or_else(|| "load".into());
    // Which team's target this is. Applications never share a ceiling, so the
    // driver picks one and stays inside it.
    let application =
        std::env::var("GATE_APP").unwrap_or_else(|_| "default".to_string());
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
    // The budget's period, and the batch, are the two knobs the ceiling
    // experiment turns. A period equal to the lease leaves the sliding window
    // no time to decay between wakeups; a batch larger than a lease-window of
    // budget guarantees a denial, and a denial parks the lane for a full lease.
    let period: i64 = std::env::args()
        .nth(5)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let batch: i64 = std::env::args()
        .nth(6)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| (per_sec.ceil() as i64).max(200));

    println!("== declaring `{target}`: {per_sec}/s ceiling over a {period}s window, batch {batch}");
    let spec = json!({
        "application": application,
        "name": target,
        "version": 1,
        "budgets": [
            { "id": "binding", "cap": per_sec * period as f64, "periodSeconds": period,
              "alignment": "rolling",
              "confidence": "documented", "source": "e2e", "asOf": "2026-08-18" }
        ],
        "lanes": [
            { "name": "urgent", "cap": "ceiling", "concurrency": 8 },
            { "name": "bulk", "cap": "ceiling-minus-measured", "concurrency": 16,
              "floor": 0.5, "default": true }
        ],
        "cost": { "field": "httpCost", "default": 1, "max": 5 },
        // The batch must not be the limiter: a lease-window's worth of budget
        // has to fit in one cycle, which is what the `batch-fits` rule checks.
        "pacing": { "leaseSeconds": 1, "batch": batch },
        "admitted": { "partitionBy": "connection", "partitions": 8 }
    });
    let res: Value = http
        .put(format!("{GATE}/v1/apps/{application}/targets/{target}"))
        .json(&spec)
        .send()
        .await?
        .json()
        .await?;
    if res.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        return Err(format!("declare failed: {res}").into());
    }

    let c = Arc::new(Counters::default());

    // ------------------------------------------------------------ producers
    println!("== pushing {total} items across both lanes");
    let t_push = Instant::now();
    let mut producers = Vec::new();
    for w in 0..8u64 {
        let (http, target, c, app_name) =
            (http.clone(), target.clone(), c.clone(), application.clone());
        producers.push(tokio::spawn(async move {
            let share = total / 8;
            for i in 0..share {
                let lane = if (i + w) % 5 == 0 { "urgent" } else { "bulk" };
                let body = json!({
                    "op": "calendar.push",
                    "cost": 1,
                    "payload": { "connection": format!("conn-{}", (i + w) % 40) }
                });
                if http
                    .post(format!(
                        "{GATE}/v1/apps/{app_name}/targets/{target}/lanes/{lane}/push"
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
    println!("== draining for {seconds}s, sampling the admitted rate every second");
    let stop = Arc::new(AtomicU64::new(0));
    let mut consumers = Vec::new();
    for lane in ["urgent", "bulk"] {
        for _ in 0..12 {
            let (http, target, c, stop, application) = (
                http.clone(),
                target.clone(),
                c.clone(),
                stop.clone(),
                application.clone(),
            );
            consumers.push(tokio::spawn(async move {
                while stop.load(Ordering::Relaxed) == 0 {
                    let r: Value = match http
                        .get(format!(
                            "{GATE}/v1/apps/{application}/targets/{target}/lanes/{lane}/next?batch=100&wait_ms=1000"
                        ))
                        .send()
                        .await
                    {
                        Ok(r) => match r.json().await {
                            Ok(v) => v,
                            Err(_) => continue,
                        },
                        Err(_) => continue,
                    };
                    let n = r.get("items").and_then(|v| v.as_array()).map_or(0, |a| a.len());
                    if n == 0 {
                        continue;
                    }
                    c.drained.fetch_add(n as u64, Ordering::Relaxed);
                    // The ack carries the real call count and the outcome: it is
                    // the feedback loop, not bookkeeping.
                    let ack = json!({
                        "lease": r.get("lease").cloned().unwrap_or(Value::Null),
                        "up_to": n,
                        "calls": n,
                        "cost_estimated": n,
                        "op": "calendar.push",
                        "outcome": "ok",
                        "target": target,
                        "application": application,
                        "lane": lane,
                    });
                    if http
                        .post(format!("{GATE}/v1/leases/ack"))
                        .json(&ack)
                        .send()
                        .await
                        .map(|r| r.status().is_success())
                        .unwrap_or(false)
                    {
                        c.acked.fetch_add(n as u64, Ordering::Relaxed);
                    }
                }
            }));
        }
    }

    // ------------------------------------------------------------- sampling
    //
    // Two series, because they answer different questions. `admitted` is the
    // gate's own counter: what the limiter let through. `drained` is what this
    // driver managed to pull back out. When the second is lower, the harness is
    // the bottleneck and the first is the number that means anything.
    async fn admitted_now(http: &reqwest::Client, application: &str, target: &str) -> u64 {
        let v: Value = match http
            .get(format!("{GATE}/api/apps/{application}/targets/{target}"))
            .send()
            .await
        {
            Ok(r) => r.json().await.unwrap_or(Value::Null),
            Err(_) => return 0,
        };
        v.get("lanes")
            .and_then(|l| l.as_array())
            .map(|ls| {
                ls.iter()
                    .filter_map(|l| l.get("admitted").and_then(|v| v.as_u64()))
                    .sum()
            })
            .unwrap_or(0)
    }

    let mut samples: Vec<u64> = Vec::new();
    let mut adm_samples: Vec<u64> = Vec::new();
    let mut last = 0u64;
    let mut last_adm = admitted_now(&http, &application, &target).await;
    let t0 = Instant::now();
    for _ in 0..seconds {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let now = c.drained.load(Ordering::Relaxed);
        samples.push(now - last);
        last = now;
        let a = admitted_now(&http, &application, &target).await;
        adm_samples.push(a.saturating_sub(last_adm));
        last_adm = a;
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
    let steady: Vec<u64> = samples.iter().skip(2).copied().collect();
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
    println!("   declared ceiling      {per_sec:.0}/s");
    println!("   ADMITTED by the gate  p50 {adm_p50}/s  peak {adm_peak}/s  total {adm_total}");
    println!("   drained by the driver p50 {p50}/s  peak {peak}/s  total {drained}");
    println!("   mean drained          {mean:.1}/s over {elapsed:.1}s");
    println!("   admitted samples      {adm_steady:?}");

    // The ceiling assertion is on what the GATE admitted: that is the limiter's
    // job. Draining is the harness's job and its shortfall is not a defect.
    // The ceiling is declared over the budget's WINDOW. A one-second sample
    // above `per_sec` is perfectly legal when the window is ten seconds, so the
    // assertion has to be on the window, not on the sample.
    let window_total: u64 = adm_steady
        .windows(period.max(1) as usize)
        .map(|w| w.iter().sum::<u64>())
        .max()
        .unwrap_or(adm_steady.iter().sum());
    let window_cap = per_sec * period as f64;
    let ok_peak = (window_total as f64) <= window_cap * 1.25;
    let ok_mean = (adm_p50 as f64) >= per_sec * 0.6;
    println!(
        "   worst {period}s window     {window_total} against a cap of {window_cap:.0}"
    );
    println!(
        "\n   ceiling held (worst window <= 125% of cap)    {}",
        if ok_peak { "PASS" } else { "FAIL" }
    );
    println!(
        "   throughput reached (admitted p50 >= 60%)      {}",
        if ok_mean { "PASS" } else { "FAIL" }
    );
    if !(ok_peak && ok_mean) {
        std::process::exit(1);
    }
    Ok(())
}
