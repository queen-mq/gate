//! The load engine: two drivers and one histogram.
//!
//! The two drivers answer different questions and are not interchangeable.
//!
//! **Closed loop** holds `concurrency` requests in flight and starts the next
//! one only when the last returns. It finds the saturation point — the number
//! to quote as "req/s" — but its latency is a lie under load: when the server
//! slows down, the driver sends less, so the queue never builds and the
//! percentiles stay flat. That is coordinated omission, and it is the reason a
//! saturated system can report a healthy p99.
//!
//! **Open loop** sends at a rate decided in advance and does not wait for
//! anything. Every request has a time it was SUPPOSED to leave, and latency is
//! measured from that instant, not from when the driver got around to it. If
//! the server cannot keep up the backlog shows up in the percentiles, which is
//! what a caller would actually feel.
//!
//! So: closed loop to find the ceiling, open loop at a fraction of it to quote
//! a latency. Quoting a latency from the saturation run is the classic way to
//! publish a number nobody can reproduce.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// What one request did. `units` exists because a request is not always one
/// item of work: a `next?batch=100` that returns 80 items is one request and
/// eighty units, and the two rates answer different questions.
pub struct Outcome {
    pub status: u16,
    pub units: u64,
}

impl Outcome {
    pub fn ok(status: u16) -> Self {
        Self { status, units: 1 }
    }
    /// A request that never reached the server — a connection error, a timeout.
    /// Status 0 is not an HTTP code and is meant to stand out in the report.
    pub fn dead() -> Self {
        Self { status: 0, units: 0 }
    }
    pub fn with_units(mut self, units: u64) -> Self {
        self.units = units;
        self
    }
}

/// Latency samples, in microseconds.
///
/// A plain sorted vector rather than a sketch: these runs record hundreds of
/// thousands of samples, not billions, and an exact p99.9 costs one sort. An
/// approximate one would have to be defended every time somebody disliked it.
#[derive(Default, Clone)]
pub struct Hist {
    us: Vec<u64>,
}

impl Hist {
    pub fn push(&mut self, us: u64) {
        self.us.push(us);
    }

    /// Samples that were collected somewhere else — the end-to-end histograms
    /// are filled by consumer tasks, not by a driver — and already ordered.
    pub fn from_sorted(us: Vec<u64>) -> Self {
        Self { us }
    }

    pub fn extend(&mut self, other: &Hist) {
        self.us.extend_from_slice(&other.us);
    }

    pub fn len(&self) -> usize {
        self.us.len()
    }

    fn sorted(&mut self) {
        self.us.sort_unstable();
    }

    /// Nearest-rank percentile on the sorted samples. Call `sorted` first;
    /// `Run::finish` does.
    pub fn pct(&self, p: f64) -> f64 {
        if self.us.is_empty() {
            return 0.0;
        }
        let rank = ((p / 100.0) * self.us.len() as f64).ceil() as usize;
        let i = rank.saturating_sub(1).min(self.us.len() - 1);
        self.us[i] as f64 / 1000.0
    }

    pub fn max_ms(&self) -> f64 {
        self.us.iter().copied().max().unwrap_or(0) as f64 / 1000.0
    }
}

/// One measured run.
pub struct Run {
    pub label: String,
    /// What was asked of it: the offered rate for an open loop, the concurrency
    /// for a closed one. Printed, because a latency without the load it was
    /// measured at is not a fact.
    pub load: String,
    pub elapsed: f64,
    pub requests: u64,
    pub units: u64,
    pub status: BTreeMap<u16, u64>,
    /// Time on the wire: send to response.
    pub service: Hist,
    /// Time from when the request was DUE to leave to when it came back. Equal
    /// to `service` in a closed loop, which has no schedule; the honest number
    /// in an open one.
    pub waited: Hist,
    /// Set when an open loop could not keep to its own schedule — the driver
    /// itself fell behind, so its latencies are measuring the driver.
    pub slipped: bool,
}

impl Run {
    fn finish(mut self) -> Self {
        self.service.sorted();
        self.waited.sorted();
        self
    }

    pub fn rps(&self) -> f64 {
        self.requests as f64 / self.elapsed
    }

    pub fn units_per_sec(&self) -> f64 {
        self.units as f64 / self.elapsed
    }

    pub fn errors(&self) -> u64 {
        self.status
            .iter()
            .filter(|(s, _)| **s == 0 || **s >= 400)
            .map(|(_, n)| *n)
            .sum()
    }

    fn status_line(&self) -> String {
        self.status
            .iter()
            .map(|(s, n)| if *s == 0 { format!("dead:{n}") } else { format!("{s}:{n}") })
            .collect::<Vec<_>>()
            .join(" ")
    }

    pub fn print(&self) {
        let h = &self.waited;
        println!(
            "  {:<22} {:>10}  {:>9.0}/s  {:>9.0}/s   {:>7.1} {:>7.1} {:>7.1} {:>7.1} {:>8.1}   {}",
            self.label,
            self.load,
            self.rps(),
            self.units_per_sec(),
            h.pct(50.0),
            h.pct(90.0),
            h.pct(99.0),
            h.pct(99.9),
            h.max_ms(),
            self.status_line(),
        );
        let bad = self.errors();
        if bad > 0 {
            println!(
                "  {:>22}   ^ {bad} of {} requests failed. A rate measured against errors is not a rate.",
                "", self.requests
            );
        }
        if self.slipped {
            // The two histograms separate the two explanations. `service` is
            // what the server took; `waited` adds what the request spent
            // waiting for the driver to send it. When they diverge this far the
            // row is about the driver.
            println!(
                "  {:>22}   ^ the DRIVER fell behind its own schedule: p50 on the wire was {:.1}ms \
                 against {:.1}ms measured\n  {:>22}     from when the request was due. This row is \
                 about the harness, not Gate.",
                "",
                self.service.pct(50.0),
                self.waited.pct(50.0),
                ""
            );
        }
    }
}

pub fn header(what: &str) {
    println!(
        "\n  {:<22} {:>10}  {:>11}  {:>11}   {:>7} {:>7} {:>7} {:>7} {:>8}   {}",
        what, "load", "requests", "units", "p50", "p90", "p99", "p99.9", "max", "statuses"
    );
    println!(
        "  {:-<22} {:->10}  {:->11}  {:->11}   {:->7} {:->7} {:->7} {:->7} {:->8}   {:->12}",
        "", "", "", "", "", "", "", "", "", ""
    );
}

fn micros(d: Duration) -> u64 {
    d.as_micros() as u64
}

/// Hold `conc` requests in flight for `duration`, after a warmup whose samples
/// are thrown away. Returns when the last in-flight request lands.
pub async fn closed<F, Fut>(
    label: &str,
    conc: usize,
    warmup: Duration,
    duration: Duration,
    f: F,
) -> Run
where
    F: Fn(usize, u64) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Outcome> + Send,
{
    let t_start = Instant::now();
    let measure_from = t_start + warmup;
    let deadline = measure_from + duration;

    let mut workers = Vec::new();
    for w in 0..conc {
        let f = f.clone();
        workers.push(tokio::spawn(async move {
            let mut h = Hist::default();
            let mut status: BTreeMap<u16, u64> = BTreeMap::new();
            let mut units = 0u64;
            let mut requests = 0u64;
            let mut seq = 0u64;
            loop {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                let t0 = Instant::now();
                let out = f(w, seq).await;
                seq += 1;
                let dt = micros(t0.elapsed());
                // Warmup samples are dropped, not merely ignored: the first
                // requests pay for TLS-less connection setup and a cold
                // registry, and a p99 that includes them is a report about
                // process start.
                if t0 >= measure_from {
                    h.push(dt);
                    *status.entry(out.status).or_default() += 1;
                    units += out.units;
                    requests += 1;
                }
            }
            (h, status, units, requests)
        }));
    }

    let mut run = Run {
        label: label.to_string(),
        load: format!("c={conc}"),
        elapsed: 0.0,
        requests: 0,
        units: 0,
        status: BTreeMap::new(),
        service: Hist::default(),
        waited: Hist::default(),
        slipped: false,
    };
    for w in workers {
        if let Ok((h, status, units, requests)) = w.await {
            run.service.extend(&h);
            run.waited.extend(&h);
            for (s, n) in status {
                *run.status.entry(s).or_default() += n;
            }
            run.units += units;
            run.requests += requests;
        }
    }
    run.elapsed = duration.as_secs_f64();
    run.finish()
}

/// Send at `rate` requests per second regardless of what the server is doing,
/// and measure each request from the instant it was DUE.
///
/// `max_inflight` is a backstop, not a throttle: if the server stalls entirely
/// this is what keeps the driver from opening sixty thousand sockets and
/// measuring its own file descriptor limit. Reaching it is reported.
pub async fn open<F, Fut>(
    label: &str,
    rate: f64,
    warmup: Duration,
    duration: Duration,
    max_inflight: usize,
    f: F,
) -> Run
where
    F: Fn(usize, u64) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Outcome> + Send + 'static,
{
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(u64, u64, u16, u64, bool)>();
    let gate = Arc::new(tokio::sync::Semaphore::new(max_inflight));
    let slipped = Arc::new(AtomicBool::new(false));
    let capped = Arc::new(AtomicU64::new(0));

    let t_start = Instant::now();
    let measure_from = t_start + warmup;
    let deadline = measure_from + duration;
    let step = Duration::from_secs_f64(1.0 / rate);

    let mut i = 0u64;
    loop {
        let due = t_start + step.mul_f64(i as f64);
        if due >= deadline {
            break;
        }
        let now = Instant::now();
        if due > now {
            tokio::time::sleep(due - now).await;
        } else if now - due > Duration::from_millis(50) {
            // The driver is late by more than a rounding error. Anything it
            // reports from here is about the driver.
            slipped.store(true, Ordering::Relaxed);
        }

        let permit = match Arc::clone(&gate).try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                capped.fetch_add(1, Ordering::Relaxed);
                slipped.store(true, Ordering::Relaxed);
                Arc::clone(&gate).acquire_owned().await.expect("semaphore open")
            }
        };

        let (f, tx) = (f.clone(), tx.clone());
        let measured = due >= measure_from;
        tokio::spawn(async move {
            let sent = Instant::now();
            let out = f(0, i).await;
            let done = Instant::now();
            let _ = tx.send((
                micros(done - due),
                micros(done - sent),
                out.status,
                out.units,
                measured,
            ));
            drop(permit);
        });
        i += 1;
    }
    drop(tx);

    // Drain what is still in flight. An open loop's tail belongs in the
    // histogram: dropping it is how a run that ended in a stall reports a clean
    // p99.
    let mut run = Run {
        label: label.to_string(),
        load: format!("{rate:.0}/s"),
        elapsed: duration.as_secs_f64(),
        requests: 0,
        units: 0,
        status: BTreeMap::new(),
        service: Hist::default(),
        waited: Hist::default(),
        slipped: false,
    };
    while let Some((waited, service, status, units, measured)) = rx.recv().await {
        if !measured {
            continue;
        }
        run.waited.push(waited);
        run.service.push(service);
        *run.status.entry(status).or_default() += 1;
        run.units += units;
        run.requests += 1;
    }
    run.slipped = slipped.load(Ordering::Relaxed);
    if capped.load(Ordering::Relaxed) > 0 {
        println!(
            "  note: {} requests waited on the in-flight cap of {max_inflight} — the offered rate was not actually offered",
            capped.load(Ordering::Relaxed)
        );
    }
    run.finish()
}

/// Send exactly `count` requests as fast as `conc` workers can, and measure all
/// of them.
///
/// Neither of the other two drivers describes a flood: a closed loop runs for a
/// duration and an open loop keeps to a schedule, and what a caller does when it
/// has a thousand items and a rate limiter is neither. There is no warmup,
/// because the first request is part of what is being measured.
pub async fn burst<F, Fut>(label: &str, conc: usize, count: u64, f: F) -> Run
where
    F: Fn(usize, u64) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Outcome> + Send,
{
    let t0 = Instant::now();
    let next = Arc::new(AtomicU64::new(0));
    let mut workers = Vec::new();
    for w in 0..conc {
        let (f, next) = (f.clone(), next.clone());
        workers.push(tokio::spawn(async move {
            let mut h = Hist::default();
            let mut status: BTreeMap<u16, u64> = BTreeMap::new();
            let mut units = 0u64;
            let mut requests = 0u64;
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= count {
                    break;
                }
                let sent = Instant::now();
                let out = f(w, i).await;
                h.push(micros(sent.elapsed()));
                *status.entry(out.status).or_default() += 1;
                units += out.units;
                requests += 1;
            }
            (h, status, units, requests)
        }));
    }

    let mut run = Run {
        label: label.to_string(),
        load: format!("c={conc}"),
        elapsed: 0.0,
        requests: 0,
        units: 0,
        status: BTreeMap::new(),
        service: Hist::default(),
        waited: Hist::default(),
        slipped: false,
    };
    for w in workers {
        if let Ok((h, status, units, requests)) = w.await {
            run.service.extend(&h);
            run.waited.extend(&h);
            for (s, n) in status {
                *run.status.entry(s).or_default() += n;
            }
            run.units += units;
            run.requests += requests;
        }
    }
    run.elapsed = t0.elapsed().as_secs_f64();
    run.finish()
}
