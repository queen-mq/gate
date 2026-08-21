//! Declaring the things the scenarios run against, and taking them away again.
//!
//! Every run builds its own graphs under a name stamped with the run id. That is
//! not tidiness: a graph that survives a previous run comes with a backlog and a
//! counter that is already half spent, and the first twenty seconds of the next
//! run would be measuring the last one.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use queen_mq::{Config, Queen, SubscriptionMode};
use serde_json::{json, Value};

use crate::load::Outcome;

pub struct Gate {
    pub http: reqwest::Client,
    pub base: String,
    pub app: String,
    pub run_id: String,
    /// The application's own client. The drain half of every scenario goes
    /// through it, because that is what a caller does now: Gate does not mediate
    /// the pop, and a bench that went through Gate to read a queue would be
    /// measuring a route that no longer exists.
    pub queen: Queen,
    pub queen_url: String,
}

impl Gate {
    pub fn new(base: String, app: String, queen_url: String) -> Self {
        let run_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() % 100_000)
            .unwrap_or(0)
            .to_string();
        let http = reqwest::Client::builder()
            // One pool big enough for the widest sweep: a driver that opens a
            // fresh connection per request measures the TCP handshake and calls
            // it Gate.
            .pool_max_idle_per_host(512)
            .timeout(Duration::from_secs(30))
            .build()
            .expect("http client");
        let queen = Queen::connect(Config::new(&queen_url)).expect("queen client");
        Self {
            http,
            base,
            app,
            run_id,
            queen,
            queen_url,
        }
    }

    pub fn graph_name(&self, what: &str) -> String {
        format!("bench-{what}-{}", self.run_id)
    }

    pub fn egress(&self, what: &str) -> String {
        format!("bench.{}.{}.out", self.app, self.graph_name(what))
    }

    pub async fn declare(&self, name: &str, doc: Value) -> Result<Value, String> {
        let url = format!("{}/v1/apps/{}/graphs/{name}", self.base, self.app);
        let res = self
            .http
            .put(&url)
            .json(&doc)
            .send()
            .await
            .map_err(|e| format!("declare `{name}`: {e}"))?;
        let status = res.status();
        let body: Value = res.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            return Err(format!("declare `{name}` -> {status}: {body}"));
        }
        // Printed, never swallowed. A declare that bought a caveat is the first
        // thing to suspect when a row comes back below its declared ceiling.
        if let Some(w) = body.get("warnings").and_then(|v| v.as_array()) {
            if !w.is_empty() {
                println!("  declare `{name}` warned: {w:?}");
            }
        }
        Ok(body)
    }

    pub async fn drop_graph(&self, name: &str) {
        let _ = self
            .http
            .delete(format!("{}/v1/apps/{}/graphs/{name}", self.base, self.app))
            .send()
            .await;
    }

    // ------------------------------------------------------------ data plane

    pub async fn push(&self, graph: &str, node: &str, body: &Value) -> Outcome {
        let url = format!(
            "{}/v1/apps/{}/graphs/{graph}/nodes/{node}/push",
            self.base, self.app
        );
        match self.http.post(&url).json(body).send().await {
            Ok(r) => Outcome::ok(r.status().as_u16()),
            Err(_) => Outcome::dead(),
        }
    }

    /// One pop of the egress queue, auto-acked, exactly as an application's own
    /// consumer would. An empty answer is a legitimate outcome — it is what a
    /// paced queue looks like from the outside, since there is no "you are
    /// throttled" response.
    pub async fn drain(&self, queue: &str, group: &str, batch: i32, wait_ms: u64) -> Outcome {
        match self
            .queen
            .queue(queue)
            .group(group)
            .subscription_mode(SubscriptionMode::All)
            .batch(batch)
            .partitions(32)
            .wait(true)
            .poll_timeout(Duration::from_millis(wait_ms))
            .pop_auto_ack()
            .await
        {
            Ok(m) => Outcome::ok(200).with_units(m.len() as u64),
            Err(_) => Outcome::dead(),
        }
    }

    /// What the stages of a graph say they have done. The gate's own counters,
    /// not the driver's: when the driver is the bottleneck these are the only
    /// ones that mean anything.
    ///
    /// `(admitted, forwarded, commits)` — and `forwarded / commits` is the
    /// number that explains a stage's throughput, because the destination
    /// partition takes one row lock per transaction whoever holds it.
    pub async fn stages(&self, graph: &str) -> (u64, u64, u64) {
        let url = format!("{}/v1/apps/{}/graphs/{graph}", self.base, self.app);
        let v: Value = match self.http.get(&url).send().await {
            Ok(r) => r.json().await.unwrap_or(Value::Null),
            Err(_) => return (0, 0, 0),
        };
        let mut out = (0u64, 0u64, 0u64);
        for s in v["stages"].as_array().cloned().unwrap_or_default() {
            out.0 += s["counters"]["admitted"].as_u64().unwrap_or(0);
            out.1 += s["counters"]["forwarded"].as_u64().unwrap_or(0);
            out.2 += s["counters"]["commits"].as_u64().unwrap_or(0);
        }
        out
    }

    /// The broker's own lifetime HTTP request counter.
    ///
    /// This is how `idle` is measured: with nothing else pointed at the broker,
    /// the delta over a quiet window IS what Gate asked it. It counts every
    /// client, so the scenario says so rather than pretending otherwise.
    pub async fn broker_requests(&self) -> Option<u64> {
        let v: Value = self
            .http
            .get(format!("{}/metrics", self.queen_url))
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()?;
        v.get("requests")?.get("total")?.as_u64()
    }

    pub async fn health(&self) -> Outcome {
        match self.http.get(format!("{}/health", self.base)).send().await {
            Ok(r) => Outcome::ok(r.status().as_u16()),
            Err(_) => Outcome::dead(),
        }
    }
}

/// Micros since the epoch. Stamped into a payload at push and read at pop, which
/// is the only way to see what an item actually waited: the driver's own clock
/// is the same clock on both ends because it is the same process.
pub fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// A ceiling high enough that the limiter is never the thing being measured.
///
/// One node, one budget, one path. The batch is declared because it is the knob
/// the throughput sweep turns: the budget is charged ONCE per batch, so the
/// batch is also the divisor on the shared counter's traffic.
pub fn wide_doc(egress: &str, per_sec: i64, batch: u32) -> Value {
    json!({
        "version": 1,
        "nodes": {
            "n": {
                "ingress": { "partitions": 16 },
                "batch": batch,
                "budgets": [ { "id": "wide", "count": per_sec, "timeMs": 1000 } ],
                "egress": egress
            }
        },
        "paths": [ { "name": "main", "nodes": ["n"] } ]
    })
}

/// A ceiling that BINDS, over a declared window. What `throttled` measures is
/// not performance: it is the ceiling doing its job, and an item that arrives
/// nine hundred deep behind a 50/s ceiling waits eighteen seconds because it was
/// told to.
pub fn capped_doc(egress: &str, count: i64, window_ms: i64, batch: u32) -> Value {
    json!({
        "version": 1,
        "nodes": {
            "n": {
                "ingress": { "partitions": 16 },
                "batch": batch,
                "budgets": [ { "id": "binding", "count": count, "timeMs": window_ms,
                               "subWindows": 1 } ],
                "egress": egress
            }
        },
        "paths": [ { "name": "main", "nodes": ["n"] } ]
    })
}

/// `n` nodes, each with its own path, all charging ONE shared counter. What
/// `contention` measures: whether a single kv row is a serialization point at
/// the rate a fleet of stages actually reaches it.
pub fn shared_doc(egress: &str, stages: usize, per_sec: i64, batch: u32) -> Value {
    let mut nodes = serde_json::Map::new();
    let mut paths = Vec::new();
    for i in 0..stages {
        nodes.insert(
            format!("n{i}"),
            json!({
                "ingress": { "partitions": 8 },
                "batch": batch,
                "budgets": [ { "id": "shared", "count": per_sec, "timeMs": 1000,
                               "sharedKey": "bench-contended" } ],
                "egress": egress
            }),
        );
        paths.push(json!({ "name": format!("p{i}"), "nodes": [format!("n{i}")] }));
    }
    json!({ "version": 1, "nodes": nodes, "paths": paths })
}
