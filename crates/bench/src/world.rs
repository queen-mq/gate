//! Declaring the things the scenarios run against, and taking them away again.
//!
//! Every run builds its own targets under a name stamped with the run id. That
//! is not tidiness: a target that survives a previous run comes with a backlog
//! and a gate whose sliding window is already half spent, and the first twenty
//! seconds of the next run would be measuring the last one.

use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

use crate::load::Outcome;

pub struct Gate {
    pub http: reqwest::Client,
    pub base: String,
    pub app: String,
    pub run_id: String,
}

impl Gate {
    pub fn new(base: String, app: String) -> Self {
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
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("http client");
        Self { http, base, app, run_id }
    }

    pub fn target_name(&self, what: &str) -> String {
        format!("bench-{what}-{}", self.run_id)
    }

    pub async fn declare(&self, name: &str, spec: Value) -> Result<Value, String> {
        let url = format!("{}/v1/apps/{}/targets/{name}", self.base, self.app);
        let res = self
            .http
            .put(&url)
            .json(&spec)
            .send()
            .await
            .map_err(|e| format!("declare `{name}`: {e}"))?;
        let status = res.status();
        let body: Value = res.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            return Err(format!("declare `{name}` -> {status}: {body}"));
        }
        // Printed, never swallowed. A declare that bought a caveat is the first
        // thing to suspect when a row comes back below its declared ceiling —
        // `lease-beats-window` alone costs about a quarter of it, and a report
        // that hid the warning would look like a limiter that undershoots.
        if let Some(w) = body.get("warnings").and_then(|v| v.as_array()) {
            if !w.is_empty() {
                println!("  declare `{name}` warned: {w:?}");
            }
        }
        Ok(body)
    }

    pub async fn declare_graph(&self, name: &str, doc: Value) -> Result<Value, String> {
        let url = format!("{}/v1/apps/{}/graphs/{name}", self.base, self.app);
        let res = self
            .http
            .put(&url)
            .json(&doc)
            .send()
            .await
            .map_err(|e| format!("declare graph `{name}`: {e}"))?;
        let status = res.status();
        let body: Value = res.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            return Err(format!("declare graph `{name}` -> {status}: {body}"));
        }
        Ok(body)
    }

    pub async fn drop_target(&self, name: &str) {
        let _ = self
            .http
            .delete(format!("{}/v1/apps/{}/targets/{name}", self.base, self.app))
            .send()
            .await;
    }

    pub async fn drop_graph(&self, name: &str) {
        let _ = self
            .http
            .delete(format!("{}/v1/apps/{}/graphs/{name}", self.base, self.app))
            .send()
            .await;
    }

    // ------------------------------------------------------------ data plane

    pub async fn push(&self, target: &str, lane: &str, body: &Value) -> Outcome {
        let url = format!(
            "{}/v1/apps/{}/targets/{target}/lanes/{lane}/push",
            self.base, self.app
        );
        match self.http.post(&url).json(body).send().await {
            Ok(r) => Outcome::ok(r.status().as_u16()),
            Err(_) => Outcome::dead(),
        }
    }

    pub async fn graph_push(&self, graph: &str, node: &str, body: &Value) -> Outcome {
        let url = format!(
            "{}/v1/apps/{}/graphs/{graph}/nodes/{node}/push",
            self.base, self.app
        );
        match self.http.post(&url).json(body).send().await {
            Ok(r) => Outcome::ok(r.status().as_u16()),
            Err(_) => Outcome::dead(),
        }
    }

    /// A pop and whatever it returned. `None` means the request itself failed;
    /// an empty item list is a legitimate answer — it is what being throttled
    /// looks like, since there is no "you are throttled" response.
    pub async fn next(
        &self,
        target: &str,
        lane: &str,
        batch: u32,
        wait_ms: u64,
    ) -> Option<(u16, Value)> {
        let url = format!(
            "{}/v1/apps/{}/targets/{target}/lanes/{lane}/next?batch={batch}&wait_ms={wait_ms}",
            self.base, self.app
        );
        let res = self.http.get(&url).send().await.ok()?;
        let status = res.status().as_u16();
        let body = res.json::<Value>().await.unwrap_or(Value::Null);
        Some((status, body))
    }

    pub async fn graph_next(
        &self,
        graph: &str,
        node: &str,
        batch: u32,
        wait_ms: u64,
    ) -> Option<(u16, Value)> {
        let url = format!(
            "{}/v1/apps/{}/graphs/{graph}/nodes/{node}/next?batch={batch}&wait_ms={wait_ms}",
            self.base, self.app
        );
        let res = self.http.get(&url).send().await.ok()?;
        let status = res.status().as_u16();
        let body = res.json::<Value>().await.unwrap_or(Value::Null);
        Some((status, body))
    }

    /// Settle a whole lease. The ack carries the real call count and the
    /// outcome, so this is the same transaction a real consumer commits — it is
    /// the expensive half of the cycle and must not be simulated away.
    pub async fn ack(&self, popped: &Value, n: usize, lane: &str) -> Outcome {
        let target = popped
            .get("target")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let body = json!({
            "lease": popped.get("lease").cloned().unwrap_or(Value::Null),
            "up_to": n,
            "calls": n,
            "cost_estimated": n,
            "op": "bench.call",
            "outcome": "ok",
            "target": target,
            "application": self.app,
            "lane": lane,
        });
        match self
            .http
            .post(format!("{}/v1/leases/ack", self.base))
            .json(&body)
            .send()
            .await
        {
            Ok(r) => Outcome::ok(r.status().as_u16()).with_units(n as u64),
            Err(_) => Outcome::dead(),
        }
    }

    /// What the gate says it has admitted, summed over the lanes. The gate's own
    /// counter, not the driver's: when the driver is the bottleneck this is the
    /// only one that means anything.
    pub async fn admitted(&self, target: &str) -> u64 {
        let url = format!("{}/api/apps/{}/targets/{target}", self.base, self.app);
        let v: Value = match self.http.get(&url).send().await {
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

/// A ceiling high enough that the limiter is never the thing being measured,
/// with the batch that `batch-fits` demands for it: a batch below a lease's
/// worth of budget makes the BATCH the limiter, and the run would be a report
/// about pacing rather than about throughput.
pub fn uncapped_spec(app: &str, name: &str, per_sec: f64, lanes: Value) -> Value {
    let lease = 1;
    json!({
        "application": app,
        "name": name,
        "version": 1,
        "budgets": [
            { "id": "wide", "cap": per_sec * 10.0, "periodSeconds": 10,
              "alignment": "rolling", "confidence": "inferred", "source": "bench" }
        ],
        "lanes": lanes,
        "cost": { "field": "httpCost", "default": 1, "max": 5 },
        "pacing": { "leaseSeconds": lease, "batch": (per_sec * lease as f64).ceil() as i64 },
        "admitted": { "partitionBy": "connection", "partitions": 8 }
    })
}

pub fn one_lane() -> Value {
    json!([{ "name": "bulk", "cap": "ceiling", "concurrency": 32, "default": true }])
}
