//! End to end against a real broker.
//!
//! These are the tests that hold the parts a unit test cannot reach: the relay's
//! exactly-once transaction, the reconcile between two replicas, a retry that is
//! paced rather than amplified, priority at a merge, per-key limits across shards.
//!
//! # Ignored by default, and why that is the honest setting
//!
//! Every test here is `#[ignore]`d. They need a queen with kv enabled — point
//! `GATE_TEST_QUEEN_URL` (or `QUEEN_URL`) at one and run:
//!
//! ```text
//! GATE_TEST_QUEEN_URL=http://127.0.0.1:6632 cargo test -- --include-ignored
//! ```
//!
//! The alternative was worse. They used to skip and PASS when no broker was
//! configured, so `cargo test` on a machine with nothing running printed thirteen
//! green lines that verified none of the above — and the only automation in the
//! repository built a Docker image without running the suite at all. Ignored, the
//! summary says `13 ignored` and nobody can mistake that for verified. CI runs them
//! with `--include-ignored` against a real broker and sets
//! `GATE_TEST_REQUIRE_LIVE`, which turns a missing broker from a skip into a failure.
//!
//! Each test owns a freshly named application, so they neither see each other's
//! queues nor each other's stored specs.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use gate_server::api;
use queen_mq::{Config, Queen};
use serde_json::{json, Value};

fn queen_url() -> Option<String> {
    std::env::var("GATE_TEST_QUEEN_URL")
        .or_else(|_| std::env::var("QUEEN_URL"))
        .ok()
}

struct Harness {
    app: api::Shared,
    base: String,
    application: String,
    http: reqwest::Client,
    /// One live test at a time.
    ///
    /// They share a broker and several of them MEASURE something — how long a
    /// priority-0 item takes to overtake a flood, how deep a queue gets, whether a
    /// second admission happened inside a window. Run concurrently they contend for
    /// the same Postgres and the same admission cycles, and the numbers stop meaning
    /// what the assertions read them as. Serial by construction rather than by
    /// remembering `--test-threads=1`.
    _serial: std::sync::MutexGuard<'static, ()>,
}

/// Deliberately a std mutex held across the test's awaits: each `#[tokio::test]` is its own
/// runtime on its own thread, so blocking one thread is exactly the intent — the other test
/// threads wait rather than interleaving their traffic with this one's.
fn one_at_a_time() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();

    // A poisoned lock means an earlier test panicked, which its own failure already
    // reports; it says nothing about this one.
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// A server in this process on an ephemeral port, plus an application name nobody
/// else will use.
///
/// # On the deadlines below
///
/// Every wait in this file is a LIVENESS bound, never the assertion: what is asserted is
/// the order things came out in, how deep a queue got, which item came back. So they are
/// generous — a machine busy with a release build must not be able to make a correct
/// implementation look broken, and a wrong one still fails the assertion after the wait.
///
/// The serial guard is a std mutex held across this test's awaits on purpose: each
/// `#[tokio::test]` is its own runtime on its own thread, so blocking one thread is exactly
/// the intent — the other test threads wait rather than interleaving their traffic with
/// this one's.
#[allow(clippy::await_holding_lock)]
async fn harness(tag: &str) -> Option<Harness> {
    let serial = one_at_a_time();
    let url = match queen_url() {
        Some(u) => u,
        None => {
            // In automation a missing broker is the failure, not the excuse: this suite
            // is the only thing that verifies the relay, the reconcile and the retro
            // path, and it must not be able to report success without running.
            assert!(
                std::env::var("GATE_TEST_REQUIRE_LIVE").is_err(),
                "GATE_TEST_REQUIRE_LIVE is set but GATE_TEST_QUEEN_URL is not: this suite cannot \
                 verify anything without a broker"
            );
            eprintln!("SKIPPED: set GATE_TEST_QUEEN_URL to a queen with kv enabled");
            return None;
        }
    };
    let application = format!(
        "it{}-{tag}",
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros()
    );
    let app = serve(&url).await;
    let base = spawn_server(app.clone()).await;
    Some(Harness {
        app,
        base,

        application,
        http: reqwest::Client::new(),
        _serial: serial,
    })
}

/// The real router, on an ephemeral port: every test drives the HTTP surface a caller
/// drives, rather than the functions behind it.
async fn spawn_server(app: api::Shared) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let router = api::router(app);
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    format!("http://{addr}")
}

async fn serve(url: &str) -> api::Shared {
    logs();
    let queen = Queen::connect(Config::new(url)).expect("connect");
    Arc::new(api::App::new(queen, url.to_string()))
}

/// `RUST_LOG=gate_server=debug` when a live test needs explaining.
fn logs() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "gate_server=info,queen_mq=warn".into()),
            )
            .with_test_writer()
            .try_init();
    });
}

impl Harness {
    async fn put_graph(&self, name: &str, doc: Value) -> (u16, Value) {
        self.send(
            reqwest::Method::PUT,
            &format!("/v1/apps/{}/graphs/{name}", self.application),
            Some(doc),
        )
        .await
    }

    async fn push(&self, graph: &str, node: &str, body: Value) -> (u16, Value) {
        self.send(
            reqwest::Method::POST,
            &format!(
                "/v1/apps/{}/graphs/{graph}/nodes/{node}/push",
                self.application
            ),
            Some(body),
        )
        .await
    }

    async fn next(&self, graph: &str, node: &str, batch: usize, wait_ms: u64) -> (u16, Value) {
        self.send(
            reqwest::Method::GET,
            &format!(
                "/v1/apps/{}/graphs/{graph}/nodes/{node}/next?batch={batch}&wait_ms={wait_ms}",
                self.application
            ),
            None,
        )
        .await
    }

    async fn ack(&self, body: Value) -> (u16, Value) {
        self.send(reqwest::Method::POST, "/v1/leases/ack", Some(body))
            .await
    }

    async fn send(&self, method: reqwest::Method, path: &str, body: Option<Value>) -> (u16, Value) {
        let mut req = self.http.request(method, format!("{}{path}", self.base));
        if let Some(b) = body {
            req = req.json(&b);
        }
        let res = req.send().await.expect("request");
        let status = res.status().as_u16();
        let text = res.text().await.unwrap_or_default();
        (
            status,
            serde_json::from_str(&text).unwrap_or_else(|_| json!({ "raw": text })),
        )
    }

    /// Pop from a terminal until `want` payloads have arrived or the deadline
    /// passes, acking each batch as the caller contract requires.
    async fn drain(
        &self,
        graph: &str,
        node: &str,
        target: &str,
        want: usize,
        within: Duration,
    ) -> Vec<Value> {
        let started = Instant::now();
        let mut out = Vec::new();
        while out.len() < want && started.elapsed() < within {
            let (status, body) = self.next(graph, node, 50, 500).await;
            assert_eq!(status, 200, "next: {body}");
            let items = body["items"].as_array().cloned().unwrap_or_default();
            if items.is_empty() {
                continue;
            }
            for i in &items {
                out.push(i["payload"].clone());
            }
            let (status, ack) = self
                .ack(json!({
                    "lease": body["lease"],
                    "application": self.application,
                    "target": target,
                    "lane": "default",
                    "op": "test",
                }))
                .await;
            assert_eq!(status, 200, "ack: {ack}");
        }
        out
    }

    async fn put_target(&self, name: &str, spec: Value) -> (u16, Value) {
        self.send(
            reqwest::Method::PUT,
            &format!("/v1/apps/{}/targets/{name}", self.application),
            Some(spec),
        )
        .await
    }

    async fn push_target(&self, name: &str, lane: &str, body: Value) -> (u16, Value) {
        self.send(
            reqwest::Method::POST,
            &format!(
                "/v1/apps/{}/targets/{name}/lanes/{lane}/push",
                self.application
            ),
            Some(body),
        )
        .await
    }

    async fn eta(&self, name: &str, lane: &str) -> (u16, Value) {
        self.send(
            reqwest::Method::GET,
            &format!(
                "/v1/apps/{}/targets/{name}/eta?lane={lane}",
                self.application
            ),
            None,
        )
        .await
    }

    async fn node_eta(&self, graph: &str, node: &str) -> (u16, Value) {
        self.send(
            reqwest::Method::GET,
            &format!(
                "/v1/apps/{}/graphs/{graph}/nodes/{node}/eta",
                self.application
            ),
            None,
        )
        .await
    }

    /// Poll the ETA until `done` accepts it, so a test asserts on a settled
    /// answer rather than on whichever moment it happened to ask in.
    async fn eta_until(
        &self,
        name: &str,
        lane: &str,
        within: Duration,
        done: impl Fn(&Value) -> bool,
    ) -> Value {
        let started = Instant::now();
        let mut last = json!(null);
        while started.elapsed() < within {
            let (status, body) = self.eta(name, lane).await;
            if status == 200 && done(&body) {
                return body;
            }
            last = body;
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        last
    }

    async fn cleanup_target(&self, name: &str) {
        let _ = self
            .send(
                reqwest::Method::DELETE,
                &format!("/v1/apps/{}/targets/{name}", self.application),
                None,
            )
            .await;
    }

    async fn cleanup(&self, graph: &str) {
        let _ = self
            .send(
                reqwest::Method::DELETE,
                &format!("/v1/apps/{}/graphs/{graph}", self.application),
                None,
            )
            .await;
    }
}

/// A broker in front of the broker.
///
/// Provisioning failures are the hardest thing in this server to test honestly: the
/// route validates the document, so a spec cannot be made bad enough to fail at the
/// broker, and the phase-1b recovery therefore went untested — the old test called
/// `supervisor::swap` directly and then hand-executed the very registry write the
/// handler is supposed to perform.
///
/// This forwards everything to the real broker except what a test asks it to refuse, so
/// a declare can fail exactly where the plan says ("declare against a queen refusing
/// configure") and the HANDLER's own recovery is what gets exercised.
struct FaultyBroker {
    url: String,
    refuse: Arc<parking_lot::RwLock<Option<String>>>,
    absent: Arc<parking_lot::RwLock<Option<String>>>,
    seen: Arc<parking_lot::RwLock<Vec<String>>>,
}

impl FaultyBroker {
    /// Refuse every request whose body or path contains `marker`; an empty marker
    /// refuses everything.
    fn refuse(&self, marker: &str) {
        *self.refuse.write() = Some(marker.to_string());
    }
    fn allow(&self) {
        *self.refuse.write() = None;
    }

    /// Answer 404 for every path containing `marker`, which is how a broker that
    /// predates a route behaves — the one failure a version fallback exists for,
    /// and one no live broker can be asked to produce.
    fn route_missing(&self, marker: &str) {
        *self.absent.write() = Some(marker.to_string());
    }

    /// How many requests have gone through whose path contains `marker`.
    fn hits(&self, marker: &str) -> usize {
        self.seen
            .read()
            .iter()
            .filter(|p| p.contains(marker))
            .count()
    }

    fn forget(&self) {
        self.seen.write().clear();
    }
}

#[derive(Clone)]
struct ProxyState {
    real: String,
    http: reqwest::Client,
    refuse: Arc<parking_lot::RwLock<Option<String>>>,
    absent: Arc<parking_lot::RwLock<Option<String>>>,
    seen: Arc<parking_lot::RwLock<Vec<String>>>,
}

async fn faulty_broker(real: &str) -> FaultyBroker {
    let refuse = Arc::new(parking_lot::RwLock::new(None));
    let absent = Arc::new(parking_lot::RwLock::new(None));
    let seen = Arc::new(parking_lot::RwLock::new(Vec::new()));
    let state = ProxyState {
        real: real.trim_end_matches('/').to_string(),
        http: reqwest::Client::new(),
        refuse: refuse.clone(),
        absent: absent.clone(),
        seen: seen.clone(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let app = axum::Router::new()
        .fallback(axum::routing::any(proxy))
        .with_state(state);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    FaultyBroker {
        url: format!("http://{addr}"),
        refuse,
        absent,
        seen,
    }
}

async fn proxy(
    axum::extract::State(st): axum::extract::State<ProxyState>,
    req: axum::extract::Request,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let (parts, body) = req.into_parts();
    let bytes = axum::body::to_bytes(body, 16 * 1024 * 1024)
        .await
        .unwrap_or_default();
    let path = parts.uri.to_string();
    st.seen.write().push(path.clone());

    if let Some(marker) = st.absent.read().clone() {
        if path.contains(&marker) {
            return (
                axum::http::StatusCode::NOT_FOUND,
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                r#"{"error":"no_such_route"}"#,
            )
                .into_response();
        }
    }

    if let Some(marker) = st.refuse.read().clone() {
        let text = String::from_utf8_lossy(&bytes);
        if marker.is_empty() || text.contains(&marker) || path.contains(&marker) {
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "refused by the test",
            )
                .into_response();
        }
    }

    let mut out = st.http.request(parts.method, format!("{}{path}", st.real));
    for (k, v) in parts.headers.iter() {
        if k != axum::http::header::HOST {
            out = out.header(k, v);
        }
    }
    match out.body(bytes.to_vec()).send().await {
        Ok(res) => {
            let status = axum::http::StatusCode::from_u16(res.status().as_u16())
                .unwrap_or(axum::http::StatusCode::BAD_GATEWAY);
            let ct = res
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/json")
                .to_string();
            let body = res.bytes().await.unwrap_or_default();
            (status, [(axum::http::header::CONTENT_TYPE, ct)], body).into_response()
        }
        Err(e) => (
            axum::http::StatusCode::BAD_GATEWAY,
            format!("proxy could not reach the broker: {e}"),
        )
            .into_response(),
    }
}

/// A budget generous enough not to be the thing under test, and slack enough that
/// the default batch of 200 is not the limiter either (`batch-fits`).
fn wide(id: &str) -> Value {
    json!({ "id": id, "cap": 500, "periodSeconds": 10, "alignment": "rolling",
            "confidence": "inferred" })
}

fn chain_doc() -> Value {
    json!({
      "version": 1,
      "nodes": {
        "messages": { "entry": true, "budgets": [wide("msg")],
                      "cost": { "field": "httpCost", "default": 1, "max": 1 } },
        "ip": { "budgets": [wide("ip")],
                "cost": { "field": "httpCost", "default": 1, "max": 100 } }
      },
      "edges": [{ "from": "messages", "to": "ip" }],
      "consume": ["ip"]
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn an_edge_moves_every_item_once_and_only_once() {
    let Some(h) = harness("edge").await else {
        return;
    };
    let (status, body) = h.put_graph("g", chain_doc()).await;
    assert_eq!(status, 200, "declare: {body}");
    assert_eq!(
        body["warnings"],
        json!([]),
        "declare bought caveats: {body}"
    );

    const N: usize = 40;
    for i in 0..N {
        let (status, out) = h
            .push(
                "g",
                "messages",
                json!({ "op": "message.post", "txn": format!("m{i}"),
                        "payload": { "connection": "c1", "n": i } }),
            )
            .await;
        assert_eq!(status, 200, "push: {out}");
    }

    let got = h.drain("g", "ip", "g.ip", N, Duration::from_secs(90)).await;
    let seen: HashSet<i64> = got.iter().filter_map(|p| p["n"].as_i64()).collect();
    assert_eq!(
        got.len(),
        N,
        "expected {N} through the graph, got {}",
        got.len()
    );
    assert_eq!(seen.len(), N, "an item was relayed twice");

    // The item carries the envelope Gate stamped at its entry, which is what a
    // breach rule reads to know where to send it back to.
    let meta = &got[0]["_gate"];
    assert_eq!(meta["entry"], json!("messages"), "{:?}", got[0]);
    assert_eq!(meta["attempt"], json!(0));

    // And nothing arrives twice after the fact: a relay that re-forwarded on
    // redelivery would show up here.
    let extra = h.drain("g", "ip", "g.ip", 1, Duration::from_secs(3)).await;
    assert!(extra.is_empty(), "the graph kept delivering: {extra:?}");

    // The relay's own idempotence, at the broker: the same transaction id inside
    // the dedup window collapses, which is what makes a replayed relay a no-op.
    let ip = h
        .app
        .registry
        .get(&h.application, "g.ip")
        .expect("node target");
    async fn push_dup(h: &Harness, queue: &str) -> Vec<queen_mq::PushResult> {
        h.app
            .queen
            .queue(queue)
            .push_items(vec![queen_mq::PushItem {
                queue: queue.to_string(),
                partition: Some("default".into()),
                payload: json!({ "op": "message.post", "httpCost": 1 }),
                transaction_id: Some("relay-replay".into()),
            }])
            .await
            .expect("push")
    }
    let first = push_dup(&h, &ip.spec.push_queue()).await;
    let second = push_dup(&h, &ip.spec.push_queue()).await;

    assert_ne!(
        format!("{:?}", first[0].status),
        format!("{:?}", second[0].status),
        "the broker did not report the replay as a duplicate: {first:?} then {second:?}"
    );

    h.cleanup("g").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn an_interior_queue_belongs_to_the_graph_and_not_to_a_caller() {
    let Some(h) = harness("interior").await else {
        return;
    };
    let (status, body) = h.put_graph("g", chain_doc()).await;
    assert_eq!(status, 200, "declare: {body}");

    // Popping an interior admitted queue would steal from the relay: two consumers
    // on one queue split the work at random and the item never reaches the node
    // that was supposed to pace it.
    let (status, body) = h.next("g", "messages", 5, 100).await;
    assert_eq!(status, 409, "{body}");
    assert!(
        body["error"].as_str().unwrap_or_default().contains("relay"),
        "the refusal should say why: {body}"
    );

    // And pushing into an interior push queue would skip every budget upstream.
    let (status, body) = h.push("g", "ip", json!({ "op": "message.post" })).await;
    assert_eq!(status, 409, "{body}");
    assert!(
        body["error"].as_str().unwrap_or_default().contains("entry"),
        "{body}"
    );

    // A node that does not exist is a 404, not a 409.
    let (status, _) = h.push("g", "nope", json!({ "op": "x" })).await;
    assert_eq!(status, 404);

    h.cleanup("g").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_throttled_call_re_enters_at_its_entry_until_its_attempts_run_out() {
    let Some(h) = harness("retro").await else {
        return;
    };
    // One node, entry and terminal at once: the shortest path that shows a retry
    // being paced rather than amplified.
    let doc = json!({
      "version": 1,
      "nodes": {
        "calls": { "entry": true, "budgets": [wide("c")],
                   "cost": { "field": "httpCost", "default": 1, "max": 1 } }
      },
      "consume": ["calls"],
      "breach": [{ "when": { "status": 429 }, "retryTo": "origin-entry", "maxAttempts": 2 }]
    });
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "declare: {body}");

    let (status, out) = h
        .push(
            "g",
            "calls",
            json!({ "op": "message.post", "txn": "one", "payload": { "connection": "c1" } }),
        )
        .await;
    assert_eq!(status, 200, "{out}");

    // Attempt 0, then 1, then 2: the third throttle has used up `maxAttempts` and
    // must not come back.
    for expected_attempt in [0u64, 1, 2] {
        let started = Instant::now();
        let mut lease = Value::Null;
        let mut attempt = None;
        while started.elapsed() < Duration::from_secs(60) && attempt.is_none() {
            let (status, body) = h.next("g", "calls", 10, 500).await;
            assert_eq!(status, 200, "{body}");
            if let Some(items) = body["items"].as_array().filter(|i| !i.is_empty()) {
                attempt = items[0]["payload"]["_gate"]["attempt"].as_u64();
                lease = body["lease"].clone();
            }
        }
        assert_eq!(
            attempt,
            Some(expected_attempt),
            "the item should come back stamped with its attempt count"
        );

        let (status, ack) = h
            .ack(json!({
                "lease": lease,
                "application": h.application,
                "target": "g.calls",
                "lane": "default",
                "op": "message.post",
                "outcome": "throttled",
                "status": 429,
            }))
            .await;
        assert_eq!(status, 200, "{ack}");
        if expected_attempt < 2 {
            assert_eq!(
                ack["retried"],
                json!(1),
                "a throttle inside the cap re-enters: {ack}"
            );
            assert_eq!(ack["exhausted"], json!(0));
        } else {
            assert_eq!(ack["retried"], json!(0), "the cap must hold: {ack}");
            assert_eq!(ack["exhausted"], json!(1), "and say so: {ack}");
        }
    }

    // Nothing left: the item was settled, not requeued for ever.
    let mut leftover = 0;
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(4) {
        let (_, body) = h.next("g", "calls", 10, 500).await;
        leftover += body["items"].as_array().map(|i| i.len()).unwrap_or(0);
    }
    assert_eq!(leftover, 0, "an exhausted item came back");

    // The breach left evidence: a paced retry is invisible in the queue depths, so
    // the trace is the only record that the vendor refused work we admitted.
    let (status, traces) = h
        .send(reqwest::Method::GET, "/api/traces?limit=200", None)
        .await;
    assert_eq!(status, 200);
    let outcomes: HashSet<String> = traces
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|t| t["outcome"].as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();
    assert!(outcomes.contains("retried"), "{outcomes:?}");
    assert!(outcomes.contains("exhausted"), "{outcomes:?}");

    h.cleanup("g").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn priority_at_the_entrance_is_priority_in_fact() {
    let Some(h) = harness("priority").await else {
        return;
    };
    // A slow terminal, so the merge is where the queue actually forms: 5 per second
    // means a window of 2 x 5 x 1s = 10 items in front of the gate, and everything
    // else waits in the lane it was pushed to.
    let doc = json!({
      "version": 1,
      "nodes": {
        "prices": { "entry": true, "budgets": [],
                    "cost": { "field": "httpCost", "default": 1, "max": 1 } },
        "bulk": { "entry": true, "budgets": [],
                  "cost": { "field": "httpCost", "default": 1, "max": 1 } },
        "ip": { "budgets": [{ "id": "ip", "cap": 5, "periodSeconds": 1, "alignment": "rolling",
                              "confidence": "inferred" }],
                "cost": { "field": "httpCost", "default": 1, "max": 1 },
                "pacing": { "leaseSeconds": 1, "batch": 20 } }
      },
      "edges": [{ "from": "prices", "to": "ip", "priority": 0 },
                { "from": "bulk", "to": "ip", "priority": 1 }],
      "consume": ["ip"]
    });
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "declare: {body}");
    let window = body["resolved"]["relays"][0]["window"]
        .as_u64()
        .expect("window");
    assert_eq!(window, 10, "2 x 5/s x 1s: {body}");

    for i in 0..200 {
        let (status, _) = h
            .push(
                "g",
                "bulk",
                json!({ "op": "calendar.push", "txn": format!("b{i}"),
                        "payload": { "connection": "c1", "kind": "bulk", "n": i } }),
            )
            .await;
        assert_eq!(status, 200);
    }
    // Let the flood take up all the room it is going to get.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let ip = h.app.registry.get(&h.application, "g.ip").expect("node");
    let depth_before: u64 = h
        .app
        .depths
        .pending_now(&h.app.queen, &ip.spec.push_queue())
        .await
        .values()
        .sum();
    assert!(
        depth_before <= window + 20,
        "the bottleneck queue must stay shallow or priority means nothing: {depth_before} > {window}"
    );

    let (status, _) = h
        .push(
            "g",
            "prices",
            json!({ "op": "price.push", "txn": "urgent",
                    "payload": { "connection": "c1", "kind": "urgent" } }),
        )
        .await;
    assert_eq!(status, 200);

    // It has to overtake ~200 bulk items. At 5/s that would be 40 seconds if it
    // queued behind them; priority is the difference between that and one window.
    let started = Instant::now();
    let mut urgent_after = None;
    let mut drained = 0usize;
    let mut worst_depth = depth_before;
    while started.elapsed() < Duration::from_secs(60) && urgent_after.is_none() {
        let (status, body) = h.next("g", "ip", 20, 500).await;
        assert_eq!(status, 200, "{body}");
        let items = body["items"].as_array().cloned().unwrap_or_default();
        for (i, item) in items.iter().enumerate() {
            if item["payload"]["kind"] == json!("urgent") {
                urgent_after = Some(drained + i);
            }
        }
        drained += items.len();
        if !items.is_empty() {
            let (status, ack) = h
                .ack(
                    json!({ "lease": body["lease"], "application": h.application,
                             "target": "g.ip", "lane": "default", "op": "x" }),
                )
                .await;
            assert_eq!(status, 200, "{ack}");
        }
        worst_depth = worst_depth.max(
            h.app
                .depths
                .pending_now(&h.app.queen, &ip.spec.push_queue())
                .await
                .values()
                .sum(),
        );
    }

    let position = urgent_after.expect("the priority-0 item never arrived");
    assert!(
        position <= window as usize + 20,
        "the urgent item came out behind {position} bulk items; the window is {window}"
    );
    assert!(
        worst_depth <= window + 20,
        "the relay overshot its window: {worst_depth} > {window}"
    );

    h.cleanup("g").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_shard_serialises_one_key_and_lets_another_through() {
    let Some(h) = harness("shard").await else {
        return;
    };
    // One photo deletion per listing per minute, at a cardinality no single state
    // document could hold: 20,000 keys over 8 shards.
    let doc = json!({
      "version": 1,
      "nodes": {
        "photos": {
          "entry": true,
          "shardBy": "entity",
          "shards": 8,
          // Rolling, and that matters: a CALENDAR window rotates on the wall clock, so
          // an admission landing late in a minute releases the held item seconds later
          // and the "nothing more arrives" check below sees it. Rolling measures from
          // the spend, so the second push for a listing is held for a full period no
          // matter when the first one landed.
          "budgets": [{ "id": "per-listing", "cap": 1, "periodSeconds": 60,
                        "alignment": "rolling", "scope": ["entity"], "maxKeys": 20000,
                        "confidence": "inferred" }],

          "cost": { "field": "httpCost", "default": 1, "max": 1 }
        }
      },
      "consume": ["photos"]
    });
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "declare: {body}");

    // Two listings that hash apart.
    let photos = h
        .app
        .registry
        .get(&h.application, "g.photos")
        .expect("node");
    let (mut a, mut b) = (None, None);
    for i in 0..200 {
        let key = format!("listing-{i}");
        let shard = photos.spec.shard_of(&key);
        match (&a, &b) {
            (None, _) => a = Some((key, shard)),
            (Some((_, sa)), None) if *sa != shard => b = Some((key, shard)),
            _ => {}
        }
    }
    let (a, _) = a.expect("a listing");
    let (b, _) = b.expect("a listing on another shard");

    for (key, txn) in [(&a, "a1"), (&a, "a2"), (&b, "b1")] {
        let (status, out) = h
            .push(
                "g",
                "photos",
                json!({ "op": "photo.delete", "txn": txn,
                        "payload": { "entity": key, "connection": "c1" } }),
            )
            .await;
        assert_eq!(status, 200, "{out}");
    }

    let got = h
        .drain("g", "photos", "g.photos", 2, Duration::from_secs(60))
        .await;
    let entities: Vec<&str> = got.iter().filter_map(|p| p["entity"].as_str()).collect();
    assert!(entities.contains(&a.as_str()), "{entities:?}");
    assert!(
        entities.contains(&b.as_str()),
        "two listings on two shards must not wait for each other: {entities:?}"
    );
    // The second push for the same listing is held: the limit is per key, and the
    // key is what a shard is.
    let more = h
        .drain("g", "photos", "g.photos", 1, Duration::from_secs(4))
        .await;
    assert!(
        more.is_empty(),
        "the same listing got through twice inside its window: {more:?}"
    );

    // A push with no shard dimension has no shard to belong to, and is refused
    // rather than defaulted into somebody else's counter.
    let (status, body) = h
        .push(
            "g",
            "photos",
            json!({ "op": "photo.delete", "payload": { "connection": "c1" } }),
        )
        .await;
    assert_eq!(status, 422, "{body}");
    assert!(
        body["error"].as_str().unwrap_or_default().contains("shard"),
        "{body}"
    );

    h.cleanup("g").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_second_replica_converges_on_the_stored_spec() {
    let Some(h) = harness("reconcile").await else {
        return;
    };
    let url = queen_url().expect("checked");

    let spec = |cap: f64| {
        json!({
          "name": "airbnb", "version": 1,
          "budgets": [{ "id": "ip", "cap": cap, "periodSeconds": 10, "alignment": "rolling",
                        "confidence": "inferred" }],
          "lanes": [{ "name": "bulk", "cap": "ceiling", "concurrency": 2, "default": true }],
          "cost": { "field": "httpCost", "default": 1, "max": 1 }
        })
    };

    let (status, body) = h
        .send(
            reqwest::Method::PUT,
            &format!("/v1/apps/{}/targets/airbnb", h.application),
            Some(spec(1000.0)),
        )
        .await;
    assert_eq!(status, 200, "{body}");

    // A second replica of the same deployment, on the same broker.
    let b = serve(&url).await;
    gate_server::reconcile(&b).await;
    let on_b = b
        .registry
        .get(&h.application, "airbnb")
        .expect("the second replica should have picked the target up from the store");
    assert_eq!(on_b.spec.budgets[0].cap, 1000.0);

    // Tighten the cap on A. Until the reconcile existed, B kept enforcing 1000 for
    // ever — and because a fleet's ceiling is the sum of what its replicas admit,
    // the LOOSER pod is the one that decides.
    let (status, body) = h
        .send(
            reqwest::Method::PUT,
            &format!("/v1/apps/{}/targets/airbnb", h.application),
            Some(spec(200.0)),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    gate_server::reconcile(&b).await;
    let on_b = b
        .registry
        .get(&h.application, "airbnb")
        .expect("still there");
    assert_eq!(
        on_b.spec.budgets[0].cap, 200.0,
        "the second replica kept enforcing the old cap"
    );

    // A delete reaches the other replica the same way.
    let (status, _) = h
        .send(
            reqwest::Method::DELETE,
            &format!("/v1/apps/{}/targets/airbnb", h.application),
            None,
        )
        .await;
    assert_eq!(status, 200);
    gate_server::reconcile(&b).await;
    assert!(
        b.registry.get(&h.application, "airbnb").is_none(),
        "a deleted target came back on the second replica"
    );

    // A graph reconciles the same way, and its nodes come with it.
    let (status, body) = h.put_graph("g", chain_doc()).await;
    assert_eq!(status, 200, "{body}");
    gate_server::reconcile(&b).await;
    assert!(
        b.registry.graph(&h.application, "g").is_some(),
        "graph not restored"
    );
    assert!(
        b.registry.get(&h.application, "g.ip").is_some(),
        "a restored graph must bring its nodes up"
    );
    // And the node is not reapable by a target sync, because the graph owns it.
    let (status, body) = h
        .send(
            reqwest::Method::PUT,
            &format!("/v1/apps/{}/targets", h.application),
            Some(json!([])),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["removed"],
        json!([]),
        "a sync reaped a graph node: {body}"
    );
    assert!(h.app.registry.get(&h.application, "g.ip").is_some());

    h.cleanup("g").await;
    gate_server::reconcile(&b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_failed_provisioning_leaves_the_old_spec_serving() {
    let Some(h) = harness("restore").await else {
        return;
    };
    // Through the DECLARE ROUTE, against a broker that refuses one `configure` — which
    // is what the plan asked for, and what the handler's own recovery branch needs in
    // order to be the thing under test rather than something a test re-implements.
    let broker = faulty_broker(&queen_url().expect("checked")).await;
    let app = Arc::new(api::App::new(
        Queen::connect(Config::new(&broker.url)).expect("connect"),
        broker.url.clone(),
    ));
    let base = spawn_server(app.clone()).await;
    let http = reqwest::Client::new();
    let application = format!("{}-faulty", h.application);

    let spec = |lanes: Value| {
        json!({
          "name": "airbnb", "version": 1,
          "budgets": [{ "id": "ip", "cap": 1000, "periodSeconds": 10, "alignment": "rolling",
                        "confidence": "inferred" }],
          "lanes": lanes,
          "cost": { "field": "httpCost", "default": 1, "max": 1 }
        })
    };
    let one_lane = json!([{ "name": "bulk", "cap": "ceiling", "concurrency": 2, "default": true }]);
    let two_lanes = json!([
        { "name": "bulk", "cap": "share:0.5", "concurrency": 2, "default": true },
        { "name": "urgent", "cap": "share:0.5", "concurrency": 2 }
    ]);

    let declare = |body: Value| {
        let http = http.clone();
        let url = format!("{base}/v1/apps/{application}/targets/airbnb");
        async move {
            let res = http.put(url).json(&body).send().await.expect("request");
            let status = res.status().as_u16();
            let text = res.text().await.unwrap_or_default();
            (status, text)
        }
    };

    let (status, body) = declare(spec(one_lane.clone())).await;
    assert_eq!(status, 200, "{body}");

    // The second lane's queue is the one the broker will not create.
    broker.refuse("admitted.urgent");
    let (status, body) = declare(spec(two_lanes)).await;
    assert_eq!(status, 502, "{body}");
    assert!(
        body.contains("still serving version 1"),
        "the handler must say what is serving now: {body}"
    );
    broker.allow();

    // And it IS serving: the restored runtime admits, which is the whole point — a
    // target left stopped but registered accepts pushes and drains nothing for ever.
    let (status, body) = {
        let res = http
            .post(format!(
                "{base}/v1/apps/{application}/targets/airbnb/lanes/bulk/push"
            ))
            .json(&json!({ "op": "x", "payload": { "connection": "c1" } }))
            .send()
            .await
            .expect("push");
        (res.status().as_u16(), res.text().await.unwrap_or_default())
    };
    assert_eq!(status, 200, "{body}");

    let started = Instant::now();
    let mut admitted = 0;
    while started.elapsed() < Duration::from_secs(60) && admitted == 0 {
        let res = http
            .get(format!(
                "{base}/v1/apps/{application}/targets/airbnb/lanes/bulk/next?batch=5&wait_ms=500"
            ))
            .send()
            .await
            .expect("next");
        let body: Value = res.json().await.unwrap_or(Value::Null);
        admitted += body["items"].as_array().map(|i| i.len()).unwrap_or(0);
    }
    assert_eq!(admitted, 1, "the restored runtime is not admitting");

    let _ = http
        .delete(format!("{base}/v1/apps/{application}/targets/airbnb"))
        .send()
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_declare_that_cannot_be_restored_leaves_nothing_registered() {
    let Some(h) = harness("unregister").await else {
        return;
    };
    // The other half of the contract: when the OLD spec cannot be restarted either,
    // nothing is serving the target — and a registered target that admits nothing is the
    // one state an operator cannot recover from. So it is unregistered, pushes are
    // refused, and the reconcile loop brings it back from the store.
    let broker = faulty_broker(&queen_url().expect("checked")).await;
    let app = Arc::new(api::App::new(
        Queen::connect(Config::new(&broker.url)).expect("connect"),
        broker.url.clone(),
    ));
    let base = spawn_server(app.clone()).await;
    let http = reqwest::Client::new();
    let application = format!("{}-faulty", h.application);

    let spec = json!({
      "name": "airbnb", "version": 1,
      "budgets": [{ "id": "ip", "cap": 1000, "periodSeconds": 10, "alignment": "rolling",
                    "confidence": "inferred" }],
      "lanes": [{ "name": "bulk", "cap": "ceiling", "concurrency": 2, "default": true }],
      "cost": { "field": "httpCost", "default": 1, "max": 1 }
    });
    let url = format!("{base}/v1/apps/{application}/targets/airbnb");

    let res = http.put(&url).json(&spec).send().await.expect("declare");
    assert_eq!(res.status().as_u16(), 200);
    assert!(app.registry.get(&application, "airbnb").is_some());

    // Nothing gets through now, so neither the new spec nor the old one can start.
    broker.refuse("");
    let mut bumped = spec.clone();
    bumped["budgets"][0]["cap"] = json!(500);
    let res = http.put(&url).json(&bumped).send().await.expect("declare");
    let status = res.status().as_u16();
    let body = res.text().await.unwrap_or_default();
    assert_eq!(status, 502, "{body}");
    assert!(
        body.contains("unregistered"),
        "the handler must say so: {body}"
    );
    assert!(
        app.registry.get(&application, "airbnb").is_none(),
        "a stopped runtime must not be left registered"
    );

    // A push is refused rather than accepted into a queue nothing drains.
    let res = http
        .post(format!(
            "{base}/v1/apps/{application}/targets/airbnb/lanes/bulk/push"
        ))
        .json(&json!({ "op": "x", "payload": { "connection": "c1" } }))
        .send()
        .await
        .expect("push");
    assert_eq!(
        res.status().as_u16(),
        404,
        "a push was accepted with nothing serving it"
    );

    // And the reconcile loop repairs it from the store, which still holds version 1.
    broker.allow();
    gate_server::reconcile(&app).await;
    assert!(
        app.registry
            .get(&application, "airbnb")
            .is_some_and(|rt| rt.is_running()),
        "the reconcile pass did not bring the target back"
    );
    let res = http
        .post(format!(
            "{base}/v1/apps/{application}/targets/airbnb/lanes/bulk/push"
        ))
        .json(&json!({ "op": "x", "payload": { "connection": "c1" } }))
        .send()
        .await
        .expect("push");
    assert_eq!(res.status().as_u16(), 200, "and it is not admitting again");

    let _ = http.delete(&url).send().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn one_owner_per_queue_family() {
    let Some(h) = harness("owner").await else {
        return;
    };
    let (status, body) = h.put_graph("g", chain_doc()).await;
    assert_eq!(status, 200, "{body}");

    let standalone = |name: &str| {
        json!({
          "name": name, "version": 1,
          "budgets": [{ "id": "x", "cap": 10, "periodSeconds": 1, "alignment": "rolling",
                        "confidence": "inferred" }],
          "lanes": [{ "name": "bulk", "cap": "ceiling", "concurrency": 1, "default": true }],
          "cost": { "field": "httpCost", "default": 1, "max": 1 }
        })
    };

    // A standalone target with a node's name would be a second declarer of the same
    // queues, and whichever wrote last would be the one enforcing.
    let (status, body) = h
        .send(
            reqwest::Method::PUT,
            &format!("/v1/apps/{}/targets/g.ip", h.application),
            Some(standalone("g.ip")),
        )
        .await;
    assert_eq!(status, 409, "{body}");

    // Nor may a node be deleted on its own: half a graph accepts work at its entry
    // and drops it at the hole.
    let (status, body) = h
        .send(
            reqwest::Method::DELETE,
            &format!("/v1/apps/{}/targets/g.ip", h.application),
            None,
        )
        .await;
    assert_eq!(status, 409, "{body}");

    // The other direction: a standalone target first, then a graph that would take
    // its name.
    let (status, body) = h
        .send(
            reqwest::Method::PUT,
            &format!("/v1/apps/{}/targets/h.ip", h.application),
            Some(standalone("h.ip")),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = h.put_graph("h", chain_doc()).await;
    assert_eq!(status, 409, "{body}");
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("standalone"),
        "{body}"
    );

    let _ = h
        .send(
            reqwest::Method::DELETE,
            &format!("/v1/apps/{}/targets/h.ip", h.application),
            None,
        )
        .await;
    h.cleanup("g").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn the_console_can_draw_what_is_running() {
    let Some(h) = harness("console").await else {
        return;
    };
    let (status, body) = h.put_graph("g", chain_doc()).await;
    assert_eq!(status, 200, "{body}");

    let (status, list) = h.send(reqwest::Method::GET, "/api/graphs", None).await;
    assert_eq!(status, 200);
    let mine = list
        .as_array()
        .and_then(|a| a.iter().find(|g| g["application"] == json!(h.application)))
        .expect("the graph should be listed");
    assert_eq!(mine["nodes"].as_array().map(|a| a.len()), Some(2));
    assert_eq!(mine["edges"].as_array().map(|a| a.len()), Some(1));

    let (status, topo) = h
        .send(
            reqwest::Method::GET,
            &format!("/api/apps/{}/graphs/g/topology", h.application),
            None,
        )
        .await;
    assert_eq!(status, 200, "{topo}");
    assert_eq!(topo["nodes"].as_array().map(|a| a.len()), Some(2));
    assert_eq!(topo["edges"][0]["from"], json!("messages"));

    let (status, view) = h
        .send(
            reqwest::Method::GET,
            &format!("/api/apps/{}/graphs/g", h.application),
            None,
        )
        .await;
    assert_eq!(status, 200, "{view}");
    assert_eq!(view["warnings"], json!([]));
    let nodes = view["nodes"].as_array().expect("nodes");
    assert!(nodes.iter().all(|n| n["running"] == json!(true)), "{view}");
    assert!(nodes.iter().any(|n| n["entry"] == json!(true)));
    assert!(nodes.iter().any(|n| n["consume"] == json!(true)));
    assert!(view["edges"][0]["lag"].is_u64(), "{view}");
    assert!(view["relays"][0]["window"].is_u64(), "{view}");

    h.cleanup("g").await;
    let (status, _) = h
        .send(
            reqwest::Method::GET,
            &format!("/api/apps/{}/graphs/g", h.application),
            None,
        )
        .await;
    assert_eq!(status, 404, "a deleted graph is gone from the console too");
    assert!(
        h.app.registry.get(&h.application, "g.ip").is_none(),
        "deleting a graph must stop its nodes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_node_cannot_be_reached_through_the_target_routes() {
    let Some(h) = harness("bypass").await else {
        return;
    };
    let (status, body) = h.put_graph("g", chain_doc()).await;
    assert_eq!(status, 200, "{body}");

    // A node's target name is public — the declare answers with it — and the target
    // routes would serve it like any other target. They must not: popping an interior
    // admitted queue means the caller executes the item AND the relay forwards it, and
    // pushing straight into a terminal skips every budget upstream of it.
    let (status, body) = h
        .send(
            reqwest::Method::GET,
            &format!(
                "/v1/apps/{}/targets/g.messages/lanes/default/next?batch=5&wait_ms=100",
                h.application
            ),
            None,
        )
        .await;
    assert_eq!(
        status, 409,
        "an interior queue was popped through the target route: {body}"
    );

    let (status, body) = h
        .send(
            reqwest::Method::POST,
            &format!("/v1/apps/{}/targets/g.ip/lanes/default/push", h.application),
            Some(json!({ "op": "message.post", "payload": { "connection": "c1" } })),
        )
        .await;
    assert_eq!(
        status, 409,
        "a terminal was pushed to through the target route: {body}"
    );
    assert!(
        body["error"].as_str().unwrap_or_default().contains("graph"),
        "{body}"
    );

    h.cleanup("g").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_batch_ack_re_enters_only_the_items_the_caller_names() {
    let Some(h) = harness("breached").await else {
        return;
    };
    // One node, so the item comes straight back to where it can be popped again.
    let doc = json!({
      "version": 1,
      "nodes": {
        "calls": { "entry": true, "budgets": [wide("c")],
                   "cost": { "field": "httpCost", "default": 1, "max": 1 } }
      },
      "consume": ["calls"],
      "breach": [{ "when": { "status": 429 }, "retryTo": "origin-entry", "maxAttempts": 2 }]
    });
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "{body}");

    for i in 0..4 {
        let (status, _) = h
            .push(
                "g",
                "calls",
                json!({ "op": "message.post", "txn": format!("b{i}"),
                        "payload": { "connection": "c1", "n": i } }),
            )
            .await;
        assert_eq!(status, 200);
    }

    // One lease, several items, and one of them refused by the vendor. `outcome` is a
    // single field for the whole lease, so without naming the item a breach rule would
    // re-enter every one of them — duplicate calls the vendor had already accepted.
    //
    // The wait is so the batch IS a batch: the gate admits on its own cycle, and a pop
    // that arrives between two of them sees half of it.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let (status, body) = h.next("g", "calls", 10, 2_000).await;
    assert_eq!(status, 200, "{body}");
    let items = body["items"].as_array().cloned().unwrap_or_default();
    assert!(
        items.len() >= 2,
        "expected one lease with several items: {body}"
    );
    let last = items.len() - 1;
    let throttled_id = items[last]["id"].as_str().expect("id").to_string();
    let throttled_n = items[last]["payload"]["n"].clone();

    let (status, ack) = h
        .ack(json!({
            "lease": body["lease"],
            "application": h.application,
            "target": "g.calls",
            "lane": "default",
            "op": "message.post",
            "outcome": "throttled",
            "status": 429,
            "breached": [throttled_id],
        }))
        .await;
    assert_eq!(status, 200, "{ack}");
    assert_eq!(
        ack["retried"],
        json!(1),
        "only the named item re-enters: {ack}"
    );
    assert_eq!(
        ack["acked"],
        json!(items.len()),
        "and the whole lease is still settled: {ack}"
    );

    // And it is the right one.
    let back = h
        .drain("g", "calls", "g.calls", 1, Duration::from_secs(60))
        .await;
    assert_eq!(
        back.len(),
        1,
        "the throttled item did not come back: {back:?}"
    );
    assert_eq!(back[0]["n"], throttled_n);
    assert_eq!(back[0]["_gate"]["attempt"], json!(1));

    h.cleanup("g").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn an_impossible_retry_is_reported_and_still_settles_the_work() {
    let Some(h) = harness("refused").await else {
        return;
    };
    // A graph with no breach rule at all: a caller asking for a re-entry is asking for
    // something this graph does not declare.
    let (status, body) = h.put_graph("g", chain_doc()).await;
    assert_eq!(status, 200, "{body}");

    let (status, _) = h
        .push(
            "g",
            "messages",
            json!({ "op": "message.post", "txn": "one", "payload": { "connection": "c1" } }),
        )
        .await;
    assert_eq!(status, 200);

    let started = Instant::now();
    let mut lease = Value::Null;
    while started.elapsed() < Duration::from_secs(60) && lease.as_array().is_none() {
        let (status, b) = h.next("g", "ip", 10, 500).await;
        assert_eq!(status, 200, "{b}");
        if b["items"].as_array().map(|i| i.len()).unwrap_or(0) > 0 {
            lease = b["lease"].clone();
        }
    }
    assert!(
        lease.as_array().is_some(),
        "nothing arrived at the terminal"
    );

    // The work has already been done by the time this ack arrives. Refusing to settle
    // it would have it redelivered and done AGAIN, so the refusal is reported in the
    // answer and the settlement stands.
    let (status, ack) = h
        .ack(json!({
            "lease": lease,
            "application": h.application,
            "target": "g.ip",
            "lane": "default",
            "op": "message.post",
            "outcome": "throttled",
            "status": 429,
            "retryTo": "messages",
        }))
        .await;
    assert_eq!(status, 200, "the ack must not fail: {ack}");
    assert_eq!(ack["acked"], json!(1));
    assert_eq!(ack["retried"], json!(0));
    assert!(
        ack["refused"]
            .as_str()
            .unwrap_or_default()
            .contains("breach rule"),
        "the answer should say why nothing was retried: {ack}"
    );

    h.cleanup("g").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn an_ack_that_arrives_twice_settles_once_and_retries_once() {
    let Some(h) = harness("replay").await else {
        return;
    };
    // A caller whose ack timed out sends it again. The retro push is keyed by the
    // attempt, so the second copy hits the broker's dedup — which inside a
    // transaction is a HARD error that rolls the ack back with it. Left alone, that
    // caller retries an ack that can never succeed, and then does the work again.
    let doc = json!({
      "version": 1,
      "nodes": {
        "calls": { "entry": true, "budgets": [wide("c")],
                   "cost": { "field": "httpCost", "default": 1, "max": 1 } }
      },
      "consume": ["calls"],
      "breach": [{ "when": { "status": 429 }, "retryTo": "origin-entry", "maxAttempts": 3 }]
    });
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "{body}");
    let (status, _) = h
        .push(
            "g",
            "calls",
            json!({ "op": "m.post", "txn": "once", "payload": { "connection": "c1" } }),
        )
        .await;
    assert_eq!(status, 200);

    let started = Instant::now();
    let mut lease = Value::Null;
    while started.elapsed() < Duration::from_secs(60) && lease.as_array().is_none() {
        let (status, b) = h.next("g", "calls", 5, 500).await;
        assert_eq!(status, 200, "{b}");
        if b["items"].as_array().map(|i| i.len()).unwrap_or(0) > 0 {
            lease = b["lease"].clone();
        }
    }
    assert!(lease.as_array().is_some(), "nothing was admitted");

    let ack = json!({
        "lease": lease, "application": h.application, "target": "g.calls",
        "lane": "default", "op": "m.post", "outcome": "throttled", "status": 429,
    });
    let (status, first) = h.ack(ack.clone()).await;
    assert_eq!(status, 200, "{first}");
    assert_eq!(first["retried"], json!(1));

    let (status, again) = h.ack(ack).await;
    assert_eq!(status, 200, "a replayed ack must not fail: {again}");
    assert_eq!(
        again["retried"],
        json!(0),
        "and must not retry twice: {again}"
    );

    // Exactly one re-entry exists, carrying attempt 1.
    let back = h
        .drain("g", "calls", "g.calls", 1, Duration::from_secs(60))
        .await;
    assert_eq!(back.len(), 1, "{back:?}");
    assert_eq!(back[0]["_gate"]["attempt"], json!(1));
    let extra = h
        .drain("g", "calls", "g.calls", 1, Duration::from_secs(4))
        .await;
    assert!(extra.is_empty(), "the item was re-entered twice: {extra:?}");

    h.cleanup("g").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn an_overlapping_ack_keeps_the_new_items_re_entry() {
    let Some(h) = harness("overlap").await else {
        return;
    };
    // The ack is positional — "you settle the first N" — so a consumer finishing a
    // lease in two goes necessarily re-sends the prefix it already settled. The
    // prefix's re-entry is then a duplicate, and a duplicate is a HARD error inside a
    // transaction: it used to roll the bundle back and take the NEW item's re-entry
    // with it, settling a vendor-refused call that nobody would ever make again.
    let doc = json!({
      "version": 1,
      "nodes": {
        "calls": { "entry": true, "budgets": [wide("c")],
                   "cost": { "field": "httpCost", "default": 1, "max": 1 } }
      },
      "consume": ["calls"],
      "breach": [{ "when": { "status": 429 }, "retryTo": "origin-entry", "maxAttempts": 3 }]
    });
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "{body}");

    for i in 0..2 {
        let (status, _) = h
            .push(
                "g",
                "calls",
                json!({ "op": "m.post", "txn": format!("ov{i}"),
                        "payload": { "connection": "c1", "n": i } }),
            )
            .await;
        assert_eq!(status, 200);
    }
    tokio::time::sleep(Duration::from_secs(4)).await;

    let (status, body) = h.next("g", "calls", 10, 2_000).await;
    assert_eq!(status, 200, "{body}");
    let items = body["items"].as_array().cloned().unwrap_or_default();
    assert_eq!(items.len(), 2, "expected both items in one lease: {body}");
    // The answer says what to ack as, so a caller never has to guess `{graph}.{node}`.
    assert_eq!(body["target"], json!("g.calls"), "{body}");
    let ns: Vec<Value> = items.iter().map(|i| i["payload"]["n"].clone()).collect();

    let base = json!({
        "lease": body["lease"], "application": h.application,
        "target": body["target"], "lane": body["lane"], "op": "m.post",
        "outcome": "throttled", "status": 429,
    });

    // First: settle the prefix of one.
    let mut first = base.clone();
    first["up_to"] = json!(1);
    let (status, one) = h.ack(first).await;
    assert_eq!(status, 200, "{one}");
    assert_eq!(one["retried"], json!(1), "{one}");

    // Then the whole lease, prefix included — the shape the API's own positional
    // contract forces on a caller.
    let (status, two) = h.ack(base).await;
    assert_eq!(status, 200, "{two}");
    assert_eq!(two["acked"], json!(2));
    assert_eq!(
        two["retried"],
        json!(1),
        "the second item's re-entry must survive the prefix's duplicate: {two}"
    );

    // Both items are now waiting for budget again, each once.
    let back = h
        .drain("g", "calls", "g.calls", 2, Duration::from_secs(60))
        .await;
    let mut seen: Vec<i64> = back.iter().filter_map(|p| p["n"].as_i64()).collect();
    seen.sort();
    let mut want: Vec<i64> = ns.iter().filter_map(|n| n.as_i64()).collect();
    want.sort();
    assert_eq!(
        seen, want,
        "every throttled item should have re-entered exactly once"
    );
    assert!(
        back.iter().all(|p| p["_gate"]["attempt"] == json!(1)),
        "{back:?}"
    );

    h.cleanup("g").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn an_ack_that_names_the_wrong_target_says_so() {
    let Some(h) = harness("naming").await else {
        return;
    };
    let doc = json!({
      "version": 1,
      "nodes": {
        "calls": { "entry": true, "budgets": [wide("c")],
                   "cost": { "field": "httpCost", "default": 1, "max": 1 } }
      },
      "consume": ["calls"],
      "breach": [{ "when": { "status": 429 }, "retryTo": "origin-entry", "maxAttempts": 3 }]
    });
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "{body}");
    let (status, _) = h
        .push(
            "g",
            "calls",
            json!({ "op": "m.post", "txn": "n1", "payload": { "connection": "c1" } }),
        )
        .await;
    assert_eq!(status, 200);

    let started = Instant::now();
    let mut popped = Value::Null;
    while started.elapsed() < Duration::from_secs(60) && popped.get("lease").is_none() {
        let (status, b) = h.next("g", "calls", 5, 500).await;
        assert_eq!(status, 200, "{b}");
        if b["items"].as_array().map(|i| i.len()).unwrap_or(0) > 0 {
            popped = b;
        }
    }
    assert!(popped.get("lease").is_some(), "nothing was admitted");

    // The node is `calls`; the TARGET is `g.calls`. Naming the node used to settle the
    // work with the meter and the breach rules silently skipped — a throttled item
    // dropped for good, answered with 200 and nothing else.
    let (status, ack) = h
        .ack(json!({
            "lease": popped["lease"], "application": h.application,
            "target": "calls", "lane": "default", "op": "m.post",
            "outcome": "throttled", "status": 429,
        }))
        .await;
    assert_eq!(status, 200, "the work is still settled: {ack}");
    assert_eq!(ack["retried"], json!(0));
    let refused = ack["refused"].as_str().unwrap_or_default();
    assert!(
        refused.contains("no target"),
        "the answer must say what was skipped: {ack}"
    );
    assert!(
        refused.contains("{graph}.{node}"),
        "and how a node is addressed: {ack}"
    );

    h.cleanup("g").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_replica_converges_on_a_redeclared_graph_instead_of_wedging() {
    let Some(h) = harness("wedge").await else {
        return;
    };
    let url = queen_url().expect("checked");

    // A version bump protects a CALLER from re-founding counters by accident. Enforcing
    // it on the reconcile path, against a runtime this replica happens to hold, is how a
    // replica wedges: a delete-and-redeclare at the same version is legal where it
    // landed and refused for ever by every pod that still holds the old document —
    // exactly the indefinite divergence the reconcile exists to end.
    let (status, body) = h.put_graph("g", chain_doc()).await;
    assert_eq!(status, 200, "{body}");

    let b = serve(&url).await;
    gate_server::reconcile(&b).await;
    let before = b.registry.graph(&h.application, "g").expect("picked up");
    assert_eq!(before.spec.nodes["ip"].budgets[0].period_seconds, 10);

    // Delete and re-declare at the SAME version, with a migration-class change (a new
    // period re-founds what the accumulated state means).
    let (status, _) = h
        .send(
            reqwest::Method::DELETE,
            &format!("/v1/apps/{}/graphs/g", h.application),
            None,
        )
        .await;
    assert_eq!(status, 200);
    let mut doc = chain_doc();
    doc["nodes"]["ip"]["budgets"][0]["periodSeconds"] = json!(20);
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "the caller's own declare is legal: {body}");

    // The replica that still holds version 1 must take the store's word for it.
    gate_server::reconcile(&b).await;
    let after = b.registry.graph(&h.application, "g").expect("still there");
    assert_eq!(
        after.spec.nodes["ip"].budgets[0].period_seconds, 20,
        "the second replica kept enforcing a document the store no longer holds"
    );
    assert!(
        b.registry
            .get(&h.application, "g.ip")
            .is_some_and(|rt| rt.is_running()),
        "and its nodes must be running, not merely registered"
    );

    h.cleanup("g").await;
    gate_server::reconcile(&b).await;
}

/// The relay's transaction, replayed by hand — which is what the plan asks for and
/// what the old test only gestured at (it pushed a fresh transaction id straight into
/// the downstream queue and asserted two Debug strings differed).
///
/// Here the message is claimed from the source's admitted queue under the relay's own
/// consumer group, the relay's `{ack, push(txn = message txn)}` transaction is built
/// and committed, and then committed AGAIN. The second commit must be refused, and the
/// graph must still deliver every item exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn replaying_the_relays_own_transaction_forwards_nothing_twice() {
    let Some(h) = harness("replay-relay").await else {
        return;
    };
    let (status, body) = h.put_graph("g", chain_doc()).await;
    assert_eq!(status, 200, "{body}");

    const N: usize = 12;
    for i in 0..N {
        let (status, _) = h
            .push(
                "g",
                "messages",
                json!({ "op": "m.post", "txn": format!("rr{i}"),
                        "payload": { "connection": "c1", "n": i } }),
            )
            .await;
        assert_eq!(status, 200);
    }

    let src = h
        .app
        .registry
        .get(&h.application, "g.messages")
        .expect("node");
    let dst = h.app.registry.get(&h.application, "g.ip").expect("node");
    let group = gate_server::edge::group_of(&h.application, "g", "messages", "ip");

    // Claim one the way the relay does. The real relay is competing for the same group,
    // so this retries until it wins one — either way the invariant below is the point.
    let started = Instant::now();
    let mut claimed = None;
    while started.elapsed() < Duration::from_secs(60) && claimed.is_none() {
        let msgs = h
            .app
            .queen
            .queue(src.spec.admitted_queue("default"))
            .group(&group)
            .subscription_mode(queen_mq::SubscriptionMode::All)
            .batch(1)
            .partitions(src.spec.admitted.partitions.max(1) as i32)
            .lease_seconds(30)
            .wait(false)
            .poll_timeout(Duration::from_millis(500))
            .pop()
            .await
            .expect("pop");
        claimed = msgs.into_iter().next();
    }
    let m = claimed.expect("no admitted message could be claimed under the relay's group");

    let relay_txn = || {
        h.app
            .queen
            .transaction()
            .ack(&m)
            .push_item(queen_mq::TxnPushItem {
                queue: dst.spec.push_queue(),
                partition: Some("default".into()),
                payload: m.data.clone(),
                // The relay carries the upstream id over rather than minting one. That is
                // the whole mechanism being tested.
                transaction_id: Some(m.transaction_id.clone()),
                trace_id: None,
            })
            .expect("stage")
    };

    relay_txn()
        .commit()
        .await
        .expect("the first forward must land");
    let replayed = relay_txn().commit().await;
    let err = replayed.expect_err("a replayed relay transaction must not commit twice");
    let text = err.to_string();
    assert!(
        text.contains("QDUP") || text.contains("QTXN") || text.contains("lease"),
        "the broker should refuse the replay as a duplicate or a spent lease, said: {text}"
    );

    // And the graph still delivers every item once — no copy, none lost.
    let got = h.drain("g", "ip", "g.ip", N, Duration::from_secs(90)).await;
    let seen: HashSet<i64> = got.iter().filter_map(|p| p["n"].as_i64()).collect();
    assert_eq!(
        seen.len(),
        N,
        "expected {N} distinct items, saw {}: {got:?}",
        seen.len()
    );
    assert_eq!(got.len(), N, "an item was delivered twice");

    h.cleanup("g").await;
}

/// The relay's recovery from a duplicate inside a batch.
///
/// A duplicate transaction id is a hard error inside a transaction and takes the whole
/// batch down with it, so the relay retries the batch one item at a time. Nothing
/// exercised that path. Here the destination is slow enough that its window holds the
/// relay back, which makes the LAST admitted item provably un-forwarded — so forwarding
/// it by hand, under the id the relay will use, poisons the relay's next batch by
/// construction rather than by racing it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_relay_batch_poisoned_by_a_duplicate_still_settles_every_item() {
    let Some(h) = harness("poison").await else {
        return;
    };
    // 5/s at the terminal: a window of 2 x 5 x 1s = 10, so with 30 items in flight the
    // relay is holding most of them and cannot have forwarded the tail.
    let doc = json!({
      "version": 1,
      "nodes": {
        "messages": { "entry": true, "budgets": [wide("msg")],
                      "cost": { "field": "httpCost", "default": 1, "max": 1 } },
        "ip": { "budgets": [{ "id": "ip", "cap": 5, "periodSeconds": 1, "alignment": "rolling",
                              "confidence": "inferred" }],
                "cost": { "field": "httpCost", "default": 1, "max": 1 },
                "pacing": { "leaseSeconds": 1, "batch": 20 } }
      },
      "edges": [{ "from": "messages", "to": "ip" }],
      "consume": ["ip"]
    });
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "{body}");

    let src = h
        .app
        .registry
        .get(&h.application, "g.messages")
        .expect("node");
    let dst = h.app.registry.get(&h.application, "g.ip").expect("node");

    const N: usize = 30;
    for i in 0..N {
        let (status, _) = h
            .push(
                "g",
                "messages",
                json!({ "op": "m.post", "txn": format!("po{i}"),
                        "payload": { "connection": "c1", "n": i } }),
            )
            .await;
        assert_eq!(status, 200);
    }

    // Read the stream without consuming the relay's copy: a second consumer group has
    // its own cursor over the same admitted queue, so this learns the transaction ids
    // the relay will forward under.
    let started = Instant::now();
    let mut peeked: Vec<queen_mq::Message> = Vec::new();
    while started.elapsed() < Duration::from_secs(60) && peeked.len() < N {
        let more = h
            .app
            .queen
            .queue(src.spec.admitted_queue("default"))
            .group("test.peek")
            .subscription_mode(queen_mq::SubscriptionMode::All)
            .batch(50)
            .partitions(src.spec.admitted.partitions.max(1) as i32)
            .wait(false)
            .poll_timeout(Duration::from_millis(500))
            .pop_auto_ack()
            .await
            .expect("peek");
        if more.is_empty() {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        peeked.extend(more);
    }
    assert_eq!(peeked.len(), N, "not everything was admitted upstream");
    // The last one: the relay is window-bound at ten items, so this is not forwarded.
    let poison = peeked.last().cloned().expect("admitted");

    h.app
        .queen
        .queue(dst.spec.push_queue())
        .push_items(vec![queen_mq::PushItem {
            queue: dst.spec.push_queue(),
            partition: Some("default".into()),
            payload: poison.data.clone(),
            transaction_id: Some(poison.transaction_id.clone()),
        }])
        .await
        .expect("pre-forward");

    // Every item still arrives, exactly once: the poisoned one through the push above,
    // the rest through the relay's one-at-a-time recovery.
    let got = h.drain("g", "ip", "g.ip", N, Duration::from_secs(90)).await;
    let seen: HashSet<i64> = got.iter().filter_map(|p| p["n"].as_i64()).collect();
    let mut missing: Vec<usize> = (0..N).filter(|i| !seen.contains(&(*i as i64))).collect();
    missing.sort();
    assert!(
        missing.is_empty(),
        "the relay lost items to a poisoned batch: {missing:?}"
    );
    assert_eq!(got.len(), N, "an item arrived twice");

    // And it took the recovery path rather than never seeing the duplicate at all.
    let (status, view) = h
        .send(
            reqwest::Method::GET,
            &format!("/api/apps/{}/graphs/g", h.application),
            None,
        )
        .await;
    assert_eq!(status, 200, "{view}");
    assert!(
        view["relays"][0]["duplicates"].as_u64().unwrap_or(0) >= 1,
        "the batch was never poisoned, so this test proved nothing: {}",
        view["relays"][0]
    );

    // And the relay is still working afterwards, rather than stuck on the batch.
    let (status, _) = h
        .push(
            "g",
            "messages",
            json!({ "op": "m.post", "txn": "after", "payload": { "connection": "c1", "n": 99 } }),
        )
        .await;
    assert_eq!(status, 200);
    let after = h.drain("g", "ip", "g.ip", 1, Duration::from_secs(60)).await;
    assert_eq!(
        after.len(),
        1,
        "the relay stopped forwarding after the duplicate"
    );
    assert_eq!(after[0]["n"], json!(99));

    h.cleanup("g").await;
}

/// A budget wide enough that nothing in a test is waiting for it: the relay runs at
/// whatever the broker will do, which is the regime the sharded relay exists for.
fn fast(id: &str) -> Value {
    json!({ "id": id, "cap": 20000, "periodSeconds": 10, "alignment": "rolling",
            "confidence": "inferred" })
}

/// A source whose admitted queue has `partitions` partitions is a relay of
/// `partitions` runners. Everything below turns that knob.
fn sharded_chain(partitions: u32) -> Value {
    json!({
      "version": 1,
      "nodes": {
        "messages": { "entry": true, "budgets": [fast("msg")],
                      "cost": { "field": "httpCost", "default": 1, "max": 1 },
                      "admitted": { "partitionBy": "connection", "partitions": partitions },
                      "pacing": { "leaseSeconds": 1, "batch": 2000 } },
        "ip": { "budgets": [fast("ip")],
                "cost": { "field": "httpCost", "default": 1, "max": 1 },
                "pacing": { "leaseSeconds": 1, "batch": 2000 } }
      },
      "edges": [{ "from": "messages", "to": "ip" }],
      "consume": ["ip"]
    })
}

/// Per-connection ordering, with the relay drained by one runner per source partition.
///
/// The guarantee is narrower than "the graph preserves order", and that is exactly why it
/// survives being parallelised: work is partitioned by the declared `partitionBy`, so one
/// connection's items live in ONE source partition, and one partition has ONE runner —
/// pinned when the runner is built, so there is no path that can put a second one on it.
/// Sixteen runners forwarding at once therefore reorder nothing a caller can observe.
///
/// A test with one connection would have passed before any of this existed, so this one
/// uses sixteen, checks the relay really is running sixteen runners, and then checks that
/// every connection came out in the order it went in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_connections_items_keep_their_order_across_parallel_relay_runners() {
    let Some(h) = harness("relay-order").await else {
        return;
    };
    let (status, body) = h.put_graph("g", sharded_chain(16)).await;
    assert_eq!(status, 200, "declare: {body}");
    assert_eq!(
        body["warnings"],
        json!([]),
        "declare bought caveats: {body}"
    );
    // The mechanism, asserted before anything that depends on it: sixteen partitions on
    // the source is sixteen runners on the edge. Without this the test could pass on a
    // relay that never parallelised at all.
    assert_eq!(
        body["resolved"]["relays"][0]["sources"][0]["runners"].as_u64(),
        Some(16),
        "the relay is not sharded by the source's partitions: {body}"
    );

    const CONNECTIONS: usize = 16;
    const EACH: usize = 15;
    // Round-robin, so every partition has a stream in flight at the same time rather
    // than one being filled and drained before the next is touched.
    for i in 0..EACH {
        for c in 0..CONNECTIONS {
            let (status, out) = h
                .push(
                    "g",
                    "messages",
                    json!({ "op": "m.post", "txn": format!("o{c}-{i}"),
                            "payload": { "connection": format!("c{c}"), "conn": c, "n": i } }),
                )
                .await;
            assert_eq!(status, 200, "push: {out}");
        }
    }

    let want = CONNECTIONS * EACH;
    let got = h
        .drain("g", "ip", "g.ip", want, Duration::from_secs(90))
        .await;
    assert_eq!(
        got.len(),
        want,
        "expected {want} through the graph, got {}",
        got.len()
    );

    // Order per connection, in arrival order at the terminal. Nothing is asserted about
    // how two connections interleave — they are different partitions and the graph never
    // promised anything about their relative order.
    let mut last: std::collections::HashMap<i64, i64> = std::collections::HashMap::new();
    for (at, payload) in got.iter().enumerate() {
        let (Some(conn), Some(n)) = (payload["conn"].as_i64(), payload["n"].as_i64()) else {
            panic!("an item arrived without its markers: {payload}");
        };
        if let Some(previous) = last.insert(conn, n) {
            assert!(
                n > previous,
                "connection c{conn} arrived out of order: {n} came after {previous} \
                 (position {at} of {want})"
            );
        }
    }
    assert_eq!(
        last.len(),
        CONNECTIONS,
        "not every connection arrived: {last:?}"
    );

    h.cleanup("g").await;
}

/// The exactly-once contract, on the transaction the sharded relay actually builds.
///
/// The relay carries the upstream message's transaction id into the downstream push, so a
/// redelivered relay collapses instead of doubling the work. Sharding changed the shape of
/// that transaction — one pinned source partition, pushes grouped by destination partition
/// — so the property is re-checked on the new shape: claim under the relay's own group,
/// pinned to one partition exactly as a runner does, commit the relay's transaction, then
/// commit it again.
///
/// The destination is deliberately slow, which is what makes the claim below possible at
/// all: a window of twenty against sixty-four items means most of them are still upstream
/// when the test goes looking, rather than the test racing sixteen runners for the last
/// message and losing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_redelivered_relay_is_refused_even_when_the_relay_is_sharded() {
    let Some(h) = harness("relay-dup").await else {
        return;
    };
    let mut doc = sharded_chain(16);
    // 10/s at the terminal: a window of 2 x 10 x 1s = 20.
    doc["nodes"]["ip"]["budgets"] = json!([
        { "id": "ip", "cap": 10, "periodSeconds": 1, "alignment": "rolling",
          "confidence": "inferred" }
    ]);
    doc["nodes"]["ip"]["pacing"] = json!({ "leaseSeconds": 1, "batch": 20 });
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "{body}");

    const N: usize = 64;
    for i in 0..N {
        let (status, _) = h
            .push(
                "g",
                "messages",
                json!({ "op": "m.post", "txn": format!("d{i}"),
                        "payload": { "connection": format!("c{}", i % 16), "n": i } }),
            )
            .await;
        assert_eq!(status, 200);
    }

    let src = h
        .app
        .registry
        .get(&h.application, "g.messages")
        .expect("node");
    let dst = h.app.registry.get(&h.application, "g.ip").expect("node");
    let group = gate_server::edge::group_of(&h.application, "g", "messages", "ip");
    let partitions = src.spec.admitted.partition_names();
    assert_eq!(
        partitions.len(),
        16,
        "the source ring is not what was declared"
    );

    // Claim one the way a runner does: pinned to a single partition, under the edge's
    // group. The real runners are competing for the same claims, so this sweeps the ring
    // until it wins one.
    let started = Instant::now();
    let mut claimed = None;
    while started.elapsed() < Duration::from_secs(60) && claimed.is_none() {
        for partition in &partitions {
            let msgs = h
                .app
                .queen
                .queue(src.spec.admitted_queue("default"))
                .partition(partition.clone())
                .group(&group)
                .subscription_mode(queen_mq::SubscriptionMode::All)
                .batch(1)
                .partitions(1)
                .lease_seconds(30)
                .wait(false)
                .poll_timeout(Duration::from_millis(500))
                .pop()
                .await
                .expect("pop");
            if let Some(m) = msgs.into_iter().next() {
                claimed = Some(m);
                break;
            }
        }
    }
    let m = claimed.expect("no admitted message could be claimed under the relay's group");
    // A runner acks only what it pops, and it pops one partition: the ack side of the
    // transaction is one partition wide, which is the narrowness the throughput rests on.
    assert!(
        partitions.contains(&m.partition),
        "claimed from `{}`, which is not one of this source's partitions",
        m.partition
    );

    let relay_txn = || {
        h.app
            .queen
            .transaction()
            .ack(&m)
            .push_item(queen_mq::TxnPushItem {
                queue: dst.spec.push_queue(),
                partition: Some("default".into()),
                payload: m.data.clone(),
                // Carried over rather than minted. That is the whole mechanism.
                transaction_id: Some(m.transaction_id.clone()),
                trace_id: None,
            })
            .expect("stage")
    };
    relay_txn()
        .commit()
        .await
        .expect("the first forward must land");
    let err = relay_txn()
        .commit()
        .await
        .expect_err("a redelivered relay transaction must not commit twice");
    let text = err.to_string();
    assert!(
        text.contains("QDUP") || text.contains("QTXN") || text.contains("lease"),
        "the broker should refuse the replay as a duplicate or a spent lease, said: {text}"
    );

    // And the graph still delivers every item exactly once, across all sixteen runners.
    let got = h.drain("g", "ip", "g.ip", N, Duration::from_secs(90)).await;
    let seen: HashSet<i64> = got.iter().filter_map(|p| p["n"].as_i64()).collect();
    assert_eq!(
        seen.len(),
        N,
        "expected {N} distinct items, saw {}",
        seen.len()
    );
    assert_eq!(got.len(), N, "an item was delivered twice");

    h.cleanup("g").await;
}

/// Priority is across legs; parallelism is inside one — and the window is shared.
///
/// Two things could have been broken by sharding the relay, and both are here:
///
/// * a priority-1 leg taking window from a priority-0 leg that still has work. The legs
///   are drained one at a time, however many runners each has, so it cannot;
/// * the window multiplied by the runner count. Each runner reading the destination's
///   depth for itself would give sixteen runners sixteen windows — a bottleneck queue
///   sixteen times deeper than the one that makes priority mean anything. The depth is
///   probed once per cycle and the allowance it yields is one pool the runners claim
///   from, so the bound is the window whatever the ring width.
///
/// Both legs are spread over sixteen connections, so both are genuinely many runners.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn priority_and_the_window_survive_the_relay_being_many_runners() {
    let Some(h) = harness("relay-priority").await else {
        return;
    };
    // 5 per second at the terminal: a window of 2 x 5 x 1s = 10 items in front of the
    // gate. Sixteen runners per leg against a window of ten is the interesting case —
    // it is smaller than the runner count, so a per-runner slice would be visible.
    let ring = json!({ "partitionBy": "connection", "partitions": 16 });
    let doc = json!({
      "version": 1,
      "nodes": {
        "prices": { "entry": true, "budgets": [], "admitted": ring,
                    "cost": { "field": "httpCost", "default": 1, "max": 1 } },
        "bulk": { "entry": true, "budgets": [], "admitted": ring,
                  "cost": { "field": "httpCost", "default": 1, "max": 1 } },
        "ip": { "budgets": [{ "id": "ip", "cap": 5, "periodSeconds": 1, "alignment": "rolling",
                              "confidence": "inferred" }],
                "cost": { "field": "httpCost", "default": 1, "max": 1 },
                "pacing": { "leaseSeconds": 1, "batch": 20 } }
      },
      "edges": [{ "from": "prices", "to": "ip", "priority": 0 },
                { "from": "bulk", "to": "ip", "priority": 1 }],
      "consume": ["ip"]
    });
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "declare: {body}");
    let window = body["resolved"]["relays"][0]["window"]
        .as_u64()
        .expect("window");
    assert_eq!(window, 10, "2 x 5/s x 1s: {body}");
    for leg in body["resolved"]["relays"][0]["sources"]
        .as_array()
        .expect("legs")
    {
        assert_eq!(
            leg["runners"].as_u64(),
            Some(16),
            "leg {leg} is not sharded"
        );
    }

    // A flood on the low-priority leg, spread over every partition of it.
    for i in 0..200 {
        let (status, _) = h
            .push(
                "g",
                "bulk",
                json!({ "op": "calendar.push", "txn": format!("b{i}"),
                        "payload": { "connection": format!("c{}", i % 16), "kind": "bulk",
                                     "n": i } }),
            )
            .await;
        assert_eq!(status, 200);
    }
    // Let the flood take up all the room it is going to get.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let ip = h.app.registry.get(&h.application, "g.ip").expect("node");
    let depth_before: u64 = h
        .app
        .depths
        .pending_now(&h.app.queen, &ip.spec.push_queue())
        .await
        .values()
        .sum();
    assert!(
        depth_before <= window + 20,
        "sixteen runners filled the bottleneck queue {depth_before} deep against a window \
         of {window}: the window is being applied per runner rather than shared"
    );

    let (status, _) = h
        .push(
            "g",
            "prices",
            json!({ "op": "price.push", "txn": "urgent",
                    "payload": { "connection": "c9", "kind": "urgent" } }),
        )
        .await;
    assert_eq!(status, 200);

    // It has to overtake ~200 bulk items. At 5/s that would be 40 seconds if it queued
    // behind them; priority is the difference between that and one window.
    let started = Instant::now();
    let mut urgent_after = None;
    let mut drained = 0usize;
    let mut worst_depth = depth_before;
    while started.elapsed() < Duration::from_secs(60) && urgent_after.is_none() {
        let (status, body) = h.next("g", "ip", 20, 500).await;
        assert_eq!(status, 200, "{body}");
        let items = body["items"].as_array().cloned().unwrap_or_default();
        for (i, item) in items.iter().enumerate() {
            if item["payload"]["kind"] == json!("urgent") {
                urgent_after = Some(drained + i);
            }
        }
        drained += items.len();
        if !items.is_empty() {
            let (status, ack) = h
                .ack(
                    json!({ "lease": body["lease"], "application": h.application,
                             "target": "g.ip", "lane": "default", "op": "x" }),
                )
                .await;
            assert_eq!(status, 200, "{ack}");
        }
        worst_depth = worst_depth.max(
            h.app
                .depths
                .pending_now(&h.app.queen, &ip.spec.push_queue())
                .await
                .values()
                .sum(),
        );
    }

    let position = urgent_after.expect("the priority-0 item never arrived");
    assert!(
        position <= window as usize + 20,
        "the urgent item came out behind {position} bulk items; the window is {window}, so \
         a priority-1 leg took window a priority-0 leg was owed"
    );
    assert!(
        worst_depth <= window + 20,
        "the relay overshot its window across its runners: {worst_depth} > {window}"
    );

    h.cleanup("g").await;
}

/// A leg that cannot be READ is not a leg that is empty — and the relay must not
/// confuse the two, or lose the graph to the difference.
///
/// The rule and both halves of it: a leg hands the rest of the window to the next
/// priority only when it is DRY, and dry means an empty read. A pop that errored is
/// unknown, and unknown keeps the window — that is what stops a priority-1 leg
/// taking a window a priority-0 leg was owed the moment the broker gets slow, which
/// is exactly how it was lost (188 of the first 300 items forwarded came from the
/// low-priority leg while the high one held a backlog of 172).
///
/// Unbounded, though, that hold is the failure this codebase refuses everywhere
/// else: one error stopping the graph for ever. So the hold has a length, and this
/// checks it has both — the low-priority leg waits while the high one is failing,
/// and then it does not wait for ever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_leg_that_is_not_dry_holds_its_window_but_not_for_ever() {
    let Some(h) = harness("stall").await else {
        return;
    };
    let broker = faulty_broker(&queen_url().expect("checked")).await;
    let app = Arc::new(api::App::new(
        Queen::connect(Config::new(&broker.url)).expect("connect"),
        broker.url.clone(),
    ));
    let base = spawn_server(app.clone()).await;
    let http = reqwest::Client::new();
    let application = format!("{}-stall", h.application);

    // Four partitions a side, so a cycle is four pops and not sixty-four: this test
    // spends its time waiting for cycles to pass, and the ring width is not what it
    // is about.
    let ring = json!({ "partitionBy": "connection", "partitions": 4 });
    let doc = json!({
      "version": 1,
      "nodes": {
        "prices": { "entry": true, "budgets": [], "admitted": ring,
                    "cost": { "field": "httpCost", "default": 1, "max": 1 } },
        "bulk": { "entry": true, "budgets": [], "admitted": ring,
                  "cost": { "field": "httpCost", "default": 1, "max": 1 } },
        "ip": { "budgets": [wide("ip")], "admitted": ring,
                "cost": { "field": "httpCost", "default": 1, "max": 1 },
                "pacing": { "leaseSeconds": 1, "batch": 200 } }
      },
      "edges": [{ "from": "prices", "to": "ip", "priority": 0 },
                { "from": "bulk", "to": "ip", "priority": 1 }],
      "consume": ["ip"]
    });
    let res = http
        .put(format!("{base}/v1/apps/{application}/graphs/g"))
        .json(&doc)
        .send()
        .await
        .expect("declare");
    assert_eq!(
        res.status().as_u16(),
        200,
        "{}",
        res.text().await.unwrap_or_default()
    );

    // Every read of the high-priority leg's admitted queue now fails. Its runners do
    // not come back empty — they come back with an error, which is the state the rule
    // is about. Set BEFORE anything is pushed, so nothing can have been forwarded
    // while the leg was healthy.
    broker.refuse("prices.admitted");

    for i in 0..20 {
        let res = http
            .post(format!(
                "{base}/v1/apps/{application}/graphs/g/nodes/bulk/push"
            ))
            .json(&json!({ "op": "calendar.push", "txn": format!("s{i}"),
                           "payload": { "connection": format!("c{}", i % 4), "n": i } }))
            .send()
            .await
            .expect("push");
        assert_eq!(res.status().as_u16(), 200);
    }

    let bulk = app.registry.get(&application, "g.bulk").expect("node");
    let upstream = bulk.spec.admitted_queue("default");
    let waiting = |queue: String| {
        let app = app.clone();
        async move {
            app.depths
                .pending_now(&app.queen, &queue)
                .await
                .values()
                .sum::<u64>()
        }
    };
    // What the relay has FORWARDED, not what is sitting at the destination: the
    // terminal's own gate admits what arrives within the second, so a depth read
    // there is zero whether the relay is holding or racing.
    let forwarded = || app.registry.graph(&application, "g").expect("graph").relays[0].forwarded();

    // The low-priority leg has work and the destination has room, so the ONLY thing
    // between them is the rule under test.
    let started = Instant::now();
    let mut admitted_upstream = 0;
    while started.elapsed() < Duration::from_secs(30) && admitted_upstream == 0 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        admitted_upstream = waiting(upstream.clone()).await;
    }
    assert!(
        admitted_upstream > 0,
        "the bulk leg never had a backlog, so this test proved nothing"
    );

    // While the high-priority leg is failing, the low-priority one waits.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let held = forwarded();
    assert_eq!(
        held, 0,
        "a leg whose pops are failing let the next priority through: {held} items were \
         forwarded while the priority-0 leg could not be read at all"
    );

    // And then it stops waiting: a leg that has never drained is broken rather than
    // busy, and the graph does not stop for it.
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(90) && forwarded() == 0 {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        forwarded() > 0,
        "the low-priority leg never got the window: one unreadable leg stopped the \
         graph for ever"
    );

    broker.allow();
    let _ = http
        .delete(format!("{base}/v1/apps/{application}/graphs/g"))
        .send()
        .await;
}

/// The reconcile LOOP, not just the pass it makes.
///
/// The pass is well covered; its wiring was not — removing the spawn from `run()` left
/// every test green, and the spawn is the whole mechanism by which a declare on one
/// replica reaches the others.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn the_reconcile_loop_converges_a_second_replica_on_its_own() {
    let Some(h) = harness("loop").await else {
        return;
    };
    let url = queen_url().expect("checked");

    let spec = |cap: f64| {
        json!({
          "name": "airbnb", "version": 1,
          "budgets": [{ "id": "ip", "cap": cap, "periodSeconds": 10, "alignment": "rolling",
                        "confidence": "inferred" }],
          "lanes": [{ "name": "bulk", "cap": "ceiling", "concurrency": 2, "default": true }],
          "cost": { "field": "httpCost", "default": 1, "max": 1 }
        })
    };
    let (status, body) = h
        .send(
            reqwest::Method::PUT,
            &format!("/v1/apps/{}/targets/airbnb", h.application),
            Some(spec(1000.0)),
        )
        .await;
    assert_eq!(status, 200, "{body}");

    // A second replica with the loop running and nothing else driving it.
    let b = serve(&url).await;
    let loop_task = gate_server::spawn_reconcile(b.clone(), Duration::from_millis(300));

    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(45)
        && b.registry.get(&h.application, "airbnb").is_none()
    {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let on_b = b
        .registry
        .get(&h.application, "airbnb")
        .expect("the loop should have picked the target up on its own");
    assert_eq!(on_b.spec.budgets[0].cap, 1000.0);

    // A tightened cap reaches it without anybody asking.
    let (status, body) = h
        .send(
            reqwest::Method::PUT,
            &format!("/v1/apps/{}/targets/airbnb", h.application),
            Some(spec(200.0)),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    let started = Instant::now();
    let mut cap = 1000.0;
    while started.elapsed() < Duration::from_secs(45) && cap != 200.0 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        cap = b
            .registry
            .get(&h.application, "airbnb")
            .map(|rt| rt.spec.budgets[0].cap)
            .unwrap_or(cap);
    }
    assert_eq!(cap, 200.0, "the loop did not converge the second replica");

    loop_task.abort();
    let _ = h
        .send(
            reqwest::Method::DELETE,
            &format!("/v1/apps/{}/targets/airbnb", h.application),
            None,
        )
        .await;
}

/// Strict priority with a window WIDER than a single forward.
///
/// The phase-4 test used a window of ten, which one pop covers — so it could not see that
/// a leg was popped once per pass and the leftover allowance handed to the next priority
/// while the first still had a backlog. The plan's own `ip` node has a window of 300
/// against a forward cap of 200, so this is the flagship graph's own arithmetic: a third
/// of the throughput went to bulk under sustained load on both legs.
///
/// Both legs have to be DEEP for the relay to have a decision to make, and a destination
/// fast enough to justify a wide window drains faster than a sequential test can push. So
/// the load is concurrent, and the test refuses to assert anything until the backlog it
/// needs actually exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_wide_window_does_not_leak_priority_to_the_next_leg() {
    let Some(h) = harness("leak").await else {
        return;
    };
    // 1500 per 10s -> 150/s -> a window of 2 x 150 x 1s = 300, wider than the 200 a
    // single forward carries.
    let doc = json!({
      "version": 1,
      "nodes": {
        "prices": { "entry": true, "budgets": [],
                    "cost": { "field": "httpCost", "default": 1, "max": 1 } },
        "bulk": { "entry": true, "budgets": [],
                  "cost": { "field": "httpCost", "default": 1, "max": 1 } },
        "ip": { "budgets": [{ "id": "ip", "cap": 1500, "periodSeconds": 10,
                              "alignment": "rolling", "confidence": "inferred" }],
                "cost": { "field": "httpCost", "default": 1, "max": 1 },
                "pacing": { "leaseSeconds": 1, "batch": 200 } }
      },
      "edges": [{ "from": "prices", "to": "ip", "priority": 0 },
                { "from": "bulk", "to": "ip", "priority": 1 }],
      "consume": ["ip"]
    });
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["resolved"]["relays"][0]["window"].as_u64(),
        Some(300),
        "the plan's own window arithmetic: {body}"
    );

    // Load both legs faster than the destination drains, so a backlog exists to
    // prioritise. Nothing is consumed at the terminal yet, so the only thing moving work
    // is the relay against its window.
    const EACH: usize = 1200;
    let mut writers = Vec::new();
    for task in 0..12usize {
        let http = reqwest::Client::new();
        let base = h.base.clone();
        let application = h.application.clone();
        writers.push(tokio::spawn(async move {
            for i in 0..(EACH / 12) {
                for (node, kind) in [("prices", "urgent"), ("bulk", "bulk")] {
                    let _ = http
                        .post(format!(
                            "{base}/v1/apps/{application}/graphs/g/nodes/{node}/push"
                        ))
                        .json(
                            &json!({ "op": "m.post", "txn": format!("{kind}-{task}-{i}"),
                                       "payload": { "connection": "c1", "kind": kind } }),
                        )
                        .send()
                        .await;
                }
            }
        }));
    }

    // Wait for both legs to be deep — and say so rather than asserting on an empty queue.
    let prices = h
        .app
        .registry
        .get(&h.application, "g.prices")
        .expect("node");
    let bulk_node = h.app.registry.get(&h.application, "g.bulk").expect("node");
    let started = Instant::now();
    let (mut deep_urgent, mut deep_bulk) = (0u64, 0u64);
    while started.elapsed() < Duration::from_secs(90) && deep_urgent.min(deep_bulk) < 150 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        deep_urgent = h
            .app
            .depths
            .pending_now(&h.app.queen, &prices.spec.admitted_queue("default"))
            .await
            .values()
            .sum();
        deep_bulk = h
            .app
            .depths
            .pending_now(&h.app.queen, &bulk_node.spec.admitted_queue("default"))
            .await
            .values()
            .sum();
    }
    assert!(
        deep_urgent >= 150 && deep_bulk >= 150,
        "the backlog this test needs did not build: urgent {deep_urgent}, bulk {deep_bulk}"
    );

    // What the relay chose to forward, read off the destination's own push queue under a
    // cursor of this test's own: the relay's output order IS its priority decision, and
    // reading it here does not take work away from the gate.
    let dst = h.app.registry.get(&h.application, "g.ip").expect("node");
    let mut order: Vec<String> = Vec::new();
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(90) && order.len() < 300 {
        let msgs = h
            .app
            .queen
            .queue(dst.spec.push_queue())
            .group("test.peek")
            .subscription_mode(queen_mq::SubscriptionMode::All)
            .batch(200)
            .wait(false)
            .poll_timeout(Duration::from_millis(500))
            .pop_auto_ack()
            .await
            .expect("peek");
        if msgs.is_empty() {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        for m in &msgs {
            if let Some(kind) = m.data.get("kind").and_then(|v| v.as_str()) {
                order.push(kind.to_string());
            }
        }
    }
    for w in writers {
        w.abort();
    }
    assert!(
        order.len() >= 300,
        "only {} items were forwarded",
        order.len()
    );

    // While priority 0 has a backlog, priority 1 waits. A pass that popped each leg once
    // mixed in a third of the window.
    let leaked = order.iter().take(300).filter(|k| *k == "bulk").count();
    assert!(
        leaked <= 45,
        "{leaked} of the first 300 forwarded items were priority-1 while priority-0 had a \
         backlog of {deep_urgent}"
    );

    h.cleanup("g").await;
}

/// A declare that could not be stored is not a declare that happened.
///
/// It used to warn and answer 200. With a reconcile loop that is a lie with a
/// fifteen-second fuse: the store still holds the previous spec, so the next pass restarts
/// the target on it and the change the caller was told had landed is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_declare_that_cannot_be_stored_is_not_acknowledged() {
    let Some(h) = harness("durable").await else {
        return;
    };
    let broker = faulty_broker(&queen_url().expect("checked")).await;
    let app = Arc::new(api::App::new(
        Queen::connect(Config::new(&broker.url)).expect("connect"),
        broker.url.clone(),
    ));
    let base = spawn_server(app.clone()).await;
    let http = reqwest::Client::new();
    let application = format!("{}-faulty", h.application);
    let url = format!("{base}/v1/apps/{application}/targets/airbnb");

    let spec = |cap: f64| {
        json!({
          "name": "airbnb", "version": 1,
          "budgets": [{ "id": "ip", "cap": cap, "periodSeconds": 10, "alignment": "rolling",
                        "confidence": "inferred" }],
          "lanes": [{ "name": "bulk", "cap": "ceiling", "concurrency": 2, "default": true }],
          "cost": { "field": "httpCost", "default": 1, "max": 1 }
        })
    };
    let res = http
        .put(&url)
        .json(&spec(1000.0))
        .send()
        .await
        .expect("declare");
    assert_eq!(res.status().as_u16(), 200);

    // Only the kv write is refused: provisioning succeeds, the store does not. The key is in
    // the PATH of a kv put (`/api/v1/kv/{ns}/{key}`, url-encoded), and nothing else in a
    // target declare goes near it.
    broker.refuse("/api/v1/kv/gate/spec");

    let res = http
        .put(&url)
        .json(&spec(200.0))
        .send()
        .await
        .expect("declare");
    let status = res.status().as_u16();
    let body = res.text().await.unwrap_or_default();
    assert_eq!(status, 502, "{body}");
    assert!(
        body.contains("NOT durable"),
        "the caller must be told: {body}"
    );
    broker.allow();

    // And the reconcile pass puts the stored spec back, which is what the answer said.
    gate_server::reconcile(&app).await;
    let rt = app
        .registry
        .get(&application, "airbnb")
        .expect("still serving");
    assert_eq!(
        rt.spec.budgets[0].cap, 1000.0,
        "the un-stored declare should have been reverted to the stored spec"
    );

    let _ = http.delete(&url).send().await;
}

/// A stored spec whose provisioning keeps failing has to be deletable.
///
/// It never reaches the registry, so a lookup-first delete answered 404 and never touched
/// the store — while the reconcile loop retried that same spec every interval, for ever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_target_that_cannot_be_provisioned_can_still_be_deleted() {
    let Some(h) = harness("undeletable").await else {
        return;
    };
    let broker = faulty_broker(&queen_url().expect("checked")).await;
    let app = Arc::new(api::App::new(
        Queen::connect(Config::new(&broker.url)).expect("connect"),
        broker.url.clone(),
    ));
    let base = spawn_server(app.clone()).await;
    let http = reqwest::Client::new();
    let application = format!("{}-faulty", h.application);
    let url = format!("{base}/v1/apps/{application}/targets/airbnb");

    // Declared cleanly, so the store holds it.
    let spec = json!({
      "name": "airbnb", "version": 1,
      "budgets": [{ "id": "ip", "cap": 1000, "periodSeconds": 10, "alignment": "rolling",
                    "confidence": "inferred" }],
      "lanes": [{ "name": "bulk", "cap": "ceiling", "concurrency": 2, "default": true }],
      "cost": { "field": "httpCost", "default": 1, "max": 1 }
    });
    let res = http.put(&url).json(&spec).send().await.expect("declare");
    assert_eq!(res.status().as_u16(), 200);

    // A fresh replica that cannot provision it: the spec is in the store and nothing is
    // running, which is the state an operator has to be able to get out of.
    let stuck = Arc::new(api::App::new(
        Queen::connect(Config::new(&broker.url)).expect("connect"),
        broker.url.clone(),
    ));
    let stuck_base = spawn_server(stuck.clone()).await;
    broker.refuse("admitted.bulk");
    gate_server::reconcile(&stuck).await;
    assert!(
        stuck.registry.get(&application, "airbnb").is_none(),
        "provisioning was supposed to fail on this replica"
    );

    // Delete it there. Nothing is registered, and that is not a reason to refuse: the
    // stored declaration is what a delete is about.
    broker.allow();
    let res = http
        .delete(format!("{stuck_base}/v1/apps/{application}/targets/airbnb"))
        .send()
        .await
        .expect("delete");
    let status = res.status().as_u16();
    let body: Value = res.json().await.unwrap_or(Value::Null);
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["registered"], json!(false), "{body}");

    // And it stops coming back: the reconcile has nothing left to retry.
    gate_server::reconcile(&stuck).await;
    assert!(stuck.registry.get(&application, "airbnb").is_none());
    gate_server::reconcile(&app).await;
    assert!(
        app.registry.get(&application, "airbnb").is_none(),
        "the delete did not reach the store"
    );
}

/// A depth the broker will not report is not a depth of zero.
///
/// The relay needs the truth or nothing, and gets `None`. Every other caller — the console,
/// the product metrics — is better served the last thing the broker DID say, held for the
/// cache's own TTL so an outage costs one round trip per interval instead of one per caller.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_depth_the_broker_will_not_report_falls_back_to_the_last_one() {
    let Some(h) = harness("depth").await else {
        return;
    };
    let broker = faulty_broker(&queen_url().expect("checked")).await;
    let app = Arc::new(api::App::new(
        Queen::connect(Config::new(&broker.url)).expect("connect"),
        broker.url.clone(),
    ));
    let base = spawn_server(app.clone()).await;
    let http = reqwest::Client::new();
    let application = format!("{}-faulty", h.application);

    let spec = json!({
      "name": "airbnb", "version": 1,
      "budgets": [{ "id": "ip", "cap": 1, "periodSeconds": 600, "alignment": "calendar",
                    "confidence": "inferred" }],
      "lanes": [{ "name": "bulk", "cap": "ceiling", "concurrency": 2, "default": true }],
      "cost": { "field": "httpCost", "default": 1, "max": 1 }
    });
    let res = http
        .put(format!("{base}/v1/apps/{application}/targets/airbnb"))
        .json(&spec)
        .send()
        .await
        .expect("declare");
    assert_eq!(res.status().as_u16(), 200);

    // One per ten minutes, so a few pushes stay pending and the depth is a real number.
    for i in 0..5 {
        let res = http
            .post(format!(
                "{base}/v1/apps/{application}/targets/airbnb/lanes/bulk/push"
            ))
            .json(&json!({ "op": "x", "txn": format!("d{i}"), "payload": { "connection": "c1" } }))
            .send()
            .await
            .expect("push");
        assert_eq!(res.status().as_u16(), 200);
    }
    let rt = app.registry.get(&application, "airbnb").expect("declared");
    let queue = rt.spec.push_queue();

    let started = Instant::now();
    let mut good = 0u64;
    while started.elapsed() < Duration::from_secs(45) && good == 0 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        good = app.depths.pending(&app.queen, &queue).await.values().sum();
    }
    assert!(
        good > 0,
        "nothing was pending, so this test has no number to hold on to"
    );

    // Now the admin API stops answering, and the cached answer goes stale.
    broker.refuse("/api/v1/resources/queues");
    tokio::time::sleep(Duration::from_millis(2_200)).await;
    let during: u64 = app.depths.pending(&app.queen, &queue).await.values().sum();
    assert_eq!(
        during, good,
        "an unanswered depth read was reported as an empty queue"
    );
    // And the relay's own read says "unknown" rather than "empty", which is what stops it
    // forwarding a full window against a depth it does not know.
    assert!(
        app.depths
            .try_pending_now(&app.queen, &queue)
            .await
            .is_none(),
        "the relay must be told the depth is unknown"
    );

    broker.allow();
    let _ = http
        .delete(format!("{base}/v1/apps/{application}/targets/airbnb"))
        .send()
        .await;
}

/// The question a product asks on behalf of somebody waiting: *when does this go
/// out*, and *what is holding it*.
///
/// A calendar budget of two per ten minutes, and ten items pushed at it. Two
/// leave, eight sit on the push queue, and the honest answer is not a rate — the
/// lane is measuring zero admissions per second precisely because its window is
/// spent. It is the schedule: this window is finished, the next one takes two,
/// and the eight behind you need four of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn an_eta_answers_from_the_declared_schedule_when_the_window_is_spent() {
    let Some(h) = harness("etabudget").await else {
        return;
    };
    let (status, body) = h
        .put_target(
            "airbnb",
            json!({
              "name": "airbnb", "version": 1,
              "budgets": [{ "id": "ip", "cap": 2, "periodSeconds": 600,
                            "alignment": "calendar", "confidence": "inferred" }],
              "lanes": [{ "name": "bulk", "cap": "ceiling", "concurrency": 2, "default": true }],
              "cost": { "field": "httpCost", "default": 1, "max": 1 },
              "pacing": { "leaseSeconds": 1, "batch": 10 }
            }),
        )
        .await;
    assert_eq!(status, 200, "declare: {body}");

    for i in 0..10 {
        let (status, out) = h
            .push_target(
                "airbnb",
                "bulk",
                json!({ "op": "listing.update", "txn": format!("e{i}"),
                        "payload": { "connection": "c1" } }),
            )
            .await;
        assert_eq!(status, 200, "push: {out}");
    }

    // Two admitted, eight held: the gate acks the allowed prefix and parks the
    // rest, so the group's cursor has moved by exactly what left.
    let body = h
        .eta_until("airbnb", "bulk", Duration::from_secs(60), |b| {
            b["waitingForBudget"] == json!(8)
        })
        .await;

    assert_eq!(body["state"], json!("waiting-budget"), "{body}");
    assert_eq!(body["waitingForBudget"], json!(8), "{body}");
    assert_eq!(
        body["boundBy"],
        json!("ip"),
        "the budget that is holding it: {body}"
    );
    // Eight units at two per window: this window is spent, so the wait is the
    // rest of it plus three whole ones. Never infinity, and never zero, which
    // are the two things a measured rate would have said here.
    let eta = body["etaSeconds"].as_f64().unwrap_or(-1.0);
    assert!(
        (1800.0..=2400.0).contains(&eta),
        "expected the rest of this window plus three more, got {eta}: {body}"
    );
    assert_eq!(
        body["aheadCost"],
        json!(8.0),
        "eight items at the declared cost of one: {body}"
    );

    let resets = body["windowResetsAt"].as_i64().expect("a window edge");
    let at = body["at"].as_i64().expect("an instant");
    assert!(
        resets > at && resets - at <= 600_000,
        "the next edge is ahead and inside one period: {body}"
    );
    assert!(
        body["assumes"]
            .as_str()
            .unwrap_or("")
            .contains("no earlier than"),
        "the answer must say it is a bound: {body}"
    );

    h.cleanup_target("airbnb").await;
}

/// The two backlogs have two owners, and telling them apart is the whole point:
/// gate holding work back is not the caller's consumers falling behind, and only
/// one of them is fixed by adding workers.
///
/// This also pins the name of the gate's own consumer group. The push queue here
/// is drained to nothing, so `waitingForBudget` is zero — and it is zero only
/// because the depth was read under the group the runner actually commits. A
/// group the broker has never seen owes its whole retained range, so a near-miss
/// on that string reports all twenty items as waiting for budget and looks
/// entirely reasonable doing it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn an_eta_tells_a_budget_backlog_from_a_worker_one() {
    let Some(h) = harness("etaworkers").await else {
        return;
    };
    let (status, body) = h
        .put_target(
            "airbnb",
            json!({
              "name": "airbnb", "version": 1,
              "budgets": [wide("ip")],
              "lanes": [{ "name": "bulk", "cap": "ceiling", "concurrency": 4, "default": true }],
              "cost": { "field": "httpCost", "default": 1, "max": 1 }
            }),
        )
        .await;
    assert_eq!(status, 200, "declare: {body}");

    const N: i64 = 20;
    for i in 0..N {
        let (status, out) = h
            .push_target(
                "airbnb",
                "bulk",
                json!({ "op": "listing.update", "txn": format!("w{i}"),
                        "payload": { "connection": "c1" } }),
            )
            .await;
        assert_eq!(status, 200, "push: {out}");
    }

    // Nothing pops the admitted queue, so everything the gate said yes to is
    // waiting on workers that do not exist.
    let body = h
        .eta_until("airbnb", "bulk", Duration::from_secs(60), |b| {
            b["waitingForWorkers"] == json!(N)
        })
        .await;

    assert_eq!(body["waitingForWorkers"], json!(N), "{body}");
    assert_eq!(
        body["waitingForBudget"],
        json!(0),
        "the gate admitted all of it, so nothing is waiting for budget: {body}"
    );
    assert_eq!(body["state"], json!("waiting-workers"), "{body}");
    assert_eq!(
        body["boundBy"],
        json!(null),
        "no budget is between this caller and their answer: {body}"
    );
    assert_eq!(body["windowResetsAt"], json!(null), "{body}");

    h.cleanup_target("airbnb").await;
}

/// An interior node's admitted queue is drained by the relay on its out-edge,
/// never by a caller's workers, and the ETA has to ask the broker about the
/// reader that actually exists. Asking under the executor group would name one
/// that has never popped anything — which owes its whole retained range — and a
/// leg that is keeping up perfectly would report every item it ever admitted as
/// a worker backlog.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn an_interior_leg_is_measured_against_the_relay_that_drains_it() {
    let Some(h) = harness("etanode").await else {
        return;
    };
    let (status, body) = h.put_graph("g", chain_doc()).await;
    assert_eq!(status, 200, "declare: {body}");

    const N: usize = 12;
    for i in 0..N {
        let (status, out) = h
            .push(
                "g",
                "messages",
                json!({ "op": "message.post", "txn": format!("n{i}"),
                        "payload": { "connection": "c1", "n": i } }),
            )
            .await;
        assert_eq!(status, 200, "push: {out}");
    }

    // Let the graph carry them through to the terminal, so the relay has moved
    // everything off the interior node's admitted queue.
    let got = h.drain("g", "ip", "g.ip", N, Duration::from_secs(90)).await;
    assert_eq!(got.len(), N, "the graph did not deliver: {got:?}");

    let started = Instant::now();
    let mut body = json!(null);
    while started.elapsed() < Duration::from_secs(30) {
        let (status, b) = h.node_eta("g", "messages").await;
        assert_eq!(status, 200, "eta: {b}");
        body = b;
        if body["waitingForWorkers"] == json!(0) && body["waitingForBudget"] == json!(0) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert_eq!(
        body["waitingForWorkers"],
        json!(0),
        "the relay drained it, so nothing is waiting on workers: {body}"
    );
    assert_eq!(body["waitingForBudget"], json!(0), "{body}");
    assert_eq!(body["target"], json!("g.messages"), "{body}");

    // A terminal a caller does pop is measured against the caller's own group,
    // and everything acked is gone from it too.
    let (status, terminal) = h.node_eta("g", "ip").await;
    assert_eq!(status, 200, "eta: {terminal}");
    assert_eq!(terminal["target"], json!("g.ip"), "{terminal}");

    h.cleanup("g").await;
}

/// An ETA costs the broker one depth read per queue per TTL, and that has to hold
/// on the version fallback too.
///
/// The depth route arrived in broker 1.0.4, and an older one answers 404 — the
/// same 404 an absent queue answers, which is why one fallback covers both. But a
/// 404 is an answer, and an answer that is not stamped is an answer asked for
/// again by the next caller: on a broker that will never have the route, the
/// probe repeats for ever, one round trip per request. A product polling this for
/// a progress bar across a dozen targets is exactly the caller that would find
/// out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn an_eta_against_an_older_broker_still_costs_one_probe_per_ttl() {
    let Some(h) = harness("etattl").await else {
        return;
    };
    let broker = faulty_broker(&queen_url().expect("checked")).await;
    let app = Arc::new(api::App::new(
        Queen::connect(Config::new(&broker.url)).expect("connect"),
        broker.url.clone(),
    ));
    let base = spawn_server(app.clone()).await;
    let http = reqwest::Client::new();
    let application = format!("{}-old", h.application);

    let res = http
        .put(format!("{base}/v1/apps/{application}/targets/airbnb"))
        .json(&json!({
          "name": "airbnb", "version": 1,
          "budgets": [wide("ip")],
          "lanes": [{ "name": "bulk", "cap": "ceiling", "concurrency": 2, "default": true }],
          "cost": { "field": "httpCost", "default": 1, "max": 1 }
        }))
        .send()
        .await
        .expect("declare");
    assert_eq!(res.status().as_u16(), 200);

    for i in 0..4 {
        let res = http
            .post(format!(
                "{base}/v1/apps/{application}/targets/airbnb/lanes/bulk/push"
            ))
            .json(&json!({ "op": "x", "txn": format!("t{i}"), "payload": { "connection": "c1" } }))
            .send()
            .await
            .expect("push");
        assert_eq!(res.status().as_u16(), 200);
    }

    // A broker that predates the route. Every depth read now 404s and falls back
    // to the queue detail, which every version has.
    broker.route_missing("/depth");
    // Past any entry the declare and the pushes left behind.
    tokio::time::sleep(Duration::from_millis(2_200)).await;
    broker.forget();

    const ASKS: usize = 6;
    let started = Instant::now();
    for _ in 0..ASKS {
        let res = http
            .get(format!(
                "{base}/v1/apps/{application}/targets/airbnb/eta?lane=bulk"
            ))
            .send()
            .await
            .expect("eta");
        assert_eq!(res.status().as_u16(), 200, "the fallback must still answer");
    }
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "this has to fit inside one TTL to be measuring anything: took {:?}",
        started.elapsed()
    );

    // Two queues are read — the push queue and the admitted one — so two probes
    // and two fallbacks, once, and not six of each.
    let probes = broker.hits("/depth");
    let details = broker.hits("/resources/queues/") - probes;
    assert!(
        probes <= 4,
        "{ASKS} asks inside one TTL cost {probes} depth probes: a 404 is being \
         re-asked instead of remembered"
    );
    assert!(
        details <= 4,
        "{ASKS} asks inside one TTL cost {details} queue-detail reads"
    );

    let _ = http
        .delete(format!("{base}/v1/apps/{application}/targets/airbnb"))
        .send()
        .await;
}
