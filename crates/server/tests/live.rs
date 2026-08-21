//! End to end against a real broker.
//!
//! These are the tests that hold the parts a unit test cannot reach: the relay's
//! exactly-once transaction, the refund that makes a denial free, the park-versus-
//! release threshold, fan-out identity, the breaker, and the reconcile between two
//! replicas.
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
//! configured, so `cargo test` on a machine with nothing running printed green
//! lines that verified none of the above. Ignored, the summary says `n ignored`
//! and nobody can mistake that for verified. CI runs them with
//! `--include-ignored` and sets `GATE_TEST_REQUIRE_LIVE`, which turns a missing
//! broker from a skip into a failure.
//!
//! Each test owns a freshly named application, so they neither see each other's
//! queues, their counters, nor their stored documents.
//!
//! # What was deleted from this file, and why
//!
//! * `a_throttled_call_re_enters_at_its_entry_until_its_attempts_run_out`,
//!   `a_batch_ack_re_enters_only_the_items_the_caller_names`,
//!   `an_impossible_retry_is_reported_and_still_settles_the_work`,
//!   `an_ack_that_arrives_twice_settles_once_and_retries_once`,
//!   `an_overlapping_ack_keeps_the_new_items_re_entry`,
//!   `an_ack_that_names_the_wrong_target_says_so` — all six hang off
//!   `POST /v1/leases/ack`, which is gone: the application consumes the egress
//!   queue with its own SDK and Gate never sees the outcome. What replaces the
//!   aggregate half is `the_breaker_stops_every_path_and_lifts_on_its_own`; the
//!   per-item half is an open question (design §16.6) and is NOT in this build.
//! * `priority_and_the_window_survive_the_relay_being_many_runners`,
//!   `a_leg_that_is_not_dry_holds_its_window_but_not_for_ever`,
//!   `a_wide_window_does_not_leak_priority_to_the_next_leg` — all three assert
//!   STRICT priority at a merge: drain the top leg to exhaustion before looking
//!   at the next one. v2 has no merge and no window; priority is a ceiling on
//!   one shared counter, so the property that is bought is the atomic reserve,
//!   and `the_high_share_path_keeps_admitting_while_the_low_one_refuses` asserts
//!   that instead. Priority is capacity now, not queue position.
//! * `a_shard_serialises_one_key_and_lets_another_through` — there are no
//!   shards. The per-key limit it tested is now one Postgres row per key, and
//!   `a_scoped_budget_limits_one_key_and_lets_another_through` is the same
//!   question asked of the new mechanism.
//! * `a_redelivered_relay_is_refused_even_when_the_relay_is_sharded` — same.
//! * `a_node_cannot_be_reached_through_the_target_routes` — a node IS reachable
//!   through the target routes now, because a target IS a one-node graph and the
//!   sugar resolves to it. The rule that survived is that an INTERIOR node
//!   cannot be pushed into, which is
//!   `an_interior_queue_belongs_to_the_graph_and_not_to_a_caller`.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use gate_server::api;
use queen_mq::{Config, Message, Queen, SubscriptionMode};
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
    queen: Queen,
    /// One live test at a time.
    ///
    /// They share a broker and several of them MEASURE something — how many
    /// items got through a window, how deep a queue got, whether a second
    /// admission happened. Run concurrently they contend for the same Postgres
    /// and the numbers stop meaning what the assertions read them as.
    _serial: std::sync::MutexGuard<'static, ()>,
}

/// Deliberately a std mutex held across the test's awaits: each `#[tokio::test]`
/// is its own runtime on its own thread, so blocking one thread is exactly the
/// intent.
fn one_at_a_time() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// A server in this process on an ephemeral port, plus an application name nobody
/// else will use.
///
/// # On the deadlines below
///
/// Every wait in this file is a LIVENESS bound, never the assertion: what is
/// asserted is the order things came out in, how many arrived, which key was
/// charged. So they are generous — a machine busy with a release build must not
/// be able to make a correct implementation look broken, and a wrong one still
/// fails the assertion after the wait.
#[allow(clippy::await_holding_lock)]
async fn harness(tag: &str) -> Option<Harness> {
    let serial = one_at_a_time();
    let url = match queen_url() {
        Some(u) => u,
        None => {
            assert!(
                std::env::var("GATE_TEST_REQUIRE_LIVE").is_err(),
                "GATE_TEST_REQUIRE_LIVE is set but GATE_TEST_QUEEN_URL is not: this suite cannot \
                 verify anything without a broker"
            );
            eprintln!("SKIPPED: set GATE_TEST_QUEEN_URL to a queen with kv enabled");
            return None;
        }
    };
    Some(harness_on(&url, tag, serial).await)
}

#[allow(clippy::await_holding_lock)]
async fn harness_on(url: &str, tag: &str, serial: std::sync::MutexGuard<'static, ()>) -> Harness {
    let application = format!(
        "it{}-{tag}",
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros()
    );
    let app = serve(url).await;
    let base = spawn_server(app.clone()).await;
    let queen = app.queen.clone();
    Harness {
        app,
        base,
        application,
        http: reqwest::Client::new(),
        queen,
        _serial: serial,
    }
}

/// The real router, on an ephemeral port: every test drives the HTTP surface a
/// caller drives, rather than the functions behind it.
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

    async fn get_graph(&self, name: &str) -> (u16, Value) {
        self.send(
            reqwest::Method::GET,
            &format!("/v1/apps/{}/graphs/{name}", self.application),
            None,
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

    async fn backoff(&self, graph: &str, node: &str, body: Value) -> (u16, Value) {
        self.send(
            reqwest::Method::POST,
            &format!(
                "/v1/apps/{}/graphs/{graph}/nodes/{node}/backoff",
                self.application
            ),
            Some(body),
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

    /// Pop an egress queue exactly as the application would: its own SDK, its own
    /// group, an ordinary queue that is sometimes empty.
    async fn drain(&self, queue: &str, want: usize, within: Duration) -> Vec<Message> {
        let started = Instant::now();
        let mut out: Vec<Message> = Vec::new();
        while out.len() < want && started.elapsed() < within {
            let got = self
                .queen
                .queue(queue)
                .group("test-workers")
                .subscription_mode(SubscriptionMode::All)
                .batch(100)
                .partitions(32)
                .wait(true)
                .poll_timeout(Duration::from_millis(500))
                .pop_auto_ack()
                .await
                .unwrap_or_default();
            out.extend(got);
        }
        out
    }

    /// Everything that arrives in `for_` — used where the assertion is that
    /// NOTHING more arrives.
    async fn drain_for(&self, queue: &str, for_: Duration) -> Vec<Message> {
        let started = Instant::now();
        let mut out: Vec<Message> = Vec::new();
        while started.elapsed() < for_ {
            let got = self
                .queen
                .queue(queue)
                .group("test-workers")
                .subscription_mode(SubscriptionMode::All)
                .batch(100)
                .partitions(32)
                .wait(true)
                .poll_timeout(Duration::from_millis(300))
                .pop_auto_ack()
                .await
                .unwrap_or_default();
            out.extend(got);
        }
        out
    }

    fn key(&self, graph: &str, node: &str, budget: &str) -> String {
        gate_core::plan::budget_key(&self.application, graph, node, budget)
    }

    async fn counter(&self, key: &str) -> i64 {
        self.app
            .budgets
            .read(std::slice::from_ref(&key.to_string()))
            .await
            .unwrap_or_default()
            .first()
            .map(|s| s.value)
            .unwrap_or(0)
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
/// Provisioning failures are the hardest thing in this server to test honestly:
/// the route validates the document, so a document cannot be made bad enough to
/// fail at the broker, and the recovery therefore went untested. This forwards
/// everything to the real broker except what a test asks it to refuse, so a
/// declare can fail exactly where it needs to and the HANDLER's own recovery is
/// what gets exercised.
///
/// v2 adds two failures it must cover that v1 had no equivalent of: a KV route
/// that refuses (does the relay refund and release, or lose the batch?) and a
/// transaction that fails AFTER a successful charge (does the refund fire?).
struct FaultyBroker {
    url: String,
    refuse: Arc<parking_lot::RwLock<Option<String>>>,
    absent: Arc<parking_lot::RwLock<Option<String>>>,
    seen: Arc<parking_lot::RwLock<Vec<String>>>,
}

impl FaultyBroker {
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

/// A harness in front of a broker that can be told to refuse.
///
/// The serial guard is a std mutex held across this test's awaits on purpose:
/// each `#[tokio::test]` is its own runtime on its own thread, so blocking one
/// thread is exactly the intent — the other test threads wait rather than
/// interleaving their traffic with this one's.
#[allow(clippy::await_holding_lock)]
async fn faulty_harness(tag: &str) -> Option<(Harness, FaultyBroker)> {
    let url = queen_url()?;
    let serial = one_at_a_time();
    let faulty = faulty_broker(&url).await;
    let h = harness_on(&faulty.url, tag, serial).await;
    Some((h, faulty))
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

// -------------------------------------------------------------------- fixtures

/// A budget generous enough not to be the thing under test.
fn wide(id: &str) -> Value {
    json!({ "id": id, "count": 5000, "timeMs": 1000 })
}

/// Two nodes, one hop: the smallest graph with a relay in it.
fn chain_doc() -> Value {
    json!({
      "version": 1,
      "nodes": {
        "messages": { "ingress": true, "budgets": [wide("msg")] },
        "ip": { "budgets": [wide("ip")], "egress": "test.chain.out" }
      },
      "paths": [{ "name": "main", "nodes": ["messages", "ip"] }]
    })
}

/// One node: the rrl.js shape.
fn one_node(egress: &str, budget: Value) -> Value {
    json!({
      "version": 1,
      "nodes": {
        "n": { "ingress": true, "budgets": [budget], "egress": egress }
      },
      "paths": [{ "name": "main", "nodes": ["n"] }]
    })
}

fn egress_of(tag: &str, application: &str) -> String {
    format!("test.{tag}.{application}.out")
}

// ============================================================== the relay

/// Exactly once, across a two-node graph. `got.len() == N` AND `distinct == N`,
/// and nothing arrives in a follow-up drain.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn every_item_crosses_a_two_node_graph_exactly_once() {
    let Some(h) = harness("once").await else {
        return;
    };
    let out = egress_of("once", &h.application);
    let mut doc = chain_doc();
    doc["nodes"]["ip"]["egress"] = json!(out);

    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "declare: {body}");
    assert_eq!(
        body["warnings"],
        json!([]),
        "declare bought caveats: {body}"
    );

    const N: usize = 40;
    for i in 0..N {
        let (status, res) = h
            .push(
                "g",
                "messages",
                json!({ "op": "test", "partition": format!("p{}", i % 4),
                        "payload": { "n": i } }),
            )
            .await;
        assert_eq!(status, 200, "push: {res}");
    }

    let got = h.drain(&out, N, Duration::from_secs(30)).await;
    assert_eq!(got.len(), N, "every item must arrive once");
    let distinct: HashSet<i64> = got
        .iter()
        .filter_map(|m| m.data.get("n").and_then(|v| v.as_i64()))
        .collect();
    assert_eq!(distinct.len(), N, "and only once");

    // Nothing more. A relay that forwards twice does it late, not never.
    let extra = h.drain_for(&out, Duration::from_secs(3)).await;
    assert!(extra.is_empty(), "{} arrived after the drain", extra.len());

    h.cleanup("g").await;
}

/// A connection's items keep their order end to end, across a partitioned source
/// and concurrent workers.
///
/// This is what PARTITION PASSTHROUGH buys: the producer's partition key survives
/// every hop, so per-connection ordering is preserved — and the relay's
/// transactions stay lane-disjoint, which is the other half of the same line.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_connections_items_keep_their_order_end_to_end() {
    let Some(h) = harness("order").await else {
        return;
    };
    let out = egress_of("order", &h.application);
    let mut doc = chain_doc();
    doc["nodes"]["ip"]["egress"] = json!(out);
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "declare: {body}");

    const CONNECTIONS: usize = 4;
    const EACH: usize = 25;
    for i in 0..EACH {
        for c in 0..CONNECTIONS {
            let (status, res) = h
                .push(
                    "g",
                    "messages",
                    json!({ "op": "test", "partition": format!("c{c}"),
                            "payload": { "c": c, "i": i } }),
                )
                .await;
            assert_eq!(status, 200, "push: {res}");
        }
    }

    let got = h
        .drain(&out, CONNECTIONS * EACH, Duration::from_secs(45))
        .await;
    assert_eq!(got.len(), CONNECTIONS * EACH);

    for c in 0..CONNECTIONS {
        let seq: Vec<i64> = got
            .iter()
            .filter(|m| m.data.get("c").and_then(|v| v.as_u64()) == Some(c as u64))
            .filter_map(|m| m.data.get("i").and_then(|v| v.as_i64()))
            .collect();
        let mut sorted = seq.clone();
        sorted.sort_unstable();
        assert_eq!(seq, sorted, "connection c{c} arrived out of order: {seq:?}");
    }

    h.cleanup("g").await;
}

/// A replayed relay transaction forwards nothing twice.
///
/// The push carries the upstream message's own transaction id (or a DETERMINISTIC
/// derivation of it), so the broker's dedup refuses the second one. That is the
/// exactly-once mechanism, and it is the only one: nothing in Gate keeps a table
/// of what it has forwarded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn replaying_the_relays_own_transaction_forwards_nothing_twice() {
    let Some(h) = harness("replay").await else {
        return;
    };
    let out = egress_of("replay", &h.application);
    let (status, body) = h.put_graph("g", one_node(&out, wide("b"))).await;
    assert_eq!(status, 200, "declare: {body}");

    let txn = format!("replay-{}", h.application);
    let (status, res) = h
        .push(
            "g",
            "n",
            json!({ "op": "test", "partition": "p0", "txn": txn, "payload": { "n": 1 } }),
        )
        .await;
    assert_eq!(status, 200, "push: {res}");

    let got = h.drain(&out, 1, Duration::from_secs(20)).await;
    assert_eq!(got.len(), 1);
    let forwarded_id = got[0].transaction_id.clone();

    // Replay the exact push the relay made. The broker must refuse it.
    let res = h
        .queen
        .transaction()
        .push_item(queen_mq::TxnPushItem {
            queue: out.clone(),
            partition: Some("p0".into()),
            payload: got[0].data.clone(),
            transaction_id: Some(forwarded_id.clone()),
            trace_id: None,
        })
        .expect("stage")
        .commit()
        .await;
    let err = res.err().map(|e| e.to_string()).unwrap_or_default();
    assert!(
        err.contains("QDUP") || err.contains("QTXN") || err.contains("duplicate"),
        "a replay must be refused, got: {err:?}"
    );

    // And the graph still delivers afterwards.
    let (status, _) = h
        .push(
            "g",
            "n",
            json!({ "op": "test", "partition": "p0", "payload": { "n": 2 } }),
        )
        .await;
    assert_eq!(status, 200);
    let after = h.drain(&out, 1, Duration::from_secs(20)).await;
    assert_eq!(after.len(), 1, "the graph stopped after a refused replay");

    h.cleanup("g").await;
}

/// A batch poisoned by a duplicate still settles every item.
///
/// A QDUP inside a transaction is a HARD error that rolls the whole bundle back.
/// Left alone that is a partition stalled for ever — the batch comes back, the
/// same push is refused, nothing is ever settled. The recovery is to settle one
/// item at a time, and `duplicates` is on the stage view because "should be zero"
/// is not a measurement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_batch_poisoned_by_a_duplicate_still_settles_every_item() {
    let Some(h) = harness("qdup").await else {
        return;
    };
    let out = egress_of("qdup", &h.application);

    // Plant the duplicate BEFORE the graph exists, so the relay's first push of
    // that id is the one that collides.
    let planted = format!("planted-{}", h.application);
    h.queen.queue(&out).create().await.ok();
    h.queen
        .queue(&out)
        .partition("p0")
        .push_items(vec![queen_mq::PushItem {
            queue: out.clone(),
            partition: Some("p0".into()),
            payload: json!({ "n": -1 }),
            transaction_id: Some(planted.clone()),
        }])
        .await
        .expect("plant");

    let (status, body) = h.put_graph("g", one_node(&out, wide("b"))).await;
    assert_eq!(status, 200, "declare: {body}");

    // Six items in one partition, one of which carries the planted id. The relay
    // will claim them as one batch and the transaction will roll back.
    const N: usize = 6;
    for i in 0..N {
        let txn = if i == 3 { Some(planted.clone()) } else { None };
        let (status, res) = h
            .push(
                "g",
                "n",
                json!({ "op": "test", "partition": "p0", "txn": txn, "payload": { "n": i } }),
            )
            .await;
        assert_eq!(status, 200, "push: {res}");
    }

    // Five of the six reach the egress (the sixth is the one already there).
    let got = h.drain(&out, N, Duration::from_secs(40)).await;
    let fresh: HashSet<i64> = got
        .iter()
        .filter_map(|m| m.data.get("n").and_then(|v| v.as_i64()))
        .filter(|n| *n >= 0)
        .collect();
    assert_eq!(
        fresh.len(),
        N - 1,
        "every un-duplicated item must arrive: {fresh:?}"
    );

    let (status, view) = h.get_graph("g").await;
    assert_eq!(status, 200);
    let dupes: u64 = view["stages"][0]["counters"]["duplicates"]
        .as_u64()
        .unwrap_or(0);
    assert!(dupes >= 1, "the recovery path must be visible: {view}");

    // And the stage keeps forwarding.
    let (status, _) = h
        .push(
            "g",
            "n",
            json!({ "op": "test", "partition": "p0", "payload": { "n": 99 } }),
        )
        .await;
    assert_eq!(status, 200);
    let after = h.drain(&out, 1, Duration::from_secs(20)).await;
    assert!(!after.is_empty(), "the stage stopped after a QDUP recovery");

    h.cleanup("g").await;
}

// ============================================================== the budget

/// A denial charges nothing.
///
/// The single most important v1 property, preserved by a different mechanism: v1
/// evaluated everything and applied only if all admitted; v2 charges everything
/// in one batch and REFUNDS what applied if any refused. After a batch the
/// counter must hold exactly what was admitted — never what was asked for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_denial_charges_nothing() {
    let Some(h) = harness("denial").await else {
        return;
    };
    let out = egress_of("denial", &h.application);

    // Two budgets on one node, deliberately mismatched: `wide` admits everything
    // and `narrow` admits five. Every batch that exceeds five must leave `wide`
    // holding exactly what `narrow` let through.
    let doc = json!({
      "version": 1,
      "nodes": {
        "n": {
          "ingress": true,
          "egress": out,
          "budgets": [
            { "id": "wide",   "count": 100000, "timeMs": 3600000, "subWindows": 1 },
            { "id": "narrow", "count": 5,      "timeMs": 3600000, "subWindows": 1 }
          ]
        }
      },
      "paths": [{ "name": "main", "nodes": ["n"] }]
    });
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "declare: {body}");

    for i in 0..20 {
        let (status, _) = h
            .push(
                "g",
                "n",
                json!({ "op": "test", "partition": "p0", "payload": { "n": i } }),
            )
            .await;
        assert_eq!(status, 200);
    }

    let got = h.drain(&out, 5, Duration::from_secs(25)).await;
    assert_eq!(got.len(), 5, "the narrow budget is the ceiling");

    // Give the relay a moment to try again and be refused again.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let wide_v = h.counter(&h.key("g", "n", "wide")).await;
    let narrow_v = h.counter(&h.key("g", "n", "narrow")).await;
    assert_eq!(narrow_v, 5, "the narrow counter holds what it admitted");
    assert_eq!(
        wide_v, 5,
        "the wide counter must hold what was ADMITTED, not what was asked for: a denial charges \
         nothing, and the refund is what enforces that now the counters are not in one document"
    );

    h.cleanup("g").await;
}

/// A budget refused for a long time releases the batch rather than nacking it —
/// and queen charges no retry budget on lease expiry, so the work is redelivered
/// with its retry budget intact and nothing is ever dead-lettered for waiting.
///
/// The two halves of §6.5's threshold, in one test: a one-second window PARKS in
/// handler (the message is not redelivered, so `parked` rises and `released`
/// does not), and an hour-long one RELEASES.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_short_wait_parks_and_a_long_one_releases() {
    let Some(h) = harness("park").await else {
        return;
    };

    // ---- short: a one-second sub-window. The wait is under the threshold, so
    // the handler sleeps holding the lease and retries in place.
    let short_out = egress_of("park-short", &h.application);
    let (status, body) = h
        .put_graph(
            "short",
            one_node(&short_out, json!({ "id": "b", "count": 2, "timeMs": 1000 })),
        )
        .await;
    assert_eq!(status, 200, "declare: {body}");
    for i in 0..8 {
        let _ = h
            .push(
                "short",
                "n",
                json!({ "op": "test", "partition": "p0", "payload": { "n": i } }),
            )
            .await;
    }
    let got = h.drain(&short_out, 8, Duration::from_secs(30)).await;
    assert_eq!(got.len(), 8, "a one-second window drains by parking");
    let (_, view) = h.get_graph("short").await;
    let parked = view["stages"][0]["counters"]["parked"]
        .as_u64()
        .unwrap_or(0);
    assert!(
        parked >= 1,
        "a sub-second wait must park, not release: {view}"
    );

    // ---- long: an hour-long window, spent. The wait is far above the threshold,
    // so the handler returns without acking and the lease lapses.
    let long_out = egress_of("park-long", &h.application);
    let (status, body) = h
        .put_graph(
            "long",
            one_node(
                &long_out,
                json!({ "id": "b", "count": 1, "timeMs": 3600000, "subWindows": 1 }),
            ),
        )
        .await;
    assert_eq!(status, 200, "declare: {body}");
    for i in 0..3 {
        let _ = h
            .push(
                "long",
                "n",
                json!({ "op": "test", "partition": "p0", "payload": { "n": i } }),
            )
            .await;
    }
    let got = h.drain(&long_out, 1, Duration::from_secs(25)).await;
    assert_eq!(got.len(), 1, "one item fits the window");
    tokio::time::sleep(Duration::from_secs(5)).await;
    let (_, view) = h.get_graph("long").await;
    let released = view["stages"][0]["counters"]["released"]
        .as_u64()
        .unwrap_or(0);
    assert!(
        released >= 1,
        "an hour-long wait must release the batch rather than park on it: {view}"
    );
    let deadlettered = view["stages"][0]["counters"]["deadlettered"]
        .as_u64()
        .unwrap_or(0);
    assert_eq!(
        deadlettered, 0,
        "waiting is not failing: nothing may be dead-lettered for being paced"
    );

    h.cleanup("short").await;
    h.cleanup("long").await;
}

/// A transaction that fails AFTER a successful charge refunds.
///
/// The budget is charged before the transaction because `applied` IS the
/// decision and it must be known before the transaction is built. The residual
/// hazard is real and bounded, and this is the test that it is closed: without
/// the refund, a broker that refuses transactions for a minute silently eats a
/// minute of ceiling.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_failed_transaction_after_a_successful_charge_refunds() {
    let Some((h, faulty)) = faulty_harness("refund").await else {
        assert!(std::env::var("GATE_TEST_REQUIRE_LIVE").is_err());
        return;
    };
    let out = egress_of("refund", &h.application);

    let (status, body) = h
        .put_graph(
            "g",
            one_node(
                &out,
                json!({ "id": "b", "count": 1000, "timeMs": 3600000, "subWindows": 1 }),
            ),
        )
        .await;
    assert_eq!(status, 200, "declare: {body}");

    // The relay may charge, and must not commit.
    faulty.refuse("/api/v1/transaction");
    for i in 0..4 {
        let (status, _) = h
            .push(
                "g",
                "n",
                json!({ "op": "test", "partition": "p0", "payload": { "n": i } }),
            )
            .await;
        assert_eq!(status, 200);
    }
    tokio::time::sleep(Duration::from_secs(6)).await;

    let key = h.key("g", "n", "b");
    let spent = h.counter(&key).await;
    assert_eq!(
        spent, 0,
        "a charge whose transaction did not commit must be given back; {spent} was eaten"
    );

    // And once the broker recovers, the work still goes out.
    faulty.allow();
    let got = h.drain(&out, 4, Duration::from_secs(40)).await;
    assert_eq!(got.len(), 4, "the batch was lost rather than redelivered");

    h.cleanup("g").await;
}

/// A KV route that refuses is NOT a refusal.
///
/// Reading a failed charge as a refusal would park the graph; reading it as an
/// admission would breach the ceiling. Neither is available, so the batch simply
/// does not happen — and comes back when the broker does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_kv_route_that_refuses_loses_nothing() {
    let Some((h, faulty)) = faulty_harness("kvdown").await else {
        assert!(std::env::var("GATE_TEST_REQUIRE_LIVE").is_err());
        return;
    };
    let out = egress_of("kvdown", &h.application);

    let (status, body) = h.put_graph("g", one_node(&out, wide("b"))).await;
    assert_eq!(status, 200, "declare: {body}");

    // Up FIRST. The relay admits within milliseconds of a push, so a refusal
    // armed afterwards would be measuring nothing. Only the budget call is
    // refused — not the pop, and not the transaction.
    faulty.refuse("\"op\":\"incr\"");
    for i in 0..5 {
        let (status, _) = h
            .push(
                "g",
                "n",
                json!({ "op": "test", "partition": "p0", "payload": { "n": i } }),
            )
            .await;
        assert_eq!(status, 200);
    }
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(
        h.drain_for(&out, Duration::from_secs(2)).await.is_empty(),
        "nothing may be admitted while the limiter cannot be consulted"
    );

    faulty.allow();
    let got = h.drain(&out, 5, Duration::from_secs(40)).await;
    assert_eq!(got.len(), 5, "the work must survive the outage");

    h.cleanup("g").await;
}

/// A per-key budget limits one key and lets another through, at a cardinality no
/// state document could hold.
///
/// This is v1's `a_shard_serialises_one_key_and_lets_another_through`, asked of
/// the mechanism that replaced it: one Postgres row per key with its own TTL,
/// where v1 needed shards, gate runners, partition leases and state documents.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_scoped_budget_limits_one_key_and_lets_another_through() {
    let Some(h) = harness("scoped").await else {
        return;
    };
    let out = egress_of("scoped", &h.application);

    let doc = json!({
      "version": 1,
      "nodes": {
        "n": {
          "ingress": true,
          "egress": out,
          "budgets": [
            { "id": "node",     "count": 10000, "timeMs": 1000 },
            { "id": "per-key",  "count": 2, "timeMs": 3600000, "subWindows": 1,
              "scopeBy": "payload.listing" }
          ]
        }
      },
      "paths": [{ "name": "main", "nodes": ["n"] }]
    });
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "declare: {body}");

    // Six for `hot`, two for `cold`. Different partitions, so the hot key's
    // refusal cannot hold the cold one's lane.
    for i in 0..6 {
        let _ = h
            .push(
                "g",
                "n",
                json!({ "op": "test", "partition": "hot",
                        "payload": { "listing": "hot", "n": i } }),
            )
            .await;
    }
    for i in 0..2 {
        let _ = h
            .push(
                "g",
                "n",
                json!({ "op": "test", "partition": "cold",
                        "payload": { "listing": "cold", "n": i } }),
            )
            .await;
    }

    let got = h.drain(&out, 4, Duration::from_secs(30)).await;
    let hot = got.iter().filter(|m| m.data["listing"] == "hot").count();
    let cold = got.iter().filter(|m| m.data["listing"] == "cold").count();
    assert_eq!(hot, 2, "the hot key is capped at two");
    assert_eq!(cold, 2, "and a different key is not");

    h.cleanup("g").await;
}

// ============================================================== priority

/// Under saturation the high-share path keeps admitting while the low-share one
/// refuses itself.
///
/// **This is what replaced strict priority.** v1 drained leg 0 to exhaustion
/// before looking at leg 1, and paid for it with a barrier per leg per cycle, a
/// shared allowance pool, a rotation cursor and a stall-tolerance counter — and
/// the lanes then DIVIDED the ceiling, because each was its own partition with
/// its own counter and two lanes both told "you may use the ceiling" genuinely
/// spent it twice (measured: 93/s against a declared 50/s).
///
/// Here there is ONE counter and two ceilings on it. The top half is an exact,
/// atomic reserve held by the same row lock that does the counting. What is given
/// up is head-of-line overtaking; what is bought is that the reserve is always
/// there. Priority is capacity, not queue position.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn the_high_share_path_keeps_admitting_while_the_low_one_refuses() {
    let Some(h) = harness("share").await else {
        return;
    };
    let out = egress_of("share", &h.application);

    // One shared node, two entries, two paths at 1.0 and 0.5. A long window, so
    // the counter does not rotate under the assertion.
    let doc = json!({
      "version": 1,
      "nodes": {
        "fast": { "ingress": true, "budgets": [wide("fast")] },
        "bulk": { "ingress": true, "budgets": [wide("bulk")] },
        "ip":   { "budgets": [{ "id": "ip", "count": 20, "timeMs": 3600000,
                                "subWindows": 1 }],
                  "egress": out }
      },
      "paths": [
        { "name": "fast", "priority": 0, "share": 1.0, "nodes": ["fast", "ip"] },
        { "name": "bulk", "priority": 1, "share": 0.5, "nodes": ["bulk", "ip"] }
      ]
    });
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "declare: {body}");

    // Flood the low-share path first, and let it settle at its own ceiling.
    for i in 0..40 {
        let _ = h
            .push(
                "g",
                "bulk",
                json!({ "op": "test", "partition": format!("p{}", i % 4),
                        "payload": { "who": "bulk", "n": i } }),
            )
            .await;
    }
    let _ = h.drain(&out, 10, Duration::from_secs(25)).await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    let spent = h.counter(&h.key("g", "ip", "ip")).await;
    assert!(
        (9..=11).contains(&spent),
        "the 0.5-share path must refuse itself at half of 20, found {spent}"
    );

    // The reserve above it belongs to the high-share path, and to nobody else.
    for i in 0..20 {
        let _ = h
            .push(
                "g",
                "fast",
                json!({ "op": "test", "partition": format!("p{}", i % 4),
                        "payload": { "who": "fast", "n": i } }),
            )
            .await;
    }
    let got = h.drain(&out, 10, Duration::from_secs(30)).await;
    let fast = got.iter().filter(|m| m.data["who"] == "fast").count();
    assert!(
        fast >= 8,
        "the high-share path must reach the reserve the low one cannot touch; only {fast} got \
         through"
    );

    let total = h.counter(&h.key("g", "ip", "ip")).await;
    assert!(
        total <= 20,
        "one counter, N ceilings: the total can never exceed the ceiling, found {total}"
    );

    h.cleanup("g").await;
}

// ============================================================== fan-out

/// A fan-out delivers to both branches with different, DETERMINISTIC transaction
/// ids — and a re-run of the same parent produces the same two ids.
///
/// Deterministic so a redelivered relay dedups; branch-unique so a later fan-in
/// on one queue does not collapse one of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_fanout_delivers_to_both_branches_with_distinct_deterministic_ids() {
    let Some(h) = harness("fanout").await else {
        return;
    };
    let left = egress_of("fanout-l", &h.application);
    let right = egress_of("fanout-r", &h.application);

    let doc = json!({
      "version": 1,
      "nodes": {
        "src":   { "ingress": true, "budgets": [wide("src")] },
        "left":  { "budgets": [wide("l")], "egress": left },
        "right": { "budgets": [wide("r")], "egress": right }
      },
      "paths": [{ "name": "main", "nodes": ["src", ["left", "right"]] }]
    });
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "declare: {body}");
    // The fan-out warning is mandatory, not incidental.
    let warnings = body["warnings"].as_array().cloned().unwrap_or_default();
    assert!(
        warnings
            .iter()
            .any(|w| w.as_str().unwrap_or("").contains("fanout-multiplies")),
        "a fan-out must say that the vendor sees the message twice: {warnings:?}"
    );

    let parent = format!("fan-{}", h.application);
    let (status, _) = h
        .push(
            "g",
            "src",
            json!({ "op": "test", "partition": "p0", "txn": parent, "payload": { "n": 1 } }),
        )
        .await;
    assert_eq!(status, 200);

    let l = h.drain(&left, 1, Duration::from_secs(25)).await;
    let r = h.drain(&right, 1, Duration::from_secs(25)).await;
    assert_eq!(l.len(), 1, "the left branch got nothing");
    assert_eq!(r.len(), 1, "the right branch got nothing");
    assert_ne!(
        l[0].transaction_id, r[0].transaction_id,
        "two branches must not carry one id, or a later fan-in collapses them"
    );

    // Deterministic: the same parent and the same label give the same id, every
    // release and from a shell.
    assert_eq!(
        l[0].transaction_id,
        gate_core::derive(&parent, "main/left"),
        "the derivation is the API"
    );
    assert_eq!(
        r[0].transaction_id,
        gate_core::derive(&parent, "main/right")
    );

    h.cleanup("g").await;
}

/// A fan-in on one queue from two paths does not dedup-collapse.
///
/// The hole §7's middle arm closes: two messages that entered by different paths
/// carrying the same upstream id — which is exactly what pub-sub over a shared
/// ingress produces — would collapse on arrival if the relay reused it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_fan_in_on_one_queue_does_not_dedup_collapse() {
    let Some(h) = harness("fanin").await else {
        return;
    };
    let out = egress_of("fanin", &h.application);

    // ONE ingress node, TWO paths. Each path's group gets every message, so one
    // push becomes two arrivals at `ip` — with the same upstream transaction id.
    let doc = json!({
      "version": 1,
      "nodes": {
        "src": { "ingress": true, "budgets": [wide("src")] },
        "ip":  { "budgets": [wide("ip")], "egress": out }
      },
      "paths": [
        { "name": "a", "priority": 0, "share": 1.0, "nodes": ["src", "ip"] },
        { "name": "b", "priority": 0, "share": 1.0, "nodes": ["src", "ip"] }
      ]
    });
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "declare: {body}");

    let parent = format!("fanin-{}", h.application);
    let (status, _) = h
        .push(
            "g",
            "src",
            json!({ "op": "test", "partition": "p0", "txn": parent, "payload": { "n": 1 } }),
        )
        .await;
    assert_eq!(status, 200);

    let got = h.drain(&out, 2, Duration::from_secs(30)).await;
    assert_eq!(
        got.len(),
        2,
        "two paths over one ingress is pub-sub: each path receives every message, and neither may \
         be dedup-collapsed away"
    );
    let ids: HashSet<String> = got.iter().map(|m| m.transaction_id.clone()).collect();
    assert_eq!(ids.len(), 2, "the two arrivals must carry distinct ids");

    h.cleanup("g").await;
}

/// A foreign-path message on a shared interior queue is acked and not forwarded,
/// and the owning path's cursor is unaffected.
///
/// Three groups read one interior queue and each sees every message. Only the one
/// whose `_gate.path` matches forwards it; the others settle it with a bare ack.
/// It is the one piece of bookkeeping v2 adds that v1 did not have.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_foreign_path_message_is_acked_and_not_forwarded() {
    let Some(h) = harness("foreign").await else {
        return;
    };
    let out = egress_of("foreign", &h.application);

    let doc = json!({
      "version": 1,
      "nodes": {
        "a":  { "ingress": true, "budgets": [wide("a")] },
        "b":  { "ingress": true, "budgets": [wide("b")] },
        "ip": { "budgets": [wide("ip")], "egress": out }
      },
      "paths": [
        { "name": "pa", "priority": 0, "share": 1.0, "nodes": ["a", "ip"] },
        { "name": "pb", "priority": 0, "share": 1.0, "nodes": ["b", "ip"] }
      ]
    });
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "declare: {body}");

    const N: usize = 10;
    for i in 0..N {
        let _ = h
            .push(
                "g",
                "a",
                json!({ "op": "test", "partition": "p0", "payload": { "who": "a", "n": i } }),
            )
            .await;
        let _ = h
            .push(
                "g",
                "b",
                json!({ "op": "test", "partition": "p0", "payload": { "who": "b", "n": i } }),
            )
            .await;
    }

    let got = h.drain(&out, 2 * N, Duration::from_secs(40)).await;
    assert_eq!(got.len(), 2 * N, "every item must leave exactly once");

    let (_, view) = h.get_graph("g").await;
    let stages = view["stages"].as_array().cloned().unwrap_or_default();
    let ip_stages: Vec<&Value> = stages.iter().filter(|s| s["node"] == "ip").collect();
    assert_eq!(ip_stages.len(), 2);
    let foreign: u64 = ip_stages
        .iter()
        .map(|s| s["counters"]["foreign"].as_u64().unwrap_or(0))
        .sum();
    assert!(
        foreign >= N as u64,
        "each group reads the other's messages and must settle them: {view}"
    );

    h.cleanup("g").await;
}

// ============================================================== the breaker

/// The breaker stops every path within one batch, and lifts on its own.
///
/// A vendor's 429 becomes `POST .../backoff`, which SPENDS the node's window: the
/// counter is written to its ceiling with a TTL of the vendor's own
/// `Retry-After`. Every path then refuses through the ordinary refusal path — no
/// new code path, no flag to check on the hot loop, nothing to forget to clear —
/// and traffic resumes exactly when the record expires.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn the_breaker_stops_every_path_and_lifts_on_its_own() {
    let Some(h) = harness("breaker").await else {
        return;
    };
    let out = egress_of("breaker", &h.application);

    let (status, body) = h
        .put_graph(
            "g",
            one_node(&out, json!({ "id": "b", "count": 1000, "timeMs": 1000 })),
        )
        .await;
    assert_eq!(status, 200, "declare: {body}");

    // Prove it flows first, or "nothing arrived" proves nothing.
    let _ = h
        .push(
            "g",
            "n",
            json!({ "op": "test", "partition": "p0", "payload": { "n": 0 } }),
        )
        .await;
    assert_eq!(
        h.drain(&out, 1, Duration::from_secs(20)).await.len(),
        1,
        "the graph must be flowing before the breaker is tripped"
    );

    let (status, res) = h
        .backoff("g", "n", json!({ "retryAfterSeconds": 6, "by": "test" }))
        .await;
    assert_eq!(status, 200, "backoff: {res}");
    assert_eq!(res["retryAfterSeconds"], 6);

    for i in 1..=5 {
        let _ = h
            .push(
                "g",
                "n",
                json!({ "op": "test", "partition": "p0", "payload": { "n": i } }),
            )
            .await;
    }
    let during = h.drain_for(&out, Duration::from_secs(3)).await;
    assert!(
        during.is_empty(),
        "the breaker must stop every path: {} got through",
        during.len()
    );

    // The record is what the console reads, and it carries the vendor's own
    // deadline. Not the ETA's `waitingForBudget`: that counts PENDING work, and
    // a stage parked on a refusal is holding its claim, so the work is leased
    // rather than pending — a true statement about the queue and the wrong
    // question to ask here.
    let (status, view) = h.get_graph("g").await;
    assert_eq!(status, 200, "{view}");
    let breaker = &view["nodes"][0]["breaker"];
    assert!(!breaker.is_null(), "the breaker must be visible: {view}");
    assert_eq!(breaker["retryAfterSeconds"], 6, "{view}");
    assert_eq!(breaker["by"], "test", "{view}");

    // And it lifts on its own: the record's own TTL is the whole mechanism.
    let after = h.drain(&out, 5, Duration::from_secs(30)).await;
    assert_eq!(
        after.len(),
        5,
        "traffic must resume when the window expires"
    );

    // The record is fleet-wide, which is the improvement over v1's per-replica
    // ring — a breach seen only by the pod nobody is looking at is a breach
    // nobody sees.
    let (status, recent) = h
        .send(reqwest::Method::GET, "/api/breaches/recent", None)
        .await;
    assert_eq!(status, 200, "{recent}");

    h.cleanup("g").await;
}

// ============================================================== the API

/// An interior queue belongs to the graph and not to a caller.
///
/// Pushing straight into a node the paths relay into would skip every budget
/// upstream of it, which is the one thing a limiter must not let a caller do by
/// accident.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn an_interior_queue_belongs_to_the_graph_and_not_to_a_caller() {
    let Some(h) = harness("interior").await else {
        return;
    };
    let out = egress_of("interior", &h.application);
    let mut doc = chain_doc();
    doc["nodes"]["ip"]["egress"] = json!(out);
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "declare: {body}");

    let (status, res) = h
        .push("g", "ip", json!({ "op": "test", "payload": { "n": 1 } }))
        .await;
    assert_eq!(
        status, 409,
        "pushing into an interior node must be refused: {res}"
    );
    let msg = res["error"].as_str().unwrap_or_default();
    assert!(
        msg.contains("skip every budget"),
        "the refusal must say why: {msg}"
    );
    assert!(
        msg.contains("messages"),
        "and name the nodes work does enter by: {msg}"
    );

    h.cleanup("g").await;
}

/// Two graphs may not consume one ingress queue.
///
/// Two consumers of one queue in different groups each get EVERY message, which
/// doubles what leaves. Checked against the registry AND the store, because a
/// declare lands on one replica.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn one_owner_per_ingress_queue() {
    let Some(h) = harness("owner").await else {
        return;
    };
    let shared_in = format!("test.owner.{}.in", h.application);

    let doc = |egress: &str| {
        json!({
          "version": 1,
          "nodes": {
            "n": { "ingress": { "queue": shared_in }, "budgets": [wide("b")], "egress": egress }
          },
          "paths": [{ "name": "main", "nodes": ["n"] }]
        })
    };

    let (status, body) = h.put_graph("first", doc("test.owner.a")).await;
    assert_eq!(status, 200, "declare: {body}");

    let (status, res) = h.put_graph("second", doc("test.owner.b")).await;
    assert!(
        status == 409 || status == 422,
        "a second owner of one ingress queue must be refused, got {status}: {res}"
    );

    h.cleanup("first").await;
    h.cleanup("second").await;
}

/// The routes that are gone say where to go instead.
///
/// A 404 would read as "wrong URL" and send somebody hunting; a 410 with the
/// queue name and two lines of SDK is the difference between a migration and an
/// outage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn the_gone_routes_name_the_egress_queue() {
    let Some(h) = harness("gone").await else {
        return;
    };
    let out = egress_of("gone", &h.application);
    let (status, body) = h.put_graph("g", one_node(&out, wide("b"))).await;
    assert_eq!(status, 200, "declare: {body}");

    let (status, res) = h
        .send(
            reqwest::Method::GET,
            &format!("/v1/apps/{}/graphs/g/nodes/n/next?batch=10", h.application),
            None,
        )
        .await;
    assert_eq!(status, 410, "{res}");
    assert!(
        res["error"].as_str().unwrap_or_default().contains(&out),
        "the headstone must name the queue: {res}"
    );

    let (status, res) = h
        .send(reqwest::Method::POST, "/v1/leases/ack", Some(json!({})))
        .await;
    assert_eq!(status, 410, "{res}");
    assert!(
        res["error"]
            .as_str()
            .unwrap_or_default()
            .contains("backoff"),
        "and point at what replaced it: {res}"
    );

    h.cleanup("g").await;
}

/// A v1 document is accepted, mapped, and answered 200 with warnings naming
/// every field that was mapped or ignored — never a silent success, and never a
/// 422 for having been written last year.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_v1_target_is_accepted_and_mapped() {
    let Some(h) = harness("v1").await else { return };

    let (status, body) = h
        .send(
            reqwest::Method::PUT,
            &format!("/v1/apps/{}/targets/legacy", h.application),
            Some(json!({
                "version": 1,
                "budgets": [{ "id": "api", "cap": 3000, "periodSeconds": 60,
                              "alignment": "calendar", "confidence": "inferred" }],
                "lanes": [{ "name": "default", "cap": "ceiling", "concurrency": 8,
                            "default": true }],
                "cost": { "field": "httpCost", "default": 1, "max": 5 },
                "pacing": { "leaseSeconds": 5, "batch": 250 },
                "admitted": { "partitionBy": "connection", "partitions": 8 }
            })),
        )
        .await;
    assert_eq!(status, 200, "a v1 document must still be accepted: {body}");
    assert_eq!(body["migrated"], json!(true), "{body}");
    let migration = body["migration"].as_array().cloned().unwrap_or_default();
    assert!(
        !migration.is_empty(),
        "a mapped document is never a silent success: {body}"
    );

    // The terminal queue name is stable across the migration, which is the whole
    // of §12.4's promise: the caller's consumers do not move.
    let (status, view) = h
        .send(
            reqwest::Method::GET,
            &format!("/v1/apps/{}/graphs/legacy", h.application),
            None,
        )
        .await;
    assert_eq!(status, 200, "{view}");
    assert_eq!(
        view["nodes"][0]["egressQueue"],
        json!(format!("gate.{}.legacy.admitted.default", h.application)),
        "{view}"
    );

    h.cleanup("legacy").await;
}

/// The console can draw what is running, from routes that need no broker round
/// trip per node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn the_console_can_draw_what_is_running() {
    let Some(h) = harness("console").await else {
        return;
    };
    let out = egress_of("console", &h.application);
    let mut doc = chain_doc();
    doc["nodes"]["ip"]["egress"] = json!(out);
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "declare: {body}");

    let (status, topo) = h
        .send(
            reqwest::Method::GET,
            &format!("/api/apps/{}/graphs/g/topology", h.application),
            None,
        )
        .await;
    assert_eq!(status, 200, "{topo}");
    assert_eq!(topo["edges"][0]["from"], "messages");
    assert_eq!(topo["edges"][0]["to"], "ip");
    assert_eq!(topo["paths"][0]["name"], "main");

    let (status, graphs) = h.send(reqwest::Method::GET, "/api/graphs", None).await;
    assert_eq!(status, 200, "{graphs}");
    assert!(graphs.as_array().is_some_and(|a| !a.is_empty()));

    let (status, overview) = h.send(reqwest::Method::GET, "/api/overview", None).await;
    assert_eq!(status, 200, "{overview}");
    // Probed, where v1 hardcoded `true` and a version string.
    assert_eq!(
        overview["queen"]["reachable"],
        json!(true),
        "the broker health must be probed: {overview}"
    );
    assert!(
        overview["admitted_per_sec"].is_null(),
        "without the counters stream this must be null, not a lifetime average: {overview}"
    );

    let (status, targets) = h.send(reqwest::Method::GET, "/api/targets", None).await;
    assert_eq!(status, 200, "{targets}");

    h.cleanup("g").await;
}

// ============================================================== lifecycle

/// A failed provisioning leaves the old document serving.
///
/// Without the restore the graph is left stopped and still registered: it accepts
/// pushes and admits nothing, for ever, which is the one failure an operator
/// cannot recover from without knowing this code.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_failed_provisioning_leaves_the_old_document_serving() {
    let Some((h, faulty)) = faulty_harness("provfail").await else {
        assert!(std::env::var("GATE_TEST_REQUIRE_LIVE").is_err());
        return;
    };
    let out = egress_of("provfail", &h.application);

    let (status, body) = h.put_graph("g", one_node(&out, wide("b"))).await;
    assert_eq!(status, 200, "declare: {body}");

    // Refuse the queue the NEW plan configures and nothing else. Refusing
    // `/api/v1/configure` outright would also break the RESTORE of the old plan,
    // and the test would then be measuring a broker that is down rather than a
    // declare that failed.
    faulty.refuse("cannot-be-configured");
    let mut v2 = json!({
      "version": 2,
      "nodes": {
        "cannot-be-configured": { "ingress": true, "budgets": [wide("b")], "egress": out }
      },
      "paths": [{ "name": "main", "nodes": ["cannot-be-configured"] }]
    });
    v2["version"] = json!(2);
    let (status, res) = h.put_graph("g", v2).await;
    assert_eq!(
        status, 502,
        "a broker that refuses configure must fail the declare: {res}"
    );
    assert!(
        res["error"]
            .as_str()
            .unwrap_or_default()
            .contains("still serving version 1"),
        "the caller must be told what IS serving: {res}"
    );
    faulty.allow();

    // And it is genuinely still serving, not merely registered.
    let (status, view) = h.get_graph("g").await;
    assert_eq!(status, 200, "{view}");
    assert_eq!(view["version"], 1);
    assert_eq!(view["running"], json!(true), "{view}");

    let (status, _) = h
        .push(
            "g",
            "n",
            json!({ "op": "test", "partition": "p0", "payload": { "n": 1 } }),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(h.drain(&out, 1, Duration::from_secs(25)).await.len(), 1);

    h.cleanup("g").await;
}

/// A declare that cannot be stored is not acknowledged.
///
/// It used to warn and answer 200. With a reconcile loop that is a lie with a
/// fifteen-second fuse: the store still holds the previous document, so the very
/// next pass restarts this graph on it and the change the caller was told had
/// landed is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_declare_that_cannot_be_stored_is_not_acknowledged() {
    let Some((h, faulty)) = faulty_harness("nostore").await else {
        assert!(std::env::var("GATE_TEST_REQUIRE_LIVE").is_err());
        return;
    };
    let out = egress_of("nostore", &h.application);

    // Refuse only the store write. It is a path-route `PUT /api/v1/kv/{ns}/{key}`,
    // so the key reaches the proxy URL-ENCODED and the marker has to be spelt the
    // way the wire spells it — `graph:` matches nothing.
    faulty.refuse("graph%3A");
    let (status, res) = h.put_graph("g", one_node(&out, wide("b"))).await;
    faulty.allow();
    assert_eq!(
        status, 502,
        "a declare that did not persist is not a declare: {res}"
    );
    assert!(
        res["error"]
            .as_str()
            .unwrap_or_default()
            .contains("NOT durable"),
        "the caller must be told to retry: {res}"
    );

    h.cleanup("g").await;
}

/// A graph that cannot be provisioned can still be deleted.
///
/// The stored declaration goes first, so a document whose provisioning keeps
/// failing is still removable — and a delete that cannot reach the store is
/// refused rather than half-applied.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_graph_that_cannot_be_provisioned_can_still_be_deleted() {
    let Some(h) = harness("delete").await else {
        return;
    };
    let (status, res) = h
        .send(
            reqwest::Method::DELETE,
            &format!("/v1/apps/{}/graphs/never-declared", h.application),
            None,
        )
        .await;
    assert_eq!(status, 200, "{res}");
    assert_eq!(
        res["registered"],
        json!(false),
        "not running here is a success: the document is gone, which is what was asked for"
    );
}

/// A second replica converges on the stored document.
///
/// A declare lands on ONE replica. Without the store and the reconcile the fleet
/// enforces whichever document each pod happens to hold — indefinitely, and with
/// the looser one winning, because the tighter pod simply admits less of the same
/// traffic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_second_replica_converges_on_the_stored_document() {
    let Some(h) = harness("replica").await else {
        return;
    };
    let out = egress_of("replica", &h.application);
    let (status, body) = h.put_graph("g", one_node(&out, wide("b"))).await;
    assert_eq!(status, 200, "declare: {body}");

    // A second process against the same broker, which has been told nothing.
    let second = serve(&h.app.queen_url).await;
    assert!(second.registry.all().is_empty());
    gate_server::restore(&second).await;

    let key = format!("{}/g", h.application);
    let rt = second
        .registry
        .by_key(&key)
        .expect("the second replica must pick the document up from the store");
    assert!(rt.is_running());
    assert_eq!(rt.doc.version, 1);

    // And it converges on a CHANGE, not only on a first read.
    let mut v2 = one_node(&out, wide("b"));
    v2["version"] = json!(2);
    v2["nodes"]["n"]["budgets"][0]["count"] = json!(7);
    let (status, body) = h.put_graph("g", v2).await;
    assert_eq!(status, 200, "redeclare: {body}");
    gate_server::reconcile(&second).await;
    let rt = second.registry.by_key(&key).expect("still registered");
    assert_eq!(
        rt.doc.version, 2,
        "the second replica must follow the store"
    );

    // ...and on a DELETE.
    h.cleanup("g").await;
    gate_server::reconcile(&second).await;
    assert!(
        second.registry.by_key(&key).is_none(),
        "a delete on one replica must reach the others"
    );
}

/// A replica converges on a redeclared graph instead of wedging.
///
/// The version-bump rule is enforced for a CALLER's declare only, never for one
/// applied from the store: enforcing it against a replica-local runtime is how a
/// replica wedges on a legal delete-and-redeclare at the same version.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_replica_converges_on_a_redeclared_graph_instead_of_wedging() {
    let Some(h) = harness("rewedge").await else {
        return;
    };
    let out = egress_of("rewedge", &h.application);
    let (status, _) = h.put_graph("g", one_node(&out, wide("b"))).await;
    assert_eq!(status, 200);

    let second = serve(&h.app.queen_url).await;
    gate_server::restore(&second).await;
    let key = format!("{}/g", h.application);
    assert!(second.registry.by_key(&key).is_some());

    // Delete and redeclare at the SAME version — legal for a caller, and a
    // migration-class diff from the second replica's point of view.
    h.cleanup("g").await;
    let mut again = one_node(&out, wide("b"));
    again["nodes"]["n"]["budgets"][0]["id"] = json!("renamed");
    let (status, body) = h.put_graph("g", again).await;
    assert_eq!(
        status, 200,
        "a fresh declare after a delete must be accepted: {body}"
    );

    gate_server::reconcile(&second).await;
    let rt = second
        .registry
        .by_key(&key)
        .expect("the second replica must not wedge");
    assert!(rt.is_running());
    assert_eq!(
        rt.doc.nodes["n"].budgets[0].id.as_deref(),
        Some("renamed"),
        "and it must be running the NEW document"
    );

    h.cleanup("g").await;
}

/// The reconcile loop converges a second replica on its own, on its own timer,
/// with nobody asking.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn the_reconcile_loop_converges_a_second_replica_on_its_own() {
    let Some(h) = harness("loop").await else {
        return;
    };
    let out = egress_of("loop", &h.application);

    let second = serve(&h.app.queen_url).await;
    let handle = gate_server::spawn_reconcile(second.clone(), Duration::from_millis(400));

    let (status, _) = h.put_graph("g", one_node(&out, wide("b"))).await;
    assert_eq!(status, 200);

    let key = format!("{}/g", h.application);
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(20) {
        if second.registry.by_key(&key).is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        second.registry.by_key(&key).is_some(),
        "the loop must converge without anybody asking"
    );
    handle.abort();

    h.cleanup("g").await;
}

// ============================================================== depth and eta

/// A depth the broker will not report falls back to the last one it did.
///
/// An outage costs one round trip per TTL instead of one per caller: a console
/// polling every few seconds across a dozen graphs would otherwise hammer an
/// admin API that is already unhappy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_depth_the_broker_will_not_report_falls_back_to_the_last_one() {
    let Some((h, faulty)) = faulty_harness("depth").await else {
        assert!(std::env::var("GATE_TEST_REQUIRE_LIVE").is_err());
        return;
    };
    let queue = format!("test.depth.{}", h.application);
    h.queen.queue(&queue).create().await.ok();
    h.queen
        .queue(&queue)
        .partition("p0")
        .push(json!({ "n": 1 }))
        .await
        .expect("push");

    let first: u64 = h.app.depths.pending(&h.queen, &queue).await.values().sum();
    assert_eq!(first, 1);

    // Wait past the cache TTL, then refuse.
    tokio::time::sleep(Duration::from_secs(3)).await;
    faulty.refuse("/depth");
    let stale: u64 = h.app.depths.pending(&h.queen, &queue).await.values().sum();
    assert_eq!(
        stale, 1,
        "the last answer is served rather than a zero, which would read as an empty queue"
    );
    faulty.allow();
}

/// A group-scoped depth against an older broker still costs one probe per TTL.
///
/// A 404 on the depth route is BOTH "this broker predates it" and "no such
/// queue", and they cannot be told apart. Both reasons persist, so an unstamped
/// 404 would re-probe on every request for ever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn an_eta_against_an_older_broker_still_costs_one_probe_per_ttl() {
    let Some((h, faulty)) = faulty_harness("oldbroker").await else {
        assert!(std::env::var("GATE_TEST_REQUIRE_LIVE").is_err());
        return;
    };
    let queue = format!("test.old.{}", h.application);
    h.queen.queue(&queue).create().await.ok();

    faulty.route_missing("/depth");
    faulty.forget();
    for _ in 0..5 {
        let _ = h.app.depths.pending_of_group(&h.queen, &queue, "g").await;
    }
    assert!(
        faulty.hits("/depth") <= 2,
        "the 404 must be remembered: {} probes for five reads",
        faulty.hits("/depth")
    );
    faulty.allow();
}

/// An ETA answers from the DECLARED schedule when the window is spent.
///
/// A window with nothing left in it measures zero admissions per second, and zero
/// per second answers "never" — at exactly the moment somebody is asking, because
/// a window is only exhausted while work is piling up behind it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn an_eta_answers_from_the_declared_schedule_when_the_window_is_spent() {
    let Some(h) = harness("etaspent").await else {
        return;
    };
    let out = egress_of("etaspent", &h.application);

    let (status, body) = h
        .put_graph(
            "g",
            one_node(
                &out,
                json!({ "id": "b", "count": 2, "timeMs": 60000, "subWindows": 1 }),
            ),
        )
        .await;
    assert_eq!(status, 200, "declare: {body}");

    for i in 0..12 {
        let _ = h
            .push(
                "g",
                "n",
                json!({ "op": "test", "partition": "p0", "payload": { "n": i } }),
            )
            .await;
    }
    // Let the window fill.
    let _ = h.drain(&out, 2, Duration::from_secs(25)).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let (status, eta) = h.node_eta("g", "n").await;
    assert_eq!(status, 200, "{eta}");
    assert_eq!(eta["state"], "waiting-budget", "{eta}");
    let seconds = eta["etaSeconds"].as_u64();
    assert!(
        seconds.is_some_and(|s| s > 0 && s < 10 * 60),
        "a spent window answers from its own schedule, not with never: {eta}"
    );
    assert_eq!(eta["boundBy"], json!("b"), "{eta}");
    assert!(
        eta["assumes"]
            .as_str()
            .unwrap_or_default()
            .starts_with("no earlier than"),
        "the answer is a bound and must read as one: {eta}"
    );

    h.cleanup("g").await;
}

/// An ETA tells a budget backlog from a worker one.
///
/// Reporting one number for both would tell a hotel that its prices are late
/// because of the vendor when the truth is that its own workers are short.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn an_eta_tells_a_budget_backlog_from_a_worker_one() {
    let Some(h) = harness("etaboth").await else {
        return;
    };
    let out = egress_of("etaboth", &h.application);

    let (status, body) = h
        .put_graph(
            "g",
            one_node(&out, json!({ "id": "b", "count": 10000, "timeMs": 1000 })),
        )
        .await;
    assert_eq!(status, 200, "declare: {body}");

    const N: usize = 12;
    for i in 0..N {
        let _ = h
            .push(
                "g",
                "n",
                json!({ "op": "test", "partition": "p0", "payload": { "n": i } }),
            )
            .await;
    }

    // Everything is admitted and nobody pops it: the backlog is the caller's own
    // workers, and it must not be reported as budget.
    let started = Instant::now();
    let mut eta = json!(null);
    while started.elapsed() < Duration::from_secs(25) {
        let (status, body) = h.node_eta("g", "n").await;
        assert_eq!(status, 200, "{body}");
        if body["waitingForWorkers"].as_u64().unwrap_or(0) as usize >= N {
            eta = body;
            break;
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    assert_eq!(eta["waitingForBudget"], json!(0), "{eta}");
    assert_eq!(eta["state"], "waiting-workers", "{eta}");

    h.cleanup("g").await;
}
