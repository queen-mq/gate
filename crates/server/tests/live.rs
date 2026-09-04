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
//!   `a_wide_window_does_not_leak_priority_to_the_next_leg`,
//!   `priority_at_the_entrance_is_priority_in_fact` — all FOUR assert STRICT
//!   priority at a merge: drain the top leg to exhaustion before looking at the
//!   next one. v2 has no merge and no window; priority is a ceiling on one
//!   shared counter, so the property that is bought is the atomic reserve, and
//!   `the_high_share_path_keeps_admitting_while_the_low_one_refuses` asserts
//!   that instead. Priority is capacity now, not queue position. (The fourth was
//!   deleted unnamed in the rewrite and is recorded here because §16.7 asks the
//!   author to sign off on this trade, and a miscount understates it.)
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
//! * `an_interior_leg_is_measured_against_the_relay_that_drains_it` — deleted
//!   unnamed in the rewrite, and it should not have been: the distinction it
//!   asserted still exists, split across the two branches of `eta::view`
//!   (`waiting_for_budget` reads the stage's own group on its source, which for
//!   an interior node is the interior queue; `waiting_for_workers` is populated
//!   only where there is an egress queue, so an interior node reports zero).
//!   `an_eta_tells_a_budget_backlog_from_a_worker_one` uses a ONE-node graph and
//!   therefore reaches only the terminal branch. Recorded as a known gap rather
//!   than as a justified deletion.

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
    sweep(&queen).await;
    Arc::new(api::App::new(queen, url.to_string()))
}

/// Remove the documents an earlier run left in the store, once per suite.
///
/// Every test owns a freshly named application (`it{micros}-{tag}`) and deletes
/// its own graph on the happy path — but a test that FAILS leaves its document
/// behind, and two of the tests here call `restore`, which is supposed to bring
/// back everything the store holds and cannot be asked to skip the litter. A
/// suite that gets slower every time it goes red is a suite people stop running.
///
/// Only the `it`-prefixed applications, so a broker shared with a real
/// deployment loses nothing.
async fn sweep(queen: &Queen) {
    use std::sync::OnceLock;
    static ONCE: OnceLock<()> = OnceLock::new();
    if ONCE.set(()).is_err() {
        return;
    }
    let Ok(res) = queen
        .kv()
        .get_prefix("gate", "graph:it")
        .limit(1000)
        .keys_only()
        .send()
        .await
    else {
        return;
    };
    let rows = res.rows.unwrap_or_default();
    for row in &rows {
        let _ = queen.kv().delete("gate", &row.key).send().await;
    }
    if !rows.is_empty() {
        eprintln!(
            "swept {} leftover graph document(s) from an earlier run",
            rows.len()
        );
    }
}

fn logs() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // A shorter parked-poll window, unless the caller asked for one.
        //
        // Nothing here tests the poll window, and the supervisor waits
        // `poll_timeout + 2s` for a stage to stop — so at the thirty-second
        // default every teardown in this file costs thirty seconds and the suite
        // takes twenty minutes to say what it could say in six. Set the knob
        // yourself to exercise the real one.
        if std::env::var("GATE_POLL_TIMEOUT_SECONDS").is_err() {
            std::env::set_var("GATE_POLL_TIMEOUT_SECONDS", "3");
        }
        // A narrow seeding margin, unless the caller asked for one.
        //
        // A new group on a Gate-owned interior queue starts at this runtime's
        // start MINUS `relay::INTERIOR_SEED_SKEW`, two minutes, which exists to
        // absorb disagreement between Gate's clock and the broker's. Here they
        // are the same machine, and a two-minute margin would mean
        // `a_path_added_to_a_running_graph_starts_at_the_tail` had to spend two
        // minutes ageing its backlog before the assertion meant anything. Three
        // seconds still absorbs a container clock a few hundred milliseconds
        // out — which Docker's VM clock routinely is — while leaving the
        // property testable in one wait. It is NOT a value to run in
        // production: the shipped margin also has to cover the spread between
        // replicas picking a declare up through the reconcile loop, and this
        // suite has one replica. `interior_seed`'s own unit tests pin the
        // shipped value.
        if std::env::var("GATE_INTERIOR_SEED_SKEW_SECONDS").is_err() {
            std::env::set_var("GATE_INTERIOR_SEED_SKEW_SECONDS", "3");
        }
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
    //
    // A terminal push DERIVES its id (§7), so what has to be planted is the id
    // the relay will compute for the item pushed as `planted`, not `planted`
    // itself — plant the raw one and the relay's push simply does not collide.
    let planted = format!("planted-{}", h.application);
    let collides = gate_core::derive(
        &planted,
        &gate_core::plan::egress_label(&h.application, "g", "main", "n"),
    );
    h.queen.queue(&out).create().await.ok();
    h.queen
        .queue(&out)
        .partition("p0")
        .push_items(vec![queen_mq::PushItem {
            queue: out.clone(),
            partition: Some("p0".into()),
            payload: json!({ "n": -1 }),
            transaction_id: Some(collides.clone()),
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
    //
    // A generous deadline, and the reason is measured. An unsettled claim comes
    // back at exactly its lease when the poller is not parked — 10.3s for a 10s
    // lease, 30.0s for a 30s one. Under the relay's own settings (sixteen
    // workers parked on a long poll) the same partition was observed taking
    // about a MINUTE to be offered again, while those workers polled it five
    // times a second and the group's own depth said four were owed. So the
    // assertion is that the work is not lost; the deadline is a liveness bound
    // around a broker behaviour this code cannot change.
    faulty.allow();
    let got = h.drain(&out, 4, Duration::from_secs(150)).await;
    assert_eq!(got.len(), 4, "the batch was lost rather than redelivered");

    h.cleanup("g").await;
}

/// A refund that arrives after the window rotated must NOT credit the next one.
///
/// The bug this pins: `min: 0` is a guard on the RESULTING VALUE, not on the
/// identity of the window (`024_kv.sql`, the `incr` UPDATE branch). It refuses a
/// refund into a REAPED key — the create branch is gated by the pure
/// `delta >= min` comparison, and `-D >= 0` is false — and it happily applies
/// one into a key another worker has just RECREATED. Sub-windows are a second
/// wide by default and the refund path fires exactly when a counter is
/// contended, which is exactly when its row is recreated at once, so a batch
/// straddling a rotation handed its whole delta to the next window and that
/// window then admitted `cap + delta`.
///
/// Driven against `Budgets` rather than through a graph, because the interleaving
/// is the assertion: charge, let the window die, let somebody else found the next
/// one, and only then refund.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_refund_cannot_credit_a_window_it_never_charged() {
    use gate_server::budget::{Charge, Refund};
    let Some(h) = harness("rotate").await else {
        return;
    };
    let budgets = h.app.budgets.clone();
    let key = format!("b:{}:rotate:n:b", h.application);
    let charge = |delta: i64| Charge {
        key: key.clone(),
        max: 100,
        ttl: 1,
        delta,
        budget_id: "b".into(),
    };

    // ---- the same window: a refund of our own charge applies, and the counter
    // comes back to where it started. A denial charges nothing, still.
    let a = budgets.charge(&[charge(8)]).await.expect("charge");
    assert_eq!(a.applied, vec![true]);
    assert_eq!(a.post, vec![Some(8)], "the value our charge left");
    budgets.refund(&a.refunds(&[charge(8)])).await;
    assert_eq!(h.counter(&key).await, 0, "the same window takes it back");

    // ---- a rotated window: charge, let the one-second row expire, let another
    // worker found the next window, and only then refund.
    let a = budgets.charge(&[charge(8)]).await.expect("charge");
    assert_eq!(a.applied, vec![true]);
    let refunds: Vec<Refund> = a.refunds(&[charge(8)]);
    assert_eq!(refunds.len(), 1);

    tokio::time::sleep(Duration::from_millis(1400)).await;
    let b = budgets.charge(&[charge(20)]).await.expect("charge");
    assert_eq!(b.applied, vec![true], "a fresh window admits");
    assert_eq!(
        h.counter(&key).await,
        20,
        "the new window holds only the new charge"
    );

    budgets.refund(&refunds).await;
    assert_eq!(
        h.counter(&key).await,
        20,
        "the refund of a charge from the PREVIOUS window must be refused: crediting it here \
         would let this window admit cap + 8"
    );

    let _ = budgets.clear(&[key]).await;
}

/// v1's push body carried `cost` beside the payload, and it still decides what
/// an item spends.
///
/// v2 reads a cost from the payload at the node's declared `cost.path`, because
/// a user-owned ingress queue has no envelope to put one in. A v1 caller sends
/// it at the top level — §12.1 says the endpoint shapes do not move — and
/// ignoring it charges every one of their items the declared default instead of
/// what they asked for: a limiter quietly enforcing the wrong limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_v1_push_body_still_says_what_an_item_costs() {
    let Some(h) = harness("pushcost").await else {
        return;
    };
    let out = egress_of("pushcost", &h.application);
    let doc = json!({
      "version": 1,
      "nodes": {
        "n": {
          "ingress": true,
          "egress": out,
          "cost": { "path": "payload.httpCost", "default": 1, "max": 50 },
          "budgets": [{ "id": "b", "count": 1000, "timeMs": 3600000, "subWindows": 1 }]
        }
      },
      "paths": [{ "name": "main", "nodes": ["n"] }]
    });
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "declare: {body}");

    // The v1 shape: `cost` at the top level, nothing in the payload.
    let (status, res) = h
        .push(
            "g",
            "n",
            json!({ "op": "test", "partition": "p0", "cost": 7, "payload": { "n": 1 } }),
        )
        .await;
    assert_eq!(status, 200, "{res}");
    assert_eq!(
        res["cost"],
        json!(7),
        "the door must answer what it charged"
    );

    assert_eq!(h.drain(&out, 1, Duration::from_secs(25)).await.len(), 1);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        h.counter(&h.key("g", "n", "b")).await,
        7,
        "the counter must hold what the caller declared, not the default of 1"
    );

    // A payload that carries its own cost wins: the producer meant that one.
    let (status, res) = h
        .push(
            "g",
            "n",
            json!({ "op": "test", "partition": "p0", "cost": 7,
                    "payload": { "httpCost": 2 } }),
        )
        .await;
    assert_eq!(status, 200, "{res}");
    assert_eq!(res["cost"], json!(2), "{res}");

    // And a v1 cost above the node's ceiling is refused at the door, exactly as
    // one written into the payload would be: it could never be admitted.
    let (status, res) = h
        .push(
            "g",
            "n",
            json!({ "op": "test", "partition": "p0", "cost": 500, "payload": {} }),
        )
        .await;
    assert_eq!(status, 422, "{res}");

    h.cleanup("g").await;
}

/// The product metrics endpoint answers the shape a v1 consumer decodes.
///
/// `GET /v1/apps/{app}/metrics` is what channel-go's `gateEgress` page scrapes
/// (one `Metrics` struct per poll), and a scraper that decodes into a struct
/// gets a silent ZERO for any field that was renamed — a dashboard then draws a
/// budget with a cap of nothing rather than failing. So the fields are asserted
/// by name here: the two backlogs kept apart, the ETA null rather than zero when
/// nothing is draining, and v1's `cap`/`period_seconds` beside v2's
/// `count`/`time_ms`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn the_product_metrics_endpoint_keeps_the_shape_a_consumer_decodes() {
    let Some(h) = harness("metrics").await else {
        return;
    };
    let out = egress_of("metrics", &h.application);
    let (status, body) = h
        .put_graph(
            "g",
            one_node(
                &out,
                json!({ "id": "b", "count": 5, "timeMs": 60000, "subWindows": 1 }),
            ),
        )
        .await;
    assert_eq!(status, 200, "declare: {body}");

    let (status, m) = h
        .send(
            reqwest::Method::GET,
            &format!("/v1/apps/{}/metrics", h.application),
            None,
        )
        .await;
    assert_eq!(status, 200, "{m}");
    assert_eq!(m["application"], json!(h.application));
    assert!(m["at"].as_i64().unwrap_or(0) > 0, "Gate's own clock: {m}");

    let t = &m["targets"][0];
    assert_eq!(
        t["name"],
        json!("g.n"),
        "a node is `{{graph}}.{{node}}`: {m}"
    );
    assert_eq!(t["state"], json!("flowing"));
    assert_eq!(t["waiting_for_budget"], json!(0));
    assert_eq!(t["waiting_for_workers"], json!(0));
    let b = &t["binding_budget"];
    assert_eq!(b["id"], json!("b"));
    assert_eq!(b["count"], json!(5), "v2's word");
    assert_eq!(b["cap"], json!(5), "v1's word for the same number");
    assert_eq!(b["time_ms"], json!(60000));
    assert_eq!(b["period_seconds"], json!(60));
    assert!(b["utilisation"].is_number(), "{b}");
    assert!(b["confidence"].is_string(), "{b}");
    // The counters stream is off for this graph, so nothing has measured a rate:
    // null, never a lifetime average, and the ETA has to say so too.
    assert!(t["admitted_per_sec"].is_null(), "{t}");
    assert_eq!(
        t["drain_eta_seconds"],
        json!(0),
        "nothing waiting is an eta of zero, which is not the same as `cannot say`"
    );
    assert!(t["last_breach_at"].is_null());

    // Fill the window and the state has to move, with the backlog on the budget
    // side rather than the worker side.
    for i in 0..12 {
        let (status, _) = h
            .push(
                "g",
                "n",
                json!({ "op": "test", "partition": "p0", "payload": { "n": i } }),
            )
            .await;
        assert_eq!(status, 200);
    }
    assert_eq!(h.drain(&out, 5, Duration::from_secs(25)).await.len(), 5);
    tokio::time::sleep(Duration::from_secs(2)).await;

    let (_, m) = h
        .send(
            reqwest::Method::GET,
            &format!("/v1/apps/{}/metrics", h.application),
            None,
        )
        .await;
    let t = &m["targets"][0];
    assert_eq!(t["state"], json!("pacing"), "{m}");
    assert!(
        t["waiting_for_budget"].as_u64().unwrap_or(0) > 0,
        "the limiter is holding these on purpose: {m}"
    );
    assert!(
        t["drain_eta_seconds"].is_null(),
        "no measured rate means `cannot say`, which is not zero: {m}"
    );

    // A breaker is the third state, and the timestamp is what a page renders.
    let (status, res) = h
        .backoff("g", "n", json!({ "retryAfterSeconds": 30, "by": "test" }))
        .await;
    assert_eq!(status, 200, "{res}");
    let (_, m) = h
        .send(
            reqwest::Method::GET,
            &format!("/v1/apps/{}/metrics", h.application),
            None,
        )
        .await;
    let t = &m["targets"][0];
    assert_eq!(t["state"], json!("breached"), "{m}");
    assert!(t["last_breach_at"].as_i64().unwrap_or(0) > 0, "{m}");

    h.cleanup("g").await;
}

/// An item that can never be admitted is dead-lettered, and does not take its
/// partition with it.
///
/// §13.3's most consequential behaviour change: v1 set `retry_limit` to zero
/// because it PACED by nacking and could not tell waiting from failing, so it
/// had no working DLQ at all. v2 paces by RELEASING — and queen charges no retry
/// budget on lease expiry — so a nack means what it says again and this path is
/// reachable. It replaces the declare-time `cost-monotonic` rule, and it shipped
/// without a test of its own: the only assertion on the counter was another test
/// checking it stays zero.
///
/// Reachable only through a USER-OWNED ingress queue, which is the point. The
/// HTTP door refuses a cost above `cost.max` with a 422; a producer pushing with
/// its own SDK has no door to refuse it, and the item then arrives declaring a
/// cost the node can never afford. Left in place it parks the head of its
/// partition FOR EVER, never reaching a DLQ, because a lease that expires
/// charges no retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn an_item_that_can_never_be_admitted_is_dead_lettered() {
    let Some(h) = harness("poison").await else {
        return;
    };
    let out = egress_of("poison", &h.application);
    let ingress = format!("app.poison.{}.in", h.application);
    h.queen.queue(&ingress).create().await.ok();

    let doc = json!({
      "version": 1,
      "nodes": {
        "n": {
          "ingress": { "queue": ingress, "http": false },
          "egress": out,
          "cost": { "path": "payload.w", "default": 1, "max": 5 },
          "budgets": [{ "id": "b", "count": 1000, "timeMs": 1000, "subWindows": 1 }]
        }
      },
      "paths": [{ "name": "main", "nodes": ["n"] }]
    });
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "declare: {body}");

    // The poison at the HEAD of its partition, with ordinary work behind it. The
    // work is what proves the point: it must arrive.
    let mut items = vec![queen_mq::PushItem {
        queue: ingress.clone(),
        partition: Some("p0".into()),
        payload: json!({ "w": 500, "n": -1 }),
        transaction_id: None,
    }];
    for i in 0..4 {
        items.push(queen_mq::PushItem {
            queue: ingress.clone(),
            partition: Some("p0".into()),
            payload: json!({ "w": 1, "n": i }),
            transaction_id: None,
        });
    }
    h.queen
        .queue(&ingress)
        .push_items(items)
        .await
        .expect("push");

    let got = h.drain(&out, 4, Duration::from_secs(40)).await;
    let ns: Vec<i64> = got
        .iter()
        .filter_map(|m| m.data.get("n").and_then(|v| v.as_i64()))
        .collect();
    assert_eq!(
        ns,
        vec![0, 1, 2, 3],
        "the work behind the poison must arrive, in order: an item that can never be admitted          must not park its partition for ever"
    );
    assert!(
        !ns.contains(&-1),
        "the poison itself must never be forwarded: {ns:?}"
    );

    let (_, view) = h.get_graph("g").await;
    let dead = view["stages"][0]["counters"]["deadlettered"]
        .as_u64()
        .unwrap_or(0);
    assert!(
        dead >= 1,
        "the dead-letter path must be visible: a recovery nobody can see is one nobody knows          ran. {view}"
    );

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
    // The stage's own counter, not a drain window: a machine busy with a build
    // can make a two-second drain prove nothing, and "forwarded" is exact.
    let (_, view) = h.get_graph("g").await;
    assert_eq!(
        view["stages"][0]["counters"]["forwarded"],
        json!(0),
        "nothing may be admitted while the limiter cannot be consulted: {view}"
    );

    // Generous, for the reason `a_failed_transaction_after_a_successful_charge_refunds`
    // spells out: an unsettled claim is re-offered at its lease when the poller
    // is not parked, and about a minute later when it is.
    faulty.allow();
    let got = h.drain(&out, 5, Duration::from_secs(150)).await;
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
    // Two derivations, because there are two hops: the fan-out branch, then the
    // terminal push into the branch's own egress queue. Both are the API.
    let through = |branch: &str| {
        let hop = gate_core::derive(
            &parent,
            &gate_core::plan::label(&h.application, "g", "main", branch),
        );
        gate_core::derive(
            &hop,
            &gate_core::plan::egress_label(&h.application, "g", "main", branch),
        )
    };
    assert_eq!(
        l[0].transaction_id,
        through("left"),
        "the derivation is the API"
    );
    assert_eq!(r[0].transaction_id, through("right"));

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
///
/// # Still meaningful under tail seeding, and this is why
///
/// Both paths are declared in ONE document, so both groups on `ip.in` are
/// created by the same runtime start and seeded from the same instant — and the
/// queue is freshly provisioned, so its tail IS its head and there is nothing
/// before the seed to skip. Every frame pushed below therefore reaches every
/// group, exactly as it did when the mode was `All`, and the foreign skip is
/// what has to settle them. The case where the seed does bite is a path added to
/// a graph that was already running, which is
/// `a_path_added_to_a_running_graph_starts_at_the_tail`.
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

/// A path added to a running graph starts at the TAIL of the interior queue it
/// joins, not at the head of the other paths' backlog.
///
/// # The incident this is the regression test for, 2026-09-02
///
/// `channel-go` redeclared `vrbo` adding a path `reviews` through the terminal
/// node `partner`, which three other paths already ended at. The new stage read
/// `partner`'s INTERIOR queue under a brand-new group, and every group Gate
/// owned was seeded with `All` — so the cursor started at the oldest retained
/// frame of all 105 partitions: ~19,800 frames belonging to the other three
/// paths, the oldest twelve days old. Every one of them was foreign, foreign
/// frames are settled by ack inside the relay transaction, and an ack resolves
/// by hash against `queen.log_txns`, which the broker purges after
/// `GREATEST(dedup_window, completed_retention, 900s)` while retaining the
/// segments for far longer. So every ack resolved nowhere, every transaction
/// rolled back, the cursor never moved, and the console blamed the budget.
///
/// What is asserted here is the property that makes that impossible: the new
/// group **never sees** what the other paths left behind (`foreign == 0`, `lag
/// == 0`), while the path itself works end to end and the path that was already
/// running is untouched.
///
/// The one timing assumption is the seed margin — see `logs()`, which narrows it
/// to three seconds for this suite, and the wait below that ages the backlog past
/// it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_path_added_to_a_running_graph_starts_at_the_tail() {
    let Some(h) = harness("tailseed").await else {
        return;
    };
    let out = egress_of("tailseed", &h.application);

    // Phase 1: one path, `push` -> `partner`, and a backlog on `partner`'s
    // interior queue that belongs entirely to it.
    let mut doc = json!({
      "version": 1,
      "nodes": {
        "push":    { "ingress": true, "budgets": [wide("push")] },
        "partner": { "budgets": [wide("partner")], "egress": out }
      },
      "paths": [{ "name": "push", "nodes": ["push", "partner"] }]
    });
    let (status, body) = h.put_graph("g", doc.clone()).await;
    assert_eq!(status, 200, "declare: {body}");

    const N: usize = 8;
    for i in 0..N {
        let (status, res) = h
            .push(
                "g",
                "push",
                json!({ "op": "test", "partition": "p0", "payload": { "n": i } }),
            )
            .await;
        assert_eq!(status, 200, "push {i}: {res}");
    }
    let got = h.drain(&out, N, Duration::from_secs(40)).await;
    assert_eq!(
        got.len(),
        N,
        "the first path must run before we add a second"
    );

    // The frames are still on `gate.{app}.g.partner.in` — draining the EGRESS
    // does not retire them — and that retained log is what a new group would be
    // handed. Age it past the seeding margin so "did not see the backlog" is a
    // statement about the seed rather than about the clock.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Phase 2: add `reviews` -> `partner`, exactly the shape of the incident.
    doc["nodes"]["reviews"] = json!({ "ingress": true, "budgets": [wide("reviews")] });
    doc["paths"] = json!([
        { "name": "push",    "nodes": ["push",    "partner"] },
        { "name": "reviews", "nodes": ["reviews", "partner"] }
    ]);
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "redeclare: {body}");

    // The new path works end to end. This is also what proves its group EXISTS:
    // the frame cannot reach the egress unless the stage popped, which is what
    // creates the cursor the assertions below read.
    let (status, res) = h
        .push(
            "g",
            "reviews",
            json!({ "op": "test", "partition": "p0", "payload": { "who": "reviews" } }),
        )
        .await;
    assert_eq!(status, 200, "push on the new path: {res}");
    let got = h.drain(&out, 1, Duration::from_secs(40)).await;
    assert_eq!(got.len(), 1, "the added path must admit and forward");
    assert_eq!(got[0].data["_gate"]["path"], "reviews");

    // The assertion. `foreign` is the discriminator that cannot be undone by
    // timing: seeded with `All`, this stage would have popped all N frames the
    // `push` path left behind and settled every one of them as foreign. Seeded
    // at the tail it never saw them.
    let (_, view) = h.get_graph("g").await;
    let stages = view["stages"].as_array().cloned().unwrap_or_default();
    let added = stages
        .iter()
        .find(|s| s["path"] == "reviews" && s["node"] == "partner")
        .unwrap_or_else(|| panic!("the added hop must be running: {view}"));
    assert_eq!(
        added["source"],
        "gate.".to_string() + &h.application + ".g.partner.in",
        "the added hop reads the interior queue"
    );
    assert_eq!(
        added["counters"]["foreign"], 0,
        "the new group must never have been handed the other path's backlog: {view}"
    );
    assert_eq!(
        added["lag"], 0,
        "and must owe nothing on a queue it joined at the tail: {view}"
    );
    assert_eq!(
        added["counters"]["wedged"], 0,
        "nothing here should be refusing to settle: {view}"
    );

    // And the path that was already running kept its own cursor: it is still
    // draining, and it settles the newcomer's frames as foreign the way §6.7
    // says it should.
    let (status, res) = h
        .push(
            "g",
            "push",
            json!({ "op": "test", "partition": "p0", "payload": { "n": 99 } }),
        )
        .await;
    assert_eq!(status, 200, "push on the original path: {res}");
    let got = h.drain(&out, 1, Duration::from_secs(40)).await;
    assert_eq!(got.len(), 1, "the original path must still run");
    assert_eq!(got[0].data["n"], 99, "and it is the frame we just pushed");

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
    doc["nodes"]["ip"]["budgets"][0]["source"] = json!("vendor limits page");
    doc["nodes"]["ip"]["budgets"][0]["asOf"] = json!("2026-08-20");
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

    let (status, detail) = h.get_graph("g").await;
    assert_eq!(status, 200, "{detail}");
    let budget = &detail["nodes"]
        .as_array()
        .and_then(|nodes| nodes.iter().find(|n| n["node"] == "ip"))
        .expect("ip node missing")["budgets"][0];
    assert_eq!(budget["source"], "vendor limits page", "{detail}");
    assert_eq!(budget["asOf"], "2026-08-20", "{detail}");

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

    // `/api/targets` is a list a console DRAWS, so what is in it is the
    // assertion: a bare 200 would pass against an empty array.
    let (status, targets) = h.send(reqwest::Method::GET, "/api/targets", None).await;
    assert_eq!(status, 200, "{targets}");
    let mine = targets
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .find(|t| t["application"] == json!(h.application) && t["name"] == json!("g"))
        .cloned()
        .unwrap_or_else(|| panic!("this graph is not in /api/targets: {targets}"));
    assert_eq!(mine["running"], json!(true), "{mine}");
    assert!(mine["worst_budget_id"].is_string(), "{mine}");
    assert!(
        mine["worst_period_seconds"].as_i64().unwrap_or(0) > 0,
        "the period beside the ceiling is the sub-window, and a hardcoded zero is what this          endpoint was called out for: {mine}"
    );

    // And the per-node flags the topology view draws are what the test is NAMED
    // for: which node work enters by, and which one it leaves by. v1 called
    // them `entry` and `consume` and put a `running` on every node; v2 draws
    // `ingress` and `egress`, and `running` is a property of the GRAPH — its
    // stages are started and stopped together — so it is asserted on the graph
    // view above rather than repeated per node.
    let nodes = topo["nodes"].as_array().cloned().unwrap_or_default();
    assert!(!nodes.is_empty(), "{topo}");
    assert!(
        nodes.iter().any(|n| n["ingress"] == json!(true)),
        "no node draws as an entry: {topo}"
    );
    assert!(
        nodes.iter().any(|n| n["egress"] == json!(true)),
        "no node draws as a terminal: {topo}"
    );
    assert!(
        nodes
            .iter()
            .all(|n| n["paths"].as_array().is_some_and(|p| !p.is_empty())),
        "a node no path visits cannot be drawn on a path: {topo}"
    );

    // A deleted graph is gone from the console too, which nothing observed
    // because `cleanup` discards its answer.
    h.cleanup("g").await;
    let (status, gone) = h
        .send(
            reqwest::Method::GET,
            &format!("/api/apps/{}/graphs/g", h.application),
            None,
        )
        .await;
    assert_eq!(
        status, 404,
        "a deleted graph is gone from the console too: {gone}"
    );
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

    // And the 502 is checked against the thing it PROMISES, not only against its
    // own wording. The runtime deliberately keeps serving the un-stored document
    // — tearing it down would add an outage to a failed write — so what makes
    // the message true is the reconcile pass putting the stored one back. Drop
    // this assertion and the test stays green while the sentence becomes a lie.
    //
    // Here the store holds nothing for `g` at all, because the FIRST declare is
    // what failed to persist. There is no stored document to revert to, so the
    // reconcile pass takes the other branch by design — "declared here, never
    // persisted" — and makes the running document durable rather than tearing it
    // down. Nothing is lost either way, which is the point.
    gate_server::reconcile(&h.app).await;
    let rt = h
        .app
        .registry
        .get(&h.application, "g")
        .expect("a declare that ran and could not be stored is not torn down");
    assert!(rt.is_running());

    // The half the 502 actually promises needs something to revert TO. Declare
    // cleanly, fail the store on a SECOND declare, and the reconcile must put
    // version 1 back.

    let mut v2 = one_node(&out, wide("b"));
    v2["version"] = json!(2);
    v2["nodes"]["n"]["budgets"][0]["count"] = json!(7);
    faulty.refuse("graph%3A");
    let (status, res) = h.put_graph("g", v2).await;
    faulty.allow();
    assert_eq!(status, 502, "{res}");

    let rt = h.app.registry.get(&h.application, "g").expect("serving");
    assert_eq!(
        rt.doc.version, 2,
        "the runtime keeps serving the new document until the reconcile swaps it back"
    );
    gate_server::reconcile(&h.app).await;
    let rt = h.app.registry.get(&h.application, "g").expect("serving");
    assert_eq!(
        rt.doc.version, 1,
        "the un-stored declare must have been reverted to the stored document, which is exactly \
         what the 502 told the caller would happen"
    );

    h.cleanup("g").await;
}

/// A declare that cannot be RESTORED leaves nothing registered.
///
/// The other half of the provisioning contract, and the failure the whole
/// unregister branch exists for: when the new plan cannot start and the old one
/// cannot be restarted either, nothing is serving the graph. A graph left
/// registered there would accept pushes and admit nothing, for ever, which is
/// the one state an operator cannot get out of without reading this code. So it
/// is unregistered — a push is then REFUSED, which is recoverable — and the
/// reconcile pass brings it back from the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_declare_that_cannot_be_restored_leaves_nothing_registered() {
    let Some((h, faulty)) = faulty_harness("unregister").await else {
        assert!(std::env::var("GATE_TEST_REQUIRE_LIVE").is_err());
        return;
    };
    let out = egress_of("unregister", &h.application);

    let (status, body) = h.put_graph("g", one_node(&out, wide("b"))).await;
    assert_eq!(status, 200, "declare: {body}");
    assert!(h.app.registry.get(&h.application, "g").is_some());

    // Nothing gets through now, so neither the new plan nor the old one can be
    // provisioned.
    faulty.refuse("");
    let mut v2 = one_node(&out, wide("b"));
    v2["version"] = json!(2);
    v2["nodes"]["n"]["budgets"][0]["count"] = json!(7);
    let (status, res) = h.put_graph("g", v2).await;
    assert_eq!(status, 502, "{res}");
    assert!(
        res["error"]
            .as_str()
            .unwrap_or_default()
            .contains("unregistered"),
        "the handler has to say what state it left behind: {res}"
    );
    assert!(
        h.app.registry.get(&h.application, "g").is_none(),
        "a stopped runtime must not be left registered: it would accept pushes into a queue \
         nothing drains"
    );

    // A push is refused rather than accepted into a queue nothing drains.
    faulty.allow();
    let (status, _) = h
        .push(
            "g",
            "n",
            json!({ "op": "test", "partition": "p0", "payload": { "n": 1 } }),
        )
        .await;
    assert_eq!(
        status, 404,
        "a push was accepted with nothing serving the graph"
    );

    // And the reconcile loop repairs it from the store, which still holds v1.
    gate_server::reconcile(&h.app).await;
    let rt = h
        .app
        .registry
        .get(&h.application, "g")
        .expect("the reconcile pass did not bring the graph back");
    assert_eq!(rt.doc.version, 1);
    assert!(rt.is_running());

    let (status, _) = h
        .push(
            "g",
            "n",
            json!({ "op": "test", "partition": "p0", "payload": { "n": 2 } }),
        )
        .await;
    assert_eq!(status, 200, "and it is admitting again");

    h.cleanup("g").await;
}

/// A graph that cannot be provisioned can still be deleted.
///
/// The stored declaration goes first, so a document whose provisioning keeps
/// failing is still removable — and a delete that answers 200 without reaching
/// the store is worse than a refusal, because the next reconcile pass brings the
/// graph back.
///
/// The stuck state is BUILT rather than assumed: declared cleanly so the store
/// holds it, then a second replica that cannot provision it, so nothing is
/// registered there and the delete has only the store to work on. Deleting a
/// graph that was never declared exercises none of that.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_graph_that_cannot_be_provisioned_can_still_be_deleted() {
    let Some((h, faulty)) = faulty_harness("delete").await else {
        assert!(std::env::var("GATE_TEST_REQUIRE_LIVE").is_err());
        return;
    };
    let out = egress_of("delete", &h.application);
    let key = format!("{}/g", h.application);

    // Declared cleanly, so the store holds it.
    let (status, body) = h.put_graph("g", one_node(&out, wide("b"))).await;
    assert_eq!(status, 200, "declare: {body}");

    // A fresh replica that CANNOT provision it: the document is in the store and
    // nothing is running, which is the state an operator has to be able to get
    // out of.
    let stuck = serve(&faulty.url).await;
    let stuck_base = spawn_server(stuck.clone()).await;
    faulty.refuse("configure");
    gate_server::restore(&stuck).await;
    assert!(
        stuck.registry.by_key(&key).is_none(),
        "provisioning was supposed to fail on this replica"
    );

    // Delete it THERE. Nothing is registered locally, and that is not a reason
    // to refuse: the stored declaration is what a delete is about.
    faulty.allow();
    let res = reqwest::Client::new()
        .delete(format!("{stuck_base}/v1/apps/{}/graphs/g", h.application))
        .send()
        .await
        .expect("delete");
    let status = res.status().as_u16();
    let body: Value = res.json().await.unwrap_or(Value::Null);
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["registered"],
        json!(false),
        "not running here is a success: the document is gone, which is what was asked for"
    );

    // And it stops coming back: the reconcile has nothing left to retry, on
    // EITHER replica. The second assertion is the one that catches a delete that
    // answered 200 and never landed.
    gate_server::reconcile(&stuck).await;
    assert!(stuck.registry.by_key(&key).is_none());
    gate_server::reconcile(&h.app).await;
    assert!(
        h.app.registry.by_key(&key).is_none(),
        "the delete did not reach the store"
    );
}

/// Deleting a graph nobody declared is a success, not a 404.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn deleting_a_graph_that_was_never_declared_is_a_success() {
    let Some(h) = harness("delete404").await else {
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
    assert_eq!(res["registered"], json!(false), "{res}");
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

// ====================================================== the broker's own rules
//
// Facts the whole admission algorithm rests on. They are the BROKER's, not
// Gate's, so they are asserted against it rather than inferred from a reading of
/// A stopped graph does not answer an ETA.
///
/// v1 refused `push`, `next` and `eta` alike for a registered-but-stopped
/// runtime, and the ETA is the one that matters most: it is the read that would
/// turn "registered but stopped" into a confident number. A graph whose swap
/// failed and whose old plan could not be restarted would report
/// `waiting-budget`, an `etaSeconds` and a `boundBy` — none of which anything is
/// going to act on, because no stage is running to act.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_stopped_graph_refuses_an_eta_instead_of_answering_one() {
    let Some(h) = harness("etastop").await else {
        return;
    };
    let out = egress_of("etastop", &h.application);
    let (status, body) = h.put_graph("g", one_node(&out, wide("b"))).await;
    assert_eq!(status, 200, "declare: {body}");

    let (status, eta) = h.node_eta("g", "n").await;
    assert_eq!(status, 200, "a running graph answers: {eta}");

    // Stop the stages without unregistering, which is exactly the state a failed
    // swap leaves behind.
    let rt = h.app.registry.get(&h.application, "g").expect("registered");
    gate_server::supervisor::cancel(&rt);
    assert!(!rt.is_running());

    let (status, res) = h.node_eta("g", "n").await;
    assert_eq!(
        status, 503,
        "a stopped graph must be refused, not answered with a number: {res}"
    );
    assert!(
        res["error"]
            .as_str()
            .unwrap_or_default()
            .contains("not running"),
        "{res}"
    );

    h.cleanup("g").await;
}

/// Re-entry puts an item back at the door it came in at, bounded, and idempotent
/// in its transaction id. Design §16.6, option (2).
///
/// The three properties v1's `plan_retro` had, asserted one by one: it lands on
/// the ORIGIN ingress queue (so it re-pays every budget on its path rather than
/// skipping the ones upstream of where it failed), the attempt rides in the id
/// (so a caller reporting one item twice collapses on the broker's dedup), and
/// it stops at `maxAttempts`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_throttled_item_re_enters_at_its_origin_until_its_attempts_run_out() {
    let Some(h) = harness("reenter").await else {
        return;
    };
    let out = egress_of("reenter", &h.application);
    let mut doc = chain_doc();
    doc["nodes"]["ip"]["egress"] = json!(out);
    doc["maxAttempts"] = json!(2);
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "declare: {body}");

    // One item across the two-node graph, popped off the egress queue exactly as
    // the application would.
    let (status, _) = h
        .push(
            "g",
            "messages",
            json!({ "op": "test", "partition": "p0", "txn": "origin-1",
                    "payload": { "n": 1 } }),
        )
        .await;
    assert_eq!(status, 200);
    let got = h.drain(&out, 1, Duration::from_secs(25)).await;
    assert_eq!(got.len(), 1);
    let arrived = got[0].data.clone();
    assert_eq!(arrived["_gate"]["path"], json!("main"));

    async fn reenter(h: &Harness, payload: &Value, txn: &str) -> (u16, Value) {
        h.send(
            reqwest::Method::POST,
            &format!("/v1/apps/{}/graphs/g/reenter", h.application),
            Some(json!({ "payload": payload, "txn": txn, "partition": "p0" })),
        )
        .await
    }

    // ---- attempt 1: back at the door, and it comes out the other end again.
    let (status, res) = reenter(&h, &arrived, &got[0].transaction_id).await;
    assert_eq!(status, 200, "{res}");
    assert_eq!(res["attempt"], json!(1));
    assert_eq!(
        res["node"],
        json!("messages"),
        "the ORIGIN entry, not the node that was throttled: {res}"
    );
    assert_eq!(
        res["queue"],
        json!(gate_core::plan::owned_ingress_queue(
            &h.application,
            "g",
            "messages"
        )),
        "{res}"
    );

    // Reporting the SAME item again is the same id, so the broker's dedup
    // collapses it and nothing re-enters twice. Nothing here keeps a table.
    let (status, again) = reenter(&h, &arrived, &got[0].transaction_id).await;
    assert_eq!(status, 200, "{again}");
    assert_eq!(again["transactionId"], res["transactionId"]);

    let round2 = h.drain(&out, 1, Duration::from_secs(25)).await;
    assert_eq!(round2.len(), 1, "exactly one item came back round");
    assert_eq!(
        round2[0].data["_gate"]["attempt"],
        json!(1),
        "the attempt has to survive every hop, or the next report starts again at one: {}",
        round2[0].data
    );

    // ---- attempt 2 is the last one the declaration allows.
    let (status, res) = reenter(&h, &round2[0].data, &round2[0].transaction_id).await;
    assert_eq!(status, 200, "{res}");
    assert_eq!(res["attempt"], json!(2));
    let round3 = h.drain(&out, 1, Duration::from_secs(25)).await;
    assert_eq!(round3.len(), 1);
    assert_eq!(round3[0].data["_gate"]["attempt"], json!(2));

    // ---- attempt 3 is refused: an unbounded re-entry is a livelock the
    // limiter would be paying for.
    let (status, res) = reenter(&h, &round3[0].data, &round3[0].transaction_id).await;
    assert_eq!(status, 422, "{res}");
    assert!(
        res["error"].as_str().unwrap_or_default().contains("2"),
        "the bound has to be named: {res}"
    );
    assert_eq!(
        h.drain_for(&out, Duration::from_secs(4)).await.len(),
        0,
        "nothing re-entered past the bound"
    );

    h.cleanup("g").await;
}

/// A claim wider than the broker's op ceiling is chunked, not refused.
///
/// One `kv` call carries at most 256 ops (`024_kv.sql`, `C_MAX_OPS_HTTP`), and a
/// `scopeBy` budget mints one counter per distinct value in the claim — so a
/// node with a scoped budget and a large batch built a call the broker rejected
/// outright with `kv_too_many_ops`. `charge` returned `Err`, the relay read that
/// as "try again later", and the identical claim came back for ever: a
/// livelocked partition with no dead letter and one repeating WARN.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_charge_wider_than_one_kv_call_is_chunked() {
    use gate_server::budget::Charge;
    let Some(h) = harness("manykeys").await else {
        return;
    };
    // 400 distinct counters in one charge: over the 256-op ceiling by enough
    // that a single call cannot be what happens.
    let charges: Vec<Charge> = (0..400)
        .map(|i| Charge {
            key: format!("b:{}:manykeys:n:per:{i}", h.application),
            max: 10,
            ttl: 60,
            delta: 1,
            budget_id: "per".into(),
        })
        .collect();

    let a = h.app.budgets.charge(&charges).await.expect(
        "a claim wider than one kv call must be chunked, not sent as one call the broker refuses",
    );
    assert_eq!(a.applied.len(), 400, "index-aligned to the charges");
    assert!(a.applied.iter().all(|ok| *ok), "all under their ceiling");
    assert_eq!(a.post.len(), 400);
    assert!(a.post.iter().all(|p| *p == Some(1)));
    assert_eq!(
        a.states.len(),
        400,
        "and the read rides along in every chunk, or the park deadline is lost for most keys"
    );

    // The refund of a chunked charge gives every counter back, exactly.
    h.app.budgets.refund(&a.refunds(&charges)).await;
    assert_eq!(h.counter(&charges[0].key).await, 0);
    assert_eq!(h.counter(&charges[399].key).await, 0);

    let _ = h
        .app
        .budgets
        .clear(&charges.iter().map(|c| c.key.clone()).collect::<Vec<_>>())
        .await;
}

// its SQL — and if a minor version ever changes one of them, this is the test
// that says so instead of a throughput graph six months later.

/// Settling a claim IN FULL re-arms its partition immediately; settling a PREFIX
/// of one leaves it parked until the lease expires; a nack re-arms it at once
/// and is not available for pacing.
///
/// Measured, on this broker: **7ms** for the full ack, **the whole lease** for
/// the prefix, **4ms** for the nack. Two decisions in the relay follow from it —
/// `plan::fitting_batch` sizes a claim to what one sub-window admits so the
/// prefix path is rare, and `GATE_LEASE_SECONDS` is ten rather than thirty so it
/// is cheap when it is taken.
///
/// Nothing is LOST by a prefix settle: the group's own depth still owes the
/// tail, and the cursor is exactly where the ack put it. It is a latency fact,
/// not a durability one, and the assertions below check both halves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn an_ack_settles_the_whole_claim_or_pays_a_lease() {
    let Some(h) = harness("ackrules").await else {
        return;
    };
    const LEASE: i32 = 6;

    async fn back(h: &Harness, q: &str, within: Duration) -> (Duration, Vec<i64>) {
        let t0 = Instant::now();
        loop {
            let got = h
                .queen
                .queue(q)
                .group("g")
                .subscription_mode(SubscriptionMode::All)
                .batch(9)
                .partitions(1)
                .auto_ack(false)
                .pop()
                .await
                .unwrap_or_default();
            if !got.is_empty() {
                return (
                    t0.elapsed(),
                    got.iter()
                        .filter_map(|m| m.data.get("n").and_then(|v| v.as_i64()))
                        .collect(),
                );
            }
            if t0.elapsed() > within {
                return (t0.elapsed(), vec![]);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn claim(h: &Harness, tag: &str) -> (String, Vec<Message>) {
        let q = format!("test.ackrules.{}.{tag}", h.application);
        h.queen
            .queue(&q)
            .configure(queen_mq::QueueOptions {
                lease_time: Some(LEASE),
                retry_limit: Some(50),
                ..Default::default()
            })
            .await
            .expect("configure");
        for i in 0..5 {
            h.queen
                .queue(&q)
                .partition("p0")
                .push(json!({ "n": i }))
                .await
                .expect("push");
        }
        let msgs = h
            .queen
            .queue(&q)
            .group("g")
            .subscription_mode(SubscriptionMode::All)
            .batch(5)
            .partitions(1)
            .auto_ack(false)
            .pop()
            .await
            .expect("pop");
        assert_eq!(msgs.len(), 5, "one claim over the whole partition");
        (q, msgs)
    }

    // ---- a prefix: the tail is still owed, and it waits the lease.
    let (q, msgs) = claim(&h, "prefix").await;
    h.queen
        .transaction()
        .ack(&msgs[0])
        .ack(&msgs[1])
        .commit()
        .await
        .expect("a prefix settles");
    let owed: u64 = h
        .app
        .depths
        .pending_of_group(&h.queen, &q, "g")
        .await
        .values()
        .sum();
    assert_eq!(owed, 3, "nothing is lost: the group still owes the tail");
    let (took, got) = back(&h, &q, Duration::from_secs(LEASE as u64 * 3)).await;
    assert_eq!(got, vec![2, 3, 4], "the tail, in order");
    assert!(
        took >= Duration::from_secs(LEASE as u64 - 1),
        "a prefix settle costs a lease; it came back in {took:?}, so the batch sizing and the \
         lease length are solving a problem that no longer exists"
    );

    // ---- the whole claim: re-armed at once.
    let (q, msgs) = claim(&h, "full").await;
    h.queen
        .transaction()
        .ack_all(&msgs)
        .commit()
        .await
        .expect("the whole claim settles");
    for i in 10..13 {
        h.queen
            .queue(&q)
            .partition("p0")
            .push(json!({ "n": i }))
            .await
            .expect("push");
    }
    let (took, got) = back(&h, &q, Duration::from_secs(LEASE as u64 * 3)).await;
    assert_eq!(got, vec![10, 11, 12]);
    assert!(
        took < Duration::from_secs(LEASE as u64 - 1),
        "the happy path must cost nothing; it waited {took:?}"
    );

    // ---- a nack: immediate, and therefore tempting — which is why the relay
    // never uses one for pacing. It charges the retry budget, and work that is
    // merely waiting would dead-letter.
    let (q, msgs) = claim(&h, "nack").await;
    h.queen
        .transaction()
        .nack(&msgs[0], "test")
        .commit()
        .await
        .expect("nack");
    let (took, got) = back(&h, &q, Duration::from_secs(LEASE as u64 * 3)).await;
    assert_eq!(got, vec![0, 1, 2, 3, 4], "a nack moves no cursor");
    assert!(took < Duration::from_secs(1), "it waited {took:?}");
}

/// The QDUP a transaction answers does not name the offending id.
///
/// That is why the recovery halves the claim rather than skipping to the
/// duplicate: there is nothing to skip to.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs a broker: set GATE_TEST_QUEEN_URL and run with --include-ignored"]
async fn a_duplicate_rolls_the_bundle_back_without_naming_itself() {
    let Some(h) = harness("dupmsg").await else {
        return;
    };
    let q = format!("test.dupmsg.{}", h.application);
    h.queen.queue(&q).create().await.ok();
    let dup = format!("dup-{}", h.application);
    h.queen
        .queue(&q)
        .partition("p0")
        .push_items(vec![queen_mq::PushItem {
            queue: q.clone(),
            partition: Some("p0".into()),
            payload: json!({ "n": -1 }),
            transaction_id: Some(dup.clone()),
        }])
        .await
        .expect("plant");

    let mut tx = h.queen.transaction();
    for (i, id) in [None, Some(dup.clone()), None].iter().enumerate() {
        tx = tx
            .push_item(queen_mq::TxnPushItem {
                queue: q.clone(),
                partition: Some("p0".into()),
                payload: json!({ "n": i }),
                transaction_id: id.clone(),
                trace_id: None,
            })
            .expect("stage");
    }
    let err = tx
        .commit()
        .await
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    assert!(
        err.contains("QDUP"),
        "the relay matches on this string: {err:?}"
    );
    assert!(
        !err.contains(&dup),
        "if the broker ever names the duplicate, the halving recovery can be replaced by a skip: \
         {err:?}"
    );

    // And nothing of the bundle landed: a QDUP is a HARD verdict inside a
    // transaction, not a per-item skip.
    let there = h
        .queen
        .queue(&q)
        .group("check")
        .subscription_mode(SubscriptionMode::All)
        .batch(10)
        .partitions(1)
        .pop_auto_ack()
        .await
        .unwrap_or_default();
    assert_eq!(there.len(), 1, "only the planted one is on the queue");
}
