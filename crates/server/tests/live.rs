//! End to end against a real broker.
//!
//! Everything here needs a running queen with kv enabled: point
//! `GATE_TEST_QUEEN_URL` (or `QUEEN_URL`) at one. Without it each test says so and
//! passes, because a laptop without a broker should still be able to run
//! `cargo test` — but these are the tests that hold the parts a unit test cannot
//! reach: the relay's exactly-once transaction, the reconcile between two
//! replicas, a retry that is paced rather than amplified, and priority at a merge.
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
async fn harness(tag: &str) -> Option<Harness> {
    let serial = one_at_a_time();
    let url = match queen_url() {

        Some(u) => u,
        None => {
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
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let router = api::router(app.clone());
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    Some(Harness {
        app,
        base: format!("http://{addr}"),
        application,
        http: reqwest::Client::new(),
        _serial: serial,
    })
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
        self.send(reqwest::Method::POST, "/v1/leases/ack", Some(body)).await
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
async fn an_edge_moves_every_item_once_and_only_once() {
    let Some(h) = harness("edge").await else { return };
    let (status, body) = h.put_graph("g", chain_doc()).await;
    assert_eq!(status, 200, "declare: {body}");
    assert_eq!(body["warnings"], json!([]), "declare bought caveats: {body}");

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

    let got = h.drain("g", "ip", "g.ip", N, Duration::from_secs(30)).await;
    let seen: HashSet<i64> = got.iter().filter_map(|p| p["n"].as_i64()).collect();
    assert_eq!(got.len(), N, "expected {N} through the graph, got {}", got.len());
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
async fn an_interior_queue_belongs_to_the_graph_and_not_to_a_caller() {
    let Some(h) = harness("interior").await else { return };
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
    assert!(body["error"].as_str().unwrap_or_default().contains("entry"), "{body}");

    // A node that does not exist is a 404, not a 409.
    let (status, _) = h.push("g", "nope", json!({ "op": "x" })).await;
    assert_eq!(status, 404);

    h.cleanup("g").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_throttled_call_re_enters_at_its_entry_until_its_attempts_run_out() {
    let Some(h) = harness("retro").await else { return };
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
        while started.elapsed() < Duration::from_secs(20) && attempt.is_none() {
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
            assert_eq!(ack["retried"], json!(1), "a throttle inside the cap re-enters: {ack}");
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
async fn priority_at_the_entrance_is_priority_in_fact() {
    let Some(h) = harness("priority").await else { return };
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
    let window = body["resolved"]["relays"][0]["window"].as_u64().expect("window");
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
    while started.elapsed() < Duration::from_secs(25) && urgent_after.is_none() {
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
                .ack(json!({ "lease": body["lease"], "application": h.application,
                             "target": "g.ip", "lane": "default", "op": "x" }))
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
async fn a_shard_serialises_one_key_and_lets_another_through() {
    let Some(h) = harness("shard").await else { return };
    // One photo deletion per listing per minute, at a cardinality no single state
    // document could hold: 20,000 keys over 8 shards.
    let doc = json!({
      "version": 1,
      "nodes": {
        "photos": {
          "entry": true,
          "shardBy": "entity",
          "shards": 8,
          "budgets": [{ "id": "per-listing", "cap": 1, "periodSeconds": 60,
                        "alignment": "calendar", "scope": ["entity"], "maxKeys": 20000,
                        "confidence": "inferred" }],
          "cost": { "field": "httpCost", "default": 1, "max": 1 }
        }
      },
      "consume": ["photos"]
    });
    let (status, body) = h.put_graph("g", doc).await;
    assert_eq!(status, 200, "declare: {body}");

    // Two listings that hash apart.
    let photos = h.app.registry.get(&h.application, "g.photos").expect("node");
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

    let got = h.drain("g", "photos", "g.photos", 2, Duration::from_secs(20)).await;
    let entities: Vec<&str> = got.iter().filter_map(|p| p["entity"].as_str()).collect();
    assert!(entities.contains(&a.as_str()), "{entities:?}");
    assert!(
        entities.contains(&b.as_str()),
        "two listings on two shards must not wait for each other: {entities:?}"
    );
    // The second push for the same listing is held: the limit is per key, and the
    // key is what a shard is.
    let more = h.drain("g", "photos", "g.photos", 1, Duration::from_secs(4)).await;
    assert!(
        more.is_empty(),
        "the same listing got through twice inside its window: {more:?}"
    );

    // A push with no shard dimension has no shard to belong to, and is refused
    // rather than defaulted into somebody else's counter.
    let (status, body) = h
        .push("g", "photos", json!({ "op": "photo.delete", "payload": { "connection": "c1" } }))
        .await;
    assert_eq!(status, 422, "{body}");
    assert!(body["error"].as_str().unwrap_or_default().contains("shard"), "{body}");

    h.cleanup("g").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_replica_converges_on_the_stored_spec() {
    let Some(h) = harness("reconcile").await else { return };
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
    let on_b = b.registry.get(&h.application, "airbnb").expect("still there");
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
    assert!(b.registry.graph(&h.application, "g").is_some(), "graph not restored");
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
    assert_eq!(body["removed"], json!([]), "a sync reaped a graph node: {body}");
    assert!(h.app.registry.get(&h.application, "g.ip").is_some());

    h.cleanup("g").await;
    gate_server::reconcile(&b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_provisioning_leaves_the_old_spec_serving() {
    let Some(h) = harness("restore").await else { return };
    let spec = json!({
      "name": "airbnb", "version": 1,
      "budgets": [{ "id": "ip", "cap": 1000, "periodSeconds": 10, "alignment": "rolling",
                    "confidence": "inferred" }],
      "lanes": [{ "name": "bulk", "cap": "ceiling", "concurrency": 2, "default": true }],
      "cost": { "field": "httpCost", "default": 1, "max": 1 }
    });
    let (status, body) = h
        .send(
            reqwest::Method::PUT,
            &format!("/v1/apps/{}/targets/airbnb", h.application),
            Some(spec),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    let old = h.app.registry.get(&h.application, "airbnb").expect("declared");

    // A spec the BROKER refuses rather than the validator: a queue name it cannot
    // store. The route would never accept this, which is the point — the failure
    // under test is provisioning, not validation.
    let mut broken = old.spec.clone();
    broken.name = format!("airbnb{}bad", '\u{0}');
    let failed = gate_server::supervisor::swap(
        &h.app.queen,
        h.app.meter.clone(),
        None,
        Some(&old),
        broken,
        None,
    )
    .await
    .err()
    .expect("the broker should have refused that queue name");
    let restored = failed
        .restored
        .expect("the old runtime must be restarted when the new one cannot start");
    assert_eq!(restored.spec.name, "airbnb");
    h.app.registry.put(restored);

    // And it still admits: a target left stopped but registered would accept pushes
    // and drain nothing, for ever.
    let (status, body) = h
        .send(
            reqwest::Method::POST,
            &format!("/v1/apps/{}/targets/airbnb/lanes/bulk/push", h.application),
            Some(json!({ "op": "x", "payload": { "connection": "c1" } })),
        )
        .await;
    assert_eq!(status, 200, "{body}");

    let started = Instant::now();
    let mut admitted = 0;
    while started.elapsed() < Duration::from_secs(15) && admitted == 0 {
        let (status, body) = h
            .send(
                reqwest::Method::GET,
                &format!(
                    "/v1/apps/{}/targets/airbnb/lanes/bulk/next?batch=5&wait_ms=500",
                    h.application
                ),
                None,
            )
            .await;
        assert_eq!(status, 200, "{body}");
        admitted += body["items"].as_array().map(|i| i.len()).unwrap_or(0);
    }
    assert_eq!(admitted, 1, "the restored runtime is not admitting");

    let _ = h
        .send(
            reqwest::Method::DELETE,
            &format!("/v1/apps/{}/targets/airbnb", h.application),
            None,
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_owner_per_queue_family() {
    let Some(h) = harness("owner").await else { return };
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
    assert!(body["error"].as_str().unwrap_or_default().contains("standalone"), "{body}");

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
async fn the_console_can_draw_what_is_running() {
    let Some(h) = harness("console").await else { return };
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
async fn a_node_cannot_be_reached_through_the_target_routes() {
    let Some(h) = harness("bypass").await else { return };
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
    assert_eq!(status, 409, "an interior queue was popped through the target route: {body}");

    let (status, body) = h
        .send(
            reqwest::Method::POST,
            &format!("/v1/apps/{}/targets/g.ip/lanes/default/push", h.application),
            Some(json!({ "op": "message.post", "payload": { "connection": "c1" } })),
        )
        .await;
    assert_eq!(status, 409, "a terminal was pushed to through the target route: {body}");
    assert!(body["error"].as_str().unwrap_or_default().contains("graph"), "{body}");

    h.cleanup("g").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_ack_re_enters_only_the_items_the_caller_names() {
    let Some(h) = harness("breached").await else { return };
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
    assert!(items.len() >= 2, "expected one lease with several items: {body}");
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
    assert_eq!(ack["retried"], json!(1), "only the named item re-enters: {ack}");
    assert_eq!(
        ack["acked"],
        json!(items.len()),
        "and the whole lease is still settled: {ack}"
    );


    // And it is the right one.
    let back = h.drain("g", "calls", "g.calls", 1, Duration::from_secs(20)).await;
    assert_eq!(back.len(), 1, "the throttled item did not come back: {back:?}");
    assert_eq!(back[0]["n"], throttled_n);
    assert_eq!(back[0]["_gate"]["attempt"], json!(1));

    h.cleanup("g").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_impossible_retry_is_reported_and_still_settles_the_work() {
    let Some(h) = harness("refused").await else { return };
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
    while started.elapsed() < Duration::from_secs(30) && lease.as_array().is_none() {
        let (status, b) = h.next("g", "ip", 10, 500).await;
        assert_eq!(status, 200, "{b}");
        if b["items"].as_array().map(|i| i.len()).unwrap_or(0) > 0 {
            lease = b["lease"].clone();
        }
    }
    assert!(lease.as_array().is_some(), "nothing arrived at the terminal");

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
        ack["refused"].as_str().unwrap_or_default().contains("breach rule"),
        "the answer should say why nothing was retried: {ack}"
    );

    h.cleanup("g").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ack_that_arrives_twice_settles_once_and_retries_once() {
    let Some(h) = harness("replay").await else { return };
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
    while started.elapsed() < Duration::from_secs(20) && lease.as_array().is_none() {
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
    assert_eq!(again["retried"], json!(0), "and must not retry twice: {again}");

    // Exactly one re-entry exists, carrying attempt 1.
    let back = h.drain("g", "calls", "g.calls", 1, Duration::from_secs(20)).await;
    assert_eq!(back.len(), 1, "{back:?}");
    assert_eq!(back[0]["_gate"]["attempt"], json!(1));
    let extra = h.drain("g", "calls", "g.calls", 1, Duration::from_secs(4)).await;
    assert!(extra.is_empty(), "the item was re-entered twice: {extra:?}");

    h.cleanup("g").await;
}
