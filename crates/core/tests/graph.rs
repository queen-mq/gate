//! The graph rules. Every one of them converts a silent failure into a refusal,
//! so each test names the failure it is buying.

use gate_core::*;
use serde_json::{json, Value};

fn parse(v: Value) -> GraphSpec {
    serde_json::from_value(v).expect("parse")
}

/// The Airbnb graph of the plan, verbatim: per-endpoint limits nested inside
/// per-egress-IP limits, a per-listing weekly limit at 200,000 keys, priority for
/// price pushes, and a 429 routed back to where the item came in.
fn airbnb() -> Value {
    json!({
      "application": "channel-manager",
      "name": "airbnb",
      "version": 1,
      "nodes": {
        "prices": {
          "entry": true,
          "budgets": [],
          "cost": { "field": "httpCost", "default": 1, "max": 100 }
        },
        "messages": {
          "entry": true,
          "budgets": [
            { "id": "msg-post", "cap": 100, "periodSeconds": 60, "alignment": "rolling",
              "confidence": "documented",
              "source": "developer.withairbnb.com/homes/docs/rate-limits",
              "asOf": "2026-05-19" }
          ],
          "cost": { "field": "httpCost", "default": 1, "max": 1 }
        },
        "photos": {
          "entry": true,
          "shardBy": "entity",
          "shards": 64,
          "budgets": [
            { "id": "photo-del-weekly", "cap": 100, "periodSeconds": 604800,
              "alignment": "rolling", "scope": ["entity"], "maxKeys": 200000,
              "confidence": "documented",
              "source": "developer.withairbnb.com/homes/docs/rate-limits",
              "asOf": "2026-05-19" }
          ],
          "cost": { "field": "httpCost", "default": 1, "max": 1 }
        },
        "ip": {
          "budgets": [
            { "id": "ip-10s", "cap": 1500, "periodSeconds": 10, "alignment": "rolling",
              "confidence": "documented", "source": "airbnb docs x 0.75", "asOf": "2026-05-19" },
            { "id": "ip-5m", "cap": 15000, "periodSeconds": 300, "alignment": "rolling",
              "confidence": "documented", "source": "airbnb docs x 0.75", "asOf": "2026-05-19" },
            { "id": "ip-1h", "cap": 150000, "periodSeconds": 3600, "alignment": "rolling",
              "confidence": "documented", "source": "airbnb docs x 0.75", "asOf": "2026-05-19" },
            { "id": "ip-1d", "cap": 3375000, "periodSeconds": 86400, "alignment": "rolling",
              "confidence": "documented", "source": "airbnb docs x 0.75", "asOf": "2026-05-19" }
          ],
          "cost": { "field": "httpCost", "default": 1, "max": 100 },
          "pacing": { "leaseSeconds": 1, "batch": 200 },
          "admitted": { "partitionBy": "connection", "partitions": 64 }
        }
      },
      "edges": [
        { "from": "prices", "to": "ip", "priority": 0 },
        { "from": "messages", "to": "ip", "priority": 1 },
        { "from": "photos", "to": "ip", "priority": 1 }
      ],
      "consume": ["ip"],
      "breach": [
        { "when": { "status": 429 }, "retryTo": "origin-entry", "maxAttempts": 3 }
      ]
    })
}

/// A two-node chain: the smallest graph that is not just a target.
fn chain() -> Value {
    json!({
      "name": "g", "version": 1,
      "nodes": {
        "messages": {
          "entry": true,
          "budgets": [{ "id": "msg", "cap": 100, "periodSeconds": 60, "alignment": "rolling",
                        "confidence": "inferred" }],
          "cost": { "field": "httpCost", "default": 1, "max": 1 }
        },
        "ip": {
          "budgets": [{ "id": "ip-10s", "cap": 1500, "periodSeconds": 10, "alignment": "rolling",
                        "confidence": "inferred" }],
          "cost": { "field": "httpCost", "default": 1, "max": 100 }
        }
      },
      "edges": [{ "from": "messages", "to": "ip" }],
      "consume": ["ip"]
    })
}

#[test]
fn the_airbnb_graph_declares_clean_and_warns_about_nothing() {
    let g = parse(airbnb());
    assert_eq!(validate_graph(&g), vec![], "the plan's own graph must declare clean");
    assert_eq!(graph_warnings(&g), vec![], "and buy no caveats");
}

#[test]
fn a_node_is_a_target_named_for_its_graph() {
    let g = parse(airbnb());
    let ip = g.node_spec("ip").expect("node");
    assert_eq!(ip.name, "airbnb.ip");
    assert_eq!(ip.application, "channel-manager");
    assert_eq!(ip.push_queue(), "gate.channel-manager.airbnb.ip.push");
    assert_eq!(ip.namespace(), "gate.channel-manager");
    // One lane unless the node says otherwise: lanes DIVIDE a ceiling, so a
    // second one is never implicit.
    assert_eq!(ip.lanes.len(), 1);
    assert!(ip.lanes[0].default);
    assert_eq!(ip.admitted.partitions, 64);
    // And a node target passes the target rules on its own.
    assert_eq!(validate(&ip), vec![]);

    // The sharded node carries its sharding into the target.
    let photos = g.node_spec("photos").expect("node");
    assert_eq!(photos.shard_count(), 64);
    assert_eq!(photos.shard_by, Some(Dim::Entity));
}

#[test]
fn the_chain_arithmetic_the_validator_has_to_accept() {
    // messages: tightest rate 100/60 = 1.67/s x 1s lease -> batch >= 2, and 200 is
    // the default; cost-fits 100 >= 1. ip: 3,375,000/86,400 = 39.06/s -> batch >= 40;
    // cost-fits 1500 >= 100. G3 along the edge: 1 <= 100.
    let g = parse(chain());
    assert_eq!(validate_graph(&g), vec![]);
}

#[test]
fn a_cycle_would_traverse_for_ever() {
    let mut v = chain();
    v["edges"] = json!([{ "from": "messages", "to": "ip" }, { "from": "ip", "to": "messages" }]);
    v["consume"] = json!(["ip"]);
    let problems = validate_graph(&parse(v));
    assert!(problems.iter().any(|p| p.rule == "acyclic"), "{problems:?}");
}

#[test]
fn a_node_may_not_relay_into_itself() {
    let mut v = chain();
    v["edges"] = json!([{ "from": "messages", "to": "messages" }, { "from": "messages", "to": "ip" }]);
    assert!(validate_graph(&parse(v)).iter().any(|p| p.rule == "edge-self"));
}

#[test]
fn one_edge_per_queue_pair_or_both_relays_forward_the_same_message() {
    let mut v = chain();
    v["edges"] = json!([{ "from": "messages", "to": "ip" }, { "from": "messages", "to": "ip" }]);
    assert!(validate_graph(&parse(v)).iter().any(|p| p.rule == "edge-unique"));
}

#[test]
fn cost_max_may_not_shrink_along_an_edge() {
    // An item admitted upstream that the downstream node can never admit parks
    // the head of its lane for ever and never reaches a DLQ.
    let mut v = chain();
    v["nodes"]["messages"]["cost"]["max"] = json!(200);
    v["nodes"]["messages"]["budgets"][0]["cap"] = json!(500);
    let problems = validate_graph(&parse(v));
    assert!(problems.iter().any(|p| p.rule == "cost-monotonic"), "{problems:?}");
}

#[test]
fn a_node_with_no_budget_needs_somewhere_to_send_its_work() {
    // Legal with an out-edge: that is a class node, and it is checked downstream.
    let mut v = chain();
    v["nodes"]["messages"]["budgets"] = json!([]);
    assert_eq!(validate_graph(&parse(v)), vec![]);

    // Not legal at the end of the line: it would admit everything to a consumer.
    let mut v = chain();
    v["nodes"]["ip"]["budgets"] = json!([]);
    let problems = validate_graph(&parse(v));
    assert!(problems.iter().any(|p| p.rule == "budgets"), "{problems:?}");
}

#[test]
fn a_consumable_node_may_not_also_feed_a_relay() {
    // Otherwise a caller popping it steals from the graph: two consumers split
    // the work at random and the relay's exactly-once guarantee is worthless.
    let mut v = chain();
    v["consume"] = json!(["messages", "ip"]);
    let problems = validate_graph(&parse(v));
    assert!(problems.iter().any(|p| p.rule == "consume-terminal"), "{problems:?}");
}

#[test]
fn work_must_be_able_to_enter_and_to_leave() {
    // Unreachable: not an entry and nothing relays into it.
    let mut v = chain();
    v["nodes"]["orphan"] = json!({ "budgets": [{ "id": "o", "cap": 10, "periodSeconds": 1,
                                                 "alignment": "rolling", "confidence": "inferred" }],
                                   "cost": { "field": "httpCost", "default": 1, "max": 1 } });
    let problems = validate_graph(&parse(v));
    assert!(problems.iter().any(|p| p.rule == "reachable"), "{problems:?}");

    // Enters but never leaves: nothing pops it and it relays nowhere.
    let mut v = chain();
    v["nodes"]["dead"] = json!({ "entry": true,
                                 "budgets": [{ "id": "d", "cap": 10, "periodSeconds": 1,
                                               "alignment": "rolling", "confidence": "inferred" }],
                                 "cost": { "field": "httpCost", "default": 1, "max": 1 } });
    let problems = validate_graph(&parse(v));
    assert!(problems.iter().any(|p| p.rule == "drains"), "{problems:?}");

    // No entry at all, and no terminal at all.
    let mut v = chain();
    v["nodes"]["messages"]["entry"] = json!(false);
    assert!(validate_graph(&parse(v)).iter().any(|p| p.rule == "entry"));
    let mut v = chain();
    v["consume"] = json!([]);
    assert!(validate_graph(&parse(v)).iter().any(|p| p.rule == "consume"));
}

#[test]
fn a_path_longer_than_three_hops_is_refused_because_smear_composes() {
    let budget = |id: &str| {
        json!({ "id": id, "cap": 1000, "periodSeconds": 10, "alignment": "rolling",
                "confidence": "inferred" })
    };
    let cost = json!({ "field": "httpCost", "default": 1, "max": 1 });
    let mut nodes = serde_json::Map::new();
    for (i, name) in ["a", "b", "c", "d", "e"].iter().enumerate() {
        nodes.insert(
            name.to_string(),
            json!({ "entry": i == 0, "budgets": [budget(name)], "cost": cost }),
        );
    }
    let v = json!({
      "name": "g", "version": 1, "nodes": nodes,
      "edges": [{ "from": "a", "to": "b" }, { "from": "b", "to": "c" },
                { "from": "c", "to": "d" }, { "from": "d", "to": "e" }],
      "consume": ["e"]
    });
    let problems = validate_graph(&parse(v.clone()));
    assert!(problems.iter().any(|p| p.rule == "path-length"), "{problems:?}");

    // Three hops is the wall, not four.
    let mut three = v.clone();
    three["nodes"].as_object_mut().unwrap().remove("e");
    three["edges"] = json!([{ "from": "a", "to": "b" }, { "from": "b", "to": "c" },
                            { "from": "c", "to": "d" }]);
    three["consume"] = json!(["d"]);
    assert_eq!(validate_graph(&parse(three)), vec![]);
}

#[test]
fn a_ceiling_declared_in_two_nodes_is_two_ceilings() {
    // Rule 1 of "what must never be done": each node keeps its own counter, so
    // the vendor sees the sum of the copies.
    let mut v = chain();
    v["nodes"]["messages"]["budgets"] = json!([
        { "id": "ip-10s", "cap": 1500, "periodSeconds": 10, "alignment": "rolling",
          "confidence": "inferred" }
    ]);
    v["nodes"]["messages"]["cost"]["max"] = json!(100);
    let problems = validate_graph(&parse(v));
    assert!(problems.iter().any(|p| p.rule == "budget-once"), "{problems:?}");
}

#[test]
fn a_scoped_budget_of_the_same_name_in_two_nodes_is_two_different_limits() {
    // Scoped budgets are per key, and two nodes keying on different work is the
    // normal case — the rule is about a CEILING copied, so this must pass.
    let mut v = chain();
    let per_listing = json!({ "id": "per-listing", "cap": 10, "periodSeconds": 60,
                              "alignment": "rolling", "scope": ["entity"], "maxKeys": 100,
                              "confidence": "inferred" });
    v["nodes"]["messages"]["budgets"] = json!([per_listing]);
    v["nodes"]["ip"]["budgets"].as_array_mut().unwrap().push(per_listing);
    v["nodes"]["ip"]["cost"]["max"] = json!(10);
    let problems = validate_graph(&parse(v));
    assert!(!problems.iter().any(|p| p.rule == "budget-once"), "{problems:?}");
}

#[test]
fn a_retro_edge_must_land_on_an_entry_and_must_be_bounded() {
    let mut v = chain();
    v["breach"] = json!([{ "when": { "status": 429 }, "retryTo": "ip", "maxAttempts": 3 }]);
    let problems = validate_graph(&parse(v));
    assert!(problems.iter().any(|p| p.rule == "retry-entry"), "{problems:?}");

    let mut v = chain();
    v["breach"] = json!([{ "when": { "status": 429 }, "retryTo": "origin-entry",
                           "maxAttempts": 0 }]);
    assert!(validate_graph(&parse(v)).iter().any(|p| p.rule == "breach-attempts"));

    let mut v = chain();
    v["breach"] = json!([{ "when": {}, "retryTo": "origin-entry", "maxAttempts": 2 }]);
    assert!(validate_graph(&parse(v)).iter().any(|p| p.rule == "breach-when"));

    // Naming the entry explicitly is fine, and so is `origin-entry`.
    let mut v = chain();
    v["breach"] = json!([{ "when": { "outcome": "throttled" }, "retryTo": "messages",
                           "maxAttempts": 2 }]);
    assert_eq!(validate_graph(&parse(v)), vec![]);
}

#[test]
fn a_throttled_ack_is_what_a_429_looks_like_on_this_wire() {
    let g = parse(airbnb());
    // The consumer classifies the vendor's refusal and acks the truth; there is
    // no status on the wire for the common case, so `status: 429` has to take it.
    assert!(g.breach_for("throttled", None).is_some());
    assert!(g.breach_for("throttled", Some(429)).is_some());
    assert!(g.breach_for("ok", None).is_none());
    assert!(g.breach_for("ok", Some(200)).is_none());
    // A rule on a different status does not take a throttle.
    let w = BreachWhen { status: Some(500), outcome: None };
    assert!(!w.matches("throttled", None));
    assert!(w.matches("failed", Some(500)));
    // Both named means both must hold.
    let w = BreachWhen { status: Some(429), outcome: Some("throttled".into()) };
    assert!(w.matches("throttled", Some(429)));
    assert!(!w.matches("throttled", Some(503)));
}

#[test]
fn origin_entry_resolves_to_where_the_item_came_in() {
    let g = parse(airbnb());
    let rule = &g.breach[0];
    assert_eq!(g.retry_entry(rule, Some("photos")).as_deref(), Some("photos"));
    // An interior node is not an entry, so it is not a re-entry either.
    assert_eq!(g.retry_entry(rule, Some("ip")), None);
    // Three entries and no stamp: we do not guess which one it was.
    assert_eq!(g.retry_entry(rule, None), None);

    // One entry and no stamp is not a guess.
    let one = parse(chain());
    let rule = BreachRule {
        when: BreachWhen { status: Some(429), outcome: None },
        retry_to: ORIGIN_ENTRY.to_string(),
        max_attempts: 3,
    };
    assert_eq!(one.retry_entry(&rule, None).as_deref(), Some("messages"));
}

#[test]
fn the_shard_rules_apply_to_a_node_like_any_other_target() {
    // G7: an unscoped budget in a sharded node is its cap enforced `shards` times.
    let mut v = airbnb();
    v["nodes"]["photos"]["budgets"].as_array_mut().unwrap().push(json!({
        "id": "photo-post-hourly", "cap": 10000, "periodSeconds": 3600, "alignment": "rolling",
        "confidence": "inferred"
    }));
    let problems = validate_graph(&parse(v));
    assert!(problems.iter().any(|p| p.rule == "shard-scope"), "{problems:?}");
    assert!(
        problems.iter().any(|p| p.detail.starts_with("node `photos`")),
        "a node problem names its node: {problems:?}"
    );

    // G8: the per-shard key bound is what a state document is re-read at.
    let mut v = airbnb();
    v["nodes"]["photos"]["shards"] = json!(4);
    let problems = validate_graph(&parse(v));
    assert!(problems.iter().any(|p| p.rule == "store-fits"), "{problems:?}");
}

#[test]
fn a_graph_name_is_one_segment_because_the_dot_joins_it_to_a_node() {
    let mut v = chain();
    v["name"] = json!("air.bnb");
    assert!(validate_graph(&parse(v)).iter().any(|p| p.rule == "name"));

    let mut v = chain();
    v["nodes"]["mes.sages"] = v["nodes"]["messages"].clone();
    let problems = validate_graph(&parse(v));
    assert!(problems.iter().any(|p| p.rule == "node-name"), "{problems:?}");
}

#[test]
fn an_edge_to_a_node_that_does_not_exist_is_refused() {
    let mut v = chain();
    v["edges"] = json!([{ "from": "messages", "to": "nowhere" }]);
    assert!(validate_graph(&parse(v)).iter().any(|p| p.rule == "edge-node"));
}

#[test]
fn rewiring_re_founds_a_path_and_needs_a_version() {
    let old = parse(airbnb());

    // A cap change is hot: same counters, new arithmetic.
    let mut v = airbnb();
    v["nodes"]["ip"]["budgets"][0]["cap"] = json!(1200);
    assert!(!needs_graph_version_bump(&old, &parse(v)));

    // A priority change is hot too: it is the relay's order, not a counter.
    let mut v = airbnb();
    v["edges"][1]["priority"] = json!(2);
    assert!(!needs_graph_version_bump(&old, &parse(v)));

    // A period is what the accumulated state MEANS.
    let mut v = airbnb();
    v["nodes"]["ip"]["budgets"][0]["periodSeconds"] = json!(20);
    assert!(needs_graph_version_bump(&old, &parse(v)));

    // Re-sharding moves keys between counters.
    let mut v = airbnb();
    v["nodes"]["photos"]["shards"] = json!(128);
    assert!(needs_graph_version_bump(&old, &parse(v)));

    // Rewiring: an item in flight was admitted against a path that has gone.
    let mut v = airbnb();
    v["edges"] = json!([{ "from": "prices", "to": "ip", "priority": 0 },
                        { "from": "messages", "to": "ip", "priority": 1 }]);
    v["nodes"].as_object_mut().unwrap().remove("photos");
    assert!(needs_graph_version_bump(&old, &parse(v)));

    // A node that has gone takes its queues and its counters with it.
    let mut v = airbnb();
    v["nodes"].as_object_mut().unwrap().remove("photos");
    v["edges"] = json!([{ "from": "prices", "to": "ip", "priority": 0 },
                        { "from": "messages", "to": "ip", "priority": 1 }]);
    assert!(needs_graph_version_bump(&old, &parse(v)));
}

#[test]
fn the_topology_a_relay_reads_off_the_document() {
    let g = parse(airbnb());
    assert_eq!(g.merge_dests(), vec!["ip".to_string()]);
    assert_eq!(g.entries().len(), 3);
    assert!(g.is_consume("ip"));
    assert!(!g.is_consume("prices"));
    assert_eq!(g.in_edges("ip").len(), 3);
    assert_eq!(g.out_edges("prices").len(), 1);
    // Priority is on the edge, and lower is sooner.
    let prices = g.out_edges("prices")[0];
    assert_eq!(prices.priority, 0);
}

#[test]
fn a_graph_round_trips_through_json() {
    let g = parse(airbnb());
    let back: GraphSpec = serde_json::from_value(serde_json::to_value(&g).unwrap()).unwrap();
    assert_eq!(g, back);
}

#[test]
fn an_unknown_field_in_a_graph_document_is_a_typo_not_a_default() {
    let mut v = chain();
    v["nodes"]["messages"]["shardby"] = json!("entity");
    assert!(serde_json::from_value::<GraphSpec>(v).is_err());
}

#[test]
fn two_out_edges_are_a_broadcast_and_not_a_split() {
    // Each edge is its own consumer group on the source's admitted queue, and a
    // group gets EVERY message — so a fan-out does not divide the stream, it copies
    // it, and one push becomes one vendor call per branch.
    let mut v = chain();
    v["nodes"]["ip2"] = v["nodes"]["ip"].clone();
    v["edges"] = json!([{ "from": "messages", "to": "ip" },
                        { "from": "messages", "to": "ip2" }]);
    v["consume"] = json!(["ip", "ip2"]);
    let problems = validate_graph(&parse(v));
    assert!(problems.iter().any(|p| p.rule == "edge-fanout"), "{problems:?}");
}

#[test]
fn a_sharded_node_takes_its_work_from_a_push_and_not_from_an_edge() {
    // A shard is chosen from a dimension ON the item; a relay cannot invent one for
    // an item that does not carry it, and choosing anyway would put one key in two
    // counters.
    let mut v = chain();
    v["nodes"]["ip"]["shardBy"] = json!("entity");
    v["nodes"]["ip"]["shards"] = json!(8);
    v["nodes"]["ip"]["budgets"][0]["scope"] = json!(["entity"]);
    v["nodes"]["ip"]["budgets"][0]["maxKeys"] = json!(1000);
    let problems = validate_graph(&parse(v));
    assert!(problems.iter().any(|p| p.rule == "shard-entry"), "{problems:?}");

    // As an entry it is fine — which is how the plan's `photos` node is declared.
    assert_eq!(validate_graph(&parse(airbnb())), vec![]);
}

#[test]
fn a_relay_has_nobody_to_ask_which_lane() {
    // Lanes divide a node's budgets. A relay pushes to the default lane, so a second
    // lane on a relayed node is capacity nothing can reach.
    let mut v = chain();
    v["nodes"]["ip"]["lanes"] = json!([
        { "name": "default", "cap": "ceiling", "concurrency": 4, "default": true },
        { "name": "bulk", "cap": "share:0.5", "concurrency": 4 }
    ]);
    let problems = validate_graph(&parse(v));
    assert!(problems.iter().any(|p| p.rule == "relay-lane"), "{problems:?}");
}

#[test]
fn one_cost_field_for_the_whole_graph() {
    // The weight is stamped once, at the push, under the ENTRY node's field name, and
    // every relay forwards the payload verbatim. A downstream node reading another
    // name finds nothing and charges its cost.default — a hundred-call item counted
    // as one, all the way to the vendor's ceiling.
    let mut v = chain();
    v["nodes"]["ip"]["cost"] = json!({ "field": "weight", "default": 1, "max": 100 });
    let problems = validate_graph(&parse(v));
    assert!(problems.iter().any(|p| p.rule == "cost-field"), "{problems:?}");
}

#[test]
fn a_shared_kv_ceiling_is_the_one_budget_two_nodes_may_both_declare() {
    // `budget-once` exists because two nodes keep two counters. A `store: kv` budget
    // is the exception that proves it: the pool keys on `(application, id)`, so two
    // nodes declaring the same id draw down ONE counter — which is how a ceiling that
    // spans nodes is expressed at all.
    let mut v = chain();
    let shared = json!({ "id": "egress-ip", "cap": 1000, "periodSeconds": 10,
                         "alignment": "calendar", "store": "kv", "confidence": "inferred" });
    v["nodes"]["messages"]["budgets"].as_array_mut().unwrap().push(shared.clone());
    v["nodes"]["ip"]["budgets"].as_array_mut().unwrap().push(shared);
    let problems = validate_graph(&parse(v));
    assert!(
        !problems.iter().any(|p| p.rule == "budget-once"),
        "a shared kv ceiling must be declarable in both nodes: {problems:?}"
    );
}

#[test]
fn a_named_re_entry_must_be_able_to_admit_what_it_receives() {
    // `origin-entry` is safe by construction: an item goes back to the door that
    // already accepted its cost. A NAMED entry receives items pushed anywhere, and one
    // costing more than its cost.max can never be admitted — it parks the head of that
    // lane for ever and never reaches a DLQ.
    let mut v = airbnb();
    v["breach"] = json!([{ "when": { "status": 429 }, "retryTo": "messages",
                           "maxAttempts": 3 }]);
    let problems = validate_graph(&parse(v));
    assert!(problems.iter().any(|p| p.rule == "retry-cost"), "{problems:?}");

    // A named entry whose ceiling is the highest of them is fine.
    let mut v = airbnb();
    v["breach"] = json!([{ "when": { "status": 429 }, "retryTo": "prices",
                           "maxAttempts": 3 }]);
    let problems = validate_graph(&parse(v));
    assert!(!problems.iter().any(|p| p.rule == "retry-cost"), "{problems:?}");
}

#[test]
fn the_cost_field_refusal_names_each_field_once() {
    // Deduplicating a name-ordered list only removes neighbours, so `a: x, b: y, c: x`
    // reported three fields and listed one of them twice. The refusal was right; what it
    // said about the document was not.
    let mut v = chain();
    v["nodes"]["mid"] = json!({
        "budgets": [{ "id": "m", "cap": 100, "periodSeconds": 10, "alignment": "rolling",
                      "confidence": "inferred" }],
        "cost": { "field": "weight", "default": 1, "max": 1 }
    });
    v["edges"] = json!([{ "from": "messages", "to": "mid" }, { "from": "mid", "to": "ip" }]);
    let problems = validate_graph(&parse(v));
    let field = problems
        .iter()
        .find(|p| p.rule == "cost-field")
        .expect("the graph names two fields");
    assert!(field.detail.contains("name 2 different"), "{}", field.detail);
    assert!(field.detail.contains("`httpCost` in ip, messages"), "{}", field.detail);
    assert!(field.detail.contains("`weight` in mid"), "{}", field.detail);
}
