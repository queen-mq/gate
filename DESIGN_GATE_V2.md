# Gate v2 — the rrl.js architecture

**Status:** implementation design. Every decision in §0 was settled with the author and is
final; everything else in this document is the working-out. Where the working-out found a
constraint the settled brief could not have known about, it says so out loud and the item is
in **§16 Open questions** — never resolved quietly.

**Branch:** `gate-v2-kv`, worktree `/Users/alice/Work/queen-rrl-v2`. The main checkout at
`/Users/alice/Work/queen-rrl` has uncommitted work in `crates/server/src/edge.rs` and is not
touched by anything here.

---

## 0. The shape of the change, in one paragraph

Gate keeps its control plane — declarations, validation, the console, sign-in, the spec store
— and throws away its entire data plane. The admission gate, the streams-cycle state
documents, the counter-funnel push queues, the admitted ring, the pinned relay runners, the
depth-probing window arithmetic, the lane algebra, the calls queue and the meter loop are all
replaced by three queen primitives used directly: **`kv.incr` with `max` is the admission
decision**, **the transaction wire is the relay**, and **a wildcard long-poll is the
scheduler**. A node stops being a partitioned counter that Gate maintains and becomes a
Postgres row that Postgres maintains. What is left of Gate at runtime is one consumer per DAG
edge set that pops a batch, asks the broker for permission in one KV call, and commits
`ack + push` in one transaction.

---

## 1. Why — the measured motivation

These numbers belong in the code comments of the modules that exist because of them. Cite
them; do not paraphrase them into adjectives.

**Production, 2026-08-21, Query Insights, one hour.** Gate made roughly **275,000 "is there
work?" calls** — `log_has_pending_v1` 138,656, `log_pop_specific_v1` 86,927, depth 39,505,
streams state 9,949 — to move messages **963 times** (`log_transaction_wire_v1`). That is
**285 polls per relay**. Nothing was broken; that is what the v1 design costs when it is
idle, and idle is most of the time.

**Bench, 32-core VM, 2026-08-20.**

| shape | result |
|---|---|
| old counter-funnel relay, single destination node | **2.8k items/s** ceiling, tuple lock waits 96–100% |
| capped shapes | **6.5 PG cores** burned to admit **172 items/s**; `streams_cycle` alone 3.2 cores |
| `txnload` with disjoint lanes (the shape this design adopts) | **23–34k items/s**, same VM |
| `kv.incr`, one key | **33k incr/s** |
| `kv` batched | **154k ops/s** |

The three facts that follow from the table and that the whole design is built on:

1. The counter-funnel is not slow because of the counting. It is slow because every admission
   is a **write transaction on one partition row**, and every partition row is a serialization
   point. `kv.incr` on one key does 33k/s because it is a HOT update on one narrow row with
   no lease, no segment, no cursor.
2. `txnload` with **disjoint lanes** is ten times the counter-funnel relay. "Disjoint lanes"
   means each transaction touches exactly one source partition and one destination partition,
   and concurrent transactions touch different ones. That discipline is a design constraint,
   not an optimisation, and §6.4 is where it is spent.
3. Because the budget is charged **once per batch** rather than once per message, the shared
   counter is not on the critical path. At batch 200 and 34k items/s the counter sees 170
   incr/s against a measured 33k/s ceiling — two orders of magnitude of headroom. This is the
   sentence that makes a single shared key acceptable where a single shared partition was not.

---

## 2. The queen facts this design stands on

Verified against `/Users/alice/Work/queen` at the time of writing. Do not re-derive them;
do re-check them if the broker minor version moves.

| Fact | Where | Consequence for Gate |
|---|---|---|
| `kv.incr` with `max` does **not saturate and does not truncate**. The call that would break the ceiling does not apply and returns the **current** value. `applied` **is** the admission decision. | `server/sql/procedures/024_kv.sql`, `WHEN 'incr'` | The whole limiter. One round trip, no CAS loop, no read-then-write race. |
| `incr`'s TTL is **create-only**: a live row keeps its expiry, an expired row reads as zero and the next incr recreates it with a fresh TTL. | same, the `expires_at = CASE WHEN kv_live_v1(...)` arms | Window rotation is automatic and costs nothing. No window index in the key, no sweeper, no `% 4` recycling. |
| `min` is a **guard, not a clamp**. `incr(-7, {min: 0})` against a current value of 5 is **refused entirely**, not clamped to 0. | same | Half of the refund semantics we want. It is a guard on the **resulting value**, not on the identity of the window, so a refund into a REAPED key refuses and a refund into a RECREATED one applies — which is why a refund carries `min == max == was - delta` instead. §6.3. |
| A **refused** `incr` result carries `applied`, `reason`, `key`, `value`, `version` — and **no `expiresAt`**. | same, the `ELSE v_res := jsonb_build_object(...)` arm | The wait deadline needs a separate read. We ride it in the same batch as a `getMany`. §6.2. |
| `getMany` rows carry `expiresAt`. | `crates/queen-protocol/src/kv.rs`, `KvRow` | One round trip serves both the decision and the deadline. |
| KV expiry is **whole seconds**, minimum 1, and `Until(deadline)` is converted to integer `ttlSeconds` at send time. | `crates/queen-protocol/src/kv.rs`, `Expiry` | **A window shorter than one second is not expressible as a TTL.** §5.3 and §11 rule `budget-window-floor`. |
| `kv.batch(ops)` applies each op independently unless one is marked `required`. | `clients/client-rust/src/kv.rs`, `Kv::batch` | Multi-budget admission is one call whose ops can partially pass — which is why §6.3 exists. |
| **Lease expiry never charges retry budget.** Only an explicit `failed` ack does. | `server/sql/procedures/004_log_pop.sql`, header: *"the RETRY BUDGET is batch_retry_count, charged only by the ack path on explicit `failed` — attempt_count is redelivery telemetry and never consumes budget, so lease expiry never eats retries"* | Pacing by **release** (return without acking) is free and cannot dead-letter waiting work. It is also why `retry_limit: 0` can be retired and a real DLQ comes back. §6.5. |
| The transaction wire commits `ack + push` atomically; a duplicate push inside a transaction is a **hard** `QDUP` that rolls the whole bundle back; **below-cursor duplicate acks are tolerated as no-ops**. | `server/sql/procedures/005_log_ack.sql`, `log_transaction_wire_v1` — and the comment naming Gate: *"a replayed relay (Gate's ack+push retried after a timeout) keeps resolving as a duplicate instead of a rollback"* | The relay is one transaction, and it still needs a QDUP split-and-settle fallback for a mixed batch. §6.4, §6.6. |
| The wildcard pop picks candidate partitions in **randomised order** and claims with **`FOR UPDATE SKIP LOCKED`**, precisely so concurrent consumers of one group spread across partitions instead of convoying. | `004_log_pop.sql`, `log_pop_wildcard_wire_v1` and the `SKIP LOCKED` NB | **This is the scheduler.** Gate does not need to pin runners to partitions, does not need to probe which partitions are hot, and does not need a rotation cursor. The broker already does all three. |
| A parked long-poll **releases its pooled PG connection before parking** and is woken by the push notifier. | `server/src/handlers/data.rs` §10 comments | An idle graph costs parked timers, not queries. This is the acceptance criterion in §15. |
| `GET /api/v1/resources/queues/:queue/depth?group=` is watermark arithmetic only, ~1ms; per-group form is `pending` per partition against that group's cursor. Broker ≥ 1.0.4. | `011_log_stats.sql` `log_queue_depth_v1`; `clients/client-rust/src/admin.rs` `queue_depth` | ETA and the console read it **on demand**. Nothing in the hot path reads a depth, ever. |
| Rust client surface: `QueueBuilder` has `group / batch / partitions / concurrency / auto_ack / wait / poll_timeout / renew_lease / lease_seconds / subscription_mode / subscription_from / cancel` and `consume_batch`; `Kv` has `incr / get / get_many / batch`; `TransactionBuilder` has `ack / push_item / kv / commit`; `Admin` has `queue_depth`. | `clients/client-rust/src/{queue,consumer,kv,transaction,admin}.rs` | **Everything v2 needs is in the `queen-mq` crate.** No fallback to Gate's internal HTTP client is required for the data plane. |

### 2.1 The one primitive queen does not have

`uuid` v5. `client-rust/src/uuid.rs` offers v7 only, and Gate depends on no `uuid` crate today.
§7 specifies RFC 4122 §4.3 written out in `gate-core`, with `sha1 = "0.10"` as the only new
dependency, for the same reason v1 wrote FNV-1a out by hand: the number must be stable across
releases and reproducible by an operator with a shell.

---

## 3. The declaration schema

One document type. **A graph is the only object.** A standalone target is a one-node graph
declared through a sugar endpoint (§12) — which removes, at a stroke, the `TargetSpec` /
`GraphSpec` split, the `node_spec` projection, the G9 "run every target rule per node" hack,
the one-owner-per-queue-family conflict check and `resolve_graph`.

`#[serde(deny_unknown_fields)]` everywhere, exactly as v1: a document a newer build wrote must
be unreadable by an older one, because that is what makes the store's `complete: false` honest.

```jsonc
{
  "application": "channel",        // required in the body or pinned by the path
  "graph": "airbnb",               // pinned by the path
  "version": 3,                    // u32; see §12.3 for what still needs a bump

  "nodes": {
    "<node>": {
      "budgets": [ <Budget>, ... ],   // >= 1, and >= 1 of them unscoped
      "cost":    <Cost>,              // default: 1
      "ingress": <Ingress>,           // optional; absent = fed only by paths
      "egress":  <Egress>             // optional; required on a terminal node
    }
  },

  "paths": [ <Path>, ... ]          // >= 1
}
```

### 3.1 `Budget`

```jsonc
{
  "id":         "ip-10s",        // optional; defaults to "b{index}". Part of the KV key.
  "count":      1500,            // integer >= 1
  "timeMs":     10000,           // integer >= 100 (see budget-window-floor, §11)
  "subWindows": 10,              // optional; default derived, see §5.3
  "scopeBy":    "payload.listingId",  // optional: one counter per distinct value
  "sharedKey":  "egress-ip",     // optional: one counter across nodes/graphs of this app
  "whenOp":     ["listing.*"],   // optional: charge only for matching payload.op
  "confidence": "documented",    // documented | inferred | assumed  (default inferred)
  "source":     "https://...",   // required when confidence = documented
  "asOf":       "2026-08-01"     // required when confidence = documented
}
```

* `count` / `timeMs` replace `cap` / `periodSeconds`, in the units the author asked for.
* **`alignment` is gone.** v1 had to choose between a fixed window and a two-bucket sliding
  one because it owned the arithmetic. KV owns a fixed window and nothing else, so the choice
  is not available; **smoothing is expressed as subdivision instead** (§5.3), which is the
  same trade in a form the primitive can actually keep. The v1 `alignment` field is accepted
  and mapped (§12.2).
* `scopeBy` replaces the whole of v1's `scope[]` + `maxKeys` + `shardBy` + `shards` +
  `store-fits` subsystem. Cardinality is now Postgres rows with a TTL, not a JSON document
  re-read whole on every cycle, so there is no `GATE_MAX_KEYS`, no `GATE_MAX_SHARDS`, no
  shard runners and no re-sharding migration.
* `sharedKey` replaces `store: kv`. It is no longer a second kind of budget with its own
  capacity-lease machinery — **every** budget is a KV counter, and `sharedKey` only changes
  which one.
* `whenOp` is v1's `Match.op`, unchanged in meaning: suffix globs on dot-separated segments,
  a bare `*` matches all, absence takes everything. It is cheap now because the batch is
  grouped before it is charged (§6.2).
* `confidence` / `source` / `asOf` keep the provenance rule. See §16.3 for the `assumed`
  discount, which v1 documented and never enforced.

### 3.2 `Cost`

```jsonc
"cost": 1
// or
"cost": { "path": "payload.msgCount", "default": 1, "max": 100 }
```

`delta = cost`. **Costs are integers**, because `kv.incr`'s delta is `i64` on this wire — a
constraint v1 did not have (its cost was `f64`) and one that must be stated in the migration
notes. A resolved cost that is absent, non-numeric, non-integral or `< 1` falls back to
`default`. A cost above `max` is refused at push (422) and, if it somehow arrives on a
user-owned ingress queue, dead-lettered with a reason rather than admitted — because an item
costing more than a cap can never be admitted and would otherwise park the head of its
partition for ever. That is v1's `cost-fits` rule and it survives verbatim.

### 3.3 `Ingress`

```jsonc
"ingress": true
// or
"ingress": { "queue": "channel.prices.in", "partitions": 32, "http": true }
```

* `true` — Gate creates and owns `gate.{app}.{graph}.{node}.ingress`.
* `{ queue }` — **Gate consumes a queue the application already owns.** Producers push with
  their normal SDK; Gate can be down without blocking ingest. This is the single most
  important operational change in v2 and it is what makes the HTTP push endpoint optional.
* `http: true` (default `true` for `ingress: true`, default `false` for a named queue) — keep
  the `POST .../push` front door, which pushes into the ingress queue. It may pre-check the
  budget and answer **429 with `Retry-After`** for shed-load semantics; it never charges.
* `partitions` — only meaningful when Gate creates the queue. For a user-owned queue the
  partition count is **read from the broker at declare time** and echoed in the response.

### 3.4 `Egress`

```jsonc
"egress": "channel.airbnb.out"
// or
"egress": { "queue": "channel.airbnb.out", "group": "channel-workers" }
```

The application consumes this queue directly with its own SDK. `group`, when given, is the
consumer group Gate asks about for the "waiting for workers" half of the ETA; without it the
ETA reports the queue-level (worst-cursor) number and says so in `assumes`.

### 3.5 `Path`

```jsonc
{
  "name":     "prices",                  // required, unique; names the consumer groups
  "priority": 0,                         // integer, lower is sooner. Default 0.
  "share":    1.0,                       // optional; default = equal steps by priority rank
  "nodes":    ["prices", ["ip", "audit"]]  // sequence; a nested array is a fan-out
}
```

A path is a **sequence of nodes**. Element `i` relays into element `i+1`. A nested array
`["ip", "audit"]` means the message goes to **both**, in one transaction (§7).

### 3.6 Priority is a ceiling, not a scheduler

This is the decision that deletes the most v1 code, so it is worth stating precisely.

In v1, priority was **arrival order at a merge**: one relay per destination drained its legs
strictly in order, which cost a barrier per leg per cycle, a shared allowance pool, a
rotation cursor, a stall-tolerance counter and three live tests. Lanes then *divided* the
ceiling, because each lane was its own partition with its own counter and two lanes both
told "you may use the ceiling" genuinely spent it twice — measured at 93/s against a declared
50/s, and 7131 against a declared 5000-per-10s.

In v2 there is **one counter per node**, and priority is **per-path `max` on that one
counter**:

```
path P's incr for node N uses  max = round(cap_sub(N) * share(P))
```

* Path `prices` at `share: 1.0` may spend the counter to the ceiling.
* Path `photos` at `share: 0.5` stops refusing-free at half the counter and **cannot** take
  the top half.
* The top half is therefore an **exact, atomic reserve** for the higher-priority path — held
  by the same row lock that does the counting, with no scheduler, no barrier, no depth probe
  and no leg ordering anywhere in the code.
* **The shares do not have to sum to 1, and normally will not.** They overlap on purpose.
  The total is still bounded by `cap` because there is one counter. Every v1 rule about
  oversubscription (`lane-shares` × 4, `lane-floor`, `ceiling-minus-measured`,
  `measured_share`, the exhaustive `every_spec_that_validates_clean_divides_exactly_one_ceiling`
  property test) exists to police an invariant that is now structural.

Default shares: with `K` distinct priority ranks present at a node, rank `r` (0 = highest)
gets `share = (K - r) / K`. Three ranks → 1.0, 0.667, 0.333. The top rank is always 1.0
(validation `share-top`, §11).

What v2 gives up by doing this: **strict** priority. A low-priority path is not stopped dead
while a high-priority one has a backlog; it is capped. Under saturation the high-priority path
always has its reserve available, which is the property the feature was bought for. What it
gains: the low-priority path is not starved into a stalled graph by one unreadable leg, which
is the failure `STALL_TOLERANCE` existed to bound. This is a deliberate exchange and it is
recorded here rather than discovered later.

### 3.7 Worked example — the author's `airbnb` graph

The v1 flagship: three traffic classes, each isolated, merging into the one node that holds
the egress-IP ceiling; prices first, photos last; the photo node limited per listing at a
cardinality no single state document could hold.

```json
{
  "application": "channel",
  "graph": "airbnb",
  "version": 1,

  "nodes": {
    "prices": {
      "ingress": true,
      "cost": { "path": "payload.rooms", "default": 1, "max": 50 },
      "budgets": [
        { "id": "prices-1s", "count": 100, "timeMs": 1000,
          "confidence": "documented", "source": "https://airbnb.dev/limits", "asOf": "2026-08-01" }
      ]
    },

    "messages": {
      "ingress": { "queue": "channel.airbnb.messages.in", "http": false },
      "cost": { "path": "payload.rooms", "default": 1, "max": 50 },
      "budgets": [
        { "id": "messages-1m", "count": 600, "timeMs": 60000, "subWindows": 60 }
      ]
    },

    "photos": {
      "ingress": true,
      "cost": { "path": "payload.rooms", "default": 1, "max": 50 },
      "budgets": [
        { "id": "photos-1m",   "count": 300, "timeMs": 60000, "subWindows": 60 },
        { "id": "per-listing", "count": 100, "timeMs": 604800000, "subWindows": 1,
          "scopeBy": "payload.listingId", "whenOp": ["photo.delete"] }
      ]
    },

    "ip": {
      "cost": { "path": "payload.rooms", "default": 1, "max": 50 },
      "budgets": [
        { "id": "ip-10s", "count": 1500,   "timeMs": 10000,   "subWindows": 10,
          "sharedKey": "egress-ip" },
        { "id": "ip-1h",  "count": 300000, "timeMs": 3600000, "subWindows": 60,
          "sharedKey": "egress-ip-hour" }
      ],
      "egress": { "queue": "channel.airbnb.out", "group": "channel-workers" }
    },

    "audit": {
      "budgets": [ { "id": "audit-1s", "count": 2000, "timeMs": 1000 } ],
      "egress": "channel.airbnb.audit"
    }
  },

  "paths": [
    { "name": "prices",   "priority": 0, "share": 1.0,  "nodes": ["prices",   "ip"] },
    { "name": "messages", "priority": 1, "share": 0.75, "nodes": ["messages", "ip"] },
    { "name": "photos",   "priority": 2, "share": 0.5,  "nodes": ["photos", ["ip", "audit"]] }
  ]
}
```

Read it back:

* `prices` may drive the shared `ip` counter to 1500-per-10s. `photos` refuses itself at 750.
  The top 375 (between `messages`' 1125 ceiling and the cap) is a reserve only `prices` can
  reach, enforced by the row lock on one KV key.
* `ip-10s` is subdivided into ten 1-second windows of 150 each, so a burst cannot take the
  whole 10-second allowance in the first 200ms and then starve the rest of the window.
* `per-listing` is 100 photo deletions per listing per week — 200,000 live keys in v1's
  arithmetic, which needed 64 shards, 64 gate runners, 64 partition leases and 64 state
  documents. Here it is 200,000 Postgres rows with a 7-day TTL and no Gate machinery at all.
* `photos` fans out to `ip` **and** `audit`: one transaction, two pushes, two derived
  transaction ids (§7). Both branches charge their own node's budgets, which is what the
  declare-time fan-out warning (§11 `fanout-multiplies`) is about.

### 3.8 Worked example — the smallest useful declaration

The rrl.js shape, one node, one budget, an ingress queue the app already owns:

```json
{
  "application": "rrl",
  "graph": "price-airbnb",
  "version": 1,
  "nodes": {
    "providerX": {
      "ingress": { "queue": "rrl.ingress.price-airbnb" },
      "cost": { "path": "payload.cost", "default": 1, "max": 10 },
      "budgets": [ { "id": "providerX", "count": 100, "timeMs": 1000 } ],
      "egress": "rrl.egress.price-airbnb"
    }
  },
  "paths": [ { "name": "main", "nodes": ["providerX"] } ]
}
```

That is `examples/rrl.js`, declared. One consumer, one KV key, one transaction per batch.

---

## 4. The compiled runtime model

The declaration compiles to a **plan**: a list of stages, a list of queues to provision, a
list of KV keys, and nothing else. The plan is pure, deterministic, and is the only thing the
supervisor reads. Everything below is derived by `gate-core::compile`, tested as a pure
function, and echoed in the declare response so a caller never has to reconstruct it.

### 4.1 Names

| Thing | Form | Notes |
|---|---|---|
| broker namespace | `gate.{app}` | as v1: two teams cannot collide |
| Gate-owned ingress queue | `gate.{app}.{graph}.{node}.ingress` | only when `ingress: true` |
| user-owned ingress queue | whatever the declaration names | Gate consumes, never creates |
| interior queue | `gate.{app}.{graph}.{node}.in` | every non-ingress node has one |
| egress queue | whatever the declaration names | Gate pushes, the app consumes |
| **stage consumer group** | `gate.{app}.{graph}.{path}.{node}` | one per (path, node) — this is the whole group taxonomy |
| budget key (node) | `b:{app}:{graph}:{node}:{bid}` | namespace `gate` |
| budget key (scoped) | `b:{app}:{graph}:{node}:{bid}:{scopeValue}` | one row per value, TTL-reaped |
| budget key (shared) | `b:{app}:shared:{sharedKey}` | one row per app, across graphs |
| breaker record | `brk:{app}:{graph}:{node}` | TTL = `retryAfterSeconds` |
| spec store | `graph:{app}:{name}` | namespace `gate`, `Expiry::forever()` — unchanged from v1 |

**Every name is minted in exactly one function in `gate-core::plan`.** This is v1's rule and
the reason for it is unchanged and still measured: a near-miss on a consumer group does not
fail loudly, because the broker answers a group with no cursor with the queue's whole
retained range — so an ETA built on a misspelling reports every message ever pushed as
waiting for budget, plausibly, for ever.

**One group per (path, node), not per node.** Two paths sharing an ingress node is
**pub-sub**: each path's group gets **every** message, so the message traverses both paths.
That is the documented, intended semantics and it composes with fan-out. It is also why the
group name carries the path.

### 4.2 Stages — the only consumers that exist

A **stage** is one hop of one path: `(path, from_node, to_nodes[])`. For the `airbnb` example
the plan is:

| # | path | consumes queue | group | charges node | pushes to |
|---|---|---|---|---|---|
| 1 | prices | `gate.channel.airbnb.prices.ingress` | `gate.channel.airbnb.prices.prices` | `prices` | `gate.channel.airbnb.ip.in` |
| 2 | prices | `gate.channel.airbnb.ip.in` | `gate.channel.airbnb.prices.ip` | `ip` | `channel.airbnb.out` |
| 3 | messages | `channel.airbnb.messages.in` | `gate.channel.airbnb.messages.messages` | `messages` | `gate.channel.airbnb.ip.in` |
| 4 | messages | `gate.channel.airbnb.ip.in` | `gate.channel.airbnb.messages.ip` | `ip` | `channel.airbnb.out` |
| 5 | photos | `gate.channel.airbnb.photos.ingress` | `gate.channel.airbnb.photos.photos` | `photos` | `gate.channel.airbnb.ip.in` **and** `gate.channel.airbnb.audit.in` |
| 6 | photos | `gate.channel.airbnb.ip.in` | `gate.channel.airbnb.photos.ip` | `ip` | `channel.airbnb.out` |
| 7 | photos | `gate.channel.airbnb.audit.in` | `gate.channel.airbnb.photos.audit` | `audit` | `channel.airbnb.audit` |

Seven stages, seven consumers, five queues, six KV keys (two of them shared) plus one row per
live listing. **Zero standing timers, zero depth probes, zero streams-cycle state rows.**

Note stages 2, 4 and 6: three groups on one queue, each getting every message. That is not a
mistake — it is what makes `ip`'s per-path share meaningful. Each stage charges the same
`ip` counter with its own path's `max`, so `prices`' messages are the ones that reach the
reserve. Which stage a given message arrives through is decided upstream, where the message
was pushed.

**Wait — three groups on `ip.in` would each forward a copy.** They do not, because a message
is pushed into `ip.in` by exactly one upstream stage, and that stage's push carries a
partition and a transaction id derived from **its own path** (§7). Every group does read every
message; the ones from a different path are recognised by the `_gate.path` stamp and
**acked without being forwarded** — a below-cursor-cheap skip, one ack per foreign batch, no
push, no budget charge. See §6.7, which is the one piece of bookkeeping v2 adds that v1 did
not have. It is also why §16.1 asks whether per-path interior queues would be cheaper.

### 4.3 The consumer

One `tokio` task per stage, holding one `queen_mq::QueueBuilder` `consume_batch` loop:

```rust
queen.queue(stage.source)
     .group(&stage.group)
     // Where a NEW group starts, and it is two rules — see below.
     //   ingress source:   .subscription_mode(All)
     //   interior source:  .subscription_mode(New)
     //                     .subscription_from(runtime_start - INTERIOR_SEED_SKEW)
     .batch(stage.batch)                         // default 200
     .partitions(1)                              // ONE source partition per claim — §6.4
     .concurrency(stage.concurrency)             // default = max(4, source partitions)
     .auto_ack(false)                            // the relay settles inside its own txn
     .lease_seconds(30)                          // a WORK lease, not a pacing quantum
     .renew_lease(Duration::from_secs(10))
     .wait(true)
     .poll_timeout(Duration::from_secs(30))      // parked, connection released, push-woken
     .cancel(stage.cancel.clone())
     .consume_batch(move |msgs| relay(stage.clone(), msgs))
     .await
```

Every line of that is load-bearing and every one gets a comment in the source:

* **No `.partition()`.** This is the whole scheduler. The wildcard pop picks a candidate in
  randomised order and claims it with `FOR UPDATE SKIP LOCKED`, so N workers spread across
  partitions with no coordination. v1's pinned runners, hot-partition depth probe,
  `FULL_SWEEP_EVERY`, rotation cursor and `MAX_IN_FLIGHT` all exist to do, badly, what the
  broker does here for free.
* **`.partitions(1)`.** A claim covers exactly one source partition, so the relay transaction
  touches one source partition row and one destination partition row. That is the txnload
  lane discipline: 64 workers acking across 16 source partitions in one transaction
  serialised to **33 txn/s with the machine 95% idle**; the same workers on disjoint
  partitions did **603 txn/s and 23,000 items/s**.
* **`.concurrency(n)`.** More workers than partitions is harmless (the extras find nothing and
  park); fewer is a throughput ceiling. Default `max(4, partitions)`, declarable per node.
* **`.wait(true)` with a 30s poll timeout.** An idle stage is a parked long-poll: no
  connection held, no query, woken by the push notifier. This is the line that turns 275,000
  polls per hour into approximately zero.
* **`renew_lease`.** The handler may park in-line for up to a sub-window. Without renewal the
  lease could lapse mid-park and the batch would be redelivered while this worker still holds
  it.
* **The subscription seed**, and it is **two rules, decided by what writes the source queue**.
  The plan carries the answer per stage as `Stage::source_is_interior`, so the consumer reads a
  bool and nobody matches on a queue name.

  * An **ingress** source — a queue the application owns, or Gate's own
    `gate.{app}.{graph}.{node}.ingress` HTTP door — gets **`All`**, never `new`. A producer
    writes it; a group created at the tail silently skips everything already waiting, which for a
    limiter means silently dropping the backlog it exists to pace. Two paths entering at one node
    is pub-sub by design (§4.2/§6.7), so this is unchanged and for the unchanged reason.
  * A Gate-owned **interior** source — `gate.{app}.{graph}.{node}.in`, written only by Gate's own
    relay — is seeded at **the tail as of the moment this graph runtime started**, expressed as a
    `subscription_from` timestamp.

  **Why that is exact and not a heuristic.** A frame lands on an interior queue only because a
  stage of some path `P` relayed it there, and it carries `P`'s `_gate.path` stamp. A group for
  `P` on that queue can therefore never need anything older than the instant `P`'s own upstream
  stage started relaying. The three cases fall out: a graph's **first declare** provisions its
  interior queues empty, so tail is head and nothing changes; a **restart** finds the group
  already there and the broker never re-seeds a cursor that exists; a **path added to a running
  graph** starts at the tail and skips the other paths' backlog, which is the whole point.

  **Why a timestamp and not `SubscriptionMode::New`.** `New` has a startup race. Every stage of a
  runtime is spawned at once, so the entry stage of the added path can relay a frame into the
  interior queue before the downstream stage's first pop has created its group; `New` seeds that
  group at `last_offset` as of the first pop, so the frame lands below the cursor and is dropped —
  the exact failure the blanket `All` was written to prevent. A cursor seeded from an instant taken
  in `supervisor::start` **before anything is provisioned or spawned** cannot lose that race:
  anything this runtime relays is at or after the seed. Verified against the broker, and the three
  facts it rests on are (`004_log_pop.sql` in the queen server):

  * a new group's per-partition cursors are seeded at the first segment with
    `created_at >= T`, cursor `= base_offset - 1`, falling back to `last_offset` when nothing is
    that recent (the group-first-contact bulk seed, `:832-843`);
  * a partition that materialises **later** — and that queue had 105 of them, created lazily per
    key — reads the group's stored `subscription_timestamp` from `consumer_groups_metadata` and
    walks the same way (`:182-199` and `:305-319`), rather than starting at that partition's tail;
  * `subscription_from` takes precedence over the mode outright (`:779-790`), and the symbolic
    `now` is resolved broker-side — but `now` is *not* what Gate sends, because it degrades to
    `new` and hands the race straight back.

  **The seed is Gate's clock and the frames carry Postgres's**
  (`003_log_push.sql:219-224`), so the instant is pushed back by `INTERIOR_SEED_SKEW`, two minutes,
  `GATE_INTERIOR_SEED_SKEW_SECONDS`. The margin is bounded on both sides: below by the skew it
  absorbs and by the spread between replicas — a declare lands on one replica and the others
  follow through the reconcile loop up to `GATE_RECONCILE_SECONDS` later, each with its own start
  instant, and whichever pops first is the one that seeds the group — above by the broker's
  `log_txns` purge at
  `now() - GREATEST(dedup_window, completed_retention, 900s)`
  (`006_log_maintenance.sql:389`) — frames inside the margin are foreign and are settled by ack,
  and an ack older than that window resolves nowhere. Which is the incident, below.

  **The incident, 2026-09-02.** `channel-go` redeclared `vrbo` adding a path `reviews` through the
  pre-existing terminal node `partner`. The new stage `(reviews, partner)` read
  `gate.channel-go.vrbo.partner.in` under a brand-new group, seeded with `All` at the oldest
  retained frame of all 105 partitions: ~19,800 frames belonging to the other three paths, the
  oldest twelve days old. Every one was foreign, foreign frames are settled by ack inside the
  single relay transaction (§6.4/§6.7), and the broker resolves those acks by transaction hash
  against `queen.log_txns` — purged long before the segments are. So every ack resolved nowhere,
  the broker raised `QTXN ack references unknown transactionId; transaction rolled back`
  (`005_log_ack.sql:1451`), and the transaction rolled back. Gate logged the refusal about ten
  times a second per replica, for ever; the cursor never moved; both replicas OOMed at 512Mi; and
  the console reported the backlog as `waiting_for_budget`, which was a lie — nothing was waiting
  for a counter. An operator fixed it by seeking the group to the end. Two things came out of it:
  this rule, and `StageCounters::wedged` (§6.4), so the next one has a number of its own.

### 4.4 What no longer runs

For the `airbnb` graph, v1 ran: 3 gate runners per entry node × shard count (64 for `photos`
= 66 runners), 1 meter task per node (5), 1 relay task per destination (2) each spawning up
to 16 workers per cycle across 2 legs, 1 reconcile loop, 1 history prune, plus a depth cache
serving four read shapes. v2 runs **7 consumers, 1 reconcile loop, 1 history prune**.

---

## 5. Budgets

### 5.1 One key, one counter, no window index

```
key   = b:{app}:{graph}:{node}:{bid}[:{scope}]     (or b:{app}:shared:{sharedKey})
max   = round(count_sub * share(path))
ttl   = window_sub_seconds
delta = cost
```

The create-only TTL does the rotation. There is no window index in the key, no `% 4`
recycling, no sweeper and no expiry pass — v1 needed all four because it owned a JSON document
that nothing else pruned; Postgres prunes this one.

### 5.2 Boundary semantics, stated plainly

This is a **fixed window whose start is the first admitted request after the previous window
expired** — not a calendar window and not a sliding one. A sliding observer can therefore see
up to **2 × count_sub** across one boundary: `count_sub` at the end of window *k* and
`count_sub` at the start of window *k+1*. Subdivision (§5.3) is what bounds that in the units
that matter: with `subWindows: 10`, the 2× exposure is 2/10 of the declared window's count,
not 2× the declared count.

v1 offered `rolling` (a two-bucket sliding counter) precisely to avoid this, at the cost of
owning the arithmetic. KV cannot express it — `incr` is one atomic add against one row — and
faking it client-side would reintroduce the read-then-write race the primitive exists to
remove. So: **document it, subdivide it, and put the exact sentence in the declare response's
warnings** where a declaration's `subWindows` leaves a sub-window longer than 2 seconds
(§11 `window-boundary`).

### 5.3 Subdivision

```
N          = subWindows, or the default below
count_sub  = floor(count / N), floored at 1
window_sub = ceil(count_sub * timeMs / (count * 1000)), floored at 1
```

Default `N`:

```
if timeMs <  2000 : N = 1
else              : N = clamp(timeMs / 1000, 1, count)      // aim for a 1s sub-window
```

`clamp(..., count)` matters: `N` may never exceed `count`, or `count_sub` floors to 1 and the
budget enforces `N` per window instead of `count` per window. Validation `subwindow-fits`
(§11) refuses an explicit `subWindows > count`.

**The property is one inequality**, and it is the only thing the arithmetic above is for:

```
count_sub / window_sub  <=  count / (timeMs / 1000)
```

The enforced rate is at or below the declared one, for every accepted document. Enforcing
tighter than declared is the safe direction; enforcing looser is a vendor block.

This section used to say *"rounding is always down, in both terms"*, and that was wrong — the
two terms are not on the same side of the fraction. Rounding the COUNT down lowers a
numerator, which is tighter; rounding the WINDOW down lowers a DENOMINATOR, which is
**looser**. `count: 200000, timeMs: 3600000, subWindows: 2000` gave `100` per
`floor(1800ms) = 1s`: 100/s enforced against 55.6/s declared, on a document that validates
clean. So `window_sub` is not derived from `timeMs / N` at all — it is derived from the
`count_sub` actually chosen and rounded UP, which is the inequality solved for the window.
Deriving it from `count_sub` also covers the case where `count_sub` hits its floor of 1
(`count: 5` over ten sub-windows enforced `1` per `1s` where `1` per `2s` was declared).

Rounding up costs exactness where the division is not clean: 20000 per 300s over 150
sub-windows is `133` per `2s`, 66.5/s against a declared 66.67/s. That is the trade, taken
deliberately in the safe direction; a caller who wants the declared rate exactly picks an `N`
that divides the period, which is what §12.2's migration does. Where `floor` loses more than
2% of the declared count the declare response warns and names both numbers
(§11 `subwindow-rounding`).

**The one-second floor.** TTLs are whole seconds (§2). A declared `timeMs` below 1000 cannot
be enforced at its own width. The rule: `window_sub` is floored at 1 second and `count_sub`
keeps the declared `count`, which enforces *at most `count` per second* — strictly tighter
than *`count` per `timeMs`* and therefore safe, but slower than declared. The declare
response says so loudly (§11 `window-sub-second`). See §16.4: this is the one place the
settled brief's `timeMs >= 100` meets a primitive it cannot be built on, and the author should
confirm the rounding direction rather than discover it.

### 5.4 Multi-window

A node's budgets are **independent keys**, all of which must admit. They are charged in one
`kv.batch` (§6.2), and because `kv.batch` applies ops independently, a partial pass is real
and must be refunded (§6.3).

---

## 6. The admission algorithm

This is the heart of the crate. It lives in `crates/server/src/relay.rs` and is one function.

### 6.1 Grouping

The consumer hands the relay `Vec<Message>`, all from **one source partition** (§4.3), in
offset order. Before anything is charged:

```
for each msg, in order:
    path_of(msg)  = msg.data._gate.path        // absent on a first-hop ingress message
    if path_of(msg) is Some(p) and p != stage.path  -> mark FOREIGN     (§6.7)
    cost(msg)     = resolve(stage.cost, msg.data)   // integer >= 1, or DEAD-LETTER
    for each budget b of the charged node:
        if b.whenOp does not match msg.data.op -> skip this budget for this message
        key(b, msg) = derived per §5.1  (scopeBy reads msg.data)
        charge[key] += cost(msg)
```

The output is `charges: Vec<(key, max, ttl, total_delta)>` — one entry per distinct key the
batch touches — and a per-message running prefix sum per key, which the fallback needs.

### 6.2 The happy path: one KV call

```rust
let mut ops = Vec::with_capacity(charges.len() + 1);
for c in &charges {
    ops.push(kv.incr(NS, &c.key, c.total_delta, Expiry::seconds(c.ttl))
               .max(c.max)
               .operation()?);
}
ops.push(KvOperation::get_many(NS, charges.iter().map(|c| c.key.clone()).collect()));
let out = kv.batch(ops).await?;
```

One round trip. `getMany` rides along **always**, not only on refusal, because:

* a refused `incr` result carries no `expiresAt` (§2), and the wait deadline needs one;
* the rows are the ones the `incr`s just touched, so the read is index-only on a hot page;
* asking only on refusal costs a second round trip at exactly the moment the system is
  saturated, which is the worst moment to add one.

If every `incr` applied: **the whole batch is admitted**, go to §6.4. This is the case that
must run at 10k+ items/s, and it is one KV call plus one transaction commit per batch — two
DB round trips per 200 messages.

### 6.3 Refusal: refund, then admit the prefix that fits

If any `incr` refused:

1. **Refund the ones that applied**, in the same `kv.batch` as step 3 where possible:
   `incr(key, -total_delta, {min: was - delta, max: was - delta})`, where `was` is the value
   that charge's own `incr` returned.

   **`min: 0` alone is not enough, and the reason is in the procedure.** `min` is a guard and
   not a clamp (§2), but it is a guard on the *resulting value*, not on the identity of the
   window: `024_kv.sql`'s UPDATE branch tests
   `kv_num_v1(k.value, k.expires_at, v_now) + v_delta >= v_min`. It therefore refuses a refund
   into a key that has been REAPED — the create branch is gated by the pure `delta >= min`
   comparison and `-D >= 0` is false — and **applies** one into a key another worker has just
   RECREATED. Sub-windows are a second wide by default and this path fires exactly when the
   counter is contended, which is exactly when the row is recreated at once, so a batch that
   straddled a rotation handed its whole delta to the next window, which then admitted
   `cap + D`. That is the same over-admission class as v1's measured 7131-against-5000,
   arriving by a different route.

   So the identity travels in the value guard: **apply only if this counter still reads
   exactly what my charge left on it.** A rotation, another worker's charge and another
   worker's refund all refuse, which is the safe direction; `was - delta` is never negative,
   so the `min: 0` property survives as a consequence. A refused refund is logged at WARN with
   the key and the delta and is otherwise dropped: it is at most one sub-window's over-count on
   one key, bounded and self-healing, and the alternative (a retry loop against a rotating
   window) is unbounded.

2. **Compute the prefix.** For every key, `remaining = max - current`, where `current` comes
   from the `getMany` in §6.2 (and from the refused `incr`'s own `value`, which is the same
   number — use the `getMany` row, it also carries `expiresAt`). Walk the batch in order,
   accumulating each message's cost into every key it touches, and stop at the first message
   that would take any key past its `remaining`. `k` is the number of messages before it.

   **Prefix, not subset.** A message that fits while an earlier one does not is *not*
   admitted, even when they touch different scoped keys. Order inside a partition is the
   guarantee the partition-passthrough design is built on, and a subset admit would break it.
   This is v1's deferral rule verbatim: *a denial stops the batch, acks the prefix, keeps the
   lease.*

3. **`k == 0`** → §6.5 (park or release). Otherwise charge exactly the prefix in one
   `kv.batch` (with the refunds from step 1 prepended). If *that* refuses — another worker
   took the headroom between the two calls — refund, recompute from the newly reported
   values, and retry. **`MAX_PREFIX_RETRIES = 2`**, then treat it as `k == 0`. An unbounded
   retry loop against a contended counter is how a limiter turns into a spin.

4. Admit `msgs[..k]`, go to §6.4. The tail is **not acked**: the lease keeps it, it is not
   claimable by another worker so it cannot overtake, and it comes back in its original order
   when the lease expires or the handler returns.

### 6.4 The relay transaction

```rust
let mut txn = queen.transaction();
for m in admitted {
    txn = txn.ack(m);                                   // below-cursor acks are tolerated
    for dest in stage.destinations {                    // 1, or N for a fan-out
        txn = txn.push_item(TxnPushItem {
            queue:          dest.queue.clone(),
            partition:      Some(m.partition.clone()),  // PARTITION PASSTHROUGH
            payload:        stamp(m.data, stage.path, dest.node),
            transaction_id: Some(txn_id_for(stage, dest, m)),   // §7
            trace_id:       None,
        })?;
    }
}
txn.commit().await
```

* **Partition passthrough.** Same-named partition on the destination. Two things fall out of
  one line: per-connection ordering is preserved end to end (a producer's partition key
  survives every hop), and the relay's transactions stay lane-disjoint end to end — worker A
  moving partition `p7` never contends with worker B moving `p12`, at any hop.
  Where the destination queue has fewer partitions than the source, the broker creates the
  named partition on first push; where the ingress queue is user-owned, the declare-time check
  reads its partition count and warns if it is 1 (§11 `single-partition`).
* **`ack` + `push` in one transaction.** Ack-then-push loses the item; push-then-ack
  duplicates it. The transaction also carries the lease as a precondition, so a lease that
  lapsed while the relay worked rolls the whole thing back instead of forwarding work somebody
  else has re-claimed.
* **Budget charged before the transaction, not inside it.** `applied` *is* the decision, so
  it must be known before the transaction is built. The residual hazard is real and bounded:
  if the KV charge commits and the relay transaction then fails, budget was spent and nothing
  moved. The handler **refunds on transaction failure** (`incr(-delta)` guarded on the value
  the charge left, same WARN-and-drop when the counter has moved) before returning without
  acking.

  Two residues stay open and are counted rather than claimed closed. A process that dies
  between the two calls loses the refund. And a KV call the broker **committed** whose answer
  was then lost — a read timeout, a dropped connection, a proxy 502 — has spent the budget
  with nothing left that knows about it; a blind compensating refund is unsound for the same
  reason `min: 0` is (§6.3), so the stage counts it as `leaked` instead.

  We deliberately do **not** ride the `incr` inside the transaction with
  `TransactionBuilder::kv`, though the wire supports it: the decision would then be known
  only after the pushes were already staged, which is the read-then-write shape `incr` exists
  to remove.
* **A transaction the broker refuses at a head that never moves is WEDGED, and says so.** Most
  rolled-back transactions clear by themselves — a lapsed lease, a concurrent seek, a broker
  restart — and are a `WARN` and a refund. One does not: `QTXN ack references unknown
  transactionId` (`005_log_ack.sql:1451`, surfaced as the `ack_rejected` kind) means the hash the
  ack names resolves nowhere, usually because its `log_txns` row was purged while the frame itself
  is still retained. Nothing Gate does moves that frame — the cursor is behind it, the ack that
  would advance the cursor is the thing being refused, and lease expiry hands the identical claim
  straight back. So after a few refusals at one claim head the stage escalates **once** to `ERROR`
  — once, because ten identical `WARN` lines a second is precisely how the 2026-09-02 incident
  stayed invisible for hours — naming the remedy
  (`POST /api/v1/consumer-groups/{group}/queues/{queue}/seek` with `{"toEnd": true}`) and bumping
  `StageCounters::wedged`. The counter matters as much as the line: with only `released`,
  `popped` and `waitingForBudget` to look at, a wedged cursor reads on the console as a busy
  limiter doing its job.

### 6.5 Park or release

When `k == 0`:

```
waitMs = max over the REFUSING keys of (expiresAt - now)
```

**Max, not min.** If key A frees in 100ms and key B in 5s, waking at 100ms finds B still
refusing. A missing `expiresAt` (the key was reaped between the incr and the read) reads as
`0` and means *retry now*.

```
PARK_THRESHOLD  = 1500 ms       // just above the 1s sub-window floor, so an ordinary
                                // rotation always parks rather than releasing
MAX_PARKS       = 3             // per handler invocation
jitter          = uniform(0, min(200ms, waitMs/4 + 20ms))
```

* `waitMs <= PARK_THRESHOLD` **and** parks so far `< MAX_PARKS` → `sleep(waitMs + jitter)`,
  then retry from §6.2. The lease is held and renewed. **The jitter is not decoration:** every
  worker refused in the same sub-window reads the same `expiresAt` and would otherwise
  stampede the same row on the same millisecond. It is the difference between 33k incr/s and
  a lock convoy.
* otherwise → **return without acking.** The lease expires and the batch is redelivered.
  Queen charges **no retry budget on lease expiry** (§2), so this costs nothing, cannot
  dead-letter waiting work, and needs no `retry_limit: 0`.
* **Never nack for pacing.** An explicit `failed` ack is reserved for real poison — a
  malformed payload, an unresolvable cost — where engaging retry and the DLQ is the point.
  This is the line that gives Gate a working DLQ back; v1 had to set `retry_limit: 0` on the
  push queue precisely because it could not tell waiting from failing.

Every sleep is a `tokio::select!` against the stage's cancel token, so a redeclare does not
wait out a park.

### 6.6 QDUP: split and settle

A `QDUP` inside a transaction is a hard error that rolls the whole bundle back (§2). It
happens when a batch mixes messages whose push already landed (a commit whose response was
lost) with fresh ones. Left alone that is a partition stalled for ever: the batch comes back,
the same push is refused, nothing is ever settled.

On a commit error whose text contains `QDUP`:

1. bump `stage.duplicates`, log WARN with the queue and partition;
2. settle **one message at a time**: one transaction of `ack + push(es)` each;
3. a per-item `QDUP` falls back to a **bare `ack`** of that message — it is already
   downstream, so it must not take its batch down with it, and it counts as *settled* but not
   as *forwarded* (it spends no budget the second time because §6.2 already charged it once
   and §6.3's refund path never ran);
4. an item that cannot be staged at all (unresolvable cost) is **nacked with a reason** so it
   reaches the DLQ, never dropped;
5. a per-item commit error that is not QDUP leaves that item alone — its lease lapses and it
   comes back.

`duplicates` is exposed on the stage view. v1's note stands: *it should be zero, and it is
here because "should be" is not a measurement, and a recovery path nobody can see is one
nobody knows ran.*

### 6.7 Foreign-path messages on a shared interior queue

Stages 2, 4 and 6 of §4.2 share `ip.in`. Each group sees every message; only the one whose
`_gate.path` matches forwards it. The others must settle it or their cursor never advances.

```
FOREIGN messages are acked in one bare `ack_all` transaction, before the charge,
and are never counted, charged or forwarded.
```

The cost is one ack per foreign message per non-owning group — that is `(paths_at_node - 1)`
extra acks per message, batched. At three paths it is two extra acks per message, or one
extra ack transaction per batch per group. Measurable, bounded, and the thing §16.1 asks
whether to trade away for per-path interior queues (`gate.{app}.{graph}.{node}.{path}.in`),
which would remove it entirely at the cost of one queue per path-hop.

**And the cost is bounded only because of where a new group starts.** "One ack per foreign
message" is cheap when the foreign messages are *recent*. It is not cheap, and on 2026-09-02 it
was not even possible, when a group's cursor is seeded at the head of a twelve-day retained log:
an ack resolves by transaction hash against `queen.log_txns`, which the broker purges after
`GREATEST(dedup_window, completed_retention, 900s)` while the segments live for far longer, so an
ack for a frame older than that window resolves nowhere and rolls the whole relay transaction
back. The skip has no way out of that — the cursor cannot move, the ack that would move it is the
thing being refused, and lease expiry hands the identical claim back. So the skip is only ever
asked to settle frames a group could plausibly have been meant to see, and that is what §4.3's
seeding rule guarantees: a new group on an interior queue starts at the tail as of its runtime's
start, plus a small clock-skew margin that is deliberately far inside the broker's transaction
window. A stage that finds itself in the old situation anyway now says so — see
`StageCounters::wedged` and the `ERROR` it escalates to once, naming the `seek` that clears it.

### 6.8 The `_gate` stamp

The one piece of per-item provenance v2 keeps, unchanged in spirit from v1:

```json
"_gate": { "graph": "airbnb", "path": "prices", "hop": 2, "at": 1755763200000 }
```

One reserved object, not four top-level keys, so it cannot collide with a `scopeBy` path or a
cost path. Stamped by the ingress push (HTTP front door) or by the first relay that handles an
unstamped message (which is how a user-owned ingress queue works — producers know nothing
about Gate). Carried verbatim by every relay, rewritten per hop. It is **not signed and not
verified**: it is trusted because it is written server-side and because writing to an interior
queue is already admission bypass (§13).

---

## 7. Transaction id derivation

```
NS_GATE = 6ba7b814-9dad-11d1-80b4-00c04fd430c8   // a fixed v5 namespace, baked in

derive(parent, label) = uuid_v5(NS_GATE, parent + "\u{1f}" + label)      // RFC 4122 §4.3
```

Written out in `gate-core::ids` over `sha1 = "0.10"`, for the same reason v1 wrote FNV-1a out
by hand: the value must be **stable across releases** and **reproducible by an operator with a
shell**, and a hasher chosen by a dependency's default is neither.

Determinism is the whole point in both directions:

* **deterministic** so a redelivered relay computes the same id and the broker's dedup refuses
  the second push — this is the exactly-once mechanism;
* **branch-unique** so a fan-out's two branches do not carry the same id, which matters when
  they later converge on one queue and dedup would silently collapse one of them.

**When to derive and when to reuse.** The settled brief says the relay carries the upstream
`transactionId` through, and derives at a fan-out. Compiling the plan makes a sharper rule
available for free, and the compiler applies it:

```
converging(queue) = the number of (path, hop) stages in this plan that push into `queue`

label(stage, dest)  = "{application}/{graph}/{path}/{dest_node}"
label(terminal)     = "{application}/{graph}/{path}/{node}/out"

txn_id_for(stage, dest, msg) =
    if dest is a TERMINAL push                   -> derive(msg.transaction_id, label)
    else if dest is one of several fan-out branches -> derive(msg.transaction_id, label)
    else if converging(dest.queue) > 1           -> derive(msg.transaction_id, label)
    else                                          -> msg.transaction_id          // reuse
```

The middle arm is the one the brief could not have named, and it closes a hole the fan-out
rule alone leaves open: in the `airbnb` plan, `ip.in` is pushed into by three stages, so two
messages that entered by different paths carrying the same upstream id (the same producer
event fed to two ingress queues, which is exactly what pub-sub over a shared ingress produces)
would dedup-collapse on arrival. Deriving per `{path}/{node}` makes them distinct while
keeping each one idempotent under its own redelivery. `converging` is computed at declare
time and recorded in the plan, so the hot path does a field read, not a graph walk.

**The first arm is a change to settled point 4 and is recorded as one.** It was added on
2026-08-21 because the middle arm alone is unsound, and the failure is silent message LOSS
rather than a duplicate. `converging` counts the stages of ONE plan: two separate graphs that
both name `channel.airbnb.out` as an egress each count one, so each reuses the upstream id
verbatim. Partition passthrough puts both copies on the same-named partition, and the broker
probes dedup by hash per partition (`003_log_push.sql:130-170`, `xxh3_128` of the transaction
id's bytes alone, `dedup_window_seconds` 3600 by default) — so one producer event id entering
both graphs means the second push is a dedup hit and that message is gone, with no error
anywhere. §11's `egress-owner` is only a WARNING, so nothing prevents the topology; and the
compiler cannot see the other graph, because reading it from the store would make the plan
depend on what a replica happened to load and two replicas would then mint different ids for
one message.

Deriving at a terminal costs nothing and keeps every property the reuse was for: the id is
still deterministic in the message's own, so a redelivered relay computes the same one and
dedup still refuses the second push, and a producer's own coalescing id still collapses two
pushes into one at the door. What changes is the id value an application reads off its egress
queue — it is now a v5 uuid derived from the one it pushed, not that one.

The `{application}/{graph}` prefix on every label is part of the same fix: without it two
graphs can mint the same id for two different messages by having the same path and node names.

See §16.2 for the middle arm, which is a **refinement** of settled point 4 rather than a
departure. Both want the author's explicit word.

---

## 8. The breaker

New in v2, and the feature that makes a vendor's `429` actionable without a Gate-mediated ack
path.

```
POST /v1/apps/{app}/graphs/{graph}/nodes/{node}/backoff
{ "retryAfterSeconds": 30, "refundCost": 1 }
```

1. **Refund first, if asked.** `incr(key, -refundCost, {min: 0})` on every unscoped budget key
   of the node. First, because after step 2 the counter is at the cap and a refund would open
   a hole in the very window we are about to spend.
2. **Spend the window.** For every unscoped budget key of the node:
   `kv.put(NS, key, value = max_for_the_widest_share, { ttl: retryAfterSeconds })`.
   `put`'s TTL is **not** create-only (only `incr`'s is), so this rewrites both the value and
   the expiry in one call.
3. Every path's `incr` now refuses through the ordinary refusal path — no new code path, no
   flag to check on the hot path, nothing to forget to clear.
4. Every parked consumer's `expiresAt` **is** the `Retry-After` deadline, so §6.5's
   `waitMs` computes the vendor's own number without being told it.
5. Record `brk:{app}:{graph}:{node}` = `{ at, retryAfterSeconds, by }` with the same TTL, so
   `/api/breaches/recent` has a **fleet-wide** source that needs no Postgres. v1's breach ring
   was per-replica, and a breach seen only by the pod nobody is looking at is a breach nobody
   sees.

The value written is the widest path's ceiling (`round(count_sub * max share)`), so no path
can slip under it. A node with only scoped budgets has no lever here, which is why §11's
`node-unscoped-budget` requires every node to declare at least one unscoped budget — the same
requirement the ETA has, for the same reason.

`retryAfterSeconds` is clamped to `[1, 3600]`. Un-breaking early is `DELETE` on the same path,
which deletes the budget keys (the next `incr` recreates them at zero) and the `brk:` record.

---

## 9. ETA

**Computed on demand, per API call. Nothing standing, nothing polling, nothing cached in the
hot path.** v1's `Depths` cache with its five read shapes, TTL, stale-serve and 404 memo
existed because the relay read depths on every cycle; the relay reads no depths at all now,
so the cache has no hot-path caller left. A small (2s) cache is kept for the console only,
where a page fan-out still asks the same question several times.

```
GET /v1/apps/{app}/graphs/{g}/nodes/{n}/eta?path={p}
```

Two broker calls:

```
depth  = GET /api/v1/resources/queues/{node.source_queue}/depth?group={stage.group}
state  = kv.batch([ getMany(NS, node.unscoped_budget_keys) ])
```

Then, per budget `b`:

```
cap_p     = round(count_sub(b) * share(path))
value     = state[key(b)].value  or 0
remaining = max(0, cap_p - value)
need      = depth * avgCost                       // avgCost from the counters stream,
                                                  // else cost.default; `assumes` says which
if need <= remaining:
    seconds_b = 0
else:
    edge      = max(0, expiresAt(b) - now) / 1000  // 0 when the key is absent
    windows   = ceil((need - remaining) / cap_p) - 1
    seconds_b = edge + windows * window_sub_seconds(b)

etaSeconds = max over b of seconds_b               // the slowest budget binds
boundBy    = the b that produced it
```

`cap_p <= 0` (a share that rounds a path out of existence — refused at declare time, but a
stored document from an older build can still carry it) answers `null`, never infinity.
`null` rather than infinity because a product can render *"we cannot say"* and would render an
infinity as a number.

The **second backlog** — work Gate has already released that the caller's own workers have not
picked up — is the egress queue's depth under `egress.group` when one is declared, and the
queue-level (worst-cursor) number otherwise. Queue-level is the worst cursor across every
group, so it is at or above the group being asked about: it can only make the answer later,
never earlier, which is the safe direction for a bound.

`assumes` survives, rewritten. It always opens with *"no earlier than: the backlog that is
there right now, at the refill schedule the spec declares"* and then names only the caveats
that actually apply: measured vs declared item cost; a shared budget whose other spenders are
invisible from here; a scoped budget whose per-key backlog this number does not resolve; a
node several paths converge on, where a higher-share path may take the headroom first; a
fan-out downstream that multiplies what the backlog will cost; and a breaker currently
holding the node. Every listed caveat is a way work can be put in front of yours **after** the
answer is given, which is exactly why the number is a bound and never a promise.

---

## 10. Observability

### 10.1 What the hot path writes

Nothing. No calls queue, no meter, no per-decision trace, no state mirror. The hot path
writes one KV batch and one transaction, and that is the entire budget.

### 10.2 In-process counters

Per stage, `AtomicU64`: `popped`, `admitted`, `deferred` (prefix refused), `parked`,
`released`, `forwarded`, `commits`, `duplicates`, `foreign`, `deadlettered`, plus
`last_refusal: RwLock<Option<(budget_id, at)>>`. Lifetime, per-replica, exposed on the graph
view. `forwarded / commits` remains **the** number that explains a stage's throughput, and it
should now be near `batch` rather than near 1.

### 10.3 The optional counters stream

`"counters": { "windowSeconds": 60 }` on the graph turns on **one** streams job per graph: a
tumbling-window aggregate over the egress queue producing `{ path, node, count, cost }` per
window, written to `gate.rollups`. This is opt-in, per graph, and off by default — the point
of the architecture is that observability is a thing you switch on, not a thing that runs
whether or not anyone is looking. It is the source for `avgCost`, `/api/flow`, `/api/rollups`
and the console's charts.

Everything in v1's PG history layer that was fed by the **calls queue** — `cost_actual`,
`throttled`, `calls`, `measured_share` — has no source in v2 and is dealt with individually in
§13.

### 10.4 Traces

`/api/traces` survives on a **bounded in-process ring of refusals** (500, drop-oldest), which
is what v1 actually did despite documenting sampling. Denials are the interesting event and
the only one worth a row. Admissions are counted, never traced. When history is configured the
ring is flushed on the same cadence as the rollups. §16.5 records that the trace stream is
strictly poorer than v1's, and why.

---

## 11. Validation — the new set

The loud-422 philosophy is kept exactly: a `Problem { rule, detail }`, all problems joined
with `; `, and a message that names the number, names the consequence and names the fix.
Rule names are asserted on in tests, so they are API.

**Naming**

| rule | when | detail |
|---|---|---|
| `application` | not `ok_name` | `` `application` must be one lowercase segment (letters, digits and dashes, starting with a letter or digit, at most 63 characters): it becomes part of a queue name and a kv key, which cannot carry anything else. Got `{v}`. `` |
| `graph-name` | not `ok_name`, or contains a dot | `` a graph name is one segment, because the dot is what joins a graph to its node: `{graph}.{node}` is the target of every queue this declaration creates. Got `{v}`. `` |
| `node-name` | not `ok_name`, or `{graph}.{node}` over 63 chars | `` node `{n}`: the queue name this becomes is `gate.{app}.{graph}.{n}.in`, which is {len} characters. Shorten the graph or the node. `` |
| `path-name` | not `ok_name`, or duplicated | `` path `{p}` is declared twice. A path names a consumer group per node it visits, so two paths of one name would share a cursor and split the stream instead of each receiving it. `` |

**Graph shape**

| rule | when | detail |
|---|---|---|
| `nodes` | empty | `a graph with no nodes limits nothing.` |
| `paths` | empty | `` a graph with no paths has no way in and no way out: declare at least one path naming the nodes a message visits, in order. `` |
| `path-node` | a path names an undeclared node | `` path `{p}` visits `{n}`, which is not a declared node. Declared nodes are: {list}. `` |
| `path-length` | a path has fewer than 1 element | `` path `{p}` is empty. `` |
| `acyclic` | the union of all path edges has a cycle | `` these nodes form a cycle: {a} -> {b} -> {a}. An item would traverse it for ever, re-paying every budget on the way round. `` |
| `path-entry` | a path's first node has no `ingress` | `` path `{p}` starts at `{n}`, which declares no ingress. Work cannot enter a node that has no queue to enter by: give `{n}` an `ingress`, or start the path at a node that has one. `` |
| `path-terminal` | a path's last element contains a node with no `egress` | `` path `{p}` ends at `{n}`, which declares no egress. Work would be admitted and then have nowhere to go. Name the queue your consumers read: `"egress": "{app}.{graph}.out"`. `` |
| `node-orphan` | a declared node appears in no path | `` node `{n}` is declared and no path visits it: it can never hold work. `` |
| `fanout-branch` | a fan-out array has fewer than 2 elements, or nested arrays | `` path `{p}` hop {i}: a fan-out is a flat array of at least two node names. `` |
| `fanout-terminal` | a fan-out is not the last hop of its path | `` path `{p}` fans out to {list} at hop {i}, which is not the last hop. After a fan-out the branches are separate streams; give each one its own path. `` |

**Budgets**

| rule | when | detail |
|---|---|---|
| `node-budget` | a node declares no budgets | `` node `{n}` declares no budget, so it limits nothing — it would admit everything straight through, which is a queue with extra steps. `` |
| `node-unscoped-budget` | every budget of a node has `scopeBy` | `` node `{n}` has only per-key budgets. It needs at least one budget on the node itself: it is what the ETA measures a rate against and what the breaker spends when a vendor says 429. `` |
| `budget-count` | `count < 1` | `` budget `{b}` of node `{n}` has count {c}. A budget that cannot admit anything never will — no schedule refills it. `` |
| `budget-window` | `timeMs < 100` | `` budget `{b}` of node `{n}` declares timeMs {t}. The floor is 100. `` |
| `budget-window-floor` | `timeMs < 1000` | **WARNING, not a refusal** — see `window-sub-second` below. |
| `budget-unique` | two budgets of one node share an `id` | `` node `{n}` declares budget `{b}` twice: the id is the kv key, so the second would spend the first's counter. `` |
| `subwindow-fits` | explicit `subWindows > count` | `` budget `{b}` of node `{n}` asks for {N} sub-windows of a count of {c}: each would carry {c}/{N} < 1, so the budget would enforce {N} per window instead of {c}. Lower subWindows to at most {c}, or raise count. `` |
| `subwindow-range` | `subWindows < 1` or `> 3600` | `` budget `{b}` of node `{n}`: subWindows must be between 1 and 3600. `` |
| `cost-fits` | `cost.max > count_sub` for any budget | `` node `{n}`: an item may cost up to {max} and budget `{b}` admits {cs} per sub-window. An item that cannot fit a window can never be admitted — it parks the head of its partition for ever and never reaches a DLQ, because a lease that expires charges no retry. Raise the budget, lower cost.max, or lower subWindows. `` |
| `cost-max` | `cost.max < cost.default` | `` node `{n}`: cost.max {m} is below cost.default {d}, so the default cost is itself inadmissible. `` |
| `cost-integer` | a constant `cost` that is not an integer >= 1 | `` node `{n}`: cost must be a whole number of at least 1. The budget counter is an integer on this wire, so a fractional cost is not expressible — express the unit differently (count tenths, and multiply the budget by ten). `` |
| `cost-path` | a `path` that is not a dotted payload path | `` node `{n}`: cost.path `{p}` is not a payload path. Write it as `payload.field` or `payload.a.b`. `` |
| `scope-path` | `scopeBy` is not a dotted payload path | `` budget `{b}` of node `{n}`: scopeBy `{p}` is not a payload path. `` |
| `shared-conflict` | two budgets in this document share a `sharedKey` with different `count`/`timeMs`/`subWindows` | `` `{k}` is declared as {c1} per {t1}ms in node `{n1}` and {c2} per {t2}ms in node `{n2}`. They are one counter, so one of those declarations is a lie about what it enforces. Make them agree or give them different keys. `` |
| `whenop-empty` | `whenOp: []` | `` budget `{b}` of node `{n}`: an empty whenOp matches nothing, so the budget charges nothing. Drop the field to take everything. `` |
| `provenance` | `confidence: documented` with no `source` or no `asOf` | `` budget `{b}` of node `{n}` claims to be documented but names no {source/asOf}. A guess must never look like a measurement. `` |

**Priority and shares**

| rule | when | detail |
|---|---|---|
| `share-range` | `share <= 0` or `> 1` | `` path `{p}`: share must be in (0, 1]. It is a fraction of the node's counter, not a rate. `` |
| `share-top` | the highest-priority path at a node has `share != 1.0` | `` node `{n}`: the highest-priority path through it is `{p}` at share {s}. The top priority must be able to reach the whole ceiling, or the headroom above every other path's share belongs to nobody. `` |
| `share-order` | a lower-priority path has a higher share than a higher-priority one, at the same node | `` node `{n}`: path `{lo}` at priority {a} has share {x}, above path `{hi}` at priority {b} with share {y}. Priority is expressed as the ceiling, so a lower priority with a larger share is the opposite of what was asked for. `` |
| `share-rounds-out` | `round(count_sub * share) < cost.max` | `` node `{n}`, path `{p}`: share {s} of {cs} per sub-window is {m}, below the item cost ceiling {max}. That path could never admit its largest item. `` |

**Ingress and egress**

| rule | when | detail |
|---|---|---|
| `ingress-queue` | a named ingress queue does not exist at the broker | **WARNING** `` node `{n}` names ingress queue `{q}`, which does not exist yet. Gate will consume it from its first message; nothing is created here. `` |
| `ingress-retention` | a named ingress queue has a retention that could delete unadmitted backlog | **WARNING** `` node `{n}`'s ingress `{q}` retains {r}. Work Gate is holding for budget lives on that queue: a retention shorter than the drain time deletes it, and a limiter that quietly loses the work it is pacing is worse than no limiter. `` |
| `ingress-owner` | two nodes in the fleet name the same ingress queue | `` `{q}` is already the ingress of node `{m}` in graph `{h}`. Two consumers of one queue in different groups each get every message, which doubles what leaves. `` |
| `egress-owner` | two graphs name the same egress queue | **WARNING** `` `{q}` is also the egress of graph `{h}`. That is legal — a queue may have many producers — and it is named here because the ETA's worker backlog will count both. `` |
| `single-partition` | an ingress queue has exactly one partition and the stage's concurrency is above 1 | **WARNING** `` `{q}` has one partition, so one worker claims it at a time and this stage cannot go faster than one loop. One partition is one order for the whole node, which is the only way to ask for strict global FIFO — it must not be a surprise. `` |

**Windows, stated as warnings because they are trades and not mistakes**

| rule | detail |
|---|---|
| `window-sub-second` | `` budget `{b}` of node `{n}` declares {t}ms. A kv TTL is whole seconds, so this is enforced as {c} per **second** — tighter than declared, never looser, but slower. Declare a whole number of seconds to get exactly what you asked for. `` |
| `subwindow-rounding` | `` budget `{b}` of node `{n}`: {c} over {N} sub-windows rounds down to {cs} each, i.e. {cs}×{N} = {total} against the declared {c}. Rounding is always down, because enforcing tighter than declared is the safe direction. `` |
| `window-boundary` | `` budget `{b}` of node `{n}` has a {w}s sub-window. This is a fixed window, so a sliding observer can see up to {2cs} across one boundary. Raise subWindows to narrow that. `` |
| `fanout-multiplies` | `` path `{p}` fans out to {list}: one message becomes {k}, and each branch charges its own node's budgets. Whatever this node admits, the vendor sees {k} times. `` |

**What is gone from the v1 set, and why** — every one of these policed an invariant v2 makes
structural or a resource v2 does not allocate: `default-lane`, `lane-unique`,
`lane-concurrency`, `lane-floor` ×2, `lane-shares` ×4, `max-keys`, `store-fits`, `kv-scope`,
`kv-match`, `kv-chunk` ×2, `batch-fits`, `pacing`, `admitted-partitions` ×2, `shard-count` ×3,
`shard-scope`, `edge-fanout`, `relay-lane`, `shard-entry`, `budget-once`, `consume-terminal`,
`edge-unique`, `edge-self`, `cost-monotonic`, `path-length` (the 3-hop wall), `retry-cost`,
`retry-entry`, `breach-when`, `breach-attempts`, and the `lease-beats-window`, `kv-rolling`
and `relay-parallelism` warnings. §13 gives each one a line.

---

## 12. Migration

### 12.1 The endpoint shapes do not move

`PUT /v1/apps/:app/graphs/:name`, `PUT /v1/apps/:app/targets/:name`, `PUT /v1/apps/:app/targets`
(sync-with-reap), the flat forms, `DELETE`, the `GET` views, `/v1/.../push`, `/v1/.../eta` and
every `/api/*` console route keep their paths. A `PUT` of a v1 document is accepted, mapped,
and answered **200 with warnings naming every field that was mapped or ignored** — never a
silent success and never a 422 for having been written last year.

### 12.2 The field map

| v1 field | v2 treatment | warning |
|---|---|---|
| `budgets[].cap` / `periodSeconds` | → `count` / `timeMs` (×1000) | none |
| `budgets[].alignment: rolling` | → `subWindows = clamp(timeMs/1000, 1, count)` | `` `alignment` is gone: kv owns a fixed window, and smoothing is expressed as subWindows. `rolling` mapped to subWindows {N}. `` |
| `budgets[].alignment: calendar` | → `subWindows: 1` | `` `calendar` mapped to subWindows 1 — a single fixed window. It is no longer wall-clock aligned; the window starts at the first admission after the previous one expired. `` |
| `budgets[].scope: [dim]` | → `scopeBy: "payload.{dim}"` | `` scope `{d}` mapped to scopeBy `payload.{d}`. `` |
| `budgets[].maxKeys` | ignored | `` `maxKeys` is a no-op: per-key counters are Postgres rows with a TTL, not entries in a document Gate re-reads. `` |
| `budgets[].store: kv` | → `sharedKey: "{id}"` | `` `store: kv` mapped to sharedKey `{id}` — same counter, same scope (the application), no capacity lease in front of it any more. `` |
| `budgets[].match.op` | → `whenOp` | none |
| `budgets[].confidence/source/asOf` | kept verbatim | see §16.3 |
| `cost` | kept; `default`/`max` rounded **up** to integers | `` cost is an integer in v2 (the counter is an integer on this wire): default {d}→{d'}, max {m}→{m'}. `` |
| `pacing.leaseSeconds` | ignored | `` `pacing` is a no-op: the lease is a work lease now, and pacing is the budget window. `` |
| `pacing.batch` | → the stage's `batch`, clamped to [1, 1000] | none |
| `admitted.partitions` | → `ingress.partitions` | `` the admitted ring is gone; {n} mapped to the ingress queue's partition count. `` |
| `admitted.partitionBy` | ignored | `` partitioning is the producer's choice now: Gate passes each message's own partition through, end to end. `` |
| `shardBy` / `shards` | ignored; any budget scoped on the shard dim already mapped to `scopeBy` | `` `shardBy`/`shards` are a no-op: cardinality is rows, not shards, so there are no shard runners to allocate. `` |
| `lanes[]` | → one **path** per lane, with `priority` by declaration order | `` lane `{l}` mapped to path `{l}`. Lanes divided a ceiling; paths cap a shared one — see the release note. `` |
| `lanes[].cap: ceiling` | → `share: 1.0` | none |
| `lanes[].cap: share:f` | → `share: f` | none |
| `lanes[].cap: absolute:n` | → `share: min(1, n / tightest rate)` | `` `absolute:{n}` mapped to share {s}: a share is a fraction of the node's own ceiling, so an absolute rate is expressed against it. `` |
| `lanes[].cap: ceiling-minus-measured` | → `share: 1 - floor`, rounded to the nearest 0.05 | `` `ceiling-minus-measured` mapped to a static share of {s}. There is no meter to derive it from any more, and there does not need to be: one counter with N ceilings cannot oversubscribe, which is what the derived cap existed to prevent. `` |
| `lanes[].concurrency` | → the stage's `concurrency` | none |
| `edges[]` | → `paths` (each maximal chain is a path) | `` {k} edges mapped to {m} paths. `` |
| `edges[].priority` | → the path's `priority` | none |
| `consume[]` | → `egress` on the named nodes, queue defaulting to `{app}.{graph}.{node}.out` | `` node `{n}` is now a terminal with egress `{q}` — your consumers pop that queue directly with the SDK instead of `GET .../next`. `` |
| `breach[].maxAttempts` | → the document's `maxAttempts` | `` {k} breach rule(s) mapped to `maxAttempts: {n}`, and the TRIGGER did not come with them: v1 watched the ack, and re-entry is something you ask for now — `POST /v1/apps/{app}/graphs/{g}/reenter`. See §16.6. `` |
| `breach[].when` / `retryTo` | ignored | named in the same warning: there is no ack to watch, and re-entry is always at the origin entry |
| `egress` (the v1 free-text field) | kept as metadata | none |

### 12.3 What still needs a version bump

Far less than v1, because almost nothing re-founds a counter any more. `needs_version_bump`
is true when, and only when:

* a **budget key changes** — its `id`, `scopeBy`, `sharedKey`, or the node it lives on. The
  old key keeps counting until its TTL runs out and the new one starts at zero, which is a
  window of double-spend the caller must mean.
* a **node is removed** while a path still names it, or a path is removed — work already in
  its interior queue has no consumer in the new plan.
* an **ingress queue name changes** — the old queue keeps its backlog and nothing drains it.

Everything else — a `count`, a `timeMs`, a `share`, a `priority`, a `cost`, a `concurrency`, a
`batch` — is a **hot change**. `count` and `share` change the `max` the next `incr` carries and
take effect on the next batch; `timeMs` changes the TTL the next *rotation* writes, so it takes
up to one old window to land, and the declare response says so.

The rule is enforced for a **caller's** declare only, never for one applied from the store —
enforcing it against a replica-local runtime is how a replica wedges on a legal
delete-and-redeclare at the same version. A caller's declare compares both the local runtime and
the exact stored document, so reaching a replica before its reconcile pass cannot make an existing
graph look new and bypass the bump. That asymmetry is v1's and it is kept verbatim.

### 12.4 Drain and redeclare

For a migration-class change the documented procedure is unchanged in shape:

1. stop pushing to the ingress queue (or let the HTTP front door 503 with `?draining`);
2. wait for `GET .../eta` to report `waitingForBudget: 0` on every node;
3. `PUT` the new document with a higher `version`;
4. resume.

Terminal queue names are stable across the migration by construction: in v2 they are declared,
not derived, so a v1 graph whose `consume` node was `gate.{app}.{graph}.{node}.admitted.default`
maps to an explicit `egress` naming that same string, and the application's consumers do not
move.

---

## 13. Feature disposition — every inventory item

**Legend:** **R** reimplemented · **O** obsolete (the architecture removes the need) ·
**D** descoped (pre-approved) · **?** in §16 Open questions.

### 13.1 Queue layout

| v1 feature | | disposition |
|---|---|---|
| Per-target queue family and namespace | R | §4.1. Three queue kinds become two (`.in` / declared egress); the `.calls` queue dies with the meter. Namespace `gate.{app}` unchanged, names still derived in one function. |
| Push queue partition = the single-writer counter | O | The counter is a KV row, not a partition. Partitions carry **order**, not counting, so nothing needs a single writer and nothing needs pinning. |
| Admitted ring (`partitionBy`, `count()`, `partition_name`, `partition_of`) | O | There is no admitted queue. Partitioning is the producer's, passed through unchanged (§6.4), so Gate neither chooses nor hashes a partition anywhere. |
| Queue provisioning (`lease = pacing quantum`, `retry_limit: 0`) | R | §4.3: `lease_time: 30` (a work lease) with renewal, and `retry_limit: 3` — a **real DLQ comes back**, because lease expiry charges no retry and Gate no longer has to disarm the DLQ to pace. |

### 13.2 The admission gate

| v1 feature | | disposition |
|---|---|---|
| One gate runner per lane per shard, pinned | O | The wildcard pop + `SKIP LOCKED` is the scheduler (§4.3). No pinning, no runner-per-partition, no `max_partitions = 1` correctness argument. |
| Gate consumer group named explicitly | R | §4.1: `gate.{app}.{graph}.{path}.{node}`, minted in one function, for the same measured reason (a group with no cursor owes its whole retained range). |
| `subscription_mode = All` | R, **narrowed 2026-09-02** | §4.3. Kept, and for the unchanged reason, on every group whose source is an **ingress** queue — a producer writes it, so a group at the tail drops the backlog the limiter exists to pace. **Not** on a group whose source is a Gate-owned **interior** queue: those are seeded at the tail of the log as of the runtime's start (`subscription_from`), because only Gate's own relay writes them and only for a path that stamped its own name on the frame. `All` there put a newly declared path's group at the head of three other paths' twelve-day backlog, and acking a frame older than the broker's `log_txns` window is not possible — the stage rolled back for ever. `Stage::source_is_interior` carries the distinction so the consumer reads a bool. |
| `reset: true` on registration | O | No streams registration, no config hash, no state to clobber. |
| `STREAM_MAX_WAIT` (the idle poll window) | R | Becomes `poll_timeout(30s)` on a parked long-poll. It can be 30s rather than 5s because the parked poll holds no connection and is push-woken; the ceiling is still shutdown latency, and the supervisor still awaits handles for `poll_timeout + 2s`. |
| Gate closure: scope extraction and cost | R | §6.1. Five fixed dimensions become an arbitrary `scopeBy` payload path; cost is a declared path with a default and a max, integer-valued (§3.2). |
| Lane cap divided by shard count | O | No shards, and one counter rather than one per runner, so there is nothing to divide. |
| Lane share (lanes divide a ceiling) | O | §3.6. The invariant is structural: one counter, N ceilings. The measured defects it existed to prevent (93/s against 50, 7131 against 5000) are not expressible. |
| Effective cap resolution at provisioning | O | `max` is computed per call from `count_sub × share`; there is no runtime cap to resolve, store or retune. |
| Two-pass admission decision | R, in a different place | §6.2–6.3. "Evaluate everything, apply only if all admit" becomes "charge everything in one batch, refund what applied if any refused". The property is identical — **a denial charges nothing** — and the refund is what enforces it now that the counters are not in one document. |
| Rolling window is two-bucket, not a token bucket | ? | §5.2/§16.4. KV owns a fixed window. Subdivision bounds the boundary exposure; it does not reproduce a sliding window. This is the largest behavioural change in the document. |
| `@lane` synthetic budget | O | Dies with lane caps. A per-path rate is a `share` on a real budget. |
| Once-per-cycle expiry sweep of the state document | O | There is no state document. Postgres reaps the rows. |
| Deferral: a denial stops the batch, acks the prefix, keeps the lease | R | §6.3–6.5, verbatim in behaviour: prefix admit, tail unacked, lease held or released, **never nacked**. This is the single most important v1 property to preserve and it is preserved. |
| Gate sink `to_partitioned` | O | Partition passthrough (§6.4). Gate does not choose a partition. |
| Lane stats counters and `last_state` mirror | R (counters) / O (mirror) | §10.2 keeps the counters per stage. The mirror is obsolete: budget state is one `kv.get` away, is fleet-wide rather than per-replica, and is read on demand instead of copied on every admit. |
| Cross-target kv budgets: the capacity-lease `Pool` | O | The whole point of the rewrite. `try_spend`/`top_up`/`release`, the chunk arithmetic, the `.max(1)` deadlock guard and the `kv-chunk` rules exist because v1's gate closure was **synchronous and could not await**. v2's relay is async and consults the counter directly. |
| kv reserve/refund wire, `% 4` window keying | O | Create-only TTL rotates the key; §5.1 has no window index. |
| Pool spend precedes the engine, per message | O | Same. |
| Meter loop (1s cadence, top-up, retune, calls drain) | O | Nothing to top up, nothing to retune. The 1s cadence was load-bearing **for the top-up**; with no lease there is no cadence. |
| `measured_share` for `ceiling-minus-measured` | O | Dies with lane division (§3.6), which is also why its multi-replica correctness argument (it *must* come from PG or every replica hands the derived lane the whole residual) evaporates. |

### 13.3 The relay

| v1 feature | | disposition |
|---|---|---|
| One relay per destination node, legs in priority order | O | §3.6: priority is a ceiling on a shared counter, not an ordering at a merge. One consumer per (path, node); no merge relay, no leg iteration, no barrier. |
| One relay runner per source admitted partition per lane | O | Wildcard pop. |
| Relay consumer group named for the EDGE | R | Becomes the stage group `gate.{app}.{graph}.{path}.{node}` — same property (one group per stream, cursors already per-partition inside it), renamed for the path model. |
| Relay window sizing (`window_for`) | O | The window existed to keep the destination's push queue shallow so that arrival order at the merge would honour priority. With no merge and no arrival-order priority there is nothing to keep shallow: the destination's own budget is the pacing, and backlog on an interior queue is exactly where held work belongs. |
| Destination depth probe: unknown is not zero | O | No depth is read in the hot path at all. |
| Allowance: one shared window pool per leg per cycle | O | Same. |
| Hot-partition selection via the group depth probe | O | The broker's randomised candidate scan does this, in the pop, for free. |
| `FULL_SWEEP_EVERY` | O | A backstop against a wrong depth answer; there is no depth answer. |
| `MAX_IN_FLIGHT` | R, as `concurrency` | A per-stage worker count, defaulted from the source's partition count and declarable. The measured reason it was capped at 16 (128 pinned pops per cycle overran the broker's pop admission) does not apply to parked long-polls, but the default is still bounded. |
| Rotation | O | Wildcard pop. |
| Strict priority across legs, parallelism inside one | ? → O | §3.6. **Strict** priority is deliberately traded for an atomic reserve. Recorded here, not discovered later; §16.7 asks for the author's explicit assent since FOUR live tests assert the strict behaviour (`priority_and_the_window_survive_the_relay_being_many_runners`, `a_leg_that_is_not_dry_holds_its_window_but_not_for_ever`, `a_wide_window_does_not_leak_priority_to_the_next_leg`, `priority_at_the_entrance_is_priority_in_fact`). |
| Dry vs blocked, and the bounded hold on errored pops | O | The rule existed because a leg yielding its window to a lower priority on a *wrong* answer gave 188 of the first 300 items to priority 1. There is no window to yield. |
| `drain()`: the pinned pop | O | §4.3. |
| `forward()`: one transaction per destination partition | R | §6.4, sharpened: one transaction per **claim**, and a claim is one source partition (`partitions(1)`), pushing to the same-named destination partition. The measured lesson (33 txn/s shared vs 603 txn/s disjoint) is the reason for `partitions(1)` and is quoted at that line. |
| Transaction id reuse | R + extended | §7. Reuse for a single-destination non-converging hop; deterministic derivation at a fan-out **and** at a convergence. |
| QDUP recovery: settle one at a time | R | §6.6, verbatim. |
| Unroutable items dead-lettered, never dropped | R | §6.6 step 4. The v1 trigger (a sharded destination missing its dimension) is gone; the v2 trigger is an unresolvable cost or a payload that is not an object. Unlike v1's, this path is **reachable** and gets a test. |
| Staging failure abandons the whole group | R | Unchanged: drop the half-built transaction, settle nothing, let the leases lapse. |
| Relay pacing and idle backoff | O | An idle stage is a parked long-poll, not a loop with a backoff. |
| Relay counters | R | §10.2, extended with `parked` / `released` / `deferred` / `foreign`. |
| Relay shutdown | R | A cancel token per stage, awaited (not a flat 600ms sleep) — `consume_batch` returns on cancel between claims, and a parked poll is cancelled through the same token. |

### 13.4 Depth probing

| v1 feature | | disposition |
|---|---|---|
| `Depths` cache and the stale-serve read | R, shrunk | §9. Kept for the console (2s TTL, stale-on-failure), removed from every bounding caller because there are none. |
| Depth route with the queue-detail 404 fallback | R | Kept verbatim: a 404 is both "old broker" and "no such queue" and cannot be told apart. |
| Group-scoped depth: no fallback, ever | R | Kept verbatim, including the measured reason (a 1.0.3 broker reported ZERO for a queue another group had drained while this group still owed thirty). |
| Cached group-scoped depth with the stamped 404 | R | Kept verbatim — both reasons for that 404 persist, so an unstamped one is a probe per caller for ever. |

### 13.5 The caller data plane

| v1 feature | | disposition |
|---|---|---|
| `push_into` (the one push) | R | §3.3. The HTTP front door survives as **optional**: it stamps `_gate`, resolves the cost, refuses `cost > max` (422), refuses a missing `scopeBy` value (422 — a counter keyed on an absent value measures the wrong thing), passes `txn` through as the coalescing lever, and may pre-check the budget to answer **429 + Retry-After** rather than accepting work it knows will wait. Partition comes from the body's `partition` (or `key`), not from a hash Gate owns. |
| `next_from` (the caller's pop) | O | The application consumes the **egress queue** with its own SDK. This removes the opaque-lease protocol, the `gate.exec.{lane}` group, and the "silence is the pacing signal" contract — the caller now sees an ordinary queue that is sometimes empty. |
| Graph ownership rules on push and next (409s) | R, halved | Pushing into an interior `.in` queue is still refused through Gate's routes (it would skip every upstream budget). There is no `next` to refuse. The one-owner check moves to `ingress-owner` at declare time (§11). |
| `ack`: one transaction for the cursor, the meter event and the re-entries | O | There is no Gate-mediated ack. The cursor is advanced by the app's own consumer against the egress queue; the meter event has no consumer; the re-entries are §16.6. |
| `ack` QDUP recovery `settle_item_by_item` | O | Same. (The *relay's* QDUP recovery survives — §6.6 — which is the one that mattered.) |
| Breach re-entry planning (`plan_retro`) and its txn ids | ? | §16.6. This is the largest single feature with no home in the settled architecture and it is **not** silently dropped. |
| `nack` and `renew` | O | Lease surface belongs to the app's own SDK now. Note that v1's `nack` documented a budget refund it never performed (an acknowledged divergence); v2 has a real refund primitive (§6.3, §8) and exposes it through the breaker instead. |

### 13.6 ETA

| v1 feature | | disposition |
|---|---|---|
| ETA: two backlogs, two clocks | R | §9. Both halves survive: the budget backlog answered from the **declared** schedule (a spent window measures zero per second and would answer "never" at exactly the moment somebody asks), the worker backlog from the egress queue's depth. |
| `eta::admits` arithmetic | R | §9, simplified: one alignment instead of two, and `remaining`/`expiresAt` read from KV instead of reconstructed from a mirrored state document. The five pure unit tests port with their fixtures. |
| The `assumes` string | R | §9, rewritten caveat by caveat. |
| ETA HTTP routes | R | Paths unchanged; `?lane=` becomes `?path=`, with `lane` accepted as a deprecated alias. |
| `GET /v1/apps/:app/metrics` | R | Same shape. `state`, `binding_budget`, `waiting_for_budget`, `waiting_for_workers`, `drain_eta_seconds` all have sources; `admitted_per_sec` comes from the counters stream when it is on and is `null` when it is off — **null, not a lifetime average**, which fixes the v1 divergence where this field and `/api/overview`'s field of the same name meant different things. |

### 13.7 Lifecycle

| v1 feature | | disposition |
|---|---|---|
| Target lifecycle: start, swap, restore, stop | R, simplified | One `Runtime` per graph holding a `Vec<StageHandle>`. Swap is still stop-then-start under the declare lock, still awaits (a task does not stop when its handle drops), still restores the previous plan on failure, still **unregisters** rather than leaving a registered-but-stopped graph — that state is still the one unrecoverable one, for the unchanged reason. |
| Declare lock and the reconcile loop | R | Verbatim, including both asymmetries: a read that **fails** ends the pass; a runtime whose spec was never persisted is **re-saved** rather than removed. Diff by value, `PartialEq` on the document, re-declare when any stage is not running. |
| Graph declare atomicity and rollback | R | Simpler: there are no per-node targets to swap one at a time. Provision queues → start stages → register → persist, with a full teardown on any failure. |
| Live-suite fault injection harness (`FaultyBroker`) | R | **Kept and extended.** It is the only way three data-plane rules are testable at all, and v2 adds two more it must cover: a KV route that refuses (does the relay refund and release, or does it lose the batch?) and a transaction that fails after a successful charge (does the refund fire?). |

### 13.8 HTTP surface, auth, config

Everything in this section is **R, unchanged**, because none of it is the data plane:
internal listener + shared router (`GATE_BIND`, :8788), public/console listener
(`GATE_PUBLIC_BIND`, :8790), boot-time refusals and startup order, the env knob table,
`GET /health`, `AuthConfig::from_env`, Google login/callback/logout, `require_session`
(method-keyed, not path-keyed), `GATE_ADMIN_EMAILS` read per request, the `GATE_DEV_EMAIL`
bypass and its two fences, the JWKS cache with stale-on-failure, `GET /api/me`, the embedded
console router with its no-cache + ETag rule and the `ui/dist` build dependency.

Two edits: the vestigial `Authorization: Bearer` the console still sends from `localStorage`
is deleted from `ui/src/lib/api.js` (the server has never read it), and `/api/overview`'s
three hardcoded fields are fixed (§13.12).

### 13.9 Control-plane routes

| v1 feature | | disposition |
|---|---|---|
| `PUT /v1/apps/:app/targets/:name` | R | Declares a one-node graph. Response keeps `resolved` (now: ingress queue, egress queue, stage groups, budget keys) and `warnings`. Still 502 on a store write failure — a 200 with a 15-second fuse is a lie. |
| `PUT /v1/targets/:name` (flat) | R | Kept, **and the parity trap is fixed**: the flat form now pins `application` from the resolved default rather than letting a body declare into another team's namespace. |
| `PUT /v1/apps/:app/targets` (sync, reap) | R | Kept verbatim, including reap-after-declare and application scoping. Graph-owned nodes are exempt. |
| `GET` target view (4 routes) | R | Fields re-sourced: budgets carry `key`, `count`, `timeMs`, `subWindows`, `value`, `expiresAt`, `utilisation` read live from KV. `utilisation` is still the **worst** counter, now across shared/scoped keys instead of across shards. |
| `DELETE` target | R | Store-first, verbatim, including `registered: false` being a success. |
| 409 version-bump | R | §12.3, with a much shorter trigger list. |
| 409 one-owner-per-queue-family | R, moved | Becomes `ingress-owner`/`node-name` collision at declare time, checked against the registry **and** the store. |
| 502 provisioning failure and the restore contract | R | Verbatim. |
| `PUT`/`DELETE`/`GET` graph, `topology`, `declare_from_store` | R | Verbatim in structure. `topology` stays broker-free so a drawing can poll it. |
| `resolve_graph` (flat-name resolution) | R | Verbatim, including the 409 on an ambiguous name — the one case the server refuses to guess. |
| `POST` push (target and graph entry) | R | §13.5. Refused for a non-ingress node with a message naming the ingress nodes. |
| `GET next` (target and graph) | O | §13.5. The route answers **410 Gone** with a message naming the egress queue and a two-line SDK snippet, for one release, then goes. |
| `POST /v1/leases/ack` / `nack` / `renew` | O | Same, same 410. |

### 13.10 Console read API

| v1 route | | disposition |
|---|---|---|
| `/api/overview` | R | Fixed: `queen.reachable` is **probed** (a `/health` with a 1s timeout, cached 5s) instead of hardcoded `true`; `queen.version` is read from the probe; `budgets_stale` is computed from `asOf` against a 90-day horizon; `admitted_per_sec` is `null` without the counters stream. |
| `/api/targets` | R | `worst_assumed` computed instead of hardcoded `false`. Backlog is one depth read per node, unchanged. |
| `/api/apps` | R | Unchanged. |
| `/api/flow` | R | Unchanged in shape, sourced from the counters stream; `durable` now means "the counters stream is on **and** history is configured". |
| `/api/rollups` | R | Same. |
| `/api/traces` | R, poorer | §10.4 and §16.5. |
| `/api/breaches/recent` | R, **better** | §8 step 5: sourced from the `brk:` KV records, which are fleet-wide. v1's was a per-replica ring, and the module doc's own complaint about that is answered. |
| `/api/budgets` (shared budgets) | R, **generalised** | One row per `(application, sharedKey)`, reading `value`/`expiresAt` live, listing `members` and `conflicts`. `conflicts` is now also a declare-time refusal within one document (`shared-conflict`) and stays a report across documents, because two graphs are already spending one counter and the console cannot tell which declaration is the lie. `local_lease` disappears — there is no lease. |
| `/api/graphs`, `/api/graphs/:name` | R | Node/stage shape, live budget values, stage counters, per-stage `lag` (one group-scoped depth read per stage). |

### 13.11 Declaration types

`TargetSpec` **R** (sugar over a one-node graph) · `Budget` **R** (§3.1) · `Dim` **O** (five
fixed dimensions become an arbitrary payload path) · `Match` **R** (as `whenOp`) · `Lane`
**O** → `Path` · `CapPolicy` **O** → `share` · `Cost` **R** (integers) · `Pacing` **O** ·
`Admitted`/`PartitionBy` **O** · `shardBy`/`shards` **O** · `GraphSpec` **R** (the only
document) · `Node` **R** · `Edge` **O** → `paths` · `BreachRule`/`BreachWhen` **?** (§16.6) ·
`consume` **O** → `egress`.

### 13.12 State

| v1 state | | disposition |
|---|---|---|
| PG schema `gate` (`rollups`, `traces`) | R | Same DDL, same always-virgin boot, same upsert-sum, same hourly `prune(90, 7)`, same "no second data system" rule. Fed by the counters stream instead of the meter. |
| History connection, pool, optionality | R | Verbatim, including `None` when `PG_HOST` is unset, the pool of 4, and direct-to-Postgres (never PgBouncer: tokio-postgres prepares every statement). |
| Rollup write: every replica upserts its increments | R | Unchanged; still what makes `replicas: 2` safe. |
| Trace write path | R | §10.4. |
| Retention (`prune`) | R | Unchanged. |
| `History::rollups` / `flow` | R | Unchanged. |
| `History::rate_per_sec` | R | Feeds `/v1/.../metrics`; `null` without the counters stream. |
| `History::avg_cost` | R | Feeds the ETA's weight; falls back to `cost.default` and says so in `assumes`. |
| `History::measured_share` | O | §13.2. |
| Trace and breach reads | R | Breaches re-sourced (§13.10). |
| Spec store in queen.kv (`Expiry::forever`, `Stored{complete}`) | R | **Verbatim**, including `forever` rather than a TTL and the add-and-change-but-never-remove rule for an incomplete read. |
| Boot restore | R | Verbatim. |
| Shared-budget counters in queen.kv | R, generalised | §5.1: every budget is one, not a special second kind. |
| Gate state document (streams engine) | O | Gone entirely. `queen_streams.state`, `log_streams_cycle`, `streams_state_get` — Gate issues none of them. |
| Cycle sweep, `@lane`, cell expiry | O | Gone with the document. |
| Consumer-group naming (one function each) | R | §4.1. |
| `last_state` mirror | O | §13.2. |
| Meter rings, `observe_gate`, `take_closed` | O | Gone with the meter. The differencing bug they defended against (a lifetime total read as a minute, saturating `measured_share` within seconds) cannot recur because the counters stream emits per-window aggregates natively. |
| Counter-refounding identity | R | §12.3. |

### 13.13 Observability pipeline

Call-event queue and meter loop **O** · `LaneStats` and `last_breach` **R** (per stage, §10.2;
`last_breach` from the `brk:` records) · relay counters **R** · depth cache **R** (console
only) · depth probe + 404 memory **R** · the ETA answer, `assumes` and routes **R** ·
`/v1/.../metrics` **R** · console read API **R** · console UI **R** (§14.7) · console identity
**R** · logging **R** (same `tracing` setup, same structured fields, nothing per message;
new WARN sites: a refused refund, a QDUP split, a stage parked past `MAX_PARKS`; one new ERROR
site, the wedged-cursor escalation of §6.4, which fires once per wedge and carries the `seek` that
clears it).

### 13.14 Attestation

| v1 feature | | disposition |
|---|---|---|
| Certificates / attestation | **D** — **descoped, pre-approved** | There was never a mechanism: the word is a metaphor in the README for the freshness of an admission decision as it crosses an edge. What must ship with the descoping is the **trust-model sentence**, in the README and the declare response's warnings: *write access to an interior or egress queue is admission bypass — the same trust model as any queen queue. Gate paces what flows through it; it does not defend a queue from a writer who already has the credentials.* v1 relied on the queues being undiscoverable (names derived, never told); v2 names the egress queue in the declaration, so the change must be said out loud. |
| `_gate` envelope | R | §6.8. Still one reserved object, still unsigned and unverified, still trusted because Gate writes it — with the honest note that a producer pushing to a user-owned ingress queue **can** write `_gate` and Gate overwrites it on the first hop. |

### 13.15 Acknowledged v1 divergences

| divergence | | disposition |
|---|---|---|
| The `assumed` cap discount (`ASSUMED_FACTOR = 0.7`) exists, is tested, and is never applied | ? | §16.3. Wire it or delete it; do not ship a third release that documents it and does not do it. |
| `nack` documents a budget refund it does not perform | O | The route is gone (§13.5); the refund it described is now real and lives in the breaker (§8). |
| `/api/overview` hardcodes `reachable`, `version`, `budgets_stale`; `/api/targets` hardcodes `worst_assumed` | R | Fixed (§13.10). |

### 13.16 Tests, CI, build, tooling

| v1 item | | disposition |
|---|---|---|
| `core/tests/engine.rs` (23 tests, 535 lines) | **rewritten** | The engine it tests does not exist. The properties that survive get new homes: *a denial charges nothing* → §6.3's refund test; *cost is weighted not counted* → the charge-grouping test; *an item costing more than a cap is unsatisfiable* → `cost-fits`; *match selects on op / a glob matches a whole segment* → `whenOp`; *the spec round-trips / an omitted application lands in default / two teams may both own something called airbnb* → the new document tests. The window tests (calendar/rolling/saturation/token-bucket property) **go**, with the reasoning recorded in the deleting commit: they assert an arithmetic Gate no longer owns. §16.4 is where the author signs that off. |
| `core/tests/graph.rs` (31 tests, 609 lines) | **mostly rewritten** | The `airbnb()` fixture is ported to the v2 document (§3.7) and must still *validate clean and warn about nothing* — that is the single most valuable test in the file and it stays, in the new vocabulary. The rule-name assertions follow §11's table. Tests for rules that are gone are deleted with a one-line reason each in the commit body. |
| `core/tests/validate.rs` (33 tests, 567 lines) | **mostly rewritten** | Same. The flagship property test (`every_spec_that_validates_clean_divides_exactly_one_ceiling`, exhaustive over 6×5×5×22) is **deleted**, and its epitaph goes in the commit: *the property it enforces is now structural — there is one counter, so N ceilings cannot oversubscribe it. The 7131-against-5000 defect is not expressible.* Its replacement is `every_share_is_a_ceiling_on_one_counter`, which asserts the compiled `max` per path is `round(count_sub × share)` and monotone in priority. |
| `server/tests/units.rs` (9 tests) | **partly ported** | `window_for` and the edge-group test go (no window, new group name). The five `eta::admits` tests port with their fixtures and their fixed instant. `the_gates_group_is_the_one_the_stream_runtime_derives` is replaced by a stage-group naming test. |
| `server/tests/live.rs` (32 tests, 3076 lines) | **kept, ported, extended** | This suite is the most valuable artefact in the repository and the port is the bulk of the work. Ported unchanged in intent: exactly-once relay, per-partition ordering, replay refusal, QDUP poison recovery, interior-queue ownership, one-owner, declare/provision/store failure recovery, both replica-convergence tests, the reconcile loop, the depth stale-serve and 404 memo, the three ETA tests, the console draw. Rewritten: the priority tests, because §3.6 changes what priority *means* — the new assertion is that under saturation the high-share path continues to admit while the low-share path refuses, which is the property that was bought. Deleted with reasons: the strict-ordering leg tests, `a_leg_that_is_not_dry_holds_its_window_but_not_for_ever`, `a_wide_window_does_not_leak_priority_to_the_next_leg`. New: budget refund on a failed transaction, park-vs-release at the threshold, jitter under contention, fan-out branch identity, fan-in non-collapse, the breaker, and a foreign-path skip on a shared interior queue. Added 2026-09-02: `a_path_added_to_a_running_graph_starts_at_the_tail`, the regression test for §4.3's seeding rule — declare one path, leave a backlog on the terminal node's interior queue, add a second path through it, and assert the new group's `foreign` and `lag` are both zero while both paths still run end to end. |
| `FaultyBroker` harness | R, extended | §13.7. |
| CI (`test.yml`) | R, **three fixes** | The trigger moves from `main` to `master` (the workflow may never have run on a push); the broker service moves to ≥ 1.0.4 so the depth route's happy path is exercised and not only its 404 fallback; clippy gets `-D warnings`. `PG_HOST` is set for Gate so the history layer stops being entirely untested. |
| Dockerfile, `docker-build.yml`, `build.rs`, `ui/dist` compile dependency | R | Unchanged, plus the one dead layer-cache line fixed (`crates/bench/Cargo.toml` is missing from the manifest pre-copy, and the pre-copy buys nothing anyway without a `cargo fetch` between it and `COPY crates`). |
| `gate-e2e` | R | Its question is unchanged — *did the ceiling hold* — and it is the acceptance gate in §15. Its target declaration is ported; the two lanes become two paths at different shares, and the assertion gains a third gate: the low-share path must be refused while the high-share path is still admitting. |
| `gate-bench` | R, extended | The API-latency vs admission-latency split is kept verbatim (quoting the second as the first is how a rate limiter gets reported as slow). New scenarios: `throughput` (one stage, one budget, batch sweep — the 10k target), `contention` (N stages on one shared key), `idle` (the §15 broker-call criterion). |
| Toolchain, MSRV, pins | R | `rust-version = "1.86"` kept **and finally checked** — a CI job on 1.86, because nothing today would notice a 1.87 feature. `queen-mq` moves to the version carrying the depth route and the batch KV surface. New dependency: `sha1 = "0.10"` (§7). |

---

## 14. Implementation plan, file by file

### 14.1 `crates/core` — the pure half

**Delete**

* `src/engine.rs` (443 lines) — the entire admission engine. Two-pass evaluation, the
  two-bucket rolling window, the calendar window, cell expiry, the `@lane` synthetic budget,
  `utilisation`/`utilisation_max`/`key_count`. Nothing of it survives; Postgres does the
  counting.
* `tests/engine.rs`, and the window half of `tests/validate.rs`.

**Rewrite**

* `src/spec.rs` → `src/doc.rs`. One document type (§3). Delete `Lane`, `CapPolicy`, `Pacing`,
  `Admitted`, `PartitionBy`, `Dim`, `shard_of`, `shard_index`, `push_partition`,
  `lane_partitions`, `admitted_queue`, `query_id`, `calls_queue`. Keep `ok_name`,
  `ok_target_name`, `namespace`, the `deny_unknown_fields` discipline and the
  identity-is-the-pair rule.
* `src/validate.rs` → §11's rule set. Roughly half the rules, all with new messages.
* `src/graph.rs` → folded into `doc.rs` + `validate.rs`; the graph/target split is gone.

**Write**

* `src/plan.rs` — **the compiler.** `compile(&GraphDoc) -> Plan`. Produces `Vec<Stage>`,
  `Vec<QueueSpec>`, `Vec<BudgetKey>`, the `converging` map (§7), the default shares (§3.6) and
  the subdivision arithmetic (§5.3). Pure, exhaustively tested, and the only place a queue
  name, a group name or a KV key is minted. This file is the new `spec.rs` and it is where the
  next reader should start.
* `src/ids.rs` — RFC 4122 §4.3 UUIDv5 over `sha1`, plus `derive(parent, label)` and the fixed
  namespace constant. Written out, with the stability argument in the module doc.
* `src/cost.rs` — payload path resolution (`payload.a.b`), integer coercion, the `whenOp` glob
  matcher (ported verbatim from `engine::budgets_for`, which is the one piece of the engine
  worth keeping).
* `src/migrate.rs` — §12.2's field map, v1 document → v2 document + `Vec<Warning>`. It needs
  the v1 `TargetSpec`/`GraphSpec` types, so they are **kept**, moved to `src/v1.rs`, and
  marked `#[deprecated]`.

`gate-core` keeps its no-I/O rule; `sha1` is the only addition and it does no I/O either.

### 14.2 `crates/server` — the runtime

**Delete outright**

| file | lines | why |
|---|---|---|
| `src/gate.rs` | 237 | The streams gate. Replaced by `relay.rs`. |
| `src/edge.rs` | 1132 | The relay. Replaced by `relay.rs`. Everything it knows about windows, allowances, hot partitions, sweeps, rotation, stalls and legs is obsolete; its QDUP recovery and its one-transaction-per-partition rule move to `relay.rs` with their comments. |
| `src/shared.rs` | 183 | The capacity-lease pool. |
| `src/meter.rs` | 584 | The meter loop, the rings, the differencing, the minute watermark. |

**Write**

* `src/relay.rs` (~450 lines) — one stage's consumer and the admission algorithm (§6). This is
  the file the whole rewrite is about. Structure: `spawn(stage, queen, counters, cancel)` →
  the `consume_batch` loop → `handle(batch)` → `group` → `charge` → `prefix` → `commit` →
  `park_or_release`. It gets the longest module doc in the crate, carrying §1's numbers.
* `src/budget.rs` (~200 lines) — the KV wrapper. `charge(&[Charge]) -> Verdict`,
  `refund(&[Charge])`, `read(&[Key]) -> Vec<State>`, `spend_window(node, ttl)` for the
  breaker. Every `kv.batch` in the crate goes through here, so there is one place that knows
  the namespace, the `min: 0` guard and the refused-refund WARN.
* `src/breaker.rs` (~120 lines) — §8, plus the `brk:` record read for the console.

**Rewrite**

* `src/supervisor.rs` — provisions queues from the plan, spawns one task per stage, stops by
  cancel-then-await. Loses the lane/shard loops, the pool construction and the meter spawn;
  keeps the stop-before-refund ordering discipline (now: stop before deleting budget keys on a
  breaker reset), the await-with-timeout instead of a guessed sleep, and the
  stop-what-already-started failure path.
* `src/registry.rs` — `GraphRuntime { plan, stages: Vec<StageHandle>, persisted, stopped }`.
  Loses `LaneRuntime`, `effective_cap`, `measured_share`, `last_state`, the pools.
* `src/eta.rs` — §9. Loses `binding_budget`'s state-document reconstruction and gains a
  `budget::read`; keeps `admits` (one alignment), `drain_groups` (now: the egress group) and
  the whole `assumes` apparatus.
* `src/api.rs` — the 2469-line file splits. `api/mod.rs` (router + `App`), `api/declare.rs`,
  `api/data.rs` (push + the 410s), `api/console.rs`, `api/eta.rs`, `api/breaker.rs`. The route
  table stays recognisable; the handlers lose `next_from`, `ack`, `plan_retro`,
  `settle_item_by_item`, `nack`, `renew` — roughly 900 lines.
* `src/graph.rs` — declare/rollback/view for the one document type; loses per-node swapping.
* `src/lib.rs` — unchanged in structure. Boot order, both listeners, the reconcile loop, the
  history prune. Loses nothing.

**Unchanged**

`src/auth.rs`, `src/webapp.rs`, `src/store.rs`, `src/history.rs` (minus `measured_share`),
`src/depth.rs` (console-only callers), `build.rs`, `main.rs`.

**Net:** roughly **2100 lines deleted**, **800 written**, `api.rs` split four ways.

### 14.3 Order of work

1. `gate-core`: `doc.rs`, `plan.rs`, `ids.rs`, `cost.rs` + their unit tests. Nothing compiles
   against the server yet; the compiler is testable on its own and the `airbnb` fixture is the
   first thing that must go green.
2. `validate.rs` + §11's messages, with the fixture-mutation test style ported from v1.
3. `budget.rs` + `relay.rs` against a real broker, driven by a hand-written integration test
   before any HTTP exists — one stage, one budget, one queue. This is where the throughput
   number is found, and it is found before the API is built on top of it.
4. `supervisor.rs`, `registry.rs`, `graph.rs`: declare → provision → run.
5. `api.rs` split, `eta.rs`, `breaker.rs`.
6. `migrate.rs` + the v1 acceptance path.
7. Live suite port (§13.16), then the console (§14.7), then CI and docs.

Commit locally on `gate-v2-kv` at each numbered step. Do not push.

### 14.4 Concurrency and shutdown

One `CancellationToken` per graph, cloned per stage. `consume_batch` takes it through
`.cancel()`, so a parked poll returns on cancel rather than at its timeout. Every in-handler
park is `tokio::select!`ed against it. `stop()` cancels all stages of all graphs first and
**then** awaits, so N stops cost the longest single poll rather than the sum — v1's rule,
worth keeping.

### 14.5 Error handling

* A KV call that **fails** (transport, 5xx) is not a refusal: log WARN, return without acking,
  let the lease redeliver. Reading a failed charge as a refusal would park the graph; reading
  it as an admission would breach the ceiling. Neither is available, so the batch simply does
  not happen.
* A transaction that fails: refund (§6.4), return without acking.
* A `QDUP`: §6.6.
* A panic in a handler: `consume_batch` absorbs it, the lease lapses, the batch comes back. A
  stage must never exit on an unexpected error — a relay that stops is a stopped graph, which
  is the failure v1's edge.rs refuses everywhere and v2 refuses in the same places.

### 14.6 Config knobs added

| knob | default | why |
|---|---|---|
| `GATE_STAGE_BATCH` | 200 | the per-claim batch when a node declares none |
| `GATE_STAGE_CONCURRENCY` | `max(4, partitions)` | worker count per stage |
| `GATE_LEASE_SECONDS` | 30 | the work lease |
| `GATE_POLL_TIMEOUT_SECONDS` | 30 | the parked long-poll window |
| `GATE_PARK_THRESHOLD_MS` | 1500 | park-vs-release (§6.5) |
| `GATE_MAX_PARKS` | 3 | in-handler parks before releasing |
| `GATE_KV_NAMESPACE` | `gate` | shared with the spec store |

Every one read once at boot, as v1 does for everything except `GATE_ADMIN_EMAILS` and
`GATE_DEV_EMAIL`, which stay per-request for the reason v1 gives.

### 14.7 The console

The Vue app survives; its data changes shape. Per view:

* **Overview** — unchanged, with the three hardcoded fields fixed.
* **Targets / TargetDetail** — "lanes" become "paths"; the budget bar reads a live KV value
  and an `expiresAt`, so it can render the window's remaining time, which v1 could not.
* **LaneDetail** → **PathDetail** — per-path share, the node ceilings it computes, the stage's
  counters, refusal traces.
* **Graphs / GraphDetail** — the diagram draws **paths** rather than edges: a path is a
  polyline through nodes, fan-outs branch, and a node several paths cross is drawn once with
  the per-path ceilings stacked inside its capacity bar. That stacked bar is the single most
  useful new picture in the product and it is what makes §3.6 legible: one counter, several
  ceilings, the reserve visible as the gap above the tallest lower ceiling.
* **SharedBudgets** — one row per `sharedKey`, live value, members, conflicts.
* **BudgetHistory / Traces** — unchanged, on the counters stream; both render an explicit
  "counters are off for this graph" state rather than an empty chart.

`StatusDot`'s taxonomy is kept exactly, including the judgement that a node at its cap is
**not** red — a refusal is the job — and only a breaker is.

---

## 15. Acceptance criteria

Every one of these is a command, a number and a place the number comes from. A criterion that
cannot be run is not a criterion.

### A. Idle cost — the reason for the rewrite

Declare the `airbnb` graph (§3.7). Push nothing. Wait five minutes.

* **`log_has_pending_v1` + `log_pop_specific_v1` + `log_queue_depth_v1` + streams-state calls
  in that window: ≤ 20 total** (seven stages' first claims, plus the reconcile loop's reads).
  v1's rate for a graph of this size was ~275,000/hour.
* Steady state: **0 database calls per stage per minute.** Every stage is a parked long-poll
  holding no connection.
* Measured with `pg_stat_statements` around the window, and by `gate-bench idle`, which reports
  the delta and fails above the threshold.

**Not met as an absolute, and the number is now a hundred and forty times better rather than
twenty.** A parked poll that times out re-issues its pop, so the floor is not zero — it is

```
stages × derived_workers × replicas ÷ poll_timeout     pops per second
```

What changed is `derived_workers`. It was `max(4, partitions of the source)`, which is a
THROUGHPUT rule: size the consumer to the width of the queue, because the queue is what bounds
you. In a rate limiter it does not — the budget does, by construction — so lanes beyond what
the cap can feed are parked polls that can never have work. Stage measured ~200 gate consumers
for a system whose largest declared budget is 400 items a second.

`plan::fitting_workers` derives it instead:

```
workers = clamp(ceil(cap_rate_per_sec / GATE_LANE_CAPACITY), 1, partitions)
```

from the stage's tightest unscoped budget with its `share` applied. For the three graphs we
run — sixteen stages, caps between 1.7 and 400 items a second — that is **one worker per
stage**: sixteen parked polls per replica, about 1,900 pops an hour, against v1's ~275,000.
`airbnb` is six of those sixteen, `vrbo` six and `google` four — a stage being one node on one
path, so the four `airbnb` nodes become six stages across its three paths.
`the_three_real_graphs_derive_one_worker_per_stage` pins the tally.

Partitions do not shrink with it. They are the ordering identity, one wildcard consumer drains
all of them, and fewer lanes than partitions costs latency at saturation and nothing else.
The burst worth checking is a full window released at once: one lane at 1000 items/s drains a
2000-token window in about two seconds, and at batch 200 that is ten claims.

### B. Throughput — the reason it is worth doing

One stage, one node, one budget large enough not to bind, one source queue with 16 partitions,
one destination.

* **≥ 2.8k items/s** — parity with the v1 counter-funnel ceiling. This is the floor; below it
  the rewrite has not paid for itself.
* **≥ 10k items/s at batch 200** on a comparable machine — the target. The arithmetic that
  makes it plausible: 2 DB round trips per batch of 200, `txnload` at 23–34k items/s on
  disjoint lanes, and 170 incr/s against a 33k/s key.
* Reported by `gate-bench throughput`, closed loop, with the batch sweep (1 / 25 / 200 / 500)
  in the output so the batching effect is visible and not asserted.
* `forwarded / commits` on the stage view must be **within 10% of the batch size** — that ratio
  is the direct evidence the batching is real, and it is the number v1's own docs call *the*
  one that explains a stage's throughput.

### C. The ceiling holds

`gate-e2e` at 50/s over a 10s window, 3000 items, two paths at shares 1.0 and 0.5:

* the worst 10-second window ≤ **125% of the declared count** (v1's gate, unchanged);
* admitted p50 ≥ **60% of the declared rate** (v1's gate, unchanged);
* **new:** while the counter is above 50% and below 100%, the 0.5-share path's refusal counter
  is rising and the 1.0-share path's admitted counter is still rising. That is §3.6's reserve,
  measured.

### D. Correctness properties, in the live suite

Each of these is one `#[ignore]`d live test against a real broker:

1. every item crosses a two-node graph **exactly once** (`got.len() == N` and
   `distinct == N`), and nothing arrives in a 3-second follow-up drain;
2. a connection's items keep their **order** end to end across a 16-partition source and
   concurrent workers;
3. a **replayed relay transaction** forwards nothing twice (`QDUP` / `QTXN` / `lease` in the
   refusal) and the graph still delivers afterwards;
4. a batch **poisoned by a duplicate** still settles every item, `duplicates >= 1`, and the
   stage keeps forwarding;
5. a **denial charges nothing**: after a refused batch, every budget key's value equals what it
   was before, within the width of one refund;
6. a **failed transaction after a successful charge** refunds — drive it with `FaultyBroker`
   refusing the transaction route, then read the key;
7. **park below the threshold, release above it**: a 1-second budget parks in-handler (the
   lease is renewed, the message is not redelivered); a 60-second budget releases (the message
   *is* redelivered, and `attempt_count` rises while the retry budget does not);
8. a **fan-out** delivers to both branches with **different, deterministic** transaction ids,
   and a re-run of the same parent produces the same two ids;
9. a **fan-in** on one queue from two paths does **not** dedup-collapse;
10. the **breaker** stops every path within one batch, `Retry-After` matches the parked
    consumers' wait, and traffic resumes exactly when the record expires;
11. a **foreign-path** message on a shared interior queue is acked and not forwarded, and the
    owning path's cursor is unaffected;
12. all of v1's surviving lifecycle tests — failed provisioning leaves the old plan serving, an
    unrestorable declare leaves nothing registered, a declare that cannot be stored is not
    acknowledged, an unprovisionable graph is still deletable, two replicas converge, the
    reconcile loop converges on its own, a redeclare does not wedge a replica.

### E. Tests green, or consciously rewritten

* `cargo clippy --workspace --all-targets -- -D warnings` clean.
* `cargo test --workspace -- --include-ignored` green against a broker ≥ 1.0.4 with
  `QUEEN_KV_ENABLED=true`, `GATE_TEST_REQUIRE_LIVE=1` and `PG_HOST` set.
* **Every deleted test is deleted in a commit whose body names it and gives the one-sentence
  reason.** §13.16 is the index of those reasons and the commits must match it. A test deleted
  without a recorded reason is a regression nobody will be able to reconstruct, and this
  repository's whole test culture — *every test names the failure it is buying* — is the thing
  the rewrite is most at risk of losing.
* The `airbnb` fixture validates clean, and warns about exactly two things, both of them the
  flagship's own shape reported back: `fanout-multiplies` (§11's mandated notice that a fan-out
  doubles what the vendor sees) and `window-head-of-line` (its `per-listing` budget is 100 per
  listing per WEEK, so one full listing holds the head of its partition — and the messages for
  every other listing behind it — until that window rotates). A fixture that warned about
  neither would mean the rules were not wired, so the test asserts the exact warning set rather
  than an empty one. The warns-about-nothing case is the smaller §3.8 `rrl` fixture. If either
  cannot, the schema is wrong, not the fixture.

### F. Migration

* Every v1 document in the store at `PUT` time is accepted, mapped, and answered 200 with
  warnings. Nothing is refused; `breach[]` keeps its bound as `maxAttempts` and is warned
  about (§16.6).
* A round trip through `migrate::from_v1` of each of the four v1 example documents in the
  README produces a document that validates clean.
* Terminal queue names are unchanged across the migration for a graph declared with the
  compatibility mapping.

---

## 16. Open questions

Nothing below is decided. Each one is a place where the working-out met something the settled
brief could not have anticipated, or where a v1 feature has no home in the target architecture.
**None of them may be resolved by the implementer alone.**

### 16.1 Per-path interior queues instead of the foreign-message skip

§4.2 puts several paths' groups on one interior queue, which costs `(paths − 1)` extra acks
per message (§6.7). The alternative is one interior queue per (path, node) —
`gate.{app}.{graph}.{node}.{path}.in` — which removes the skip entirely at the cost of one
queue per path-hop (12 instead of 5 for `airbnb`) and a proportional increase in parked
long-polls. The skip is cheap and batched; the queue-per-path is simpler to reason about and
gives per-path depth for free (the ETA currently has to subtract). **Recommendation: ship the
shared queue, keep the per-path form as a declarable `isolate: true`.** Needs a decision
because it changes the compiled plan and therefore the migration.

### 16.2 Deriving the transaction id at a convergence, not only at a fan-out

§7's middle arm. Settled point 4 says the relay carries the upstream id through; settled point
5 says a fan-out derives. The compiler can see a third case — several stages pushing into one
queue — where reuse silently collapses two legitimate messages. The proposal is to derive
there too, decided at declare time. This is a refinement of a settled decision and needs an
explicit yes.

### 16.3 The `assumed` cap discount

`ASSUMED_FACTOR = 0.7` exists in v1, is unit-tested, is documented in the README (*"an assumed
cap is enforced at 70% of what it claims"*), and **has never been applied** — `effective_cap`
has no caller. v2 can wire it in one line (`max = round(count_sub × share × factor)`) or
delete the concept and correct the README. Shipping a third release that documents a discount
it does not apply is the option that is not available. **Recommendation: wire it**, since the
console already draws assumed bars hatched and operators presumably believe the sentence.

### 16.4 Sub-second windows, and the loss of the rolling window

Two related items, both consequences of KV owning the arithmetic.

**(a) `timeMs < 1000`.** Settled point 12 sets the floor at 100ms. A KV TTL is whole seconds,
so a 200ms window is not expressible. §5.3 enforces such a budget as `count` per **second** —
tighter than declared, never looser — and warns. The alternatives are to refuse `timeMs < 1000`
outright, or to scale the count up to a per-second rate (looser than declared, and therefore
refused here without instruction). **Confirm the rounding direction.**

**(b) `alignment: rolling` is gone.** v1's two-bucket sliding window was chosen deliberately
and is defended by a saturation property test, with a stated motivating case: *"no more than
2000 from an IP in any ten seconds"*, where a fixed window's boundary doubling is exactly the
breach the limiter exists to prevent. v2 offers subdivision instead, which bounds the exposure
to `2 × count/N` rather than `2 × count`. For the `airbnb` `ip-10s` budget at `subWindows: 10`
that is 300 across a boundary against a declared 1500 — better than v1's fixed window and worse
than v1's rolling one at the boundary itself, though far smoother across the window as a whole.
**This is the largest behavioural change in the document and the author should see the numbers
before it ships.**

### 16.5 Traces are poorer

v1 wrote one trace row per decision through the calls queue, in the same transaction as the
ack, and the design notes call that atomicity load-bearing. v2 has no ack and no calls queue,
so traces become a bounded per-replica ring of **refusals only** (§10.4), flushed to Postgres
when it is configured. What is lost: per-item admission traces, the estimate-vs-actual cost
comparison (`cost_actual` has no source without an ack), and the guarantee that a trace and a
settlement agree. **Recommendation: accept, and say so in the README's observability section**
— but it is a real reduction and it is not being smuggled through.

### 16.6 Breach rules, retro edges and the `_gate` re-entry protocol — **the big one**

v1's breach machinery is `plan_retro`, `BreachRule`, `BreachWhen`, `maxAttempts`,
`origin-entry`, the `:r{n}` transaction ids, the `_gate.attempt` counter, the `retry-cost` and
`retry-entry` validations, and six live tests. It is the answer to *"the vendor said 429 —
re-enter this item at the door it came in at and make it re-pay every budget on its path"*, and
it is genuinely good: the pacing **is** the backoff, no timer is involved, and the attempt is
in the transaction id so a replayed ack cannot double-retry.

It hangs entirely off `POST /v1/leases/ack`, which the settled architecture removes: the
application consumes the egress queue with its own SDK and Gate never sees the outcome.

Three options, none of them chosen here:

1. **The breaker replaces it (§8).** A 429 becomes a node-wide backoff rather than a per-item
   re-entry. The item itself is the application's problem: it re-pushes to the ingress queue
   with its own SDK. Simplest; loses per-item bounded retry and the `origin-entry` guarantee.
2. **A minimal re-entry endpoint.** `POST /v1/apps/:app/graphs/:g/reenter` taking
   `{ payload, path, attempt }`, which pushes into the path's ingress queue with
   `transaction_id = derive(originalTxnId, "r{attempt}")` and a bumped `_gate.attempt`,
   refusing past a declared `maxAttempts`. About 120 lines, keeps the bounded-retry and
   re-pay-the-path properties, needs the caller to hand back the payload it popped.
3. **Descope entirely**, and document the recipe (re-push to the ingress queue with your own
   idempotency key).

**Recommendation: (2)**, with (1) as the aggregate signal alongside it — together they cover
what v1's breach rules covered. But this is a feature deletion or a feature redesign either
way, and it is the one item in this document that most needs the author's word.

**Resolved as recommended, 2026-08-21: (2) and (1), both.** `POST /v1/apps/:app/graphs/:g/reenter`
takes `{ payload, txn, path?, attempt?, partition? }` and pushes into the ingress queue of the
FIRST node of that path — the origin entry, so the item re-pays every budget on its path rather
than skipping the ones upstream of where it failed. The transaction id is
`derive(txn, "r{attempt}")`, so a caller reporting one item twice collapses on the broker's
dedup and nothing keeps a table. The bound is `maxAttempts` on the document (default 3, range
1–20, rule `max-attempts-range`), counted in `_gate.attempt`, which every relay hop now carries
forward rather than rewriting — without that line the count resets at the first hop and the
bound is not a bound.

What did NOT come back is the TRIGGER. v1 watched the ack and re-entered by itself; v2 never
sees an outcome, so re-entry is something the application asks for. `migrate` therefore maps
`breach[]` to `maxAttempts` with a warning saying exactly that, instead of refusing the
document — which also puts §12.1's promise back ("never a 422 for having been written last
year").

### 16.7 Strict priority is traded for an atomic reserve

§3.6 and §13.3. v1's priority is *drain leg 0 to exhaustion before looking at leg 1*, asserted
by FOUR live tests, one of them measuring that a priority-0 item overtakes ~200 bulk items
within `window + 20` positions. v2's priority is *the low path refuses itself at its share*,
which guarantees the reserve atomically but does **not** guarantee that a high-priority item
overtakes a queued low-priority one — a low-priority message already in the interior queue is
still ahead of it in that partition.

If head-of-line overtaking is required, it can be had back by giving each path its own interior
queue (§16.1) and having the downstream stage prefer the high-priority queue — which
reintroduces a merge, a leg order and a scheduler, i.e. most of what §13.3 deletes.
**Recommendation: accept the trade and document it as "priority is capacity, not queue
position".** Needs the author's assent because the README's flagship picture is about
priority.

### 16.8 What `GET .../next` and `POST /v1/leases/*` do for one release

§13.9 proposes **410 Gone** with a message naming the egress queue and a two-line SDK snippet,
for one release, then removal. The alternative is a compatibility shim that proxies to a
consumer group on the egress queue — which reintroduces the opaque lease protocol and the
`gate.exec.{lane}` group for the sake of callers who have to change anyway. **Recommendation:
the 410.** Needs confirmation because it is a breaking change for every existing caller on the
same day the declaration changes.

---

## 17. Summary of what ships

* **One document type**, one compiler, one runtime object per stage.
* **Admission is `kv.incr` with `max`.** `applied` is the decision. One call per batch.
* **Priority is a ceiling on a shared counter**, so the reserve is atomic and there is no
  scheduler.
* **The relay is `ack + push` in one transaction**, one source partition per claim, partition
  passed through.
* **The scheduler is the broker's wildcard pop.** No pinning, no probing, no sweeping.
* **Idle costs parked timers.** 275,000 polls an hour becomes approximately zero.
* **~2100 lines deleted, ~800 written**, and every deleted test deleted with its reason on the
  record.
