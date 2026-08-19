<img src="assets/logo.svg" alt="" width="72">

# Gate

An egress rate limiter built on [QueenMQ](https://queenmq.com). You declare what a
vendor lets you do; Gate decides what leaves and when, and holds the work that does
not fit until it does.

It is not a library. Callers speak HTTP and never learn there is a queue.

A limit is rarely one number. A portal gives you a per-endpoint rate, a per-account
rate, a per-listing rate, and a ceiling on the egress IP that counts all of them at
once — and the interesting question is not how to count, it is how those limits
**compose**. That is what a graph is for, and most of this document is about it.

<a href="assets/dashboard.png"><img src="assets/dashboard.png" width="600"
  alt="The Gate console drawing a declared graph: three entry nodes — content, messages
  and prices — relaying into one terminal called ip, each edge labelled with its
  priority, and dashed retro edges running back from the terminal to the entries."></a>

*The console drawing what is running: three traffic classes, each isolated in its own
node, merging into the one node that holds the egress-IP ceiling — and the dashed
edges a throttled call takes back to the door it came in at.*

---


## Run it locally

Gate needs a QueenMQ broker and, optionally, a PostgreSQL for history.

```bash
docker run -d --name queen-pg -e POSTGRES_PASSWORD=postgres -p 5434:5432 postgres:18
docker run -d --name queen --link queen-pg \
  -e PG_HOST=queen-pg -e PG_PASSWORD=postgres -e QUEEN_KV_ENABLED=true \
  -p 6632:6632 ghcr.io/queen-mq/queen:latest
```

`QUEEN_KV_ENABLED` is not optional: every declared spec and every cross-target
ceiling lives in `queen.kv`.

Then Gate itself:

```bash
docker run -d --name gate -p 8788:8788 -p 8790:8790 \
  -e QUEEN_URL=http://host.docker.internal:6632 \
  -e GATE_BIND=0.0.0.0:8788 \
  -e GATE_PUBLIC_BIND=0.0.0.0:8790 \
  -e GATE_DEV_EMAIL=you@example.com \
  -e GATE_ADMIN_EMAILS=you@example.com \
  -e PG_HOST=host.docker.internal -e PG_PORT=5434 \
  -e PG_USER=postgres -e PG_PASSWORD=postgres -e PG_DATABASE=postgres \
  ghcr.io/queen-mq/gate:latest
```

The console is on <http://localhost:8790>, the API on `:8788`.

**Two ports, on purpose.** `GATE_BIND` is the plane your applications call and has no
authentication — the assumption is that only your network can reach it.
`GATE_PUBLIC_BIND` is the console and requires a Google session on every route. Do not
expose the first one.

`GATE_DEV_EMAIL` is the local bypass: it skips sign-in and treats every request as
that identity. Gate refuses to boot with it set on an `https` public URL. In a real
deployment you set `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`,
`GOOGLE_ALLOWED_DOMAINS`, `GATE_SESSION_SECRET` and `GATE_PUBLIC_URL` instead — see
[helm/gate/README.md](helm/gate/README.md).

Without `PG_HOST` Gate still limits exactly as well; it just cannot answer anything
about yesterday, and with several replicas each would remember a different past. It
creates schema `gate` and its tables itself at boot.

---

## Concepts

**Target** — one thing that limits you: a vendor, an API, an account. It owns queues,
budgets and lanes, and its identity is the pair `application/name`.

**Application** — who owns the target. Applications never share a ceiling and never
see each other's targets. Two teams calling the same vendor with their own credentials
are two applications; two callers sharing one credential are two lanes of one target.

**Budget** — a ceiling: `cap` per `periodSeconds`. All of a target's budgets must
admit, so they compose without ordering. `alignment` has no default — `calendar`
resets on the wall clock, `rolling` slides — because guessing wrong is a
factor-of-two overshoot. `confidence` records whether the number is `documented`,
`inferred` or `assumed`; an assumed cap is enforced at 70% of what it claims, and the
console never draws it as if it were known.

**Lane** — how the ceiling is divided. Each lane is its own queue and its own gate, so
an urgent push does not queue behind a photo upload. `cap: ceiling` takes what the
others leave; `share: 0.3` reserves a fixed slice; `ceiling-minus-measured` takes the
residual and may only *shrink* when its neighbours are busy. Lanes **divide** a
ceiling and never replicate it — two lanes both told "you may use the ceiling" spend
it twice, which a load run demonstrated at 93/s against a declared 50/s.

**Cost** — not every call is one call. `cost.field` names where the caller stamps the
weight, `cost.max` is a validation gate: a cost above it is refused at declare time
rather than blocking a lane forever at run time.

**Pacing** — `leaseSeconds` is both the pacing quantum and the failover window; a
denial parks the lane for one lease at zero billing cost, because the work is never
popped. `batch` must fit a lease's worth of budget or the batch becomes the limiter.

**Shared budget** — a ceiling that crosses targets (an egress IP, a shared account).
No gate can see past its own partition, so these live in `queen.kv`: declare a budget
with `store: kv` and the same `id` on every target that draws on it, inside one
application. The key is what makes it shared. A shared budget cannot be scoped or
selected on — the pool keys on `(application, id)` alone — and a spec that tries is
refused rather than silently flattened.

**Graph** — targets as nodes of a declared DAG, which is the rest of this document.

**Shard** — `shardBy` splits a node's queue into `shards` partitions, one gate and one
state document each. It is how a per-key limit survives high cardinality.

---

## The shape of a limit

Everything about the graph follows from one property of the engine, so it is worth
stating before the syntax.

A gate runs as a streaming operator pinned to one partition of one queue. The broker's
exclusive partition lease makes it the **single writer** of that partition's counters
— which is why nested windows can be evaluated in memory, with no distributed
transaction anywhere, and why the answer is exact. Everything else is a consequence of
what that single writer can and cannot see.

**Budgets in one node are exact, and share a queue.** Every budget a node declares is
evaluated against the same instant and charged in a second pass only if all of them
admit. Nothing can slip between two of them. The price is the shared queue: a denial
stops the batch, and everything behind that item waits a lease, whatever it was.

**An edge between two nodes is isolated, and smeared.** Give the second limit its own
node and its own queue and a denial there parks nothing upstream — each class waits in
its own line. The price is that the two checks no longer happen at the same instant:
the item was admitted upstream at T and reaches the downstream gate at T + queueing,
so the upstream certificate has aged. Whichever limit is enforced **last** is the
exact one; the ones before it are approximations, and they get worse the deeper the
downstream queue is.

**Exact *and* isolated at once is not expressible.** Two counters in two partitions
cannot be charged atomically without a distributed transaction, and this system does
not have one. For each pair of limits you choose: same node, or a path.

In practice that choice has an obvious answer. The severe limit — the egress IP, where
a breach blocks the whole fleet — goes in the **terminal** node, where it is enforced
last and therefore exactly. The mild ones — a per-endpoint rate whose breach is a 429
on one call — go **upstream**, where isolation is worth more than the last decimal.
And paths stay short, because the smear composes at every hop and the latency floor is
one lease per hop.

---

## A graph, end to end

This is the Airbnb graph the design was argued from: per-endpoint limits nested inside
per-egress-IP limits, a per-listing weekly limit at a cardinality no single counter
document could hold, and price pushes that must overtake a calendar flood.

```
   push                                                                  pop
                 ┌──────────────┐
  price.push ──► │    prices    │──── priority 0 ───┐
                 │  no budget   │                   │
                 │  cost ≤ 100  │                   │
                 └──────────────┘                   ▼
                 ┌──────────────┐            ┌──────────────┐
  message.post ► │   messages   │─ prio 1 ──►│      ip      │──► consumer ──► vendor
                 │  100 / 60s   │            │  1500 / 10s  │
                 └──────────────┘            │ 15000 / 5m   │
                 ┌──────────────┐            │  150k / 1h   │
  photo.delete ► │    photos    │─ prio 1 ──►│ 3.375M / 1d  │
                 │  ×64 shards  │            │  cost ≤ 100  │
                 │ 100/listing  │            └──────────────┘
                 │   / week     │                   │
                 └──────────────┘                   │
                        ▲                           │
                        └─── 429: back to the entry it came in at,
                             where it waits for budget again (×3)
```

Read it as three statements:

* **the IP ceiling is declared once, in the terminal.** It counts every call the
  egress makes, so it must be one counter — declared in two nodes it would be two, and
  the vendor would see the sum. Being last, it is also the exact one.
* **the mild limits are upstream, one per class.** A message backlog cannot delay a
  price, because they are different queues. `prices` declares no budget at all: it
  exists to isolate a class and to carry a priority, and the limit it is checked
  against is downstream.
* **the severe path is short.** One hop. The certificate `messages` issues is at most
  one queue old by the time `ip` re-checks it.

### The life of one push

```http
POST /v1/apps/channel-manager/graphs/airbnb/nodes/prices/push
{ "op": "price.push", "cost": 3, "txn": "listing-42:price",
  "payload": { "connection": "conn-7", "entity": "listing-42" } }
```

1. Gate stamps the item — `op`, the weight under `cost.field`, and a `_gate` envelope
   recording the graph, the entry and the attempt count — and appends it to
   `gate.channel-manager.airbnb.prices.push`. The call returns as soon as the work is
   durable. `txn` is the coalescing lever: two pushes with the same value inside the
   dedup window collapse to one, so lag compresses the backlog instead of growing it.

2. The `prices` gate wakes once per lease, claims a batch and asks the engine. This
   node has no budget of its own, so everything is admitted and appended to
   `…prices.admitted.p{n}`, partitioned by connection so one connection's work stays
   ordered.

3. The **relay** into `ip` moves it: one queen transaction carrying `ack(admitted
   message)` and `push(ip.push)` together. Never two calls — ack-then-push loses the
   item, push-then-ack duplicates it. The downstream push reuses the upstream message's
   transaction id, so a redelivered relay is refused as a duplicate rather than
   doubling the work.

4. The `ip` gate evaluates all four IP budgets against one instant and charges them
   only if every one admits, then appends the item to `…ip.admitted.p{n}`. If any
   budget refuses, nothing is charged, the batch stops, the lease is kept, and the lane
   tries again next lease. That silence is the pacing signal — there is no "you are
   throttled" response for a caller to interpret or back off from.

5. The consumer long-polls `GET …/graphs/airbnb/nodes/ip/next`, makes the vendor call
   and acks. The ack advances the cursor and pushes the measurement event in **one
   transaction**: split in two, an ack that lands without its event leaves the meter
   under-reading, the derived cap rising, and the limiter believing it has budget it
   does not.

6. If the vendor refused with a 429, the caller acks `outcome: throttled` and the
   breach rule sends the item back to `prices` — in that same transaction — where it
   queues for budget like anything else. The pacing **is** the backoff.

---

## What a graph is made of

### Nodes

A node is a target named `{graph}.{node}`, and it behaves exactly like one: the same
queues, the same gate runners, the same lease, the same admission engine. Nothing in
the data plane knows what a graph is.

What a node adds is a role. `entry: true` means callers may push to it; being listed
in `consume` means callers may pop it. Everything else is interior and belongs to the
relays.

A node with no budgets is legal **only** if it has an out-edge — that is a class node,
checked downstream. One with no budget and nowhere to send its work would be a queue
with extra steps.

### Edges

An edge is a relay: a task that moves work from one node's admitted queue to the next
node's push queue, under its own consumer group
(`gate.edge.{app}.{graph}.{from}.{to}`).

**One transaction per batch.** `{ack, push}` commit together or not at all, and the
transaction carries the messages' leases as a precondition — so a lease that expired
while the relay was working rolls the whole thing back instead of forwarding work
somebody else has already re-claimed.

**One relay per destination, not per edge.** Priority is only real where the streams
meet: two independent relays into one queue would each forward as fast as they could,
and the destination's FIFO would then order by arrival — which is exactly what a
priority is meant to override. So the tasks feeding a node are one task, draining its
upstreams in strict `priority` order: a leg is drained until it runs dry or the window
closes, before the next one is looked at.

**A window keeps the bottleneck shallow.** The relay forwards only while the
destination's push queue holds fewer than `2 × rate × leaseSeconds` items, where the
rate is that node's most generous unscoped budget. Two lease-windows so the gate never
runs dry; no more, because a deep bottleneck queue is a priority nobody can act on — a
price push cannot overtake what has already been forwarded.

**Fan-out is refused.** Each edge is its own consumer group, and a consumer group
receives every message — so two out-edges from one node would copy the stream rather
than split it, and one push would become one vendor call per branch.

### Entries and terminals

Pushing into an interior node, or popping one, is a `409`. This is not tidiness:

* an interior node's push queue is fed by its in-edges. A caller pushing there injects
  work that has paid none of the upstream budgets — which is the whole point of the
  path it skipped.
* an interior node's admitted queue is a relay's source. A caller popping it takes
  work the graph is routing: the item is executed by that caller *and* forwarded to
  the node that was supposed to pace it.

Node targets are reachable by name (`airbnb.ip` is a target), and the target routes
refuse to serve them for the same reason — the entry and terminal rules live on the
graph routes, and going round them is how an item gets executed twice.

### Retro edges

A vendor throttle is the one event that proves the numbers are wrong, and retrying it
blind is how a rate limiter becomes a rate amplifier.

A `breach` rule turns an ack into a re-entry. When the ack's `outcome`/`status`
matches, Gate pushes the item back to the entry it came in at — inside the ack's own
transaction, using the payloads the lease already carries, so it costs no round trip
and no state — and the item then waits for budget like anything else.

* it re-enters at **`origin-entry`**, the door stamped on it at its first push. That
  makes it re-pay every budget on its path, which is correct rather than harsh: the
  vendor counted the call that failed, so the retry is a new call and owes the whole
  path.
* `maxAttempts` is not optional. Past it the item is settled and the breach recorded —
  a retro edge without a bound is a livelock with a vendor's rate limit as its only
  brake.
* the re-entry's transaction id is `{txn}:r{attempt}`, so a replayed ack cannot make
  two of them.

### Shards

`shardBy: entity, shards: 64` splits a node's push queue into 64 partitions, each with
its own gate runner, its own partition lease and its own state document. It exists
because that document is re-read in full on every cycle: 200,000 listings in one
document is 200,000 counters re-read every cycle, and 3,125 per document is not.

The single-writer argument survives because a key hashes to exactly one shard — never
two. Which is also why **every budget in a sharded node must carry the shard dimension
in its `scope`**: an unscoped budget would get one counter per shard, and its cap would
be enforced 64 times over.

A sharded node takes its work from a push, not from an edge. A relay cannot choose a
shard for an item that does not carry the dimension, and choosing one anyway would put
one key in two counters.

---

## What a declare refuses

Every rule below converts a silent runtime failure into a rejected `PUT`. None of them
is a style check.

| Rule | What it prevents |
|---|---|
| `acyclic` | an item traversing for ever, re-paying every budget on the way round |
| `reachable`, `drains` | work that can never enter, or that enters and is never popped |
| `cost-monotonic` | an item admitted upstream that the next node can never admit — parking that lane's head for ever, with no DLQ to fall into |
| `budget-once` | one ceiling declared in two nodes: two counters, and the vendor sees the sum |
| `edge-fanout` | a broadcast dressed as a split — one push, one call per branch |
| `edge-unique`, `edge-self` | two relays on one queue pair; a node relaying into itself |
| `consume-terminal` | a caller popping a queue a relay is also draining |
| `path-length` | more than three hops, where the smear and the per-hop lease floor stop being worth it |
| `retry-entry`, `retry-cost` | a re-entry that skips upstream budgets, or that lands where it could never be admitted |
| `breach-attempts`, `breach-when` | an unbounded retry loop; a rule that matches nothing |
| `relay-lane` | a second lane on a relayed node — a relay has nobody to ask which lane, so the other lane's share is capacity nothing can reach |
| `cost-field` | two nodes reading the weight under different names; the downstream one silently charges its default |
| `shard-scope`, `shard-entry`, `shard-count` | a cap enforced once per shard; a shard a relay cannot choose; an unbounded number of runners |
| `store-fits`, `max-keys` | a state document re-read at a size nobody sized it for |
| `lane-shares`, `lane-floor`, `default-lane` | a ceiling divided into more than one ceiling |
| `cost-fits`, `batch-fits` | an item no budget can admit; a batch smaller than a lease's worth of budget, which makes the batch the limiter |
| `kv-scope`, `kv-match`, `kv-chunk` | a shared budget pretending to be per-key, per-op, or large enough to lease |
| `provenance` | a cap claiming to be `documented` with no source and no date |

Two things are warned about rather than refused, because they are trade-offs and not
mistakes: `lease-beats-window` (a lease as long as the tightest window costs about a
quarter of the ceiling) and `kv-rolling` (a shared budget is a fixed window whatever it
declares, so a rolling one accepts up to twice its cap at the boundary).

A change that re-founds a counter — a period, an alignment, a scope, a store, a
partitioning, a re-shard, a rewiring — needs `version` bumped, or the declare is a
`409` naming what it would have re-founded.

---

## The target API

Everything below is on the internal port. `:app` and `:name` are the target's
identity; the unscoped `/v1/targets/{name}` forms default the application.

### Declare

```http
PUT /v1/apps/{app}/targets/{name}
```

```json
{
  "application": "channel-manager",
  "name": "airbnb",
  "version": 1,
  "budgets": [
    { "id": "api", "cap": 3000, "periodSeconds": 60, "alignment": "calendar",
      "confidence": "documented", "source": "portal docs", "asOf": "2026-08-18" }
  ],
  "lanes": [
    { "name": "urgent", "cap": "ceiling", "concurrency": 8 },
    { "name": "bulk", "cap": "ceiling-minus-measured", "concurrency": 16,
      "floor": 0.5, "default": true }
  ],
  "cost": { "field": "httpCost", "default": 1, "max": 5 },
  "pacing": { "leaseSeconds": 5, "batch": 250 },
  "admitted": { "partitionBy": "connection", "partitions": 8 }
}
```

The whole document, every time. A `200` means validated, provisioned **and** stored: a
store that will not take it answers `502` saying the target is running the new spec, is
not durable, and will be put back by the next reconcile pass.

`PUT /v1/apps/{app}/targets` declares a whole SET and reaps what is missing — scoped to
that application, so one team cannot delete another's targets. Graph nodes are exempt:
they are owned by a graph document, and reaping one would tear down half a topology.

`GET` and `DELETE` read and remove one target. A delete is about the stored
declaration — it forgets that first, then stops the runners — so a spec whose
provisioning keeps failing can still be removed, and a delete that cannot reach the
store is refused rather than undone by the next reconcile.

### Push

```http
POST /v1/apps/{app}/targets/{name}/lanes/{lane}/push
{ "op": "calendar.push", "cost": 1, "txn": "listing-42:availability",
  "payload": { "connection": "conn-7" } }
```

### Consume

```http
GET /v1/apps/{app}/targets/{name}/lanes/{lane}/next?batch=100&wait_ms=1000
→ { "items": [...], "lease": [...], "target": "airbnb", "lane": "bulk" }
```

You get work only when the budget allows it. The answer names the `target` and `lane`
to settle it as — which matters for a graph, where you popped the node `messages` and
the target is `airbnb.messages`.

```http
POST /v1/leases/ack
{ "lease": [...], "up_to": 100, "calls": 100, "outcome": "ok",
  "application": "channel-manager", "target": "airbnb", "lane": "bulk",
  "op": "calendar.push" }
```

The ack is the feedback loop, not bookkeeping: `calls` is what the work really cost,
and `outcome: "throttled"` is the vendor telling you the cap you enforce is higher than
the real one. Settlement is by prefix (`up_to`), never an arbitrary subset. An ack that
names a target Gate does not know still settles the work and says in `refused` what it
could not do with it — no calls event, no breach rule.

`POST /v1/leases/nack` returns work for redelivery and refunds it, because the vendor
never saw the call; `POST /v1/leases/renew` extends a lease for slow work.

### Watch

```http
GET /v1/apps/{app}/metrics
```

Per lane: what is waiting for budget, what is waiting for workers, the admitted rate
and a drain ETA — enough for another product to render limit status on its own
frontend. The two backlogs are kept apart because they have different owners: one is
Gate holding work back on purpose, the other is your consumers not keeping up.

---

## The graph API

A graph is declared whole — nodes, edges, terminals and breach rules validated
together, provisioned atomically — because half a graph accepts work at its entry and
drops it at the hole.

```http
PUT /v1/apps/{app}/graphs/{name}
```

```json
{
  "version": 1,
  "nodes": {
    "prices":   { "entry": true, "budgets": [],
                  "cost": { "field": "httpCost", "default": 1, "max": 100 } },
    "messages": { "entry": true,
                  "budgets": [ { "id": "msg-post", "cap": 100, "periodSeconds": 60,
                                 "alignment": "rolling", "confidence": "documented",
                                 "source": "portal docs", "asOf": "2026-05-19" } ],
                  "cost": { "field": "httpCost", "default": 1, "max": 1 } },
    "photos":   { "entry": true, "shardBy": "entity", "shards": 64,
                  "budgets": [ { "id": "photo-del-weekly", "cap": 100,
                                 "periodSeconds": 604800, "alignment": "rolling",
                                 "scope": ["entity"], "maxKeys": 200000,
                                 "confidence": "documented", "source": "portal docs",
                                 "asOf": "2026-05-19" } ],
                  "cost": { "field": "httpCost", "default": 1, "max": 1 } },
    "ip":       { "budgets": [ { "id": "ip-10s", "cap": 1500, "periodSeconds": 10,
                                 "alignment": "rolling", "confidence": "documented",
                                 "source": "portal docs", "asOf": "2026-05-19" } ],
                  "cost": { "field": "httpCost", "default": 1, "max": 100 },
                  "admitted": { "partitionBy": "connection", "partitions": 64 } }
  },
  "edges":   [ { "from": "prices", "to": "ip", "priority": 0 },
               { "from": "messages", "to": "ip", "priority": 1 },
               { "from": "photos", "to": "ip", "priority": 1 } ],
  "consume": [ "ip" ],
  "breach":  [ { "when": { "status": 429 }, "retryTo": "origin-entry",
                 "maxAttempts": 3 } ]
}
```

```http
POST /v1/apps/{app}/graphs/{graph}/nodes/{node}/push   # entry nodes only
GET  /v1/apps/{app}/graphs/{graph}/nodes/{node}/next   # terminal nodes only
GET  /v1/apps/{app}/graphs/{name}                      # the graph, live
DELETE /v1/apps/{app}/graphs/{name}
```

The declare answers with the resolved topology: which node is pushable, which is
poppable, the queues each one owns, and the window every relay is holding its
destination to.

### Acking a breach

`outcome` is one field for a whole lease, so a consumer that runs a batch and acks once
has to say **which** items the vendor refused:

```http
POST /v1/leases/ack
{ "lease": [...], "outcome": "throttled", "status": 429,
  "breached": ["<message id>"], "target": "airbnb.ip", "lane": "default", … }
```

Without `breached` the outcome is taken to be about every item the ack settles — right
for a one-item ack, and forty-nine duplicate calls for a batch of fifty. The answer
says what happened: `retried`, `exhausted`, `unroutable`, and `refused` when the graph
declares nothing that could have been done. An impossible retry never fails the ack:
the work has already been made, and refusing to settle it would have it made again.

---

## Operating it

**Replicas.** Every replica runs every gate, and the broker's partition lease decides
which one is admitting on a given partition at a given moment. Two replicas do not
enforce two ceilings.

**Reconcile.** A declare lands on ONE replica. A background pass every 15 seconds
(`GATE_RECONCILE_SECONDS`) diffs the store against what this replica is running and
re-provisions the difference: restarting a changed target, starting one it has never
seen, removing one the store no longer holds, repairing a graph whose node is not
running. Without it the fleet enforces whichever spec each pod happened to receive,
indefinitely — and the **looser** one decides, because the tighter pod simply admits
less of the same traffic.

The store is the authority. A pass that cannot read it changes nothing, and a pass that
reads it incompletely — a clamped page, a document a newer build wrote — may add and
change but never remove.

**What is fleet-wide, and what is one replica's.** With `PG_HOST` set, every replica
writes the minute it saw into `gate.rollups` and the row is a sum, so the flow chart,
the roll-ups, the rates, the traces and the breaches are the fleet's from any pod. The
queue depths come from the broker, so they always were. What stays local: the lifetime
`admitted`/`denied` counters on a target page, the "last throttled at" stamp, and the
per-relay counters on a graph page — each is what one process did since it booted.
Without `PG_HOST` the history surfaces are one replica's memory too: twelve hours of
it, lost on restart.

**What a failed declare leaves behind.** Provisioning is stop-then-start, so a failure
half way is the dangerous moment. If the new spec cannot start, the old one is
restarted and the caller is told which version is serving; if even that fails the
target is *unregistered*, so a push is refused — recoverable — rather than accepted
into a queue nobody drains. The next reconcile pass brings it back from the store.

---

## The console

`/graphs` draws what is running: the nodes laid out in the order work moves through
them, each with its worst budget and its two backlogs, the edges labelled with their
priority and lag, and the retro edges drawn dashed back to the entries. It is the same
diagram this design was argued from, rendered from `/api/apps/{app}/graphs/{name}`.

The rest answers an operator's question — "are we still in control?" — rather than a
delivery console's. A lane sitting at its cap and refusing work is the system
succeeding, so it is not painted red. The one thing painted red is a vendor throttle:
the only proof that the caps we enforce are looser than the real ones.

---

## Build

```bash
cd ui && npm ci && npm run build && cd ..
cargo build --release
```

The console is compiled into the binary, so `ui/dist` must exist before the Rust build.
`docker build` does both.

## Test

```bash
cargo test
```

The live suite is `#[ignore]`d, so that run reports it as ignored rather than as
passed: those tests need a broker, and a suite that silently verifies nothing is worse
than one that says it did not run. With a queen to point at:

```bash
GATE_TEST_QUEEN_URL=http://127.0.0.1:6632 cargo test -- --include-ignored
```

CI runs exactly that against a real broker
([.github/workflows/test.yml](.github/workflows/test.yml)), with
`GATE_TEST_REQUIRE_LIVE=1` so a missing broker fails the build instead of skipping the
only tests that cover the relay, the reconcile and the retro path.
