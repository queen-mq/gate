# Gate

An egress rate limiter built on [QueenMQ](https://queenmq.com). You declare what
a vendor lets you do; Gate decides what leaves and when, and holds the work that
does not fit until it does.

It is not a library. Callers speak HTTP and never learn there is a queue.

---

## Run it locally

Gate needs a QueenMQ broker and, optionally, a PostgreSQL for history.

```bash
docker run -d --name queen-pg -e POSTGRES_PASSWORD=postgres -p 5434:5432 postgres:18
docker run -d --name queen --link queen-pg \
  -e PG_HOST=queen-pg -e PG_PASSWORD=postgres -p 6632:6632 ghcr.io/queen-mq/queen:latest
```

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

**Two ports, on purpose.** `GATE_BIND` is the plane your applications call and
has no authentication — the assumption is that only your network can reach it.
`GATE_PUBLIC_BIND` is the console and requires a Google session on every route.
Do not expose the first one.

`GATE_DEV_EMAIL` is the local bypass: it skips sign-in and treats every request
as that identity. Gate refuses to boot with it set on an `https` public URL. In a
real deployment you set `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`,
`GOOGLE_ALLOWED_DOMAINS`, `GATE_SESSION_SECRET` and `GATE_PUBLIC_URL` instead —
see [helm/gate/README.md](helm/gate/README.md).

Without `PG_HOST` Gate still limits exactly as well; it just cannot answer
anything about yesterday, and with several replicas each would remember a
different past. It creates schema `gate` and its tables itself at boot.

---

## Concepts

**Target** — one thing that limits you: a vendor, an API, an account. It owns
queues, budgets and lanes, and its identity is the pair `application/name`.

**Application** — who owns the target. Applications never share a ceiling and
never see each other's targets. Two teams calling the same vendor with their own
credentials are two applications; two callers sharing one credential are two
lanes of one target.

**Budget** — a ceiling: `cap` per `periodSeconds`. All of a target's budgets must
admit, so they compose without ordering. `alignment` has no default —
`calendar` resets on the wall clock, `rolling` slides — because guessing wrong is
a factor-of-two overshoot. `confidence` records whether the number is
`documented`, `inferred` or `assumed`; an assumed cap is enforced at 70% of what
it claims, and the console never draws it as if it were known.

**Lane** — how the ceiling is divided. Each lane is its own queue and its own
gate, so an urgent push does not queue behind a photo upload. `cap: ceiling`
takes what the others leave; `share: 0.3` reserves a fixed slice;
`ceiling-minus-measured` takes the residual and may only *shrink* when its
neighbours are busy. At most one lane may claim the whole ceiling — two that both
do would enforce it twice.

**Cost** — not every call is one call. `cost.field` names where the caller stamps
the weight, `cost.max` is a validation gate: a cost above it is refused at
declare time rather than blocking a lane forever at run time.

**Pacing** — `leaseSeconds` is both the pacing quantum and the failover window; a
denial parks the lane for one lease at zero billing cost, because the work is
never popped. `batch` must fit a lease's worth of budget or the batch becomes the
limiter.

**Shared budget** — a ceiling that crosses targets (an egress IP, a shared
account). No gate can see past its own partition, so these live in `queen.kv`:
declare a budget with `store: kv` and the same `id` on every target that draws on
it, inside one application. The key is what makes it shared. A shared budget
cannot be scoped or selected on — the pool keys on `(application, id)` alone —
and a spec that tries is refused rather than silently flattened.

**Graph** — targets as nodes of a declared DAG. Work enters at an *entry* node,
is re-checked against every node it traverses, and is consumed at a *terminal*.
The reason to reach for one: budgets in a single node are exact but share a
queue, while an edge between two nodes isolates them completely — so the severe
limit (an egress IP block takes out the fleet) goes in the terminal, where it is
enforced last and exactly, and the mild ones go upstream where isolation is worth
more. Priority lives on the edges; a vendor throttle can be routed back to the
entry it came from, where it waits for budget instead of retrying blind.

**Shard** — `shardBy` splits a node's queue into `shards` partitions, one gate
and one state document each. It is how a per-key limit survives high
cardinality — 200,000 listings over 64 shards is 3,125 counters per document —
and every budget in a sharded node must carry the shard dimension in its `scope`,
or its cap would be enforced once per shard.

---


## The target API

Everything below is on the internal port. `:app` and `:name` are the target's
identity; the unscoped `/v1/targets/{name}` forms default the application.

### Declare

```http
PUT /v1/apps/{app}/targets/{name}
```

The whole document, every time. Gate creates the queues, starts one gate per
lane, and answers with what it resolved. A change that re-founds the counters
(alignment, scope, store, partitioning) needs `version` bumped, or the reply is
409 with the reason. An invalid document is 422 naming the rule it broke.

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

`PUT /v1/apps/{app}/targets` declares a whole SET and reaps what is missing —
scoped to that application, so one team cannot delete another's targets.

`GET` and `DELETE` on the same path read and remove one target. A delete is about the
stored DECLARATION — it forgets that first, then stops the runners — so a spec whose
provisioning keeps failing can still be removed, and a delete that could not reach the
store is refused rather than undone by the next reconcile. The answer's `registered`
says whether anything was running. Work already pushed stays in the broker.

A declare answers `200` only when the spec is validated, provisioned AND stored. A
store that will not take it is a `502` naming what happened: the target is running the
new spec, it is not durable, and the next reconcile pass will put the stored one back.


### Push

```http
POST /v1/apps/{app}/targets/{name}/lanes/{lane}/push
{ "op": "calendar.push", "cost": 1, "txn": "listing-42:availability",
  "payload": { "connection": "conn-7" } }
```

Returns as soon as the work is durable. `txn` is the coalescing lever: two pushes
with the same value inside the dedup window collapse into one, so lag compresses
the backlog instead of growing it.

### Consume

```http
GET /v1/apps/{app}/targets/{name}/lanes/{lane}/next?batch=100&wait_ms=1000
→ { "items": [ { "id": "...", "payload": {...} } ], "lease": [...],
    "target": "airbnb", "lane": "bulk" }
```

You get work only when the budget allows it. Long-polls until `wait_ms`. The answer
names the `target` and `lane` to settle it as — which matters for a graph, where the
node you popped is `messages` and the target is `airbnb.messages`. An ack that names
something else settles the work and tells you what it could not do with it.


```http
POST /v1/leases/ack
{ "lease": [...], "up_to": 100, "calls": 100, "outcome": "ok",
  "application": "channel-manager", "target": "airbnb", "lane": "bulk",
  "op": "calendar.push" }
```

The ack is the feedback loop, not bookkeeping: `calls` is what the work really
cost, and `outcome: "throttled"` is the vendor telling you the cap you enforce is
higher than the real one — the only evidence that your numbers are wrong.
Settlement is by prefix (`up_to`), never an arbitrary subset.

`POST /v1/leases/nack` returns work for redelivery and refunds it;
`POST /v1/leases/renew` extends a lease that is taking longer than expected.

### Watch

```http
GET /v1/apps/{app}/metrics
```

Per lane: what is waiting for budget, what is waiting for workers, the admitted
rate and a drain ETA — enough for another product to render limit status on its
own frontend. The console reads `/api/*` (overview, targets, flow, rollups,
traces, budgets); those are the same data shaped for a screen.

---

## The graph API

A graph is declared whole — nodes, edges, terminals and breach rules validated
together, provisioned atomically — because half a graph accepts work at its entry
and drops it at the hole.

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

A node is a target named `{graph}.{node}` and behaves exactly like one — same
queues, same gate, same lease. What a graph adds is the relay between them: one
transaction that acks upstream and pushes downstream, so an item is never lost
and never doubled; one relay per destination, draining its upstreams in strict
`priority` order while the destination's queue is shallower than a window, so
priority at the entrance is priority in fact.

```http
POST /v1/apps/{app}/graphs/{graph}/nodes/{node}/push   # entry nodes only
GET  /v1/apps/{app}/graphs/{graph}/nodes/{node}/next   # terminal nodes only
GET  /v1/apps/{app}/graphs/{name}                      # the graph, live
DELETE /v1/apps/{app}/graphs/{name}
```

Pushing into an interior node, or popping one, is a `409`: those queues belong to
the relays, and a caller on either end would skip a budget or steal the work.

**Breach rules** are what makes a retry paced instead of amplified. When an ack's
`outcome`/`status` matches a rule, Gate re-enters the item at the entry it came
in at — inside the ack's own transaction — and it waits for budget like anything
else. It re-pays every budget on its path, because the vendor counted the call
that failed. `maxAttempts` is not optional; past it the item is settled and the
breach is recorded.

`outcome` is one field for a whole lease, so a consumer that runs a batch and acks
once has to say WHICH items the vendor refused:

```http
POST /v1/leases/ack
{ "lease": [...], "outcome": "throttled", "status": 429,
  "breached": ["<message id>"], "target": "airbnb.ip", "lane": "default", … }
```

Without `breached` the outcome is taken to be about every item the ack settles —
right for a one-item ack, and forty-nine duplicate calls for a batch of fifty. The
answer says what happened: `retried`, `exhausted`, `unroutable`, and `refused` when
the graph declares nothing that could have been done. An impossible retry never
fails the ack: the work has already been made, and refusing to settle it would have
it made again.

Validation refuses, at declare time: a cycle, a node nothing can reach, a path
that ends nowhere a caller may pop, a `cost.max` that shrinks along an edge, a
budget-less node with no out-edge, a ceiling declared in two nodes (a `store: kv`
one excepted — that is one counter by construction), a `retryTo` that is not an
entry or could not admit what it receives, a path longer than three hops, an
unscoped budget in a sharded node, a sharded node fed by an edge, a second lane on
a node fed by an edge, two out-edges from one node (each edge is its own consumer
group, so a fan-out is a broadcast and not a split), two different `cost.field`
names, and a node name a standalone target already owns.

A node is also reachable by its target name, and the target routes refuse to serve
it: the entry and terminal rules live on the graph routes, and going round them is
how an item gets executed twice or skips a budget.

---

## The console

`/graphs` draws what is running: the nodes laid out in the order work moves
through them, each with its worst budget and its two backlogs, the edges labelled
with their priority and lag, and the retro edges drawn dashed back to the entries.
It is the same diagram the design was argued from, rendered from
`/api/apps/{app}/graphs/{name}`.

---

## Build

```bash
cd ui && npm ci && npm run build && cd ..
cargo build --release
```

The console is compiled into the binary, so `ui/dist` must exist before the Rust
build. `docker build` does both.

## Test

```bash
cargo test
```

The live suite is `#[ignore]`d, so that run reports it as ignored rather than as
passed: those tests need a broker, and a suite that silently verifies nothing is
worse than one that says it did not run. With a queen to point at:

```bash
GATE_TEST_QUEEN_URL=http://127.0.0.1:6632 cargo test -- --include-ignored
```

CI runs exactly that against a real broker
([.github/workflows/test.yml](.github/workflows/test.yml)), with
`GATE_TEST_REQUIRE_LIVE=1` so a missing broker fails the build instead of skipping
the only tests that cover the relay, the reconcile and the retro path.

