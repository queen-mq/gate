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
it, inside one application. The key is what makes it shared.

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

`GET` and `DELETE` on the same path read and remove one target. Deleting stops
the runners and forgets the declaration; work already pushed stays in the broker.

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
→ { "items": [ { "id": "...", "payload": {...} } ], "lease": [...] }
```

You get work only when the budget allows it. Long-polls until `wait_ms`.

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

## Build

```bash
cd ui && npm ci && npm run build && cd ..
cargo build --release
```

The console is compiled into the binary, so `ui/dist` must exist before the Rust
build. `docker build` does both.
