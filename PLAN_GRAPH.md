# PLAN_GRAPH — Gate as a declarative rate-limit DAG

Rev 1.0, 2026-08-19. Hand-off plan: this document is self-contained and is the
brief for the agent building it. It supersedes nothing; `README.md` still
describes what Gate is today. The historical `TARGET_SPEC.md` / `PLAN.md` were
deleted in commit `6e72f00` — recover them with `git show 0659881:TARGET_SPEC.md`
when a config-contract question is not answered here.

**What this builds.** Gate today is a single-stage limiter: a *target* is a push
queue, a gate runner per lane, an admitted queue. This plan turns targets into
**nodes of a declared DAG**: work enters at an entry node, traverses edges
(each hop re-checked against that node's budgets), is consumed at a terminal
node, and on a vendor 429 can **re-enter** the graph so the retry is paced
instead of amplified. Five phases, each independently shippable and useful.

**The driving use case** is Airbnb in channel-go (application
`channel-manager`): per-endpoint limits (messaging 100/min) nested inside
per-egress-IP limits (2000/10s … 4.5M/day), per-listing weekly limits (photos
100/listing/week), and priority (price pushes ahead of bulk) — all exact, no
ceiling replicated, no ceiling divided by accident.

---

## 0. Read first

| What | Where |
|---|---|
| Gate server | `crates/server/src/{api,gate,supervisor,registry,shared,meter,depth,store,history}.rs` |
| Pure engine + spec + validation | `crates/core/src/{engine,spec,validate}.rs` |
| Queen Rust SDK (streams, transaction wire) | `/Users/alice/Work/queen/clients/client-rust/src/` |
| The caller | `/Users/alice/Work/ciaochannel/channel-go/internal/platform/gate/` (Go client), `internal/app/syncer/{gate.go,gatespec.go}` (Airbnb wiring) |
| Integration tests against a live Gate | `/Users/alice/Work/ciaochannel/channel-go/test/gateintegration/` |
| Broker | queen 1.0.3 Rust (`ghcr.io/queen-mq/queen:1.0.3`), **must run `DEFAULT_SUBSCRIPTION_MODE=all`** — under the default `new`, a consumer group starts at the tail and skips every pending message |

Run the existing tests before touching anything: `cargo test` in `queen-rrl`,
and the Go suite `GATE_URL=http://127.0.0.1:8788 go test ./test/gateintegration/`
in channel-go (needs a running gate-server against a queen with kv enabled).

---

## 1. Verified ground truth — do not re-derive these wrong

Every line below was traced in code during design. Cite-checked at the listed
locations; re-verify if the file has moved, but do not assume the opposite.

| # | Fact | Where |
|---|---|---|
| T1 | Gate state is one JSON document per `(query_id, partition_id)`, cells at `state["b"][budget_id][scope_key]`, **rewritten whole every cycle**. | `engine.rs:85-115` |
| T2 | **Nothing prunes cells.** A scope key written once lives forever. `maxKeys` is checked only at declare (`GATE_MAX_KEYS = 5000`), never at runtime. | `engine.rs`, `validate.rs:26,165` |
| T3 | The partition lease makes each counter **single-writer**; that is the entire correctness argument. A counter shared across partitions loses updates silently. Never split a node that holds an unscoped budget into multiple gate partitions. | design invariant; `gate.rs` spawn |
| T4 | The gate fn is **synchronous** (`Fn(&Record, &mut GateCtx) -> bool`), cannot await, cannot do I/O. Anything consulted per-message must already be in memory. | client-rust `streams/mod.rs:352-357` |
| T5 | `lane_share`: `ceiling-minus-measured` resolves to **exactly its floor** for every measurement. Lanes *divide* a ceiling; they never borrow. Two lanes = every budget in the target enforced at its share. | `spec.rs:336-366` |
| T6 | `takers == 0` (no `ceiling`/`absolute` lane) makes every derived lane claim the whole residual: shares sum > 1, ceiling enforced multiple times, **validates clean today**. | `validate.rs:101-113` (missing rule) |
| T7 | The registry is a process-local `RwLock<HashMap>`, written only at `api.rs:217` (declare) and `main.rs:107` (boot restore). **No reconcile**: at replicas≥2 a PUT lands on one replica and the fleet enforces whichever spec is looser, indefinitely. | `registry.rs:56-58` |
| T8 | A failed `supervisor::start` inside declare leaves the old runtime **stopped but still registered**: the target accepts pushes and admits nothing. | `api.rs:211-217` |
| T9 | `store: kv` shared budgets join on `(application, budget id)`. The pool **drops `scope` and `match`** entirely, and deadlocks when `cap < 2 × periodSeconds` (integer `chunk/2` → threshold 0 → `top_up` never fires). `release()` runs only at target teardown, so fragmentation is permanent within a window: stranded fraction ≈ `1.5 × pools / periodSeconds`. Window is **fixed** regardless of declared alignment. | `supervisor.rs:86-95`, `shared.rs:85,91-93,124-130`, `supervisor.rs:107` |
| T10 | The engine **skips kv budgets** ("settled out of band"). | `engine.rs` decide |
| T11 | Ack already commits `ack + push` in **one atomic transaction** (the calls event). This is the exactly-once primitive for edges and retro. | `api.rs` ack handler, queen transaction wire |
| T12 | The lease returned by `next` **contains the full message payloads** (`serde_json::to_value(&msgs)`), and the ack posts them back. Server-side re-entry needs no payload round-trip. | `api.rs` do_next |
| T13 | `push` carries `txn` through to queen `PushItem.transaction_id` → per-partition dedup. Same-txn re-push inside the dedup window collapses. Relays and retries are idempotent through this. | `api.rs:452-462` |
| T14 | A deny breaks the batch for that lane-partition and keeps the lease; the denied item stays at head, everything behind it waits one lease. Deny is blind to sub-keys. | `gate.rs:96-131` |
| T15 | `do_sync` declares specs one at a time, in order, then reaps; any cross-target validation must tolerate mid-sync states. | `api.rs:253-287` |
| T16 | `crates/core/tests/validate.rs:53-56` currently **asserts the kv-scope defect** (sets `store: kv` on a high-cardinality scoped budget and expects it to pass). It must be inverted, not obeyed. | tests |

**The composition algebra** (drives every design decision):

- Budgets in **one node**, selected by `match`: AND evaluated at one instant —
  exact, no isolation (a deny parks the whole lane batch).
- An **edge** between nodes: AND evaluated in sequence — full isolation (each
  node parks its own queue), but *smeared*: downstream queueing delays age the
  upstream certificate. Whichever limit is enforced **last** is exact.
- Therefore: the severe limit (IP block = whole fleet) goes in the **terminal**
  node; mild limits (per-endpoint 429s) go upstream. Keep paths short — smear
  composes per hop.
- Parallel-AND across two nodes (exact + isolated simultaneously) is **not
  expressible** and never will be without distributed transactions. Per pair of
  limits you choose: same node (exact) or path (isolated).

---

## 2. The graph model

A **graph** is the new declarable resource. Nodes are targets (same anatomy,
same queues, internally named `{graph}.{node}`); edges are Gate-run relays;
`consume` lists the terminal nodes callers may pop; `breach` rules route
vendor throttles back into the graph.

```yaml
# The Airbnb graph this plan builds toward (phases 2-5 add the pieces)
# PUT /v1/apps/channel-manager/graphs/airbnb
version: 1
nodes:
  prices:                      # class node: isolation + priority, no own limit
    entry: true
    budgets: []                # legal for a node with out-edges (phase 2)
    cost:   { field: httpCost, default: 1, max: 100 }
  messages:
    entry: true
    budgets:
      - { id: msg-post, cap: 100, periodSeconds: 60, alignment: rolling,
          confidence: documented,
          source: "developer.withairbnb.com/homes/docs/rate-limits",
          asOf: "2026-05-19" }
    cost:   { field: httpCost, default: 1, max: 1 }
  photos:                      # phase 5: sharded per listing
    entry: true
    shardBy: entity
    shards: 64
    budgets:
      - { id: photo-del-weekly, cap: 100, periodSeconds: 604800,
          alignment: rolling, scope: [entity], maxKeys: 200000,
          confidence: documented,
          source: "developer.withairbnb.com/homes/docs/rate-limits",
          asOf: "2026-05-19" }
    cost:   { field: httpCost, default: 1, max: 1 }
  ip:                          # terminal: the severe limit, enforced last, ONE partition
    budgets:                   # 75% of vendor numbers: 25% reserved for the Airbnb
      - { id: ip-10s, cap: 1500,    periodSeconds: 10,    alignment: rolling, ... }
      - { id: ip-5m,  cap: 15000,   periodSeconds: 300,   alignment: rolling, ... }
      - { id: ip-1h,  cap: 150000,  periodSeconds: 3600,  alignment: rolling, ... }
      - { id: ip-1d,  cap: 3375000, periodSeconds: 86400, alignment: rolling, ... }
    cost:     { field: httpCost, default: 1, max: 100 }
    pacing:   { leaseSeconds: 1, batch: 200 }
    admitted: { partitionBy: connection, partitions: 64 }
edges:
  - { from: prices,   to: ip, priority: 0 }   # phase 4: high priority
  - { from: messages, to: ip, priority: 1 }
  - { from: photos,   to: ip, priority: 1 }
consume: [ ip ]
breach:                        # phase 3: retro edges
  - { when: { status: 429 }, retryTo: origin-entry, maxAttempts: 3 }
```

Why the caps are what they are: the IP budgets count **all** Airbnb traffic
from the egress, and today only part of it routes through Gate — reads, OAuth,
listing actions do not. `0.75` is `airbnbUnroutedReserve` in channel-go
(`internal/app/syncer/gatespec.go`), a named placeholder to be replaced by
measurement. Per-endpoint budgets whose traffic routes *fully* through Gate
(messaging, once wired) take the vendor's full number — the reserve applies per
budget, only to budgets whose traffic is partially outside.

The caller contract is unchanged and must stay unchanged: producers push to an
entry node, consumers pop a terminal and ack with the truth. Nobody outside
Gate learns there is a queue, let alone a graph.

---

## 3. Routes — complete table

### Existing (unchanged, from `api.rs routes()`)

| Verb + path | Semantics |
|---|---|
| `PUT /v1/apps/:app/targets` | declare the app's whole target set, reap the rest |
| `PUT/GET/DELETE /v1/apps/:app/targets/:name` | one target |
| `POST /v1/apps/:app/targets/:name/lanes/:lane/push` | `{op, key, cost, txn, payload}` |
| `GET /v1/apps/:app/targets/:name/lanes/:lane/next?batch&wait_ms` | long poll → `{items, lease}` |
| `POST /v1/leases/ack` | `{lease, up_to, calls, cost_estimated, outcome, target, lane, op, application}` |
| `POST /v1/leases/nack` | not attempted → back to lane + refund |
| `POST /v1/leases/renew` | extend for slow work |
| `/v1/targets…` flat variants | resolve in the default application |
| `GET /v1/apps/:app/metrics` | product-facing state (drain ETA) |
| `GET /api/{overview,targets,apps,budgets,breaches/recent,rollups,traces,me}` | console reads |
| `GET /health` | liveness |

### New (phase 2 unless noted)

| Verb + path | Semantics |
|---|---|
| `PUT /v1/apps/:app/graphs/:name` | declare the whole graph **atomically**: validate nodes+edges+breach+consume together, provision nodes, spawn relays. Returns resolved topology + warnings. Idempotent. Version rules in §4. |
| `GET /v1/apps/:app/graphs/:name` | the declared graph plus live per-node state: depths (push/admitted), budget utilisation, lane state, edge lag |
| `DELETE /v1/apps/:app/graphs/:name` | stop relays, drain, remove nodes |
| `POST /v1/apps/:app/graphs/:g/nodes/:node/push` | push into an **entry** node; `409` for interior nodes (their push queues belong to relays) |
| `GET /v1/apps/:app/graphs/:g/nodes/:node/next?batch&wait_ms` | pop a **terminal** node's admitted queue; `409` for interior nodes (their admitted queues belong to relays — a caller popping one steals from the graph) |
| `POST /v1/leases/ack` **(extended, phase 3)** | gains nothing on the wire for the common case; when `outcome` matches a declared `breach` rule, Gate applies the rule's `retryTo` server-side (T12: the lease already carries payloads). An explicit `retryTo` field is accepted for overrides and validated against the graph. |
| `GET /api/graphs` | console list: name, nodes, edges, per-node depth — enough to render the live diagram |
| `GET /api/graphs/:name/topology` | nodes+edges+consume as data for the console's graph view |

Entry/terminal enforcement is not cosmetic: interior queues are Gate-internal,
and the 409s are what keep the graph's invariants (exactly-once relay, single
consumer group per edge) from being violated by a well-meaning caller.

---

## 4. Graph validation (extends `validate.rs`; graph-level checks live in the server because they need the registry)

Numbered like the old TARGET_SPEC §9 — every rule converts a silent failure
into a refusal:

| # | Rule | Failure prevented |
|---|---|---|
| G1 | forward edges form a DAG (no cycles); retro (`retryTo`) edges are exempt but require `maxAttempts ≥ 1` | infinite traversal / retry livelock |
| G2 | every node reachable from an entry; every path ends in a `consume` node | work that enters and can never leave |
| G3 | `cost.max` non-decreasing along every forward edge | an item admitted upstream that the downstream node can never admit → parks its lane head forever |
| G4 | a node with zero budgets is legal **iff** it has ≥1 out-edge (scheduler node) | otherwise a no-op node that admits everything to a consumer directly |
| G5 | `retryTo` targets an entry node of the same graph (`origin-entry` = the item's own entry, stamped on first push) | re-entry that skips upstream budgets: a 429'd call is a NEW call and re-pays the whole path |
| G6 | path length ≤ 3 forward hops | smear composes per hop; latency floor is ~1 lease per hop |
| G7 | `shardBy` present ⇒ **every** budget in that node carries the shard dim in `scope` (phase 5) | an unscoped budget in a sharded node = one counter per shard = ceiling × shards (T3) |
| G8 | `maxKeys / shards ≤ GATE_MAX_KEYS` per shard (phase 5) | state document unbounded per cycle rewrite (T1) |
| G9 | the target-level rules run per node exactly as today (cost-fits, batch-fits, default-lane, lane-shares incl. the new `takers == 0` rule, provenance, kv quarantine) | all of the existing silent failures |
| G10 | a graph node name may not collide with a standalone target in the same application | two owners of one queue family |

Versioning: the graph document carries one `version`. Any change that is
migration-class for any node (period/alignment/scope/store of an existing
budget, `partitionBy`, node removal, forward-edge rewiring) requires a version
bump; the declare is refused in place without one, exactly like targets today.

---

## 5. The phases

Each phase: goal → changes (file-level) → tests → acceptance. Ship in order;
1 is a prerequisite for every cross-node validation in 2.

### Phase 1 — the substrate (correctness fixes, no new features)

All four are live bugs today at `helm/gate` `replicas: 2`.

**1a. Registry reconcile (~40 lines, `main.rs`).** Background task, every 15s:
`store::load_all`, diff against `registry` by `TargetSpec` equality (it derives
`PartialEq`), stop-and-restart changed runtimes under a `declare_lock` shared
with the declare handler. Without this a cap change takes effect on one replica
and the fleet enforces the looser spec forever (T7).
*Test:* two `Shared` instances on one queen; declare a tightened cap on A;
B's registry matches within one interval. No two-process test exists today.

**1b. Restore on failed provisioning (~10 lines, `api.rs` declare).** If
`supervisor::start` for the new spec fails, restart the old runtime; if that
also fails, **unregister** the target so pushes are refused (recoverable) rather
than accepted into a queue nobody drains (not recoverable) (T8).
*Test:* declare against a queen refusing `configure`; target still serves the
old spec; push still admits.

**1c. `takers == 0` validation (~10 lines, `validate.rs`).** Reject a spec
where no lane claims the ceiling and reservations sum < 1 (T6). Plus the
property test the code comments claim but nothing holds: for every spec that
validates clean, `Σ lane_share ≤ 1 + ε`.

**1d. Cell expiry (~15 lines, `engine.rs`).** Drop cells whose window ended
more than one period ago; run once per cycle (detect cycle boundary via the
once-per-cycle `stream_time_ms`, tracked in a reserved top-level state field —
not `__`-prefixed, that namespace belongs to the runtime). Without it, scoped
budgets grow state without bound regardless of `maxKeys` (T2).
*Test:* write cells for 3 keys in window W, advance clock 2 periods, one
decide() call → stale cells gone, live key intact.

**1e. kv quarantine (~35 lines, `validate.rs`).** Reject `store: kv` +
non-empty `scope` (dropped silently — T9); reject `store: kv` + `match` (pool
charges every op — T9); reject `store: kv` with `cap < 2 × periodSeconds`
(chunk deadlock — T9). Reword `store-fits` so it stops recommending kv for
scoped budgets. **Invert the test that asserts the defect** (T16).

### Phase 2 — edges and the graph resource

**The edge runner** (`crates/server/src/edge.rs`, new, ~150 lines): one task
per edge per replica, queen consumer group `gate.edge.{app}.{graph}.{from}.{to}`
on `from`'s admitted queue. Per message: **one transaction** `{ack(admitted),
push(to.push, txn = message txn)}` (T11). Crash before commit → lease expiry →
redelivery; after → txn dedup absorbs it (T13). The relay never interprets the
payload; it forwards `op`, cost field, `key` verbatim (Gate merged them into
the payload at the original push).

**The graph resource** (`api.rs` + `crates/core/src/graph.rs`, ~200 lines):
the spec types of §2, the routes of §3, the validation of §4. Provision nodes
as targets named `{graph}.{node}`; spawn/stop relays with the node runtimes;
persist the graph document in the store alongside targets so the phase-1
reconcile loop restores relays too.

**Airbnb example for this phase** — first real two-node chain, live the day
messaging routes through Gate:

```yaml
nodes:
  messages: { entry: true, budgets: [ msg-post 100/60s documented ],
              cost: { field: httpCost, default: 1, max: 1 } }
  ip:       { budgets: [ ip-10s 1500/10s, ip-5m 15000/300s,
                          ip-1h 150000/3600s, ip-1d 3375000/86400s ],
              cost: { field: httpCost, default: 1, max: 100 },
              pacing: { leaseSeconds: 1, batch: 200 },
              admitted: { partitionBy: connection, partitions: 64 } }
edges:   [ { from: messages, to: ip } ]
consume: [ ip ]
```

Arithmetic the validator must accept: `messages` batch-fits — tightest rate
100/60 = 1.67/s × 1s lease → batch ≥ 2 (default 200 fine); cost-fits 100 ≥ 1.
`ip` batch-fits — slowest budget 3,375,000/86,400 = 39.06/s → batch ≥ 40
(200 ✓); cost-fits — min cap 1500 ≥ cost.max 100 ✓. G3: 1 ≤ 100 along the
edge ✓.

*Tests:* e2e — push into `messages`, consume at `ip`, item carries the original
txn; replay the same relay transaction → no duplicate downstream; cycle
declared → refused; `cost.max` inversion along an edge → refused; pop of an
interior admitted queue → 409.

### Phase 3 — retro edges (paced retries)

Extend `AckBody` handling: when the ack's `outcome` matches a `breach` rule
with `retryTo`, run **one transaction** `{ack(admitted), push(retry-entry,
txn = orig + ":r{attempt}")}` using the payloads already in the lease (T12).
Stamp `origin-entry` and `attempt` into the payload at first push; refuse
(park to DLQ trace, count, and settle) when `attempt > maxAttempts`.

This is the structural fix for the discovery finding that retries amplify
(a 429 became up to 6 delay-free redeliveries): a 429'd item re-enters at its
class node and *waits for budget* — the pacing is the backoff. G5 makes the
re-entry re-pay every budget on the path, which is correct: the vendor counted
the failed call.

**Airbnb example:** `breach: [{ when: { status: 429 }, retryTo: origin-entry,
maxAttempts: 3 }]`. channel-go's consumer changes only its classifier: on an
Airbnb 429 it acks `outcome: throttled` instead of swallowing the error —
`internal/ota/adapters/airbnb` has **zero** 429 handling today; the classifier
must be added there regardless of this phase.

*Tests:* ack throttled → item reappears on the entry push queue with `:r1`
suffix and same payload; 4th attempt → not re-enqueued, breach trace recorded;
retro edge to a non-entry node → refused at declare.

### Phase 4 — priority at the merge

Replace per-edge relays *into the same node* with **one merge relay per
destination node**: drain upstream admitted queues in strict `priority` order
(0 first), and forward only while `pending(dest.push) < window`, where
`window ≈ 2 × (tightest gate-stored budget rate × leaseSeconds)` — read from
`depth.rs` pending counts. The bottleneck queue stays shallow, so priority at
the entrance is priority in fact; the FIFO and its single counter stay exact
(T3, T14).

**Airbnb example:** `prices → ip` at priority 0, `messages`/`photos` at 1.
`ip`'s window: tightest rate 150/s × 1s × 2 = 300 items. A calendar flood
sits in its own upstream queue; a price stop-sell overtakes everything not yet
forwarded.

*Tests:* saturate via priority-1 edge, inject one priority-0 item → admitted
within 2 lease windows; `pending(dest.push)` never exceeds `window` ± one batch.

### Phase 5 — `shardBy` (per-key limits at any cardinality)

Node attrs `shardBy: dim, shards: N`: the node's push queue gets N partitions
per lane (`{lane}:{hash(dim) % N}`), one gate runner per partition, state per
shard (small documents — T1). Entry push route computes the shard from the
item's dim value (refuse the push if the dim is absent, matching engine
behaviour). Edges out of a sharded node: the relay consumes all N admitted
partitions (order preserved per partition). Validation G7/G8.

**Airbnb example:** `photos` with `shardBy: entity, shards: 64`,
budget `photo-del-weekly 100/604800s scope [entity] maxKeys 200000` →
200,000/64 = 3,125 keys per shard ≤ 5,000 ✓. The per-app photos budget
(10,000/hour POST) must NOT live in this node (G7) — it goes in a downstream
unsharded node or stays on `ip`'s path.

*Tests:* two entities hashing to different shards admit concurrently; the same
entity serialises; a per-app (unscoped) budget declared in a sharded node →
refused; per-shard state stays under the per-shard key bound after expiry (1d).

---

## 6. channel-go integration (per phase)

- **Phases 1: nothing.** The current single target `airbnb` v2 (one lane,
  four IP budgets at 75%, `cost.max` 100) keeps working unchanged; reconcile
  makes its future cap changes actually land on both Gate replicas.
- **Phase 2:** Go client gains `DeclareGraph` + graph push/next paths
  (`internal/platform/gate/`). The Airbnb *calendar* flow may stay on the flat
  target — a one-node graph is equivalent — and migrates when messaging ships:
  declare the graph alongside the target, point the consumer at the graph
  terminal, drain the old target, delete it. Never run both accepting pushes
  for the same traffic class (two independent ordered channels per connection).
- **Phase 3:** the Airbnb adapter gets a 429 classifier (none exists) and the
  consumer runtime maps it to `outcome: throttled`. The always-positive-ack
  rule stays: retro is applied by Gate at ack time, not by a client nack.
- **Phases 4–5:** config only (graph document), no Go changes.

## 7. What must never be done

1. Never declare the same unscoped budget (e.g. the IP ceiling) in more than
   one node/target: each copy is its own counter — the ceiling multiplies (T3).
2. Never add a second lane to a node without dividing its budgets on purpose:
   lanes divide, `ceiling-minus-measured` is exactly its floor (T5).
3. Never build an edge as separate `ack` + `push` calls — only the transaction
   wire (T11); and never let a caller pop an interior admitted queue.
4. Never route a synchronous request path (e.g. channel-go's
   `feedhttp/proxy.go` booking proxy) into any graph: the latency floor is one
   lease per hop.
5. Never ship a retro edge without `maxAttempts`.
6. Never let graph queues pass through channel-go's `bus.EnsureQueue` (it
   force-upserts queue config, including the lease, on every boot).

## 8. Definition of done

- `cargo test` green including: the Σ-shares property test, the inverted kv
  test, two-instance reconcile, edge exactly-once (replayed transaction),
  cycle/monotonicity/shard-scope refusals, retro attempt cap, priority-window
  bound, shard distribution.
- The full Airbnb graph of §2 declares clean with an **empty warnings array**,
  and `GET /api/graphs/airbnb` renders nodes, edges, depths.
- channel-go `make test` green, and `test/gateintegration` green against a
  gate-server running this build (it declares the real `AirbnbTarget()` and
  round-trips items).
- The console can draw the graph live — the drawing this plan came from,
  rendered from `/api/graphs/:name/topology`.
