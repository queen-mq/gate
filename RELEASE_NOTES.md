# Gate release notes

## Unreleased

Gate adds guided target setup and traffic insights to help configure rules,
observe demand before choosing limits, and investigate delivery delays.

### Console and target setup

- **Guided rule editor.** Switch between a form and JSON while keeping the same
  draft. Configure queues, budgets, costs and paths with field validation.
- **New target wizard.** Create a target through identity, queue configuration,
  limits or observation settings, and a final review. Start from a global,
  per-account, per-operation or shared-budget template. Example allowances are
  marked as assumptions and remain editable.
- **Cloning.** Copy a graph's rules and paths into a new draft. The clone starts
  with a new identity and Watch timer, uses its own ingress queues, and requires
  outgoing queue names. Shared budget keys are preserved for review.
- **Integration guide.** After creation, see the resolved queue names and worker
  consumer group, with JavaScript producer and consumer examples and an HTTP
  example when HTTP push is enabled. The guide remains available on the target.

### Watch and traffic insights

- **Watch mode.** Relay traffic without applying rate limits or ingress load
  shedding while observing demand for two minutes to 30 days. The observation
  timer survives redeclarations and restarts. Its deadline marks the target ready
  for review; limits become active only after an explicit configuration change.
- **Limit simulation.** Replay Watch samples against a candidate global allowance
  and estimate delayed items, peak and remaining backlog, and drain time without
  new arrivals. Previewing a limit leaves the running configuration unchanged.
  This first version supports one path with a full share and a fixed item cost;
  scoped, operation-specific, shared and multiple-path policies are outside the
  simulation's scope.
- **Traffic diagnostics.** Identify broker availability problems, stopped relays,
  repeated cursor failures, active provider backoff, budget refusals and work
  waiting for consumers. Findings include a suggested next check and a link to
  the relevant node or outgoing queue in the topology. Duration is measured from
  first detection on the responding replica.
- **Traffic history.** View incoming estimates, relayed items, relay and worker
  backlog, and the age of the oldest pending message over 15 minutes, 1 hour,
  6 hours or 24 hours. Configuration changes appear as chart annotations.
  Unavailable measurements remain gaps instead of appearing as zero traffic.
- **Recording improvements.** Persisted Watch samples combine contributions from
  multiple replicas without double-counting retries. Minute history records idle
  periods explicitly, and counter checkpoints distinguish restarted relays.

### Upgrade notes

- Gate-owned ingress and interior queues now enable 30-day retention for messages
  already passed by every consumer group. Pending work has no age limit. The
  policy is applied during graph provisioning and restoration; application-owned
  ingress and egress queues keep their independently configured retention.
- Watch and persisted history require Gate's PostgreSQL history connection.
  Additional tables and indexes are created automatically when history connects.
  Queue history requires counters, enabled automatically by Watch and wizard
  templates.
- Upgrade every Gate replica before declaring Watch targets. Older builds reject
  the new `watch` field. Changing between Watch and enforcement requires a higher
  graph version; the guided editor handles that increment. Unchanged routing
  retains the same queues and consumer groups.
- Simulations use samples recorded every second and cover up to the last 24 hours
  of an observation. They start with an empty queue and cannot reconstruct bursts
  within a second, delays already present during Watch, or older minute-only
  history. Samples are flushed every ten seconds; a crash can lose unflushed
  data, and known recording gaps are shown in the preview.
- Incoming traffic is estimated from consumer positions for nodes with a single
  ingress path. Retention and cursor changes can affect it. Message age is shown
  only when the relevant broker timestamps are available.
- Fine Watch samples, queue snapshots and configuration annotations are retained
  for 31 days. Minute rollups retain their existing 90-day history.

### Validation

- 274 Rust tests passed, including 60 integration tests against Queen.
- An additional Queen integration test verifies the 30-day retention settings on
  creation and redeclaration, and preserves application-owned queue configuration.
- 21 UI tests passed, along with frontend and backend builds and Clippy checks.
- Browser checks covered creation from a template, cloning, integration examples,
  simulation, diagnosis links, history charts and the mobile layout.

See the [README](README.md#target-setup-and-operations) for setup instructions and
the read-only diagnostics, timeline and simulation endpoints.
