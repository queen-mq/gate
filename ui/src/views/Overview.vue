<script setup>
import { ref, computed, watch } from 'vue'
import FlowChart from '../components/FlowChart.vue'
import SpatialGraph from '../components/dashboard/SpatialGraph.vue'
import StatusDot from '../components/StatusDot.vue'
import Icon from '../components/Icon.vue'
import {
  api,
  num,
  rate,
  pct,
  ago,
  traceRef,
  traceRefPath,
  graphApi,
  DEFAULT_APP,
  isAdmin,
} from '../lib/api.js'
import { usePoll } from '../lib/poll.js'
import { targetKey, targetUsage, breachMatches } from '../lib/dashboard.js'

const overview = ref(null)
const targets = ref(null)
const breaches = ref(null)
const errors = ref({ overview: '', targets: '', breaches: '' })
const updatedAt = ref(null)
const loading = ref(false)
const query = ref('')
const attentionOnly = ref(false)
const selection = ref('')
const graph = ref(null)
const graphError = ref('')
const graphLoading = ref(false)

async function load() {
  loading.value = true
  const results = await Promise.allSettled([
    api.get('/api/overview'),
    api.get('/api/targets'),
    api.get('/api/breaches/recent?limit=10'),
  ])
  const refs = [overview, targets, breaches]
  const keys = ['overview', 'targets', 'breaches']
  results.forEach((result, i) => {
    if (result.status === 'fulfilled') {
      const value = result.value
      const valid =
        i === 0
          ? value && typeof value.queen?.reachable === 'boolean'
          : i === 1
            ? Array.isArray(value)
            : Array.isArray(value) || Array.isArray(value?.breaches)
      if (!valid) {
        errors.value[keys[i]] = 'The API returned an invalid response.'
        return
      }
      refs[i].value =
        i === 2
          ? Array.isArray(value)
            ? value
            : (value?.breaches ?? [])
          : value
      errors.value[keys[i]] = ''
    } else
      errors.value[keys[i]] =
        result.reason?.message || 'Could not refresh this data.'
  })
  if (Object.values(errors.value).every((error) => !error))
    updatedAt.value = Date.now()
  loading.value = false
}
const refresh = usePoll(load)
const failed = computed(() => Object.values(errors.value).some(Boolean))
const selectedTarget = computed(() =>
  (targets.value ?? []).find((t) => targetKey(t) === selection.value),
)
watch(targets, (list) => {
  if (!(list ?? []).some((t) => targetKey(t) === selection.value))
    selection.value = list?.length ? targetKey(list[0]) : ''
})
async function loadGraph() {
  const target = selectedTarget.value
  if (!target) return
  const key = targetKey(target)
  graphLoading.value = true
  try {
    const result = await api.get(graphApi(target.application, target.name))
    if (selection.value !== key) return
    if (!Array.isArray(result?.nodes) || !Array.isArray(result?.paths))
      throw new Error('The API returned an invalid topology.')
    graph.value = result
    graphError.value = ''
  } catch (error) {
    if (selection.value === key) graphError.value = error.message
  } finally {
    if (selection.value === key) graphLoading.value = false
  }
}
const refreshGraph = usePoll(loadGraph)
watch(selection, () => {
  graph.value = null
  graphError.value = ''
  graphLoading.value = !!selection.value
  refreshGraph()
})
const recentThrottle = (t) =>
  (breaches.value ?? []).some((b) => breachMatches(b, t))
const needsAttention = (t) =>
  recentThrottle(t) ||
  ['down', 'breached', 'saturating'].includes(t.state) ||
  t.assumed_budgets > 0
const attention = computed(() => (targets.value ?? []).filter(needsAttention))
const filtered = computed(() =>
  (targets.value ?? []).filter(
    (t) =>
      (!attentionOnly.value || needsAttention(t)) &&
      `${t.application || DEFAULT_APP} ${t.name}`
        .toLowerCase()
        .includes(query.value.trim().toLowerCase()),
  ),
)
const paths = computed(() =>
  (targets.value ?? []).reduce(
    (sum, t) => sum + (t.paths ?? t.lanes ?? []).length,
    0,
  ),
)
const backlog = computed(() =>
  (targets.value ?? []).reduce((sum, t) => sum + (t.backlog ?? 0), 0),
)
const peak = computed(() => {
  const values = (targets.value ?? [])
    .map(targetUsage)
    .filter((v) => v !== null)
  return values.length ? Math.max(...values) : null
})
const unreadable = computed(
  () => (targets.value ?? []).filter((t) => targetUsage(t) === null).length,
)
const status = computed(() => {
  if (overview.value?.queen?.reachable === false && !errors.value.overview)
    return {
      state: 'down',
      title: 'Broker unreachable',
      sub: 'Queen is not answering. Live queue and budget readings may be unavailable.',
    }
  if (failed.value)
    return {
      state: 'degraded',
      title: 'Some readings are unavailable',
      sub: 'Last known values are marked below. A failed read is not evidence of healthy traffic.',
    }
  if (!overview.value || !targets.value || breaches.value === null)
    return {
      state: 'idle',
      title: 'Connecting to Gate…',
      sub: 'Reading your graphs, their budgets and recent backoffs.',
    }
  if (!targets.value.length)
    return {
      state: 'idle',
      title: 'Ready for your first graph',
      sub: 'Declare a graph to see its paths and budget usage here.',
    }
  const down = targets.value.filter((t) => t.state === 'down').length
  if (down)
    return {
      state: 'down',
      title: `${down} graph${down === 1 ? '' : 's'} not running`,
      sub: 'Inspect the affected graphs and their stages.',
    }
  const throttled = targets.value.filter(
    (t) => recentThrottle(t) || t.state === 'breached',
  ).length
  if (throttled)
    return {
      state: 'breached',
      title: `${throttled} target${throttled === 1 ? '' : 's'} with reported backoffs`,
      sub: 'A vendor reported a throttle. Inspect the graph for active holds and review its limits.',
    }
  const growing = targets.value.filter((t) => t.state === 'saturating').length
  if (growing)
    return {
      state: 'saturating',
      title: `${growing} target${growing === 1 ? '' : 's'} with a growing backlog`,
      sub: 'Work is arriving faster than its budget allows it out.',
    }
  const assumed = targets.value.reduce(
    (sum, t) => sum + (t.assumed_budgets ?? 0),
    0,
  )
  if (assumed)
    return {
      state: 'blind',
      title: `${assumed} budget${assumed === 1 ? '' : 's'} based on assumptions`,
      sub: 'Capacity depends on limits that have not been confirmed by the vendor.',
    }
  if (unreadable.value)
    return {
      state: 'idle',
      title: 'Some budget usage is unknown',
      sub: 'Not every target has a readable aggregate budget. Inspect its scope and counters.',
    }
  if (targets.value.some((t) => t.state === 'pacing'))
    return {
      state: 'pacing',
      title: 'Traffic is pacing at its limits',
      sub: 'The limiter is holding work for available budget. Nothing here implies lost work.',
    }
  return {
    state: 'flowing',
    title: 'Every target under its caps',
    sub: 'No backlog or vendor backoff has been reported.',
  }
})
const cardState = (t) =>
  errors.value.targets || errors.value.breaches
    ? 'idle'
    : recentThrottle(t)
      ? 'breached'
      : t.assumed_budgets
        ? 'blind'
        : t.state
const label = (t) =>
  errors.value.targets
    ? 'last known'
    : errors.value.breaches
      ? 'backoffs unknown'
      : recentThrottle(t)
        ? 'backoff reported'
        : undefined
const colour = () => 'var(--brand)'
function clearFilters() {
  query.value = ''
  attentionOnly.value = false
}
</script>

<template>
  <div class="live-dashboard">
    <header class="dashboard-heading">
      <div>
        <div class="dashboard-eyebrow"><span></span>TRAFFIC CONTROL</div>
        <h1>Everything in flow.</h1>
        <p>Your traffic, its ceilings, and a clearer perspective.</p>
      </div>
      <div class="dashboard-heading-actions">
        <span class="dashboard-live" role="status"
          ><i :class="failed ? 'warn' : overview ? 'good' : 'muted'"></i
          >{{
            failed
              ? 'Refresh failed'
              : overview
                ? 'Live · every 4s'
                : 'Connecting…'
          }}</span
        ><button class="btn" :disabled="loading" @click="refresh">
          {{ loading ? 'Refreshing…' : 'Refresh' }}
        </button>
      </div>
    </header>

    <section
      class="dashboard-health"
      :class="`health-${status.state}`"
      role="status"
    >
      <StatusDot :state="status.state" :label="status.title" />
      <p>{{ status.sub }}</p>
    </section>
    <div v-if="failed" class="dashboard-errors" role="alert">
      <p v-for="(error, key) in errors" :key="key">
        <template v-if="error"
          ><strong>{{ key }}:</strong> {{ error }}</template
        >
      </p>
      <p v-if="updatedAt">Last complete refresh {{ ago(updatedAt) }}.</p>
    </div>

    <section class="dashboard-kpis" aria-label="Traffic summary">
      <article class="card dashboard-kpi">
        <div>Admitted throughput<Icon name="breach" :size="16" /></div>
        <strong
          >{{
            overview?.admitted_per_sec == null
              ? '—'
              : rate(overview.admitted_per_sec)
          }}<small v-if="overview?.admitted_per_sec != null">/s</small></strong
        >
        <p>
          {{
            errors.overview
              ? 'Last known · refresh failed'
              : overview?.history_error
                ? 'History unavailable'
                : overview?.admitted_per_sec == null
                  ? 'Rate unavailable · counters may be off'
                  : 'Stage admissions · not unique messages'
          }}
        </p>
      </article>
      <article class="card dashboard-kpi">
        <div>Configured paths<Icon name="graph" :size="16" /></div>
        <strong
          >{{ targets === null ? '—' : num(paths) }}<small>paths</small></strong
        >
        <p>
          {{
            errors.targets
              ? 'Last known · refresh failed'
              : targets === null
                ? 'Reading graph inventory'
                : `${num(targets.length)} graphs across your applications`
          }}
        </p>
      </article>
      <article class="card dashboard-kpi">
        <div>Waiting for budget<Icon name="lane" :size="16" /></div>
        <strong
          >{{ targets === null ? '—' : num(backlog)
          }}<small>queued</small></strong
        >
        <p>
          {{
            errors.targets
              ? 'Last known · refresh failed'
              : 'Stage backlogs · excludes worker queues'
          }}
        </p>
      </article>
      <article class="card dashboard-kpi">
        <div>Peak budget usage<Icon name="gauge" :size="16" /></div>
        <strong
          >{{ pct(peak)
          }}<svg viewBox="0 0 70 40" class="dashboard-gauge" aria-hidden="true">
            <path d="M7 35A28 28 0 0 1 63 35" />
            <path
              v-if="peak !== null"
              d="M7 35A28 28 0 0 1 63 35"
              class="dashboard-gauge-fill"
              pathLength="100"
              :stroke-dasharray="`${Math.min(100, peak * 100)} 100`"
            /></svg
        ></strong>
        <p>
          {{
            errors.targets
              ? 'Last known · refresh failed'
              : unreadable
                ? `${unreadable} target readings unavailable`
                : 'Busiest unscoped budget'
          }}
        </p>
      </article>
    </section>
    <p v-if="overview?.history_error" class="dashboard-errors" role="alert">
      Throughput history: {{ overview.history_error }}
    </p>

    <div v-if="selectedTarget" class="dashboard-topology-selection">
      <label for="dashboard-graph">Explore a graph</label
      ><select id="dashboard-graph" v-model="selection" class="input">
        <option v-for="t in targets" :key="targetKey(t)" :value="targetKey(t)">
          {{ t.application || DEFAULT_APP }} / {{ t.name }}
        </option></select
      ><span v-if="graphLoading && graph">Refreshing topology…</span>
    </div>
    <p v-if="graphError" class="dashboard-errors" role="alert">
      Topology: {{ graphError }}
      <button class="btn btn-sm" :disabled="graphLoading" @click="refreshGraph">
        Retry
      </button>
    </p>
    <SpatialGraph
      v-if="graph && selectedTarget"
      :graph="graph"
      :stale="
        !!graphError ||
        !!errors.targets ||
        !!errors.overview ||
        overview?.queen?.reachable !== true
      "
      detail-link
    />
    <div
      v-else-if="selectedTarget && !graphError"
      class="card dashboard-topology-loading"
      role="status"
    >
      <div class="skeleton"></div>
      <p>Reading the declared topology…</p>
    </div>
    <div
      v-else-if="targets?.length === 0 && !errors.targets"
      class="card dashboard-empty"
    >
      <Icon name="graph" :size="32" />
      <h2>Your first flow starts here.</h2>
      <p>Graphs connect your entry queues, budgets and consumers.</p>
      <RouterLink v-if="isAdmin" to="/graphs/new" class="btn btn-primary"
        >Declare a graph<Icon name="plus" :size="14" /></RouterLink
      ><RouterLink v-else to="/graphs" class="btn"
        >View graph inventory</RouterLink
      >
    </div>

    <section
      class="dashboard-targets"
      aria-labelledby="dashboard-targets-title"
    >
      <header class="dashboard-section-heading">
        <h2 id="dashboard-targets-title">
          Your targets
          <span>{{ targets === null ? '—' : num(targets.length) }}</span>
        </h2>
        <div class="dashboard-filters">
          <button
            class="btn btn-sm"
            :aria-pressed="attentionOnly"
            :class="{ 'dashboard-filter-active': attentionOnly }"
            @click="attentionOnly = !attentionOnly"
          >
            Needs attention <span>{{ attention.length }}</span></button
          ><label class="dashboard-search"
            ><Icon name="search" :size="14" /><input
              v-model="query"
              placeholder="Find a target…"
              aria-label="Filter targets"
          /></label>
        </div>
      </header>
      <div
        v-if="targets === null && !errors.targets"
        class="dashboard-target-grid"
        role="status"
        aria-label="Loading targets"
      >
        <div v-for="i in 4" :key="i" class="card dashboard-target-skeleton">
          <div class="skeleton"></div>
        </div>
      </div>
      <div v-else-if="filtered.length" class="dashboard-target-grid">
        <button
          v-for="t in filtered"
          :key="targetKey(t)"
          class="dashboard-target card"
          :class="{
            'is-selected': selection === targetKey(t),
            'is-stale': errors.targets,
          }"
          :aria-pressed="selection === targetKey(t)"
          @click="selection = targetKey(t)"
        >
          <div class="dashboard-target-top">
            <span
              class="dashboard-target-monogram"
              :style="{ color: colour(t) }"
              >{{ t.name.slice(0, 1).toUpperCase() }}</span
            >
            <div>
              <h3>{{ t.name }}</h3>
              <p>{{ t.application || DEFAULT_APP }}</p>
            </div>
            <Icon name="chevron" :size="15" />
          </div>
          <div class="dashboard-target-usage">
            <strong>{{ pct(targetUsage(t)) }}<small>capacity</small></strong
            ><StatusDot :state="cardState(t)" :label="label(t)" />
          </div>
          <div
            class="dashboard-target-meter"
            :class="{ unknown: targetUsage(t) === null }"
          >
            <span
              v-if="targetUsage(t) !== null"
              :style="{
                width: `${Math.min(100, targetUsage(t) * 100)}%`,
                background: colour(t),
              }"
            ></span>
          </div>
          <div class="dashboard-target-foot">
            <span>{{ num(t.backlog) }} waiting for budget</span
            ><span>{{ (t.paths ?? t.lanes ?? []).length }} paths</span>
          </div>
          <p v-if="t.assumed_budgets" class="dashboard-target-assumed">
            {{ t.assumed_budgets }} assumed budget{{
              t.assumed_budgets === 1 ? '' : 's'
            }}
          </p>
        </button>
      </div>
      <div v-else-if="targets?.length" class="card dashboard-no-results">
        <p>No targets match these filters.</p>
        <button class="btn" @click="clearFilters">Clear filters</button>
      </div>
    </section>

    <div class="dashboard-bottom">
      <div class="dashboard-history"><FlowChart /></div>
      <section class="dashboard-backoffs card">
        <header class="dashboard-section-heading">
          <div>
            <h2>Recent backoffs</h2>
            <p>Signals reported by your vendors.</p>
          </div>
          <Icon name="trace" :size="17" />
        </header>
        <p v-if="errors.breaches" class="dashboard-errors" role="alert">
          Backoff history unavailable. {{ errors.breaches }}
        </p>
        <div
          v-else-if="breaches === null"
          class="dashboard-backoff-empty"
          role="status"
        >
          Reading recent backoffs…
        </div>
        <div v-else-if="!breaches.length" class="dashboard-backoff-empty">
          <div class="dashboard-quiet-orbit">
            <Icon name="check" :size="26" />
          </div>
          <h3>No backoffs reported.</h3>
          <p>
            {{
              overview?.queen?.reachable === false
                ? 'The broker is unreachable. Current health is unknown.'
                : 'This feed records vendor reports; an empty feed does not prove every limit is correct.'
            }}
          </p>
        </div>
        <div v-else class="dashboard-backoff-list">
          <RouterLink
            v-for="(b, i) in breaches"
            :key="`${b.at}-${i}`"
            :to="traceRefPath(b)"
            ><span class="dashboard-event-icon"
              ><Icon name="pause" :size="14"
            /></span>
            <div>
              <strong>{{ traceRef(b).name }}</strong>
              <p>
                {{ traceRef(b).application || 'Unscoped report' }} · backoff
                {{ b.retryAfterSeconds }}s
              </p>
              <small v-if="b.by">{{ b.by }}</small>
            </div>
            <time>{{ ago(b.at) }}</time></RouterLink
          >
        </div>
      </section>
    </div>
    <footer class="dashboard-footer">
      <span>Built for flow.</span
      ><span
        >Live counters and declared limits <span>·</span> Motion is
        illustrative</span
      >
    </footer>
  </div>
</template>
