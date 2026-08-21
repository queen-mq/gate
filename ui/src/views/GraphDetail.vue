<script setup>
/*
  The live graph: the diagram, the nodes behind it, and the stages that move the
  work.

  Three questions, in the order they get asked when a portal starts refusing:

    1. which counter is the binding one right now
    2. which path is holding work because of it
    3. is the number we are enforcing even real

  (3) is the one no other console asks, and it is why every budget row carries
  its provenance. A count with `confidence: assumed` is arithmetic on a guess,
  and an operator staring at a saturated bar deserves to know which kind it is
  before they go and change anything.
*/
import { ref, computed, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import PageHeader from '../components/PageHeader.vue'
import GraphDiagram from '../components/GraphDiagram.vue'
import CeilingBar from '../components/CeilingBar.vue'
import StatusDot from '../components/StatusDot.vue'
import Metric from '../components/Metric.vue'
import ConfirmModal from '../components/ConfirmModal.vue'
import Icon from '../components/Icon.vue'
import {
  api, num, pct, period, window as windowOf, ago, utilisation, ceilingOf,
  isAdmin, READ_ONLY_NOTE, graphApi, graphPath, DEFAULT_APP,
} from '../lib/api.js'
import { usePoll } from '../lib/poll.js'

const props = defineProps({ app: String, name: String })
const route = useRoute()
const router = useRouter()
const application = computed(() => props.app || DEFAULT_APP)

const graph = ref(null)
const error = ref('')
/* `?path=` selects one path, which is what a `/lanes/:lane` link becomes. */
const selected = ref(String(route.query.path ?? ''))

async function load() {
  try {
    graph.value = await api.get(graphApi(application.value, props.name))
    error.value = ''
  } catch (e) {
    error.value = e.message
  }
}
usePoll(load)
watch(() => [props.app, props.name], () => {
  graph.value = null
  load()
})

const nodes = computed(() => graph.value?.nodes ?? [])
const stages = computed(() => graph.value?.stages ?? [])
const paths = computed(() => graph.value?.paths ?? [])

/* The edges every path implies, for the diagram. The document says paths; the
   picture wants pairs, and deriving them here keeps the server's topology route
   free of a second shape that has to agree with this one. */
const edges = computed(() => {
  const out = []
  const seen = new Set()
  for (const p of paths.value) {
    const hops = p.hops ?? []
    for (let i = 0; i + 1 < hops.length; i++) {
      for (const from of split(hops[i])) {
        for (const to of split(hops[i + 1])) {
          const k = `${from}->${to}`
          if (!seen.has(k)) {
            seen.add(k)
            out.push({ from, to, priority: p.priority ?? 0 })
          }
        }
      }
    }
  }
  return out
})

/* A hop is either a node name or `[a, b]` — a fan-out. */
function split(hop) {
  const s = String(hop ?? '')
  if (!s.startsWith('[')) return [s]
  return s.slice(1, -1).split(',').map((x) => x.trim()).filter(Boolean)
}

function stagesOf(node) {
  return stages.value.filter((s) => s.node === node)
}

function counters(node) {
  return stagesOf(node).reduce(
    (a, s) => ({
      admitted: a.admitted + (s.counters?.admitted ?? 0),
      deferred: a.deferred + (s.counters?.deferred ?? 0),
      parked: a.parked + (s.counters?.parked ?? 0),
      released: a.released + (s.counters?.released ?? 0),
      forwarded: a.forwarded + (s.counters?.forwarded ?? 0),
      commits: a.commits + (s.counters?.commits ?? 0),
      duplicates: a.duplicates + (s.counters?.duplicates ?? 0),
      foreign: a.foreign + (s.counters?.foreign ?? 0),
      deadlettered: a.deadlettered + (s.counters?.deadlettered ?? 0),
    }),
    { admitted: 0, deferred: 0, parked: 0, released: 0, forwarded: 0, commits: 0,
      duplicates: 0, foreign: 0, deadlettered: 0 },
  )
}

const totals = computed(() =>
  nodes.value.reduce(
    (a, n) => {
      const c = counters(n.node)
      return {
        admitted: a.admitted + c.admitted,
        deferred: a.deferred + c.deferred,
        forwarded: a.forwarded + c.forwarded,
        commits: a.commits + c.commits,
      }
    },
    { admitted: 0, deferred: 0, forwarded: 0, commits: 0 },
  ),
)

/* A node at its ceiling is NOT red: a refusal is the job. Only a breaker is,
   and a stage that is not running. */
function nodeState(n) {
  if (!graph.value?.running) return 'down'
  if (n.breaker) return 'breached'
  if ((n.budgets ?? []).some((b) => b.confidence === 'assumed')) return 'blind'
  if (counters(n.node).deferred > 0) return 'pacing'
  return 'flowing'
}

const state = computed(() => {
  if (!graph.value?.running) return 'down'
  if (nodes.value.some((n) => n.breaker)) return 'breached'
  if (nodes.value.some((n) => (n.budgets ?? []).some((b) => b.confidence === 'assumed')))
    return 'blind'
  return totals.value.deferred > 0 ? 'pacing' : 'flowing'
})

const CONFIDENCE_NOTE = {
  documented: 'the vendor publishes this number',
  inferred: 'deduced from real sources, not quoted',
  assumed: 'we do not know this number',
}

/* A `source` is whatever the discovery document cited: sometimes a URL, often a
   ticket, a run name or a person. Only the first kind survives being turned
   into a link, and offering a dead click on the others is worse than plain
   text. */
function sourceHref(s) {
  return typeof s === 'string' && /^https?:\/\//i.test(s) ? s : null
}

function utilTone(b) {
  const u = utilisation(b)
  return u > 1 ? 'text-bad' : u >= 0.85 ? 'text-warn' : ''
}

function budgetPath(node, budget) {
  return graphPath(
    application.value,
    props.name,
    `/nodes/${encodeURIComponent(node)}/budgets/${encodeURIComponent(budget)}`,
  )
}

/* ---------------------------------------------------------------- deleting */

const deleting = ref(false)
const deleteBusy = ref(false)

async function remove() {
  deleteBusy.value = true
  try {
    await api.del(
      `/v1/apps/${encodeURIComponent(application.value)}/graphs/${encodeURIComponent(props.name)}`,
    )
    router.push('/graphs')
  } catch (e) {
    error.value = e.message
    deleting.value = false
  } finally {
    deleteBusy.value = false
  }
}
</script>

<template>
  <div>
    <div v-if="error" class="card border-transparent bg-bad-dim px-5 py-4 text-[13.5px] text-bad">
      {{ error }}
    </div>

    <div v-else-if="!graph" class="space-y-4">
      <div class="skeleton h-8 w-56" />
      <div class="card px-6 py-8"><div class="skeleton h-5 w-1/3" /></div>
    </div>

    <template v-else>
      <PageHeader
        :title="graph.graph"
        mono
        :crumbs="[{ to: '/graphs', label: 'Graphs' }]"
        :sub="`${nodes.length} node${nodes.length === 1 ? '' : 's'}, ${paths.length} path${paths.length === 1 ? '' : 's'}, ${stages.length} stage${stages.length === 1 ? '' : 's'} — one consumer each, and nothing else running.`"
      >
        <template #actions>
          <span class="chip">{{ application }}</span>
          <span class="chip">v{{ graph.version }}</span>
          <span v-if="graph.counters" class="chip">counters {{ graph.counters }}s</span>
          <template v-if="isAdmin">
            <RouterLink :to="graphPath(application, name, '/edit')" class="btn">
              <Icon name="edit" :size="14" /> Edit
            </RouterLink>
            <button class="btn btn-danger" @click="deleting = true">
              <Icon name="x" :size="14" /> Delete
            </button>
          </template>
        </template>
      </PageHeader>

      <p v-if="!isAdmin" class="-mt-4 mb-6 text-[12px] text-fg-3">{{ READ_ONLY_NOTE }}</p>

      <!-- --------------------------------------------------- headline -->
      <section class="card px-6 py-6 flex flex-col md:flex-row md:items-center gap-6">
        <div class="flex-1 min-w-0">
          <StatusDot :state="state" size="lg" avatar>
            <p v-if="nodes.some((n) => n.breaker)"
               class="text-[13px] text-fg-2 mt-2 ml-[22.5px] max-w-[52ch] leading-relaxed">
              A breaker is holding a node: its window has been spent on purpose, so every path
              refuses until it expires. Nothing is lost — the work waits.
            </p>
            <p v-else-if="state === 'blind'"
               class="text-[13px] text-fg-2 mt-2 ml-[22.5px] max-w-[52ch] leading-relaxed">
              At least one count below is a guess. Everything on this page is arithmetic on top of it.
            </p>
            <p v-else class="text-[13px] text-fg-2 mt-2 ml-[22.5px] max-w-[52ch] leading-relaxed">
              No vendor throttle has been reported against this graph.
            </p>
          </StatusDot>
        </div>
        <!-- `forwarded / commits` is THE number that explains a stage's
             throughput: the destination partition takes one row lock per
             transaction whoever holds it, so items-per-transaction is the
             multiplier on everything the workers do in parallel. It sat near 1
             in v1; here it should sit near the batch. -->
        <div class="grid grid-cols-2 sm:grid-cols-4 gap-7 md:gap-9 md:pl-8 md:border-l border-line shrink-0">
          <Metric label="Admitted" :value="num(totals.admitted)" />
          <Metric label="Deferred" :value="num(totals.deferred)" />
          <Metric label="Relayed" :value="num(totals.forwarded)" />
          <Metric label="Per txn"
                  :value="totals.commits ? (totals.forwarded / totals.commits).toFixed(0) : '—'" />
        </div>
      </section>

      <section class="mt-8 rounded-xl border border-line bg-surface p-5">
        <GraphDiagram :nodes="nodes.map((n) => ({
                        name: n.node,
                        entry: !!n.ingressQueue,
                        consume: !!n.egressQueue,
                        running: graph.running,
                        paths: n.paths ?? [],
                        budgets: n.budgets ?? [],
                      }))"
                      :edges="edges" />
      </section>

      <!-- ----------------------------------------------------- paths -->
      <section class="mt-10">
        <h2 class="section-title">Paths
          <span class="section-count">priority is a ceiling, not a queue position</span>
        </h2>
        <div class="card divide-y divide-line">
          <button v-for="p in paths" :key="p.name" type="button"
                  class="w-full text-left px-5 py-4 flex items-center gap-5 hover:bg-surface-2 transition-colors"
                  :class="selected === p.name ? 'bg-surface-2' : ''"
                  @click="selected = selected === p.name ? '' : p.name">
            <div class="min-w-0 flex-[2]">
              <div class="flex items-center gap-2.5">
                <span class="font-medium text-[14px] font-mono">{{ p.name }}</span>
                <span class="chip">priority {{ p.priority }}</span>
                <span v-if="p.share !== null && p.share !== undefined" class="chip">
                  share {{ p.share }}
                </span>
              </div>
              <div class="text-[12.5px] text-fg-2 mt-0.5 truncate font-mono">
                {{ (p.hops ?? []).join(' › ') }}
              </div>
            </div>
            <span class="text-[11.5px] text-fg-3">
              {{ selected === p.name ? 'highlighted' : 'show its ceilings' }}
            </span>
          </button>
        </div>
      </section>

      <!-- ----------------------------------------------------- nodes -->
      <section v-for="n in nodes" :key="n.node" class="mt-8">
        <h2 class="section-title">
          <span class="font-mono">{{ n.node }}</span>
          <span class="section-count">
            {{ (n.paths ?? []).join(', ') || 'no path' }}
          </span>
        </h2>

        <div class="card px-5 py-5">
          <div class="flex items-baseline gap-3 flex-wrap">
            <StatusDot :state="nodeState(n)" />
            <span v-if="n.ingressQueue" class="chip font-mono">
              in {{ n.ingressQueue }}<span v-if="!n.ingressOwnedByGate" class="text-fg-3"> (yours)</span>
            </span>
            <span v-if="n.egressQueue" class="chip font-mono">out {{ n.egressQueue }}</span>
            <span v-if="n.httpPush" class="chip">http push</span>
          </div>

          <p v-if="n.breaker" class="mt-3 rounded-lg bg-bad-dim px-3.5 py-3 text-[12.5px] text-bad">
            <Icon name="alert" :size="14" class="inline mb-px" />
            Backed off {{ ago(n.breaker.at) }} for {{ n.breaker.retryAfterSeconds }}s<span
              v-if="n.breaker.by"> by {{ n.breaker.by }}</span>. The window is spent, so every path
            refuses through the ordinary refusal path until it expires.
          </p>

          <!-- ------------------------------------------- budgets -->
          <div class="mt-4 space-y-5">
            <div v-for="b in n.budgets" :key="b.id">
              <div class="flex items-center gap-3 flex-wrap">
                <RouterLink :to="budgetPath(n.node, b.id)"
                            class="font-mono text-[13px] font-medium hover:underline">{{ b.id }}</RouterLink>
                <span class="chip">{{ num(b.count) }} / {{ windowOf(b.timeMs) }}</span>
                <span v-if="b.subWindows > 1" class="chip">
                  {{ num(b.countSub) }} / {{ period(b.windowSubSeconds) }} × {{ b.subWindows }}
                </span>
                <span v-if="b.scopeBy" class="chip font-mono">per {{ b.scopeBy }}</span>
                <span v-if="b.sharedKey" class="chip font-mono">shared {{ b.sharedKey }}</span>

                <span class="ml-auto text-[13px] font-semibold tabular-nums" :class="utilTone(b)">
                  {{ b.value === null ? '—' : pct(utilisation(b)) }}
                </span>
              </div>

              <div class="mt-2.5">
                <CeilingBar :value="b.value" :ceilings="b.ceilings ?? {}"
                            :highlight="selected" :assumed="b.confidence === 'assumed'" />
              </div>

              <div class="flex items-center gap-2 mt-2 text-[11.5px] flex-wrap">
                <span :class="b.confidence === 'documented' ? 'text-fg-3' : 'text-warn'">
                  {{ b.confidence }} — {{ CONFIDENCE_NOTE[b.confidence] || 'provenance not declared' }}
                </span>
                <a v-if="sourceHref(b.source)" :href="sourceHref(b.source)" target="_blank"
                   rel="noreferrer" class="text-link hover:underline inline-flex items-center gap-1">
                  <Icon name="link" :size="11" /> source
                </a>
                <span v-else-if="b.source" class="text-fg-3">source: {{ b.source }}</span>
                <span v-if="b.expiresAt" class="ml-auto text-fg-3">
                  window rotates {{ ago(b.expiresAt) }}
                </span>
              </div>
            </div>
            <p v-if="!(n.budgets ?? []).length" class="text-[12.5px] text-fg-3">
              No budget: a node that limits nothing is a queue with extra steps, and a declare
              refuses one.
            </p>
          </div>

          <!-- -------------------------------------------- stages -->
          <table class="w-full mt-5 text-[12.5px]">
            <thead>
              <tr class="text-left text-[11px] text-fg-3">
                <th class="font-normal pb-1.5">path</th>
                <th class="font-normal pb-1.5">ceiling</th>
                <th class="font-normal pb-1.5">admitted</th>
                <th class="font-normal pb-1.5">deferred</th>
                <th class="font-normal pb-1.5">parked</th>
                <th class="font-normal pb-1.5">released</th>
                <th class="font-normal pb-1.5">per txn</th>
                <th class="font-normal pb-1.5">foreign</th>
                <th class="font-normal pb-1.5">already there</th>
                <th class="font-normal pb-1.5">dead-lettered</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="s in stagesOf(n.node)" :key="s.path" class="border-t border-line"
                  :class="selected === s.path ? 'bg-surface-2' : ''">
                <td class="py-2 font-mono">{{ s.path }}</td>
                <td class="py-2 tabular-nums text-fg-2">{{ pct(s.share) }}</td>
                <td class="py-2 tabular-nums text-fg-2">{{ num(s.counters?.admitted) }}</td>
                <td class="py-2 tabular-nums text-fg-2">{{ num(s.counters?.deferred) }}</td>
                <!-- Parked is in-handler, holding the lease; released let the
                     lease lapse. Queen charges no retry budget on lease expiry,
                     so a release costs nothing and cannot dead-letter work that
                     is merely waiting. -->
                <td class="py-2 tabular-nums text-fg-2">{{ num(s.counters?.parked) }}</td>
                <td class="py-2 tabular-nums text-fg-2">{{ num(s.counters?.released) }}</td>
                <td class="py-2 tabular-nums text-fg-2">
                  {{ s.counters?.itemsPerCommit ? s.counters.itemsPerCommit.toFixed(0) : '—' }}
                </td>
                <td class="py-2 tabular-nums text-fg-3">{{ num(s.counters?.foreign) }}</td>
                <!-- Should be zero; visible because a recovery nobody can see is
                     a recovery nobody knows ran. -->
                <td class="py-2 tabular-nums"
                    :class="s.counters?.duplicates ? 'text-warn' : 'text-fg-3'">
                  {{ num(s.counters?.duplicates) }}
                </td>
                <td class="py-2 tabular-nums"
                    :class="s.counters?.deadlettered ? 'text-bad' : 'text-fg-3'">
                  {{ num(s.counters?.deadlettered) }}
                </td>
              </tr>
            </tbody>
          </table>
          <p v-if="stagesOf(n.node).some((s) => s.lastRefusal)"
             class="mt-2 text-[12px] text-fg-2">
            <template v-for="s in stagesOf(n.node)" :key="s.path">
              <span v-if="s.lastRefusal" class="mr-4">
                <span class="font-mono">{{ s.path }}</span> last held by
                <span class="font-mono">{{ s.lastRefusal.budget }}</span>
                {{ ago(s.lastRefusal.at) }}
              </span>
            </template>
          </p>
        </div>
      </section>
    </template>

    <ConfirmModal
      v-if="deleting"
      :title="`Delete ${application}/${name}`"
      body="The stages stop, the graph stops admitting and the stored declaration is forgotten, so it does not come back on the next boot. Work already pushed stays in the broker."
      confirm="Delete" danger :busy="deleteBusy"
      @close="deleting = false" @confirm="remove"
    />
  </div>
</template>
