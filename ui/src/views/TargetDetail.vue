<script setup>
/*
  The page an operator lands on when a portal starts refusing. Three questions,
  in this order, because that is the order they get asked:

    1. which budget is the binding one right now
    2. which lane is holding work because of it
    3. is the number we are enforcing even real

  (3) is the one no other console asks, and it is why every budget row carries
  its provenance. A cap with `confidence: assumed` is arithmetic on a guess, and
  an operator staring at a saturated bar deserves to know which kind it is
  before they go and change anything.
*/
import { ref, computed, watch } from 'vue'
import { useRouter } from 'vue-router'
import PageHeader from '../components/PageHeader.vue'
import StatusDot from '../components/StatusDot.vue'
import BudgetBar from '../components/BudgetBar.vue'
import Sparkline from '../components/Sparkline.vue'
import Metric from '../components/Metric.vue'
import ConfirmModal from '../components/ConfirmModal.vue'
import Icon from '../components/Icon.vue'
import {
  api, num, pct, period, ago, utilisation,
  isAdmin, READ_ONLY_NOTE, targetApi, targetPath, DEFAULT_APP,
} from '../lib/api.js'
import { fetchRollups, perMinute, budgetSeries } from '../lib/rollups.js'
import { usePoll } from '../lib/poll.js'

/*
  The pair, always: a flat `/targets/:name` link is resolved to an application
  by the router before this page is reached, so nothing here has to guess.
*/
const props = defineProps({ app: String, name: String })
const router = useRouter()
const application = computed(() => props.app || DEFAULT_APP)

const target = ref(null)
// State and backlog are properties of the running gate, and only the list
// endpoint computes them; the detail endpoint describes the declaration.
const row = ref(null)
const error = ref('')

/* One roll-up query serves every budget on the page: the meter aggregates per
   minute over the whole target, and each budget's line is that same series
   read against its own window. `null` means there is no history to read at
   all, which reads differently on screen from a history that is empty. */
const minutes = ref(null)
let historyFetchedAt = 0

async function loadHistory() {
  minutes.value = perMinute(await fetchRollups(application.value, props.name, 120))
}

function sparkline(b) {
  return (budgetSeries(minutes.value, b) ?? []).map((w) => w.utilisation)
}

async function load() {
  try {
    const [d, list] = await Promise.all([
      api.get(targetApi(application.value, props.name)),
      api.get('/api/targets').catch(() => []),
    ])
    target.value = d
    row.value =
      (list ?? []).find(
        (t) => t.name === props.name && (t.application || DEFAULT_APP) === application.value
      ) ?? null
    error.value = ''
  } catch (e) {
    error.value = e.message
    return
  }
  // An hour of one-minute windows barely moves between two four-second polls,
  // so the history rides a much slower clock than the gauges do.
  if (Date.now() - historyFetchedAt > 30_000) {
    historyFetchedAt = Date.now()
    await loadHistory()
  }
}
usePoll(load)
watch(() => [props.app, props.name], () => {
  target.value = null
  minutes.value = null
  historyFetchedAt = 0
  load()
})

const budgets = computed(() => target.value?.budgets ?? [])
const lanes = computed(() => target.value?.lanes ?? [])

const binding = computed(() => {
  if (!budgets.value.length) return null
  return budgets.value.reduce((worst, b) => (utilisation(b) > utilisation(worst) ? b : worst), budgets.value[0])
})

/* The target's counters are the sum of its lanes' — there is no separate
   target-level meter, and inventing one that disagreed with the rows below it
   would be worse than adding them up here. */
const totals = computed(() =>
  lanes.value.reduce(
    (a, l) => ({
      admitted: a.admitted + (l.admitted || 0),
      denied: a.denied + (l.denied || 0),
      calls: a.calls + (l.calls || 0),
      throttled: a.throttled + (l.throttled || 0),
    }),
    { admitted: 0, denied: 0, calls: 0, throttled: 0 }
  )
)

const state = computed(() => {
  if (target.value?.last_breach_at || totals.value.throttled) return 'breached'
  if (budgets.value.some((b) => b.confidence === 'assumed')) return 'blind'
  return row.value?.state ?? 'flowing'
})

const CONFIDENCE_NOTE = {
  documented: 'the vendor publishes this number',
  inferred: 'deduced from real sources, not quoted',
  assumed: 'we do not know this number',
}

/* A `source` is whatever the discovery document cited: sometimes a URL, often
   a ticket, a run name or a person. Only the first kind survives being turned
   into a link, and offering a dead click on the others is worse than plain
   text. */
function sourceHref(s) {
  return typeof s === 'string' && /^https?:\/\//i.test(s) ? s : null
}

function utilTone(b) {
  const u = utilisation(b)
  return u > 1 ? 'text-bad' : u >= 0.85 ? 'text-warn' : ''
}

/* ---------------------------------------------------------------- deleting */

const deleting = ref(false)
const deleteBusy = ref(false)

async function remove() {
  deleteBusy.value = true
  try {
    await api.del(`/v1/apps/${encodeURIComponent(application.value)}/targets/${encodeURIComponent(props.name)}`)
    router.push('/targets')
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

    <div v-else-if="!target" class="space-y-4">
      <div class="skeleton h-8 w-56" />
      <div class="card px-6 py-8"><div class="skeleton h-5 w-1/3" /></div>
    </div>

    <template v-else>
      <PageHeader
        :title="target.name"
        :crumbs="[{ to: '/targets', label: 'Targets' }]"
        :sub="binding
          ? `Binding right now: ${binding.id}, ${pct(utilisation(binding))} of ${num(binding.cap)} per ${period(binding.periodSeconds)}.`
          : 'No budget declared.'"
      >
        <template #actions>
          <span class="chip">{{ application }}</span>
          <span class="chip">v{{ target.version }}</span>
          <span v-if="target.egress" class="chip">egress {{ target.egress }}</span>
          <template v-if="isAdmin">
            <RouterLink :to="targetPath(application, name, '/edit')" class="btn">
              <Icon name="edit" :size="14" /> Edit
            </RouterLink>
            <button class="btn btn-danger" @click="deleting = true">
              <Icon name="x" :size="14" /> Delete
            </button>
          </template>
        </template>
      </PageHeader>

      <!-- One quiet sentence rather than disabled buttons nobody can explain. -->
      <p v-if="!isAdmin" class="-mt-4 mb-6 text-[12px] text-fg-3">{{ READ_ONLY_NOTE }}</p>

      <!-- --------------------------------------------------- headline -->
      <section class="card px-6 py-6 flex flex-col md:flex-row md:items-center gap-6">
        <div class="flex-1 min-w-0">
          <StatusDot :state="state" size="lg" />
          <p v-if="target.last_breach_at" class="text-[13px] text-fg-2 mt-2 ml-[22.5px] max-w-[52ch] leading-relaxed">
            Last throttled {{ ago(target.last_breach_at) }} on
            <span class="font-mono">{{ target.last_breach_budget || 'an unattributed call' }}</span>.
            A breach means the enforced cap is higher than the real one.
          </p>
          <p v-else-if="state === 'blind'" class="text-[13px] text-fg-2 mt-2 ml-[22.5px] max-w-[52ch] leading-relaxed">
            At least one cap below is a guess. Everything on this page is arithmetic on top of it.
          </p>
          <p v-else class="text-[13px] text-fg-2 mt-2 ml-[22.5px] max-w-[52ch] leading-relaxed">
            No vendor throttle has been recorded against this target.
          </p>
        </div>
        <!-- `calls` is the count the caller reported at ack time — the real
             number of HTTP requests the admitted work produced. Where it drifts
             from what was admitted, the cost model is wrong, and a wrong cost
             model breaks every TPS budget silently. -->
        <div class="grid grid-cols-2 sm:grid-cols-4 gap-7 md:gap-9 md:pl-8 md:border-l border-line shrink-0">
          <Metric label="Admitted" :value="num(totals.admitted)" />
          <Metric label="Denied" :value="num(totals.denied)" />
          <Metric label="Calls" :value="num(totals.calls)" />
          <Metric label="Throttled" :value="num(totals.throttled)"
                  :tone="totals.throttled ? 'bad' : 'plain'" />
        </div>
      </section>

      <!-- ----------------------------------------------------- lanes -->
      <section class="mt-10">
        <h2 class="section-title">Lanes
          <span class="section-count">how the ceiling is divided</span>
        </h2>
        <div class="card divide-y divide-line">
          <RouterLink
            v-for="l in lanes" :key="l.name"
            :to="targetPath(application, name, `/lanes/${encodeURIComponent(l.name)}`)"
            class="px-5 py-4 flex items-center gap-5 hover:bg-surface-2 transition-colors group"
          >
            <div class="min-w-0 flex-[2]">
              <div class="flex items-center gap-2.5">
                <span class="font-medium text-[14px]">{{ l.name }}</span>
                <span v-if="l.default" class="chip">default</span>
                <StatusDot :state="l.state" />
              </div>
              <div class="text-[12.5px] text-fg-2 mt-0.5 truncate">
                cap <span class="font-mono">{{ l.cap_policy }}</span>
                · {{ l.concurrency }} consumers
                · lease {{ period(l.lease_seconds) }}
              </div>
            </div>

            <div class="hidden md:block flex-1 min-w-0 text-[12px] text-fg-2 truncate">
              <template v-if="l.last_denial_budget">
                last held by <span class="font-mono">{{ l.last_denial_budget }}</span>
              </template>
              <span v-else class="text-fg-3">never held back</span>
            </div>

            <div class="w-20 text-right shrink-0 tabular-nums">
              <div class="text-[15px] font-semibold">{{ num(l.admitted) }}</div>
              <div class="text-[11px] text-fg-3">{{ num(l.denied) }} denied</div>
            </div>

            <Icon name="chevron" :size="14" class="text-fg-3 group-hover:text-fg-2 transition-colors shrink-0" />
          </RouterLink>
        </div>
      </section>

      <!-- --------------------------------------------------- budgets -->
      <section class="mt-10">
        <h2 class="section-title">Budgets
          <span class="section-count">all must admit</span>
        </h2>
        <div class="card divide-y divide-line">
          <div v-for="b in budgets" :key="b.id" class="px-5 py-4">
            <div class="flex items-center gap-3 flex-wrap">
              <RouterLink :to="targetPath(application, name, `/budgets/${encodeURIComponent(b.id)}`)"
                          class="font-mono text-[13px] font-medium hover:underline">{{ b.id }}</RouterLink>
              <span class="chip">{{ num(b.cap) }} / {{ period(b.periodSeconds) }}</span>
              <span class="chip">{{ b.alignment }}</span>
              <span v-if="b.scope?.length" class="chip">per {{ b.scope.join(' + ') }}</span>
              <span class="chip">{{ b.store }}</span>

              <span class="ml-auto flex items-center gap-3 shrink-0">
                <RouterLink :to="targetPath(application, name, `/budgets/${encodeURIComponent(b.id)}`)"
                            class="flex items-center gap-1.5 text-fg-3 hover:text-fg-2 transition-colors"
                            title="Utilisation over the last two hours">
                  <Sparkline :points="sparkline(b)" :assumed="b.confidence === 'assumed'" />
                  <Icon name="chevron" :size="13" />
                </RouterLink>
                <span class="text-[13px] font-semibold tabular-nums w-[42px] text-right"
                      :class="utilTone(b)">{{ pct(utilisation(b)) }}</span>
              </span>
            </div>

            <div class="mt-2.5">
              <BudgetBar :used="b.used" :cap="b.cap" :assumed="b.confidence === 'assumed'" :height="7" />
            </div>

            <!-- Provenance, on every row, always. -->
            <div class="flex items-center gap-2 mt-2 text-[11.5px] flex-wrap">
              <span :class="b.confidence === 'documented' ? 'text-fg-3' : 'text-warn'">
                {{ b.confidence }} — {{ CONFIDENCE_NOTE[b.confidence] || 'provenance not declared' }}
              </span>
              <a v-if="sourceHref(b.source)" :href="sourceHref(b.source)" target="_blank" rel="noreferrer"
                 class="text-link hover:underline inline-flex items-center gap-1">
                <Icon name="link" :size="11" /> source
              </a>
              <span v-else-if="b.source" class="text-fg-3">source: {{ b.source }}</span>
              <span v-if="b.as_of" class="text-fg-3">· as of {{ b.as_of }}</span>
              <span v-if="b.match?.op?.length" class="ml-auto font-mono text-fg-3 truncate">
                {{ b.match.op.join(', ') }}
              </span>
            </div>
          </div>
        </div>
      </section>
    </template>

    <ConfirmModal
      v-if="deleting"
      :title="`Delete ${application}/${name}`"
      body="The runners stop, the lanes stop admitting and the stored declaration is forgotten, so this target does not come back on the next boot. Work already pushed stays in the broker."
      confirm="Delete" danger :busy="deleteBusy"
      @close="deleting = false" @confirm="remove"
    />
  </div>
</template>
