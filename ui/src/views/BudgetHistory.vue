<script setup>
/*
  The bar on the target page says how close this budget is right now. This page
  says how it got there, and it is the one an operator takes to the vendor: a
  window that has sat above 85% every afternoon for a week is a capacity
  conversation, and one that touches the cap for a minute a day is a shaping
  conversation. Neither is visible in a gauge.

  Drawn with inline SVG because the console is embedded in the binary, and no
  chart library is worth a megabyte an operator carries forever to plot a few
  hundred windows.
*/
import { ref, computed, watch } from 'vue'
import PageHeader from '../components/PageHeader.vue'
import BudgetBar from '../components/BudgetBar.vue'
import Metric from '../components/Metric.vue'
import Icon from '../components/Icon.vue'
import {
  api, num, pct, period, window as windowOf, datetime, utilisation, ceilingOf,
  graphPath, graphApi, DEFAULT_APP,
} from '../lib/api.js'
import { fetchRollups, perMinute, budgetSeries } from '../lib/rollups.js'
import { usePoll } from '../lib/poll.js'

const props = defineProps({ app: String, name: String, node: String, budget: String })
const application = computed(() => props.app || DEFAULT_APP)

/* The roll-up table keys a node as `{graph}.{node}`, which is what a target
   name was — so ninety days of history survive the rewrite under the same
   key. */
const target = ref(null)
const rollupKey = computed(() => `${props.name}.${props.node}`)
const rows = ref(undefined) // undefined = not asked yet, null = nothing to read
const error = ref('')

const RANGES = [
  { key: '1h', label: '1 hour', minutes: 60 },
  { key: '6h', label: '6 hours', minutes: 360 },
  { key: '12h', label: '12 hours', minutes: 720 },
]
const range = ref(RANGES[1])

async function load() {
  try {
    const [d, r] = await Promise.all([
      api.get(graphApi(application.value, props.name)),
      fetchRollups(application.value, rollupKey.value, range.value.minutes),
    ])
    target.value = d
    rows.value = r === null ? null : perMinute(r)
    error.value = ''
  } catch (e) {
    error.value = e.message
  }
}
// Slower than the gauges: the series is one point per minute, and it does not
// change between two four-second polls.
usePoll(load, 15000)
watch([() => props.app, () => props.name, () => props.node, () => props.budget, range], load)

const node = computed(() => (target.value?.nodes ?? []).find((n) => n.node === props.node) ?? null)
const spec = computed(() => (node.value?.budgets ?? []).find((b) => b.id === props.budget) ?? null)

const points = computed(() => (spec.value ? budgetSeries(rows.value, spec.value) ?? [] : []))
const peak = computed(() => points.value.reduce((a, w) => Math.max(a, w.utilisation ?? 0), 0))
const totals = computed(() =>
  points.value.reduce(
    (a, w) => ({
      admitted: a.admitted + (w.admitted || 0),
      denied: a.denied + (w.denied || 0),
      estimated: a.estimated + (w.cost_estimated || 0),
      actual: a.actual + (w.cost_actual || 0),
      throttled: a.throttled + (w.throttled || 0),
    }),
    { admitted: 0, denied: 0, estimated: 0, actual: 0, throttled: 0 }
  )
)

/* The cost model breaks silently: we count what was declared at push time, the
   vendor counts what actually left, and every TPS budget in the system is
   spent in the first currency. The drift between the two is the only warning
   that comes before a breach. */
const drift = computed(() => {
  const { estimated, actual } = totals.value
  if (!estimated || !actual) return null
  return actual / estimated - 1
})

// A window shorter than a minute cannot be resolved by a per-minute roll-up,
// and saying so is cheaper than an operator discovering it from a flat line.
const smoothed = computed(() => (spec.value?.windowSubSeconds ?? 60) < 60)

/* ------------------------------------------------------------ geometry */

const W = 720
const H = 200
const PAD_L = 38
const PAD_R = 10
const PAD_T = 10
const PAD_B = 22
const plotW = W - PAD_L - PAD_R
const plotH = H - PAD_T - PAD_B

// Pinned to the cap unless something went over it. An autoscaled axis makes a
// budget that never passed 4% look exactly like one that lived on its ceiling.
const top = computed(() => Math.max(1.05, peak.value * 1.05))

function px(i) {
  const n = points.value.length
  return n < 2 ? PAD_L + plotW / 2 : PAD_L + (i / (n - 1)) * plotW
}
function py(v) {
  return PAD_T + plotH - (Math.max(v ?? 0, 0) / top.value) * plotH
}

const line = computed(() =>
  points.value.map((w, i) => `${i ? 'L' : 'M'}${px(i).toFixed(1)} ${py(w.utilisation).toFixed(1)}`).join(' ')
)
const area = computed(() =>
  points.value.length
    ? `${line.value} L${px(points.value.length - 1).toFixed(1)} ${PAD_T + plotH} L${px(0).toFixed(1)} ${PAD_T + plotH} Z`
    : ''
)

const barW = computed(() => {
  const n = points.value.length
  return n ? Math.max(1, Math.min(10, plotW / n - 1)) : 1
})

/* The denial strip is scaled to its own busiest window, not to the utilisation
   axis above it: the two measure different things, and one denial in a quiet
   hour still deserves to be visible. */
const DENIAL_H = 40
const maxDenied = computed(() => Math.max(1, ...points.value.map((w) => w.denied || 0)))
function denialH(w) {
  if (!w.denied) return 0
  return Math.max(2, (w.denied / maxDenied.value) * DENIAL_H)
}

// Three labels and no more: an axis dense enough to read every tick is an axis
// nobody reads.
const xLabels = computed(() => {
  const n = points.value.length
  if (!n) return []
  const idx = [...new Set(n < 3 ? [0, n - 1] : [0, Math.floor((n - 1) / 2), n - 1])]
  return idx.map((i, k) => ({
    x: px(i),
    label: datetime(points.value[i].t),
    // The outer two are anchored inward: a centred label on the last point
    // hangs half of itself outside the viewBox and gets clipped.
    anchor: k === 0 ? 'start' : k === idx.length - 1 ? 'end' : 'middle',
  }))
})

const tone = computed(() => (peak.value > 1 ? 'text-bad' : peak.value >= 0.85 ? 'text-warn' : 'text-good'))
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
        :title="budget" mono
        :sub="spec
          ? `${num(spec.count)} per ${windowOf(spec.timeMs)}, enforced as ${num(spec.countSub)} per ${period(spec.windowSubSeconds)}.`
          : 'This budget is no longer declared on the node.'"
        :crumbs="[
          { to: '/graphs', label: 'Graphs' },
          { to: graphPath(application, name), label: `${application}/${name}` },
        ]"
      >
        <template #actions>
          <div class="flex items-center gap-1">
            <button
              v-for="r in RANGES" :key="r.key"
              class="h-[28px] px-2.5 rounded-md text-[12px] transition-colors"
              :class="range.key === r.key ? 'bg-fg text-bg font-medium' : 'text-fg-2 hover:bg-surface-2'"
              @click="range = r"
            >{{ r.label }}</button>
          </div>
        </template>
      </PageHeader>

      <!-- Where it stands now, so the page still answers something when no
           history has been kept. -->
      <section v-if="spec" class="card px-6 py-5">
        <div class="flex items-baseline gap-3 mb-3 flex-wrap">
          <span class="text-[26px] font-semibold tabular-nums tracking-tight leading-none">
            {{ pct(utilisation(spec)) }}
          </span>
          <span class="text-[12.5px] text-fg-2">of the window spent right now</span>
          <span v-if="spec.confidence !== 'documented'" class="chip text-warn ml-auto">
            {{ spec.confidence }} cap
          </span>
        </div>
        <BudgetBar :used="spec.value" :cap="ceilingOf(spec)"
                   :assumed="spec.confidence === 'assumed'" :height="8" />
      </section>

      <!-- --------------------------------------------------- history -->
      <section class="mt-10">
        <h2 class="section-title">Utilisation over time
          <span class="section-count">{{ range.label }}</span>
        </h2>

        <div v-if="rows === null" class="card px-6 py-12 text-center">
          <p class="text-[13.5px] text-fg-2">No history is being served.</p>
          <p class="text-[12.5px] text-fg-3 mt-1 max-w-[54ch] mx-auto leading-relaxed">
            The gauge above comes from the gate itself and is live. The windows behind it are kept by
            the meter, and this build is not answering for them.
          </p>
        </div>

        <div v-else-if="rows === undefined" class="card px-6 py-12">
          <div class="skeleton h-[140px] w-full" />
        </div>

        <div v-else-if="!points.length" class="card px-6 py-12 text-center">
          <p class="text-[13.5px] text-fg-2">No window recorded yet.</p>
          <p class="text-[12.5px] text-fg-3 mt-1 max-w-[54ch] mx-auto leading-relaxed">
            The meter holds its windows in memory, so the history reaches back only as far as this
            process has been running.
          </p>
        </div>

        <div v-else class="card px-4 py-5 sm:px-6">
          <svg :viewBox="`0 0 ${W} ${H}`" class="w-full h-auto" role="img"
               :aria-label="`Utilisation of ${budget} over ${range.label}`">
            <!-- The cap is a solid rule and 85% a dashed one: the same two
                 thresholds BudgetBar changes colour at, so the chart and the
                 gauge above it can never disagree. -->
            <g class="text-line">
              <line :x1="PAD_L" :y1="py(0)" :x2="W - PAD_R" :y2="py(0)" stroke="currentColor" />
              <line :x1="PAD_L" :y1="py(0.85)" :x2="W - PAD_R" :y2="py(0.85)"
                    stroke="currentColor" stroke-dasharray="3 3" />
              <line :x1="PAD_L" :y1="py(1)" :x2="W - PAD_R" :y2="py(1)" stroke="currentColor" />
            </g>
            <g class="text-fg-3" fill="currentColor" font-size="10" text-anchor="end">
              <text :x="PAD_L - 6" :y="py(0) + 3">0</text>
              <text :x="PAD_L - 6" :y="py(0.85) + 3">85%</text>
              <text :x="PAD_L - 6" :y="py(1) + 3">cap</text>
            </g>

            <g :class="tone">
              <path :d="area" fill="currentColor" fill-opacity="0.12" />
              <path :d="line" fill="none" stroke="currentColor" stroke-width="1.6"
                    stroke-linejoin="round" stroke-linecap="round"
                    :stroke-dasharray="spec?.confidence === 'assumed' ? '4 3' : undefined" />
            </g>

            <g class="text-fg-3" fill="currentColor" font-size="10">
              <text v-for="(l, i) in xLabels" :key="i" :x="l.x" :y="H - 6" :text-anchor="l.anchor">
                {{ l.label }}
              </text>
            </g>
          </svg>

          <p class="hint mt-2">
            Reconstructed from one-minute roll-ups: the admissions inside this budget's window against
            what the window allows.
            <template v-if="smoothed">
              The window is shorter than a minute, so a burst inside one is averaged away here — the
              live gauge above is what sees those.
            </template>
          </p>

          <!-- Denials get their own strip and a neutral colour. They are the
               ceiling holding, not damage, and painting them red would train an
               operator to page themselves every time the limiter does its job.
               A throttle is the exception: that one says our cap is wrong. -->
          <div class="mt-5 pt-4 border-t border-line">
            <div class="flex items-baseline justify-between mb-2">
              <span class="label mb-0">Denials per minute</span>
              <span class="text-[11.5px] text-fg-3">a denial is the ceiling holding, not a failure</span>
            </div>
            <svg :viewBox="`0 0 ${W} ${DENIAL_H + 4}`" class="w-full h-auto" aria-hidden="true">
              <rect
                v-for="(w, i) in points" :key="i"
                :x="px(i) - barW / 2"
                :y="DENIAL_H + 4 - denialH(w)"
                :width="barW"
                :height="denialH(w)"
                :class="w.throttled ? 'text-bad' : 'text-fg-3'"
                fill="currentColor"
              />
            </svg>
          </div>

          <div class="grid grid-cols-2 sm:grid-cols-4 gap-6 mt-6 pt-5 border-t border-line">
            <Metric label="Peak" :value="pct(peak)"
                    :tone="peak > 1 ? 'bad' : peak >= 0.85 ? 'warn' : 'plain'" />
            <Metric label="Admitted" :value="num(totals.admitted)" />
            <Metric label="Denied" :value="num(totals.denied)" />
            <Metric label="Throttled" :value="num(totals.throttled)"
                    :tone="totals.throttled ? 'bad' : 'plain'" />
          </div>

          <p v-if="drift !== null && Math.abs(drift) >= 0.05"
             class="flex gap-2 mt-5 text-[12.5px] leading-relaxed"
             :class="Math.abs(drift) >= 0.2 ? 'text-bad' : 'text-warn'">
            <Icon name="alert" :size="14" class="mt-px shrink-0" />
            The work cost {{ pct(Math.abs(drift)) }} {{ drift > 0 ? 'more' : 'less' }} in real calls than it
            declared at push time. Every budget here is spent in the declared currency, so a drift this
            size is a breach waiting to happen rather than an accounting detail.
          </p>

          <p v-if="totals.throttled" class="flex gap-2 mt-3 text-[12.5px] text-bad leading-relaxed">
            <Icon name="alert" :size="14" class="mt-px shrink-0" />
            The vendor threw {{ num(totals.throttled) }} throttle{{ totals.throttled === 1 ? '' : 's' }} inside
            this range. The cap declared here is higher than the real one.
          </p>
        </div>
      </section>

      <!-- Provenance travels with the chart: a day of utilisation drawn
           against a guessed cap is a day of guesses. -->
      <section v-if="spec" class="mt-10">
        <h2 class="section-title">Where the cap comes from</h2>
        <div class="card px-6 py-5 text-[13px] space-y-1.5">
          <p :class="spec.confidence === 'documented' ? 'text-fg-2' : 'text-warn'">
            {{ spec.confidence }}<template v-if="spec.source"> — {{ spec.source }}</template>
          </p>
          <p v-if="spec.as_of" class="text-fg-3 text-[12.5px]">as of {{ spec.as_of }}</p>
          <p v-if="spec.scope?.length" class="text-fg-3 text-[12.5px]">
            counted per {{ spec.scope.join(' + ') }}
          </p>
        </div>
      </section>
    </template>
  </div>
</template>
