<script setup>
/*
  A lane is not a queue with a name on it — it is a claim on the target's
  ceiling. Two lanes both told "you may use the ceiling" enforce the ceiling
  twice and the fleet spends double, so the ceiling is divided rather than
  replicated, and this page exists to show which slice this lane holds and what
  it did with it.

  Everything else here answers the question that brings an operator to a lane
  in the first place: it is not moving, and they want to know which number
  stopped it.
*/
import { ref, computed, watch } from 'vue'
import PageHeader from '../components/PageHeader.vue'
import StatusDot from '../components/StatusDot.vue'
import Metric from '../components/Metric.vue'
import TraceList from '../components/TraceList.vue'
import Icon from '../components/Icon.vue'
import Sparkline from '../components/Sparkline.vue'
import { api, num, pct, period, rate, sameTarget, targetPath, targetApi, DEFAULT_APP } from '../lib/api.js'
import { fetchRollups, perMinute } from '../lib/rollups.js'
import { usePoll } from '../lib/poll.js'

const props = defineProps({ app: String, lane: String, name: String })
const application = computed(() => props.app || DEFAULT_APP)

const target = ref(null)
const rawTraces = ref(null) // null = the trace log is not being served
const laneMinutes = ref(null)
const targetMinutes = ref(null)
const error = ref('')
let historyFetchedAt = 0

async function load() {
  try {
    const [d, tr] = await Promise.all([
      api.get(targetApi(application.value, props.name)),
      api.get('/api/traces?outcome=denied&limit=200').catch(() => null),
    ])
    target.value = d
    rawTraces.value = tr === null ? null : (Array.isArray(tr) ? tr : (tr?.traces ?? []))
    error.value = ''
  } catch (e) {
    error.value = e.message
    return
  }
  // The series is one point per minute; polling it at the rate of the gauges
  // would redraw the same line sixteen times between two new points.
  if (Date.now() - historyFetchedAt > 30_000) {
    historyFetchedAt = Date.now()
    const rows = await fetchRollups(application.value, props.name, 120)
    laneMinutes.value = perMinute(rows, props.lane)
    targetMinutes.value = perMinute(rows)
  }
}
usePoll(load)
watch(() => [props.app, props.name, props.lane], () => {
  target.value = null
  laneMinutes.value = null
  historyFetchedAt = 0
  load()
})

/* The trace log narrows by outcome and nothing else, so target and lane are
   applied here rather than pretended at in the query string. A trace names its
   target as the pair; rows restored under the bare name are matched too, or a
   lane would look as though it had never held anything back. */
const traces = computed(() =>
  rawTraces.value === null
    ? null
    : rawTraces.value.filter(
        (t) => sameTarget(t.target, application.value, props.name) && t.lane === props.lane
      )
)

const info = computed(() => (target.value?.lanes ?? []).find((l) => l.name === props.lane) ?? null)
const declared = computed(() => (target.value?.spec?.lanes ?? []).find((l) => l.name === props.lane) ?? null)

/*
  The same arithmetic the gate does, and deliberately the same shape: a lane
  that reserves a share takes it off the top, `ceiling` takes what is left, and
  `ceiling-minus-measured` falls back to its floor. What the console cannot do
  is the "measured" half — the meter's number is not on the wire — so that
  policy is shown as the floor it is guaranteed, and labelled as a floor.
*/
function staticShare(l) {
  const p = l?.cap
  if (typeof p === 'string' && p.startsWith('share:')) return Number(p.slice(6)) || 0
  if (p === 'ceiling-minus-measured') return Math.max(l.floor ?? 0, 0)
  return 0
}
function shareOf(laneName) {
  const spec = target.value?.spec
  const l = (spec?.lanes ?? []).find((x) => x.name === laneName)
  if (!l) return 0
  const reserved = (spec.lanes ?? [])
    .filter((x) => x.name !== laneName)
    .reduce((a, x) => a + staticShare(x), 0)
  if (l.cap === 'ceiling') return Math.max(1 - reserved, 0)
  if (l.cap === 'ceiling-minus-measured') return Math.max(l.floor ?? 0, 0)
  if (typeof l.cap === 'string' && l.cap.startsWith('share:')) return Number(l.cap.slice(6)) || 0
  // An absolute rate is not a share of anything; it is its own ceiling laid on
  // top of whatever is left.
  return Math.max(1 - reserved, 0)
}

const share = computed(() => shareOf(props.lane))
const slices = computed(() =>
  (target.value?.spec?.lanes ?? []).map((l) => ({ name: l.name, share: shareOf(l.name) }))
)

const CAP_NOTE = {
  ceiling: 'takes whatever the other lanes have not reserved',
  'ceiling-minus-measured': 'takes what the meter says the others are not using, never less than its floor',
}
function capNote(p) {
  if (CAP_NOTE[p]) return CAP_NOTE[p]
  if (typeof p === 'string' && p.startsWith('share:')) return 'holds a fixed fraction of the binding budget, reserved off the top'
  if (typeof p === 'string' && p.startsWith('absolute:')) return 'holds a fixed rate of its own, on top of the target budgets'
  return 'policy not recognised by this console'
}

/* Observed against declared: the share is what the lane may spend, this is
   what it did spend. They differ for the honest reason that a lane with a
   large claim and no work to do stays empty. */
const admittedTotal = computed(() =>
  (target.value?.lanes ?? []).reduce((a, l) => a + (l.admitted || 0), 0)
)
const observed = computed(() =>
  admittedTotal.value > 0 ? (info.value?.admitted || 0) / admittedTotal.value : 0
)

/* The same ratio over time. A lane whose observed share sits far under its
   claim every minute is a lane that could give some away — and one that
   climbs to its claim and flattens there is a lane being held by the divide,
   not by a lack of work. */
const observedSeries = computed(() => {
  const lane = laneMinutes.value
  const all = targetMinutes.value
  if (!lane || !all) return []
  return lane.map((w, i) => {
    const total = all[i]?.admitted ?? 0
    return total > 0 ? w.admitted / total : 0
  })
})

const summary = computed(() => {
  if (!info.value) return 'This lane is not declared on the target.'
  if (info.value.throttled) return 'The vendor threw a throttle at work this lane admitted: the cap being enforced is higher than the real one.'
  if (info.value.denied) return `Held back ${num(info.value.denied)} times, which is the ceiling being enforced rather than work being lost.`
  return 'Nothing has been held back on this lane.'
})
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

    <div v-else-if="!info" class="card px-6 py-12 text-center">
      <p class="text-[13.5px] text-fg-2">No lane named {{ lane }} on {{ name }}.</p>
      <RouterLink :to="targetPath(application, name)" class="btn mt-5">Back to the target</RouterLink>
    </div>

    <template v-else>
      <PageHeader
        :title="lane" mono :sub="summary"
        :crumbs="[
          { to: '/targets', label: 'Targets' },
          { to: targetPath(application, name), label: `${application}/${name}` },
        ]"
      >
        <template #actions>
          <span v-if="info.default" class="chip">default</span>
          <StatusDot :state="info.throttled ? 'breached' : info.state" />
        </template>
      </PageHeader>

      <section class="card px-6 py-6 grid grid-cols-2 sm:grid-cols-4 gap-7">
        <Metric label="Admitted" :value="num(info.admitted)" />
        <Metric label="Denied" :value="num(info.denied)" />
        <Metric label="Calls" :value="num(info.calls)" />
        <Metric label="Throttled" :value="num(info.throttled)" :tone="info.throttled ? 'bad' : 'plain'" />
      </section>

      <!-- ------------------------------------------- share of ceiling -->
      <section class="mt-10">
        <h2 class="section-title">Share of the target ceiling
          <span class="section-count">divided, never replicated</span>
        </h2>
        <div class="card px-6 py-5">
          <div class="flex items-baseline gap-2 mb-3">
            <!-- A floor is a guaranteed minimum, not an allowance: a bare
                 "50%" on a ceiling-minus-measured lane understates what it is
                 actually allowed on a quiet minute. -->
            <span class="text-[22px] font-semibold tabular-nums tracking-tight whitespace-nowrap">
              {{ info.cap_policy === 'ceiling-minus-measured' ? 'at least ' : '' }}{{ pct(share) }}
            </span>
            <span class="text-[12.5px] text-fg-2">
              of every budget this target declares —
              <span class="font-mono">{{ info.cap_policy }}</span>, {{ capNote(info.cap_policy) }}
            </span>
          </div>

          <!-- Every lane's claim on one strip, this one filled: a share only
               means something next to the ones it was taken from. -->
          <div class="flex gap-px h-[10px] rounded-full overflow-hidden bg-surface-2">
            <span v-for="s in slices" :key="s.name"
                  :style="{ flex: Math.max(s.share, 0.001) }"
                  :class="s.name === lane ? 'bg-fg' : 'bg-line-2'"
                  :title="`${s.name} — ${pct(s.share)}`" />
          </div>
          <div class="flex flex-wrap gap-x-4 gap-y-1 mt-2.5 text-[11.5px] text-fg-3">
            <span v-for="s in slices" :key="s.name" class="tabular-nums"
                  :class="s.name === lane ? 'text-fg' : ''">
              {{ s.name }} {{ pct(s.share) }}
            </span>
          </div>

          <div class="grid grid-cols-2 sm:grid-cols-4 gap-6 mt-6 pt-5 border-t border-line">
            <div>
              <div class="label mb-0.5">Effective cap</div>
              <div class="text-[13.5px] tabular-nums">
                {{ info.effective_cap === null || info.effective_cap === undefined
                  ? 'unbounded' : `${rate(info.effective_cap)}/s` }}
              </div>
              <!-- `ceiling` and `ceiling-minus-measured` start with no number
                   of their own: the target's budgets are the only limit until
                   the meter has something to say. -->
              <p class="hint mt-1">
                {{ info.effective_cap === null || info.effective_cap === undefined
                  ? 'no lane cap of its own — the target budgets are the limit'
                  : 'cost units per second, enforced on this lane alone' }}
              </p>
            </div>
            <div>
              <div class="label mb-0.5">Lease</div>
              <div class="text-[13.5px] tabular-nums">{{ period(info.lease_seconds) }}</div>
              <p class="hint mt-1">a denied lane parks for one lease before trying again</p>
            </div>
            <div>
              <div class="label mb-0.5">Consumers</div>
              <div class="text-[13.5px] tabular-nums">{{ num(info.concurrency) }}</div>
              <p class="hint mt-1">in flight against this lane at once</p>
            </div>
            <div>
              <div class="label mb-0.5">Declared floor</div>
              <div class="text-[13.5px] tabular-nums">{{ pct(declared?.floor ?? 0) }}</div>
              <p class="hint mt-1">what it keeps even when every other lane is busy</p>
            </div>
          </div>

          <div class="mt-5 pt-5 border-t border-line">
            <div class="flex items-baseline justify-between gap-3 mb-2">
              <span class="label mb-0">Observed share of admissions</span>
              <Sparkline v-if="observedSeries.length > 1" neutral :points="observedSeries" :width="120" :height="20" />
              <span class="text-[12.5px] tabular-nums">{{ pct(observed) }}</span>
            </div>
            <div class="h-[6px] rounded-full bg-surface-2 overflow-hidden">
              <div class="h-full rounded-full bg-fg-3 transition-[width] duration-500 ease-spring"
                   :style="{ width: `${Math.min(100, observed * 100)}%` }" />
            </div>
            <p class="hint mt-2">
              What it actually spent, against the {{ pct(share) }} it holds. Under is a lane with nothing
              to do, not a misconfiguration; over means the meter handed it room the other lanes were
              not using, which is what <span class="font-mono">ceiling-minus-measured</span> is for.
            </p>
          </div>
        </div>
      </section>

      <!-- ------------------------------------------------ what held it -->
      <section class="mt-10">
        <h2 class="section-title">Last held back by</h2>
        <div class="card px-6 py-5">
          <template v-if="info.last_denial_budget">
            <div class="flex items-center gap-2.5">
              <RouterLink
                :to="targetPath(application, name, `/budgets/${encodeURIComponent(info.last_denial_budget)}`)"
                class="font-mono text-[14px] font-medium hover:underline">{{ info.last_denial_budget }}</RouterLink>
              <Icon name="chevron" :size="13" class="text-fg-3" />
              <span class="text-[12.5px] text-fg-2">see its history</span>
            </div>
            <p class="hint mt-2 max-w-[68ch]">
              The budget that refused this lane most recently. Every budget on the target must admit, so
              this is the tightest one at the moment it asked — not necessarily the tightest one now.
            </p>
          </template>
          <p v-else class="text-[13.5px] text-fg-2">
            No budget has refused this lane.
          </p>
        </div>
      </section>

      <!-- ---------------------------------------------------- traces -->
      <section class="mt-10">
        <h2 class="section-title">Recent denials
          <span v-if="traces?.length" class="section-count">{{ traces.length }}</span>
        </h2>
        <div class="card">
          <div v-if="traces === null" class="px-6 py-10 text-center">
            <p class="text-[13.5px] text-fg-2">Decision traces are not being served.</p>
            <p class="text-[12.5px] text-fg-3 mt-1 max-w-[52ch] mx-auto leading-relaxed">
              The counters above come from the gate itself; traces are written separately and this build
              is not answering for them.
            </p>
          </div>
          <div v-else-if="!traces.length" class="px-6 py-10 text-center">
            <p class="text-[13.5px] text-fg-2">Nothing has been refused on this lane.</p>
          </div>
          <TraceList v-else :traces="traces" />
        </div>
      </section>
    </template>
  </div>
</template>
