<script setup>
/*
  An operator's first question is not "how much traffic went out" — it is
  "am I about to get throttled, and if I already was, which number was wrong".
  This page IS that answer: one status sentence, the numbers behind it, the
  targets that need attention, and the breaches that prove our model is off.

  Note what is deliberately NOT on this page: a throughput chart. Throughput is
  whatever the vendor allows; it is not news. Proximity to a ceiling is.
*/
import { ref, computed } from 'vue'
import Metric from '../components/Metric.vue'
import FlowChart from '../components/FlowChart.vue'
import StatusDot from '../components/StatusDot.vue'
import BudgetBar from '../components/BudgetBar.vue'
import Icon from '../components/Icon.vue'
import { api, num, rate, pct, ago, targetPath, traceRef, traceRefPath, DEFAULT_APP } from '../lib/api.js'
import { usePoll } from '../lib/poll.js'

const overview = ref(null)
const targets = ref(null)
const breaches = ref([])
const error = ref('')

async function load() {
  try {
    const [ov, ts, bs] = await Promise.all([
      api.get('/api/overview'),
      api.get('/api/targets'),
      // The breach log is written by a consumer that may not be running, and a
      // console that refuses to render because its optional history is absent
      // would be useless exactly when the gate itself is the only thing up.
      api.get('/api/breaches/recent?limit=10').catch(() => null),
    ])
    overview.value = ov
    targets.value = ts
    breaches.value = Array.isArray(bs) ? bs : (bs?.breaches ?? [])
    error.value = ''
  } catch (e) {
    error.value = e.message
  }
}
usePoll(load)

const paths = computed(() => (targets.value ?? []).reduce((a, t) => a + ((t.paths ?? t.lanes)?.length || 0), 0))
const backlog = computed(() => (targets.value ?? []).reduce((a, t) => a + (t.backlog || 0), 0))
const assumed = computed(() => (targets.value ?? []).reduce((a, t) => a + (t.assumed_budgets || 0), 0))

/* The target list carries no breach timestamp — only the per-target detail
   does — so the set of recently throttled targets is read off the breach log
   instead, and is simply empty when that log is not being served. */
/* A breach names its target as one string, sometimes as the pair and sometimes
   as the bare name. Matching compares the pair when the row carries one —
   otherwise one team's throttle would light up every other team's target of
   the same name. */
const breachedKeys = computed(() =>
  breaches.value.map((b) => traceRef(b)).filter((k) => k.name)
)
const isThrottled = (t) =>
  breachedKeys.value.some(
    (k) => k.name === t.name && (!k.scoped || k.application === (t.application || DEFAULT_APP))
  )
const keyOf = (t) => `${t.application || DEFAULT_APP}/${t.name}`

/* "Needs attention" is not "is denying". A target denying at its cap is the
   product working. It earns a row here only when the model and reality have
   come apart: the vendor threw a throttle, the backlog is outgrowing the
   drain, or the cap it is enforcing was never a published number. */
const attention = computed(() =>
  (targets.value ?? []).filter(
    (t) => isThrottled(t) || t.state === 'breached' || t.state === 'saturating' || t.assumed_budgets > 0
  )
)

function reason(t) {
  if (isThrottled(t) || t.state === 'breached')
    return 'the vendor threw a throttle — the cap being enforced is higher than the real one'
  if (t.assumed_budgets)
    return `${t.assumed_budgets} of ${t.budgets_total} budgets are assumed, not published`
  return `${num(t.backlog)} waiting behind ${t.worst_budget_id || 'the tightest window'}`
}

const status = computed(() => {
  if (!overview.value || !targets.value) return null
  if (!overview.value.queen?.reachable) {
    return { state: 'down', title: 'Broker unreachable',
             sub: `Nothing is being admitted. Queen at ${overview.value.queen?.url} is not answering.` }
  }
  const breached = (targets.value ?? []).filter((t) => isThrottled(t) || t.state === 'breached').length
  if (breached) {
    return { state: 'breached', title: `${breached} target${breached === 1 ? '' : 's'} throttled by the vendor`,
             sub: 'We admitted a call the vendor refused: the cap being enforced is higher than the real one.' }
  }
  const sat = (targets.value ?? []).filter((t) => t.state === 'saturating').length
  if (sat) {
    return { state: 'saturating', title: `${sat} target${sat === 1 ? '' : 's'} with a growing backlog`,
             sub: 'Work is arriving faster than the ceiling allows it out. Nothing is lost; it is queuing.' }
  }
  if (assumed.value) {
    return { state: 'blind', title: `${assumed.value} budget${assumed.value === 1 ? '' : 's'} enforced on a guess`,
             sub: 'Every other number on this page is arithmetic on top of those, and is only as good as they are.' }
  }
  const pacing = (targets.value ?? []).filter((t) => t.state === 'pacing').length
  if (pacing) {
    return { state: 'pacing', title: `${pacing} target${pacing === 1 ? '' : 's'} pacing at a cap`,
             sub: 'They are refusing work at the ceiling, which is the limiter doing its job.' }
  }
  return { state: 'flowing', title: 'Every target under its caps',
           sub: 'Nothing has had to be held back anywhere.' }
})
</script>

<template>
  <div>
    <header class="mb-8 flex items-center">
      <h1 class="text-[28px] font-semibold tracking-[-0.02em] leading-tight">Overview</h1>
      <span class="ml-auto flex items-center gap-2 text-[11.5px] text-fg-3">
        <span class="w-[7px] h-[7px] rounded-full bg-good animate-pulse2" /> live
      </span>
    </header>

    <div v-if="error" class="card border-transparent bg-bad-dim px-5 py-4 text-[13.5px] text-bad">
      {{ error }}
    </div>

    <div v-else-if="!status" class="card px-6 py-8"><div class="skeleton h-5 w-1/3" /></div>

    <template v-else>
      <!-- ------------------------------------------------ status hero -->
      <section class="card px-6 py-6 md:px-7 flex flex-col md:flex-row md:items-center gap-6">
        <div class="flex-1 min-w-0">
          <StatusDot :state="status.state" size="lg" :label="status.title" avatar>
            <p class="text-[13px] text-fg-2 mt-2 ml-[22.5px] max-w-[52ch] leading-relaxed">{{ status.sub }}</p>
          </StatusDot>
        </div>
        <!-- Admitted and denied stand side by side, in the same weight and the
             same colour, because they are two halves of one measurement: the
             denials are the work the ceiling held back, not the work we lost. -->
        <div class="grid grid-cols-2 sm:grid-cols-4 gap-7 md:gap-9 md:pl-8 md:border-l border-line shrink-0">
          <Metric label="Targets" :value="num(targets.length)" />
          <Metric label="Paths" :value="num(paths)" />
          <Metric label="Admitted" :value="rate(overview.admitted_per_sec)" unit="/s" />
          <Metric label="Denied" :value="num(overview.denied_total)" />
        </div>
      </section>

      <!-- ------------------------------------------------- the flow -->
      <!-- Below the hero and above the exceptions: the hero says what is true
           right now, this says how we got here, and what needs attention is
           what to do about it. -->
      <div class="mt-10">
        <FlowChart />
      </div>

      <!-- ------------------------------------------- needs attention -->
      <section v-if="attention.length" class="mt-10">
        <h2 class="section-title">Needs attention
          <span class="section-count">{{ attention.length }}</span>
        </h2>
        <div class="card divide-y divide-line">
          <RouterLink
            v-for="t in attention" :key="keyOf(t)"
            :to="targetPath(t.application, t.name)"
            class="flex items-center gap-4 px-5 py-4 hover:bg-surface-2 transition-colors group"
          >
            <div class="min-w-0 flex-1">
              <div class="flex items-center gap-2.5">
                <span class="font-medium text-[14px]">{{ t.name }}</span>
                <StatusDot :state="isThrottled(t) ? 'breached' : (t.assumed_budgets ? 'blind' : t.state)" />
              </div>
              <div class="text-[12.5px] text-fg-2 truncate mt-0.5">{{ reason(t) }}</div>
            </div>
            <div class="w-28 hidden sm:block shrink-0">
              <BudgetBar :used="t.worst_used" :cap="t.worst_cap" :assumed="t.worst_assumed" />
              <div class="text-[11px] text-fg-3 tabular-nums mt-1 text-right">
                {{ pct(t.worst_cap ? t.worst_used / t.worst_cap : 0) }}
              </div>
            </div>
            <Icon name="chevron" :size="15" class="text-fg-3 group-hover:text-fg-2 transition-colors" />
          </RouterLink>
        </div>
      </section>

      <!-- ------------------------------------------------- breaches -->
      <section class="mt-10">
        <h2 class="section-title">Recent breaches
          <span class="section-count">the only proof our numbers are wrong</span>
        </h2>
        <div class="card">
          <div v-if="!breaches.length" class="px-6 py-10 text-center">
            <p class="text-[13.5px] text-fg-2">No vendor has throttled us recently.</p>
            <p class="text-[12.5px] text-fg-3 mt-1">
              Either the caps are right, or nothing has pushed against them.
            </p>
          </div>
          <div v-else class="divide-y divide-line">
            <RouterLink v-for="(b, i) in breaches" :key="i"
                        :to="traceRefPath(b)"
                        class="flex items-center gap-4 px-5 py-3 text-[13px] hover:bg-surface-2 transition-colors">
              <span class="chip">{{ traceRef(b).name }}</span>
              <span v-if="traceRef(b).scoped"
                    class="chip text-fg-3 hidden sm:inline-flex">{{ traceRef(b).application }}</span>
              <span class="font-mono text-[12px] text-bad truncate flex-1">{{ b.budget_id || b.op || 'unattributed' }}</span>
              <span class="text-fg-3 text-[12px] tabular-nums whitespace-nowrap">{{ ago(b.at) }}</span>
            </RouterLink>
          </div>
        </div>
      </section>
    </template>
  </div>
</template>
