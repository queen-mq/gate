<script setup>
/*
  The roll-ups say how much; this says which. When a caller swears a request was
  refused and the counters agree that thousands were, the only thing that settles
  it is the decision itself — which path, and which budget held it.

  **Denials, and only denials.** v1 wrote one row per decision through a calls
  queue, in the same transaction as the ack; there is no ack any more, so there
  is nothing to ride along with and nothing that could compare an estimated cost
  with an actual one. What is kept is the interesting event: the refusal.
  Admissions are counted and never traced.
*/
import { ref, computed, watch } from 'vue'
import PageHeader from '../components/PageHeader.vue'
import TraceList from '../components/TraceList.vue'
import { api, splitTargetKey, traceRef, DEFAULT_APP } from '../lib/api.js'
import { usePoll } from '../lib/poll.js'

const OUTCOMES = [
  { key: 'denied', label: 'Denied', note: 'the gate refused the batch — the last 500, and whatever has been flushed' },
  { key: 'all', label: 'All', note: 'the same list: an admission is counted, never traced' },
]

const outcome = ref('denied')
const target = ref('')
const raw = ref(undefined) // undefined = first load, null = nothing to read
const targets = ref([])
const error = ref('')

async function load() {
  const qs = new URLSearchParams({ limit: '200' })
  if (outcome.value !== 'all') qs.set('outcome', outcome.value)
  try {
    const [tr, ts] = await Promise.all([
      // No trace log is a successful empty response. A configured history
      // store that cannot be read must reach this page's error state instead.
      api.get(`/api/traces?${qs.toString()}`),
      api.get('/api/targets').catch(() => []),
    ])
    raw.value = tr === null ? null : (Array.isArray(tr) ? tr : (tr?.traces ?? []))
    targets.value = ts ?? []
    error.value = ''
  } catch (e) {
    error.value = e.message
  }
}
usePoll(load, 6000)
watch(outcome, load)

/* The endpoint narrows by outcome and nothing else, so the target filter is
   applied here. It costs nothing at the sizes this log is capped to, and a
   select that quietly did nothing would be worse than no select.

   The select is keyed on `application/name`, and so is the comparison, because
   filtering on the bare name would mix two teams' targets into one list the
   day both declare an `airbnb`. */
const traces = computed(() => {
  if (!raw.value) return raw.value
  if (!target.value) return raw.value
  const want = splitTargetKey(target.value)
  return raw.value.filter((t) => {
    const k = traceRef(t)
    return k.name === want.name && (!k.scoped || k.application === want.application)
  })
})

const note = computed(() => OUTCOMES.find((o) => o.key === outcome.value)?.note ?? '')
</script>

<template>
  <div>
    <PageHeader
      title="Traces"
      sub="Refusals, as they were taken. Admissions are counted and never traced — they are 99% of the volume and almost none of the interest, and the hot path writes one KV batch and one transaction and nothing else."
    >
      <template #actions>
        <select v-model="target" class="input w-[180px]">
          <option value="">Every target</option>
          <option
            v-for="t in targets" :key="`${t.application}/${t.name}`"
            :value="`${t.application || DEFAULT_APP}/${t.name}`"
          >{{ t.name }} — {{ t.application || DEFAULT_APP }}</option>
        </select>
      </template>
    </PageHeader>

    <div class="flex items-center gap-1 mb-3 flex-wrap">
      <button
        v-for="o in OUTCOMES" :key="o.key"
        class="h-[28px] px-2.5 rounded-md text-[12px] transition-colors"
        :class="outcome === o.key ? 'bg-fg text-bg font-medium' : 'text-fg-2 hover:bg-surface-2'"
        @click="outcome = o.key"
      >{{ o.label }}</button>
      <span class="ml-auto text-[11.5px] text-fg-3">{{ note }}</span>
    </div>

    <div v-if="error" class="card border-transparent bg-bad-dim px-5 py-4 text-[13.5px] text-bad">
      {{ error }}
    </div>

    <div v-else-if="traces === undefined" class="card divide-y divide-line">
      <div v-for="i in 4" :key="i" class="px-5 py-4"><div class="skeleton h-4 w-1/3" /></div>
    </div>

    <div v-else-if="traces === null" class="card px-6 py-14 text-center">
      <p class="text-[15px] font-medium">Decision traces are not being served</p>
      <p class="hint mt-2 max-w-md mx-auto">
        Refusals are kept in a bounded ring in each replica and flushed to Postgres when one is
        configured, never written in line with a decision — so the gate can be perfectly healthy
        while this page has nothing to read.
      </p>
    </div>

    <div v-else-if="!traces.length" class="card px-6 py-14 text-center">
      <p class="text-[15px] font-medium">Nothing to show</p>
      <p class="hint mt-2 max-w-md mx-auto">
        <template v-if="outcome === 'denied'">
          No refusal has been recorded{{ target ? ` on ${target}` : '' }}. Denials are the one outcome
          kept in full rather than sampled, so an empty page here is a quiet limiter and not a gap.
        </template>
        <template v-else>
          Nothing has been refused{{ target ? ` on ${target}` : '' }}. A vendor throttle is not here
          either — it is reported to the backoff endpoint and appears on the overview.
        </template>
      </p>
    </div>

    <div v-else class="card">
      <TraceList :traces="traces" :show-target="!target" />
    </div>
  </div>
</template>
