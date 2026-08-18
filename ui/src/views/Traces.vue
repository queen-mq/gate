<script setup>
/*
  The roll-ups say how much; this says which. When a caller swears a request
  was refused and the counters agree that thousands were, the only thing that
  settles it is the decision itself — which lane, which op, and which budget
  held it.

  The default filter is denials, and not for tidiness: every denial and every
  breach is kept whole while the rest is sampled, so denials are the only view
  of this log that is complete. Each filter says which of the two it is.
*/
import { ref, computed, watch } from 'vue'
import PageHeader from '../components/PageHeader.vue'
import TraceList from '../components/TraceList.vue'
import { api, splitTargetKey, traceRef, DEFAULT_APP } from '../lib/api.js'
import { usePoll } from '../lib/poll.js'

const OUTCOMES = [
  { key: 'denied', label: 'Denied', note: 'the gate refused the call — every one is kept' },
  { key: 'throttled', label: 'Breached', note: 'the vendor refused it after we admitted it — every one is kept' },
  { key: 'ok', label: 'Completed', note: 'work the caller acked as done — sampled, never complete' },
  { key: 'all', label: 'All', note: 'denials and breaches whole, the rest sampled' },
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
      // A build with no trace log is not a broken console; it is a console
      // with nothing to show on one page.
      api.get(`/api/traces?${qs.toString()}`).catch(() => null),
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
      sub="Individual decisions, as they were taken. Denials and breaches are kept in full; the rest is sampled, because admissions are 99% of the volume and almost none of the interest."
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
        Traces are written away from the admission path, never in line with it, so the gate can be
        perfectly healthy while this page has nothing to read.
      </p>
    </div>

    <div v-else-if="!traces.length" class="card px-6 py-14 text-center">
      <p class="text-[15px] font-medium">Nothing to show</p>
      <p class="hint mt-2 max-w-md mx-auto">
        <template v-if="outcome === 'denied'">
          No refusal has been recorded{{ target ? ` on ${target}` : '' }}. Denials are the one outcome
          kept in full rather than sampled, so an empty page here is a quiet limiter and not a gap.
        </template>
        <template v-else-if="outcome === 'throttled'">
          No vendor has refused work we admitted{{ target ? ` on ${target}` : '' }} — the caps being
          enforced have held.
        </template>
        <template v-else>
          Completed calls are sampled, so an empty page here means the sample missed rather than that
          nothing ran.
        </template>
      </p>
    </div>

    <div v-else class="card">
      <TraceList :traces="traces" :show-target="!target" />
    </div>
  </div>
</template>
