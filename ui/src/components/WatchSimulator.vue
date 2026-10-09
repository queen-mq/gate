<script setup>
import { computed, ref, watch } from 'vue'
import { api, graphApi, num, datetime } from '../lib/api.js'
import Field from './Field.vue'
import DurationField from './editor/DurationField.vue'
const props = defineProps({ application: String, graph: String, nodes: Array })
const node = ref(''), count = ref(100), timeMs = ref(1000), subdivisions = ref(''), minutes = ref(60)
const busy = ref(false), error = ref(''), result = ref(null)
const selected = computed(() => node.value || props.nodes?.[0]?.node || '')
let token = 0
watch(() => [props.application, props.graph, node.value, count.value, timeMs.value, subdivisions.value, minutes.value], () => { token++; result.value = null; error.value = ''; busy.value = false })
async function simulate() {
  const id = ++token
  busy.value = true; error.value = ''; result.value = null
  try {
    const query = new URLSearchParams({ node: selected.value, count: count.value, timeMs: timeMs.value, minutes: minutes.value })
    if (subdivisions.value !== '') query.set('subWindows', subdivisions.value)
    const next = await api.get(`${graphApi(props.application, props.graph)}/simulate?${query}`)
    if (id === token) result.value = next
  } catch (failure) { if (id === token) error.value = failure.message }
  finally { if (id === token) busy.value = false }
}
</script>
<template>
  <div class="border-t border-line pt-5 space-y-4">
    <div><h3 class="font-semibold">Try a limit on observed traffic</h3><p class="hint mt-1">Preview one global allowance before activating it.</p></div>
    <form class="grid sm:grid-cols-2 lg:grid-cols-5 gap-3 items-start" @submit.prevent="simulate">
      <Field label="Node" for="simulate-node"><select id="simulate-node" :value="selected" class="input" @change="node = $event.target.value"><option v-for="n in nodes" :key="n.node" :value="n.node">{{ n.node }}</option></select></Field>
      <Field label="Cost allowance" for="simulate-count"><input id="simulate-count" v-model.number="count" type="number" min="1" max="1000000000" step="1" required class="input" /></Field>
      <Field label="Window" for="simulate-window"><DurationField v-model="timeMs" id="simulate-window" /></Field>
      <Field label="Subdivisions" for="simulate-sub" hint="Blank = automatic"><input id="simulate-sub" v-model="subdivisions" class="input" type="number" min="1" max="3600" step="1" placeholder="Automatic" /></Field>
      <Field label="Traffic range" for="simulate-range"><select id="simulate-range" v-model.number="minutes" class="input"><option :value="15">Last 15 minutes</option><option :value="60">Last hour</option><option :value="360">Last 6 hours</option><option :value="1440">Last 24 hours</option></select></Field>
      <button class="btn btn-primary justify-self-start" :disabled="busy || !selected">{{ busy ? 'Simulating…' : 'Simulate limit' }}</button>
    </form>
    <p v-if="error" class="text-bad" role="alert">{{ error }}</p>
    <div v-if="result" aria-live="polite" class="space-y-3">
      <p v-if="!result.items" class="editor-note">{{ result.coverage.until > result.coverage.since ? 'No relayed items were recorded in this range.' : 'No recorded interval is available yet. Samples start with this version; minute-only history cannot be replayed.' }}</p>
      <template v-else>
        <div class="simulation-results">
          <div><span>Items delayed · estimate</span><strong>{{ num(result.delayedItems) }} <small>({{ result.delayedPercent?.toFixed(1) }}%)</small></strong></div>
          <div><span>Peak backlog</span><strong>{{ num(result.peakBacklog) }}</strong></div>
          <div><span>Backlog at end</span><strong>{{ num(result.remainingBacklog) }}</strong></div>
          <div><span>Time to drain · no new traffic</span><strong>{{ num(result.drainSeconds) }}s</strong></div>
        </div>
        <p class="hint">{{ num(result.items) }} items · {{ datetime(result.coverage.since) }} – {{ datetime(result.coverage.until) }} · effective allowance {{ result.effective.count }} cost units / {{ result.effective.seconds }}s.</p>
        <p v-if="result.hasKnownGaps" class="text-warn text-sm">Incomplete recording: {{ result.coverage.gapSeconds }}s of known gaps; {{ num(result.coverage.lostItems) }} dropped items in this observation. These estimates cover only recorded traffic.</p>
      </template>
    </div>
    <p class="editor-note">One-second samples of successfully relayed Watch traffic; up to 24 hours, starting with an empty queue. Assumes one path, fixed item cost and a global counter used only by this node. Scoped, operation-specific and shared quotas need a different replay. Sub-second bursts and delays already present during Watch are not reconstructed. Nothing is activated by this preview.</p>
  </div>
</template>
