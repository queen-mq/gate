<script setup>
import { computed, ref, watch } from 'vue'
import { api, graphApi, datetime } from '../lib/api.js'
import { usePoll } from '../lib/poll.js'
import { timelineSeries } from '../lib/insights-series.js'
import HistoryChart from './HistoryChart.vue'
const props = defineProps({ application: String, graph: String, nodes: Array })
const node = ref(''), minutes = ref(60), data = ref(null), error = ref(''), at = ref(Date.now()), showTable = ref(false)
const selected = computed(() => props.nodes?.find(n => n.node === node.value)?.node ?? props.nodes?.[0]?.node ?? '')
async function load() {
  const key = `${graphApi(props.application, props.graph)}/timeline?minutes=${minutes.value}`
  try {
    const next = await api.get(key)
    if (key !== `${graphApi(props.application, props.graph)}/timeline?minutes=${minutes.value}`) return
    data.value = next; at.value = Date.now(); error.value = ''
  } catch (failure) { if (key === `${graphApi(props.application, props.graph)}/timeline?minutes=${minutes.value}`) { data.value = null; error.value = failure.message } }
}
const refresh = usePoll(load, 15000)
watch(() => [props.application, props.graph, minutes.value], () => { data.value = null; error.value = ''; refresh() })
const rows = computed(() => timelineSeries(data.value, selected.value, at.value - minutes.value * 60_000, Math.floor(at.value / 60_000) * 60_000))
const traffic = [{ key:'incoming', label:'Incoming · estimate', color:'var(--chart-incoming)' }, { key:'relayed', label:'Relayed', color:'var(--chart-relayed)' }]
const queues = [{ key:'before', label:'Before relay', color:'var(--chart-relay-queue)' }, { key:'workers', label:'Waiting for workers', color:'var(--chart-worker-queue)' }]
const ages = [{ key:'oldest', label:'Oldest pending', color:'var(--chart-age)' }]
</script>
<template>
  <section class="card editor-section mb-5" aria-label="Traffic history">
    <div class="flex flex-wrap justify-between items-start gap-4 mb-5">
      <div><h2 class="editor-section-title">Traffic over time</h2><p class="hint mt-1">Flow, queue pressure and the age of waiting work.</p></div>
      <div class="flex gap-2"><select :value="selected" aria-label="History node" class="input max-w-44" @change="node = $event.target.value"><option v-for="n in nodes" :key="n.node" :value="n.node">{{ n.node }}</option></select><select v-model.number="minutes" class="input max-w-40" aria-label="History range"><option :value="15">15 minutes</option><option :value="60">1 hour</option><option :value="360">6 hours</option><option :value="1440">24 hours</option></select></div>
    </div>
    <p v-if="error" role="alert" class="text-bad">{{ error }}</p>
    <p v-else-if="data && !data.enabled" class="editor-note">Enable history counters in the rule editor to collect traffic and queue history.</p>
    <template v-else>
      <div class="grid xl:grid-cols-3 gap-4"><HistoryChart title="Incoming and relayed" unit="items / minute" :rows="rows" :lines="traffic" :events="data?.events" /><HistoryChart title="Backlog" unit="items" :rows="rows" :lines="queues" :events="data?.events" /><HistoryChart title="Oldest pending message" unit="seconds" :rows="rows" :lines="ages" :events="data?.events" /></div>
      <p class="hint mt-4">One-minute samples. Incoming is estimated from broker consumer positions for single-path ingress nodes; retention and cursor changes can affect it. Oldest age uses broker timestamps for the relevant groups; unavailable timestamps remain blank. Relay counts include every hop through the selected node.</p>
      <details v-if="data?.events?.length" class="mt-4"><summary class="text-xs cursor-pointer text-fg-2">Configuration changes · {{ data.events.length }} <span class="text-fg-3">(dashed chart markers)</span></summary><ul class="mt-3 space-y-2 text-xs"><li v-for="(event, i) in data.events" :key="i"><time class="text-fg-3 mr-3">{{ datetime(event.at) }}</time>{{ event.change.label }} · v{{ event.change.version }} · {{ event.change.mode }}</li></ul></details>
      <button class="editor-text-button mt-4" :aria-expanded="showTable" @click="showTable = !showTable">{{ showTable ? 'Hide' : 'Show' }} recent measurements</button>
      <div v-if="showTable" class="overflow-x-auto mt-3"><table class="w-full text-xs text-left"><thead><tr><th>Time</th><th>Incoming / min</th><th>Relayed / min</th><th>Before relay</th><th>Workers</th><th>Oldest · s</th></tr></thead><tbody><tr v-for="row in rows.slice(-15).reverse()" :key="row.t" class="border-t border-line"><td class="py-2 pr-4">{{ datetime(row.t) }}</td><td v-for="key in ['incoming','relayed','before','workers','oldest']" :key="key">{{ row[key] === null || row[key] === undefined ? '—' : Math.round(row[key] * 10) / 10 }}</td></tr></tbody></table></div>
    </template>
  </section>
</template>
