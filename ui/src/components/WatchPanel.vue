<script setup>
import WatchSimulator from './WatchSimulator.vue'
import { ref, watch } from 'vue'
import { api, graphApi, graphPath, isAdmin, num, datetime } from '../lib/api.js'
import { usePoll } from '../lib/poll.js'
const props = defineProps({ application: String, graph: String })
const summary = ref(null), error = ref('')
async function load() {
  const app = props.application, name = props.graph
  try {
    const result = await api.get(`${graphApi(app, name)}/watch`)
    if (app !== props.application || name !== props.graph) return
    summary.value = result
    error.value = ''
  } catch (failure) {
    if (app !== props.application || name !== props.graph) return
    error.value = failure.message
  }
}
const refresh = usePoll(load, 15000)
watch(() => [props.application, props.graph], () => { summary.value = null; error.value = ''; refresh() })
</script>

<template>
  <section class="card editor-section mb-5 space-y-4" aria-label="Traffic observation">
    <div class="flex flex-wrap items-start justify-between gap-4">
      <div>
        <h2 class="editor-section-title">{{ summary?.ready ? 'Observation period complete' : 'Watching traffic' }}</h2>
        <p class="hint mt-1">Traffic keeps flowing without rate limits. Activate limits after reviewing the measurements.</p>
      </div>
      <RouterLink v-if="isAdmin" :to="{ path: graphPath(application, graph, '/edit'), query: { configureLimits: '1' } }" class="btn btn-primary">Configure limits</RouterLink>
    </div>
    <p v-if="error" role="alert" class="text-bad">{{ error }}</p>
    <template v-else-if="summary">
      <p class="hint">Started {{ datetime(summary.startedAt) }} · {{ summary.ready ? 'Review due' : 'Review scheduled' }} {{ datetime(summary.endsAt) }}</p>
      <div class="overflow-x-auto">
        <table class="w-full text-sm text-left">
          <thead class="text-fg-3 text-xs"><tr><th class="py-2 pr-4 font-medium">Node</th><th class="py-2 pr-4 font-medium">Items relayed</th><th class="py-2 pr-4 font-medium">Average cost / second</th><th class="py-2 font-medium">Peak cost / minute</th></tr></thead>
          <tbody><tr v-for="item in summary.nodes" :key="item.node" class="border-t border-line"><td class="py-3 pr-4 font-mono">{{ item.node }}</td><td class="py-3 pr-4 tabular-nums">{{ num(item.traffic.admitted) }}</td><td class="py-3 pr-4 tabular-nums">{{ item.traffic.averageCostPerSecond === null ? '—' : item.traffic.averageCostPerSecond.toLocaleString(undefined, { maximumFractionDigits: 2 }) }}</td><td class="py-3 tabular-nums">{{ item.traffic.peakCostPerMinute === null ? '—' : num(item.traffic.peakCostPerMinute) }}</td></tr></tbody>
        </table>
      </div>
      <p v-if="!summary.minutes" class="hint">Waiting for the first complete minute of observation.</p>
      <p class="editor-note">Measured over {{ summary.minutes }} complete {{ summary.minutes === 1 ? 'minute' : 'minutes' }}, including idle time. The first and last partial minute are excluded; history may arrive one minute later. The peak is a one-minute total, so it does not describe bursts within a second. Observed traffic is a sizing reference; verify the provider's quota before choosing a limit.</p>
    </template>
    <div v-else class="skeleton h-12 w-full" />
    <WatchSimulator v-if="summary" :application="application" :graph="graph" :nodes="summary.nodes" />
  </section>
</template>
