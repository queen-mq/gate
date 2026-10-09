<script setup>
import { ref, watch } from 'vue'
import { api, graphApi, datetime } from '../lib/api.js'
import { usePoll } from '../lib/poll.js'
const props = defineProps({ application: String, graph: String })
const emit = defineEmits(['locate'])
const result = ref(null), error = ref('')
async function load() {
  const key = graphApi(props.application, props.graph)
  try {
    const next = await api.get(`${key}/diagnostics`)
    if (key !== graphApi(props.application, props.graph)) return
    result.value = next; error.value = ''
  } catch (failure) { if (key === graphApi(props.application, props.graph)) { error.value = failure.message; result.value = null } }
}
const refresh = usePoll(load, 10000)
watch(() => [props.application, props.graph], () => { result.value = null; error.value = ''; refresh() })
function elapsed(since) {
  const seconds = Math.max(0, Math.floor(((result.value?.at ?? since) - since) / 1000))
  return seconds < 60 ? `${seconds}s` : seconds < 3600 ? `${Math.floor(seconds / 60)}m` : `${Math.floor(seconds / 3600)}h ${Math.floor(seconds % 3600 / 60)}m`
}
</script>
<template>
  <section class="card editor-section mb-5" aria-label="Traffic diagnostics">
    <div class="flex items-start justify-between gap-3 mb-3"><div><h2 class="editor-section-title">Why is traffic waiting?</h2><p class="hint mt-1">Live checks of the broker, relay, limits and workers.</p></div><span v-if="result && !result.issues.length" class="chip">No waiting work detected</span></div>
    <p v-if="error" class="text-bad" role="alert">{{ error }}</p>
    <div v-else-if="result?.issues.length" class="space-y-3">
      <article v-for="(issue, i) in result.issues" :key="i" class="diagnostic-row" :class="`diagnostic-${issue.severity}`">
        <div class="flex flex-wrap justify-between gap-2"><strong>{{ issue.title }}</strong><span class="text-xs text-fg-3" :title="datetime(issue.since)">Observed for {{ elapsed(issue.since) }}</span></div>
        <p class="text-sm text-fg-2 mt-1">{{ issue.action }}</p>
        <button v-if="issue.node" class="editor-text-button mt-2" @click="emit('locate', issue)">{{ issue.node }}{{ issue.path ? ` / ${issue.path}` : '' }}{{ issue.budget ? ` · ${issue.budget}` : '' }} → Locate in graph</button>
      </article>
      <p class="hint">{{ result.durationScope }}</p>
    </div>
    <p v-else-if="result" class="hint">No current backlog or relay failure was found by these checks.</p>
    <div v-else class="skeleton h-10 w-full" />
  </section>
</template>
