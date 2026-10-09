<script setup>
import { computed, ref } from 'vue'
import { integrationExamples } from '../lib/target-setup.js'
const props = defineProps({ graph: Object, open: Boolean })
const selected = ref(''), tab = ref('producer'), copied = ref(''), copyError = ref('')
const nodes = computed(() => (props.graph.nodes ?? []).filter(n => n.ingressQueue || n.egressQueue))
const node = computed(() => nodes.value.find(n => n.node === selected.value) ?? nodes.value[0])
const examples = computed(() => node.value ? integrationExamples(props.graph, node.value) : {})
const options = computed(() => [{ id: 'producer', label: 'Producer · JavaScript' }, { id: 'http', label: 'Producer · HTTP' }, { id: 'consumer', label: 'Worker · JavaScript' }].filter(t => examples.value[t.id]))
const active = computed(() => options.value.find(t => t.id === tab.value)?.id ?? options.value[0]?.id)
async function copy() {
  try { await navigator.clipboard.writeText(examples.value[active.value]); copied.value = 'Copied'; copyError.value = '' }
  catch { copyError.value = 'Copy is unavailable here. Select and copy the code below.' }
}
</script>
<template>
  <details class="card editor-section mb-5 integration-guide" :open="open">
    <summary class="font-semibold cursor-pointer">Integration guide <span class="font-normal text-fg-3 ml-2">Queues, groups and ready-to-adapt examples</span></summary>
    <div class="mt-5 space-y-4">
      <div class="flex items-center gap-3"><label for="integration-node" class="label mb-0">Node</label><select id="integration-node" :value="node?.node" @change="selected = $event.target.value" class="input max-w-56"><option v-for="n in nodes" :key="n.node" :value="n.node">{{ n.node }}</option></select></div>
      <dl v-if="node" class="wizard-review">
        <div v-if="node.ingressQueue"><dt>Producer sends to</dt><dd class="font-mono break-all">{{ node.ingressQueue }}</dd></div>
        <div v-if="node.egressQueue"><dt>Worker consumes</dt><dd class="font-mono break-all">{{ node.egressQueue }}</dd></div>
        <div v-if="node.egressQueue"><dt>Worker group</dt><dd class="font-mono break-all">{{ node.egressGroup || 'Queue mode · no named group' }}</dd></div>
      </dl>
      <div class="flex flex-wrap items-center gap-2"><button v-for="option in options" :key="option.id" class="btn" :class="{ 'btn-primary': active === option.id }" @click="tab = option.id; copied = ''">{{ option.label }}</button><button v-if="active" class="btn ml-auto" @click="copy">{{ copied || 'Copy code' }}</button></div>
      <p v-if="copyError" class="hint" role="status">{{ copyError }}</p>
      <pre v-if="active" class="editor-json-preview" tabindex="0">{{ examples[active] }}</pre>
      <p class="editor-note">Install <code>queen-mq</code> for JavaScript and set <code>QUEEN_URL</code> to your broker. For HTTP, set <code>GATE_URL</code> to Gate's internal service address. Replace the sample payload and implement <code>handleWork</code>; a successful handler acknowledges the message. A new consumer uses <code>all</code> to include queued work.</p>
      <p v-if="graph.spec?.watch" class="hint">This target is in Watch: work reaches the outgoing queue without rate limits.</p>
    </div>
  </details>
</template>
