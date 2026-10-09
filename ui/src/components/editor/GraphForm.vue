<script setup>
import { computed, ref, useId } from 'vue'
import Field from '../Field.vue'
import Icon from '../Icon.vue'
import NodeFields from './NodeFields.vue'
import PathFields from './PathFields.vue'
import WatchSettings from './WatchSettings.vue'
import { NAME_HINT, newNode, nodeIsUsed, optional, renameNode, trafficMode } from '../../lib/graph-editor.js'
const props = defineProps({ modelValue: { type: Object, required: true }, lockedIdentity: Boolean })
const emit = defineEmits(['update:modelValue'])
const names = computed(() => Object.keys(props.modelValue.nodes))
const nameError = ref('')
const id = useId()
function set(key, value) { emit('update:modelValue', { ...props.modelValue, [key]: value }) }
function node(name, next) { set('nodes', { ...props.modelValue.nodes, [name]: next }) }
function rename(previous, event) {
  try { emit('update:modelValue', renameNode(props.modelValue, previous, event.target.value)); nameError.value = '' }
  catch (error) { nameError.value = error.message; event.target.value = previous }
}
function addNode() {
  let n = names.value.length + 1
  while (names.value.includes(`node-${n}`)) n++
  node(`node-${n}`, newNode())
}
function removeNode(name) {
  if (names.value.length <= 1 || nodeIsUsed(props.modelValue, name)) return
  set('nodes', Object.fromEntries(Object.entries(props.modelValue.nodes).filter(([key]) => key !== name)))
}
</script>

<template>
  <div class="space-y-5">
    <section class="card editor-section">
      <h2 class="editor-section-title mb-4">Identity</h2>
      <div class="grid sm:grid-cols-[1fr_1fr_120px] gap-4">
        <Field label="Application" :for="`${id}-app`" :hint="lockedIdentity ? 'The application owning this graph.' : NAME_HINT"><input :id="`${id}-app`" :value="modelValue.application" :readonly="lockedIdentity" class="input font-mono" @input="set('application', $event.target.value)" /></Field>
        <Field label="Graph name" :for="`${id}-graph`" :hint="lockedIdentity ? 'Identity stays fixed while editing.' : NAME_HINT"><input :id="`${id}-graph`" :value="modelValue.graph" :readonly="lockedIdentity" class="input font-mono" placeholder="provider-api" @input="set('graph', $event.target.value)" /></Field>
        <Field label="Version" :for="`${id}-version`" hint="Your declaration's revision."><input :id="`${id}-version`" :value="modelValue.version" class="input" type="number" min="1" step="1" @input="set('version', $event.target.value === '' ? '' : Number($event.target.value))" /></Field>
      </div>
    </section>
    <section class="card editor-section space-y-4">
      <h2 class="editor-section-title">Traffic mode</h2>
      <Field label="Mode" :for="`${id}-mode`">
        <select :id="`${id}-mode`" class="input" :value="modelValue.watch ? 'watch' : 'enforce'" @change="emit('update:modelValue', trafficMode(modelValue, $event.target.value === 'watch', lockedIdentity))">
          <option value="enforce">Enforce limits</option><option value="watch">Watch traffic</option>
        </select>
      </Field>
      <WatchSettings v-if="modelValue.watch" :model-value="modelValue.watch" @update:model-value="set('watch', $event)" />
      <p v-if="lockedIdentity" class="hint">Changing traffic mode increases the declaration version. Saving applies the selected mode to every node.</p>
      <p v-if="modelValue.watch" class="hint">Limits below are kept for later and remain inactive while watching.</p>
    </section>
    <div class="flex items-center justify-between">
      <h2 class="editor-section-title">Nodes <span class="section-count">{{ names.length }}</span></h2>
      <button type="button" class="btn" @click="addNode"><Icon name="plus" :size="14" /> Add node</button>
    </div>
    <p v-if="nameError" role="alert" class="text-bad">{{ nameError }}</p>
    <section v-for="(item, name) in modelValue.nodes" :key="name" class="card editor-section">
      <div class="flex flex-wrap justify-between items-start gap-4 mb-5">
        <Field label="Node name" :for="`${id}-${name}-name`" :hint="NAME_HINT" class="w-full sm:w-[280px]"><input :id="`${id}-${name}-name`" :value="name" class="input font-mono" @change="rename(name, $event)" /></Field>
        <div class="sm:pt-6 text-right">
          <button type="button" class="btn btn-sm btn-danger" :disabled="names.length <= 1 || nodeIsUsed(modelValue, name)" @click="removeNode(name)"><Icon name="x" :size="13" /> Remove node</button>
          <p v-if="names.length > 1 && nodeIsUsed(modelValue, name)" class="text-[11px] text-fg-3 mt-1">Remove it from its paths first.</p>
        </div>
      </div>
      <NodeFields :model-value="item" :watching="!!modelValue.watch" :queue-prefix="`gate.${modelValue.application || '<application>'}.${modelValue.graph || '<graph>'}.${name}`" @update:model-value="node(name, $event)" />
    </section>
    <PathFields :model-value="modelValue.paths" :names="names" @update:model-value="set('paths', $event)" />
    <section class="card editor-section">
      <details class="editor-details" :open="modelValue.maxAttempts !== undefined || !!modelValue.counters">
        <summary>History and re-entry</summary>
        <div class="grid sm:grid-cols-2 gap-4 mt-4">
          <Field label="Re-entry attempts (optional)" :for="`${id}-attempts`" hint="Maximum times an item can re-enter after a reported throttle. From 1 to 20."><input :id="`${id}-attempts`" :value="modelValue.maxAttempts" type="number" min="1" max="20" step="1" class="input" placeholder="Default" @input="emit('update:modelValue', optional(modelValue, 'maxAttempts', $event.target.value === '' ? '' : Number($event.target.value)))" /></Field>
          <label class="flex items-center gap-3 self-center"><input type="checkbox" :checked="!!modelValue.counters || !!modelValue.watch" :disabled="!!modelValue.watch" @change="emit('update:modelValue', optional(modelValue, 'counters', $event.target.checked ? { ...modelValue.counters, windowSeconds: 60 } : undefined))" /><span>Enable one-minute history counters</span></label>
        </div>
      </details>
    </section>
  </div>
</template>
