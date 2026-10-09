<script setup>
import { useId } from 'vue'
import Field from '../Field.vue'
import Icon from '../Icon.vue'
import { optional } from '../../lib/graph-editor.js'
const props = defineProps({ modelValue: { type: Array, required: true }, names: { type: Array, required: true } })
const emit = defineEmits(['update:modelValue'])
const id = useId()
function path(index, next) { emit('update:modelValue', props.modelValue.map((p, i) => i === index ? next : p)) }
function hop(index, position, next) { const p = props.modelValue[index]; path(index, { ...p, nodes: p.nodes.map((n, i) => i === position ? next : n) }) }
function branch(index, position, name, checked) {
  const names = props.modelValue[index].nodes[position]
  hop(index, position, checked ? [...names, name] : names.filter((n) => n !== name))
}
function addPath() {
  let n = props.modelValue.length + 1
  while (props.modelValue.some((p) => p.name === `path-${n}`)) n++
  emit('update:modelValue', [...props.modelValue, { name: `path-${n}`, nodes: [props.names[0] ?? ''] }])
}
</script>

<template>
  <section class="card editor-section space-y-4">
    <div class="flex items-start justify-between gap-3">
      <div><h2 class="editor-section-title">Paths</h2><p class="hint mt-1">Choose the order work follows. Start at an ingress and finish at a node with an outgoing queue.</p></div>
      <button type="button" class="btn shrink-0" @click="addPath"><Icon name="plus" :size="14" /> Add path</button>
    </div>
    <div v-for="(item, i) in modelValue" :key="i" class="editor-budget space-y-4">
      <div class="grid sm:grid-cols-[1fr_120px_140px_auto] gap-3 items-start">
        <Field label="Path name" :for="`${id}-${i}-name`"><input :id="`${id}-${i}-name`" :value="item.name" class="input font-mono" @input="path(i, { ...item, name: $event.target.value })" /></Field>
        <Field label="Priority" :for="`${id}-${i}-priority`" help="Lower numbers are higher priority. Priority determines default share ceilings, not queue scheduling."><input :id="`${id}-${i}-priority`" :value="item.priority ?? 0" class="input" type="number" min="0" step="1" @input="path(i, { ...item, priority: $event.target.value === '' ? '' : Number($event.target.value) })" /></Field>
        <Field label="Share (0–1)" :for="`${id}-${i}-share`" help="The fraction of the node allowance this path may use. Empty uses the default for its priority rank."><input :id="`${id}-${i}-share`" :value="item.share" class="input" type="number" min="0.000001" max="1" step="any" placeholder="Automatic" @input="path(i, optional(item, 'share', $event.target.value === '' ? '' : Number($event.target.value)))" /></Field>
        <button type="button" class="btn btn-sm btn-danger sm:mt-6" :disabled="modelValue.length <= 1" :aria-label="`Remove path ${item.name}`" @click="emit('update:modelValue', modelValue.filter((_, j) => i !== j))"><Icon name="x" :size="13" /> Remove</button>
      </div>
      <ol class="space-y-2" :aria-label="`Hops in ${item.name}`">
        <li v-for="(node, j) in item.nodes" :key="j" class="flex flex-wrap items-start gap-2">
          <span class="editor-hop-number">{{ j + 1 }}</span>
          <div class="flex-1 min-w-[180px]">
            <select v-if="!Array.isArray(node)" :value="node" class="input font-mono" :aria-label="`${item.name} hop ${j + 1}`" @change="hop(i, j, $event.target.value)">
              <option value="" disabled>Select a node</option>
              <option v-if="node && !names.includes(node)" :value="node">{{ node }} (missing)</option>
              <option v-for="name in names" :key="name" :value="name">{{ name }}</option>
            </select>
            <fieldset v-else class="rounded-md border border-line px-3 py-2">
              <legend class="text-xs text-fg-3">Fan out to all selected nodes</legend>
              <label v-for="name in [...new Set([...names, ...node])]" :key="name" class="inline-flex items-center gap-2 mr-4 py-1 font-mono text-xs">
                <input type="checkbox" :checked="node.includes(name)" @change="branch(i, j, name, $event.target.checked)" /> {{ name }}{{ names.includes(name) ? '' : ' (missing)' }}
              </label>
            </fieldset>
          </div>
          <button v-if="j === item.nodes.length - 1" type="button" class="btn btn-sm" @click="hop(i, j, Array.isArray(node) ? node[0] ?? names[0] ?? '' : [node].filter(Boolean))">{{ Array.isArray(node) ? 'Single node' : 'Fan out' }}</button>
          <button type="button" class="btn btn-sm btn-danger" :disabled="item.nodes.length <= 1" :aria-label="`Remove ${item.name} hop ${j + 1}`" @click="path(i, { ...item, nodes: item.nodes.filter((_, k) => k !== j) })"><Icon name="x" :size="13" /></button>
        </li>
      </ol>
      <button type="button" class="btn btn-sm" :disabled="Array.isArray(item.nodes.at(-1))" @click="path(i, { ...item, nodes: [...item.nodes, names[0] ?? ''] })"><Icon name="plus" :size="13" /> Add hop</button>
    </div>
  </section>
</template>
