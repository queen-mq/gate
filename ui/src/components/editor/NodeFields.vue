<script setup>
import { computed, useId } from 'vue'
import Field from '../Field.vue'
import Icon from '../Icon.vue'
import BudgetFields from './BudgetFields.vue'
import { newBudget, optional } from '../../lib/graph-editor.js'
const props = defineProps({
  modelValue: { type: Object, required: true },
  sections: { type: Array, default: () => ['routing', 'limits'] },
  queuePrefix: String,
  standalone: Boolean,
  watching: Boolean,
})
const emit = defineEmits(['update:modelValue'])
const id = useId()
const mode = computed(() => !props.modelValue.ingress ? 'none' : typeof props.modelValue.ingress === 'object' && props.modelValue.ingress.queue !== undefined ? 'existing' : 'owned')
const egress = computed(() => typeof props.modelValue.egress === 'object' ? props.modelValue.egress : { queue: props.modelValue.egress })
const cost = computed(() => typeof props.modelValue.cost === 'object' ? props.modelValue.cost : null)
function set(key, value) { emit('update:modelValue', { ...props.modelValue, [key]: value }) }
function opt(key, value) { emit('update:modelValue', optional(props.modelValue, key, value)) }
function number(event) { return event.target.value === '' ? '' : Number(event.target.value) }
function ingressMode(value) {
  if (value === 'none') opt('ingress', undefined)
  else if (value === 'existing') set('ingress', { queue: '' })
  else set('ingress', true)
}
function ingress(key, value) {
  const original = typeof props.modelValue.ingress === 'object' ? props.modelValue.ingress : {}
  set('ingress', optional(original, key, value))
}
function output(key, value) {
  if (key === 'group' && props.modelValue.egress === undefined && value === '') return
  const next = optional(egress.value, key, value)
  // Preserve an existing object shape and its other fields.
  set('egress', typeof props.modelValue.egress === 'object' || next.group !== undefined ? next : next.queue ?? '')
}
function budget(index, next) { set('budgets', (props.modelValue.budgets ?? []).map((b, i) => i === index ? next : b)) }
function addBudget() {
  const budgets = props.modelValue.budgets ?? []
  let n = budgets.length + 1
  while (budgets.some((b, i) => (b.id ?? `b${i}`) === `limit-${n}`)) n++
  set('budgets', [...budgets, newBudget(n)])
}
</script>

<template>
  <div class="space-y-6">
    <section v-if="sections.includes('routing')" class="space-y-4">
      <div class="grid sm:grid-cols-2 gap-4">
        <Field label="Incoming work" :for="`${id}-ingress`" hint="Choose how messages enter this node.">
          <select :id="`${id}-ingress`" :value="mode" class="input" @change="ingressMode($event.target.value)">
            <option value="owned">Gate creates an ingress queue</option>
            <option value="existing">Consume an existing queue</option>
            <option v-if="!standalone" value="none">Receive from another node</option>
          </select>
        </Field>
        <Field v-if="mode === 'existing'" label="Existing ingress queue" :for="`${id}-queue`" hint="This queue must already exist in QueenMQ.">
          <input :id="`${id}-queue`" :value="modelValue.ingress.queue" class="input font-mono" placeholder="your-app.requests.in" @input="ingress('queue', $event.target.value)" />
        </Field>
        <div v-else-if="mode === 'owned'" class="editor-note self-end">
          Work enters <code>{{ queuePrefix || 'gate.<application>.<target>.<node>' }}.ingress</code>.
        </div>
      </div>
      <div class="grid sm:grid-cols-2 gap-4">
        <Field label="Outgoing queue" :for="`${id}-egress`" :hint="standalone ? watching ? 'The queue your workers consume while Gate observes traffic.' : 'The queue your workers consume after work meets every limit.' : 'The queue your workers consume. Required for a node at the end of a path.'">
          <input :id="`${id}-egress`" :value="egress.queue" class="input font-mono" placeholder="your-app.requests.out" @input="output('queue', $event.target.value)" />
          <button v-if="!standalone && modelValue.egress !== undefined" type="button" class="editor-text-button mt-1" @click="opt('egress', undefined)">No outgoing queue (intermediate node)</button>
        </Field>
        <Field label="Worker consumer group (optional)" :for="`${id}-group`" hint="Makes the backlog estimate specific to your workers' consumer group.">
          <input :id="`${id}-group`" :value="egress.group" class="input font-mono" placeholder="your-workers" @input="output('group', $event.target.value)" />
        </Field>
      </div>
      <details v-if="mode !== 'none'" class="editor-details">
        <summary>Advanced queue settings</summary>
        <div class="grid sm:grid-cols-3 gap-4 mt-4">
          <Field v-if="mode === 'owned'" label="Ingress partitions" :for="`${id}-partitions`" hint="More partitions allow independent work to proceed concurrently.">
            <input :id="`${id}-partitions`" :value="modelValue.ingress?.partitions" class="input" type="number" min="1" step="1" placeholder="Default" @input="ingress('partitions', number($event))" />
          </Field>
          <Field label="HTTP push endpoint" :for="`${id}-http`">
            <select :id="`${id}-http`" :value="modelValue.ingress?.http === undefined ? '' : String(modelValue.ingress.http)" class="input" @change="ingress('http', $event.target.value === '' ? undefined : $event.target.value === 'true')">
              <option value="">Default ({{ mode === 'owned' ? 'enabled' : 'disabled' }})</option><option value="true">Enabled</option><option value="false">Disabled</option>
            </select>
          </Field>
          <Field label="At the limit" :for="`${id}-shed`" :hint="watching ? 'Load shedding is inactive while watching traffic.' : 'Queueing preserves work. Refusing HTTP pushes requires callers to retry.'">
            <select :id="`${id}-shed`" :disabled="watching" :value="modelValue.ingress?.shed === undefined ? '' : String(modelValue.ingress.shed)" class="input" @change="ingress('shed', $event.target.value === '' ? undefined : $event.target.value === 'true')">
              <option value="">Queue work (default)</option><option value="false">Queue work</option><option value="true">Refuse HTTP pushes with 429</option>
            </select>
          </Field>
        </div>
      </details>
    </section>

    <section v-if="sections.includes('limits')" class="space-y-4">
      <div class="flex items-start justify-between gap-3">
        <div><h3 class="font-semibold">{{ watching ? 'Candidate limits (inactive)' : 'Rate limits' }}</h3><p class="hint mt-1">{{ watching ? 'Optional limits to prepare for later. Watch does not apply them.' : 'Every item must fit every applicable limit. Keep at least one limit for all work.' }}</p></div>
        <button type="button" class="btn shrink-0" @click="addBudget"><Icon name="plus" :size="14" /> Add limit</button>
      </div>
      <div v-for="(item, i) in modelValue.budgets ?? []" :key="i" class="editor-budget">
        <div class="flex items-center justify-between mb-4">
          <h4 class="text-[12px] font-semibold">Limit {{ i + 1 }}</h4>
          <button type="button" class="btn btn-sm btn-danger" :disabled="!watching && modelValue.budgets.length <= 1" :aria-label="`Remove limit ${i + 1}`" @click="set('budgets', modelValue.budgets.filter((_, j) => j !== i))"><Icon name="x" :size="13" /> Remove</button>
        </div>
        <BudgetFields :model-value="item" @update:model-value="budget(i, $event)" />
      </div>
      <details class="editor-details" :open="!!cost || !!modelValue.batch || !!modelValue.concurrency">
        <summary>Cost and processing settings</summary>
        <div class="grid sm:grid-cols-2 gap-4 mt-4">
          <Field label="How much does one item cost?" :for="`${id}-cost-mode`">
            <select :id="`${id}-cost-mode`" :value="cost ? 'payload' : 'fixed'" class="input" @change="set('cost', $event.target.value === 'fixed' ? 1 : { path: 'payload.cost', default: 1 })">
              <option value="fixed">Fixed cost per item</option><option value="payload">Read cost from the payload</option>
            </select>
          </Field>
          <Field v-if="!cost" label="Cost units per item" :for="`${id}-fixed`" hint="Use 1 to count requests or messages.">
            <input :id="`${id}-fixed`" :value="modelValue.cost ?? 1" type="number" min="1" step="1" class="input" @input="set('cost', number($event))" />
          </Field>
          <template v-else>
            <Field label="Cost payload path" :for="`${id}-cost-path`"><input :id="`${id}-cost-path`" :value="cost.path" class="input font-mono" @input="set('cost', { ...cost, path: $event.target.value })" /></Field>
            <Field label="Default cost" :for="`${id}-default`"><input :id="`${id}-default`" :value="cost.default ?? 1" type="number" min="1" step="1" class="input" @input="set('cost', { ...cost, default: number($event) })" /></Field>
            <Field label="Maximum cost (optional)" :for="`${id}-max`" hint="Must fit the smallest limit subdivision."><input :id="`${id}-max`" :value="cost.max" type="number" min="1" step="1" class="input" @input="set('cost', optional(cost, 'max', number($event)))" /></Field>
          </template>
          <Field label="Batch size (optional)" :for="`${id}-batch`" hint="Items processed per claim. From 1 to 1000."><input :id="`${id}-batch`" :value="modelValue.batch" type="number" min="1" max="1000" step="1" class="input" placeholder="Automatic" @input="opt('batch', number($event))" /></Field>
          <Field label="Workers per node (optional)" :for="`${id}-concurrency`" hint="Empty lets Gate choose based on the rate and partitions."><input :id="`${id}-concurrency`" :value="modelValue.concurrency" type="number" min="1" step="1" class="input" placeholder="Automatic" @input="opt('concurrency', number($event))" /></Field>
        </div>
      </details>
    </section>
  </div>
</template>
