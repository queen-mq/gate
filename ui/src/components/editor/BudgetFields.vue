<script setup>
import { useId } from 'vue'
import Field from '../Field.vue'
import DurationField from './DurationField.vue'
import { optional } from '../../lib/graph-editor.js'
const props = defineProps({ modelValue: { type: Object, required: true } })
const emit = defineEmits(['update:modelValue'])
const id = useId()
function set(key, value) { emit('update:modelValue', { ...props.modelValue, [key]: value }) }
function opt(key, value) { emit('update:modelValue', optional(props.modelValue, key, value)) }
function number(event) { return event.target.value === '' ? '' : Number(event.target.value) }
</script>

<template>
  <div class="space-y-4">
    <div class="grid sm:grid-cols-3 gap-4">
      <Field label="Limit ID" :for="`${id}-id`" hint="A stable name for this counter. Changing it starts a new counter.">
        <input :id="`${id}-id`" :value="modelValue.id" class="input font-mono" placeholder="per-minute" @input="opt('id', $event.target.value)" />
      </Field>
      <Field label="Allowance" :for="`${id}-count`" hint="Requests or messages per window when each item costs 1.">
        <input :id="`${id}-count`" :value="modelValue.count" type="number" min="1" step="1" class="input" @input="set('count', number($event))" />
      </Field>
      <Field label="Time window" :for="`${id}-window`" hint="For example: 100 requests every 1 minute.">
        <DurationField :id="`${id}-window`" :model-value="modelValue.timeMs" @update:model-value="set('timeMs', $event)" />
      </Field>
    </div>
    <div class="grid sm:grid-cols-3 gap-4">
      <Field label="How was this limit established?" :for="`${id}-confidence`">
        <select :id="`${id}-confidence`" :value="modelValue.confidence ?? 'inferred'" class="input" @change="set('confidence', $event.target.value)">
          <option value="inferred">Inferred from observations</option>
          <option value="documented">Published by the provider</option>
          <option value="assumed">An assumption to verify</option>
        </select>
      </Field>
      <Field :label="`Source${modelValue.confidence === 'documented' ? ' (required)' : ''}`" :for="`${id}-source`" hint="Documentation link or a note explaining the estimate.">
        <input :id="`${id}-source`" :value="modelValue.source" class="input" placeholder="https://provider.com/rate-limits" @input="opt('source', $event.target.value)" />
      </Field>
      <Field :label="`Verified on${modelValue.confidence === 'documented' ? ' (required)' : ''}`" :for="`${id}-date`">
        <input :id="`${id}-date`" :value="modelValue.asOf" type="date" class="input" @input="opt('asOf', $event.target.value)" />
      </Field>
    </div>
    <details class="editor-details" :open="!!(modelValue.subWindows || modelValue.scopeBy || modelValue.sharedKey || modelValue.whenOp)">
      <summary>Advanced limit settings</summary>
      <div class="grid sm:grid-cols-2 gap-4 mt-4">
        <Field label="Subdivisions" :for="`${id}-sub`" hint="Split the allowance into smaller windows to spread traffic. Empty uses Gate's automatic smoothing.">
          <input :id="`${id}-sub`" :value="modelValue.subWindows" type="number" min="1" max="3600" step="1" class="input" placeholder="Automatic" @input="opt('subWindows', number($event))" />
        </Field>
        <Field label="Shared counter key" :for="`${id}-shared`" hint="Use the same key to share one allowance across nodes in this application.">
          <input :id="`${id}-shared`" :value="modelValue.sharedKey" class="input font-mono" placeholder="provider-account" @input="opt('sharedKey', $event.target.value)" />
        </Field>
        <Field label="One allowance per payload value" :for="`${id}-scope`" hint="Empty applies to the whole node. A payload path creates one counter per distinct value.">
          <input :id="`${id}-scope`" :value="modelValue.scopeBy" class="input font-mono" placeholder="payload.accountId" @input="opt('scopeBy', $event.target.value)" />
        </Field>
        <Field label="Only these operations" :for="`${id}-ops`" hint="Comma-separated patterns, such as listings.put, messages.*. Empty applies to all operations.">
          <input :id="`${id}-ops`" :value="modelValue.whenOp?.join(', ')" class="input font-mono" placeholder="All operations" @change="opt('whenOp', $event.target.value.trim() ? $event.target.value.split(',').map(s => s.trim()).filter(Boolean) : undefined)" />
        </Field>
      </div>
    </details>
  </div>
</template>
