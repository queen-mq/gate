<script setup>
import { useId } from 'vue'
import Field from '../Field.vue'
import DurationField from './DurationField.vue'
const props = defineProps({ modelValue: { type: Object, required: true } })
const emit = defineEmits(['update:modelValue'])
const id = useId()
</script>

<template>
  <div class="space-y-4">
    <Field label="Observation period" :for="`${id}-duration`" hint="From two minutes to 30 days. History is measured in complete one-minute windows.">
      <DurationField :id="`${id}-duration`" :model-value="modelValue.durationSeconds === '' ? '' : modelValue.durationSeconds * 1000" @update:model-value="emit('update:modelValue', { ...props.modelValue, durationSeconds: $event === '' ? '' : $event / 1000 })" />
    </Field>
    <p class="editor-note">Watch relays work to your outgoing queue without charging rate limits or shedding incoming traffic. History is enabled automatically. After this period, review the observed traffic and activate limits when you are ready.</p>
    <p v-if="modelValue.startedAt" class="hint">Observation started {{ new Date(modelValue.startedAt).toLocaleString() }}. Editing this period keeps the same start time.</p>
  </div>
</template>
