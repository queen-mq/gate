<script setup>
import { computed, ref } from 'vue'
const props = defineProps({ modelValue: [Number, String], id: String })
const emit = defineEmits(['update:modelValue'])
const units = [[86400000, 'days'], [3600000, 'hours'], [60000, 'minutes'], [1000, 'seconds'], [1, 'milliseconds']]
const unit = ref(units.find(([size]) => props.modelValue > 0 && props.modelValue % size === 0)?.[0] ?? 1000)
const amount = computed(() => props.modelValue === '' || props.modelValue === undefined ? '' : props.modelValue / unit.value)
function input(event) { emit('update:modelValue', event.target.value === '' ? '' : Math.round(Number(event.target.value) * unit.value)) }
function changeUnit(event) {
  const value = amount.value
  unit.value = Number(event.target.value)
  emit('update:modelValue', value === '' ? '' : Math.round(value * unit.value))
}
</script>

<template>
  <div class="flex gap-2">
    <input :id="id" :value="amount" type="number" min="0" step="any" class="input min-w-0" @input="input" />
    <select :value="unit" class="input max-w-[140px]" aria-label="Time window unit" @change="changeUnit">
      <option v-for="[size, label] in units" :key="size" :value="size">{{ label }}</option>
    </select>
  </div>
</template>
