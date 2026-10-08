<script setup>
import Icon from './Icon.vue'

defineProps({ modelValue: { type: String, default: 'system' } })
const emit = defineEmits(['update:modelValue'])
const options = [
  { value: 'light', label: 'Light', icon: 'sun' },
  { value: 'dark', label: 'Dark', icon: 'moon' },
  { value: 'system', label: 'System', icon: 'monitor' },
]
</script>

<template>
  <div class="theme-selector" role="group" aria-label="Color theme">
    <button v-for="option in options" :key="option.value" type="button"
            :aria-label="`${option.label} theme`" :title="`${option.label} theme`"
            :aria-pressed="modelValue === option.value" :class="{ selected: modelValue === option.value }"
            @click="emit('update:modelValue', option.value)">
      <Icon :name="option.icon" :size="14" />
      <span>{{ option.label }}</span>
    </button>
  </div>
</template>

<style scoped>
.theme-selector { display: inline-flex; align-items: center; gap: 2px; padding: 3px; border: 1px solid var(--border-2); border-radius: 6px; background: var(--surface); flex-shrink: 0; }
.theme-selector button { display: inline-flex; align-items: center; justify-content: center; gap: 6px; height: 27px; padding: 0 8px; border-radius: 4px; color: var(--text-3); font-size: 10px; transition: color .12s, background .12s; }
.theme-selector button:hover { color: var(--text); background: var(--surface-2); }
.theme-selector button.selected { color: var(--spatial-accent, var(--text)); background: var(--selected); }
@media (max-width: 639px) { .theme-selector button { width: 29px; padding: 0; } .theme-selector button span { display: none; } }
</style>
