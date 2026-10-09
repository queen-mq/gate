<script setup>
import { computed } from 'vue'

/* Denials are the limiter doing its job. Healthy and pacing stay neutral;
   amber means a growing backlog / assumed cap, coral a vendor throttle.
   Shapes reinforce the words so state never depends on colour alone. */
const props = defineProps({
  state: { type: String, required: true },
  label: { type: String, default: null },
  size: { type: String, default: 'md' },
})

const TONE = {
  flowing: 'good', pacing: 'good', admitting: 'good', ok: 'good', live: 'good',
  watch: 'muted', parked: 'muted', idle: 'muted', draining: 'muted',
  saturating: 'warn', blind: 'warn', degraded: 'warn', lagging: 'warn',
  breached: 'bad', throttled: 'bad', unreachable: 'bad', down: 'bad', blocked: 'bad',
}
const LABEL = {
  watch: 'watching traffic',
  pacing: 'at cap, pacing',
  parked: 'parked until lease expiry',
  saturating: 'backlog growing',
  blind: 'cap is assumed',
  breached: 'throttled by the vendor',
}
const tone = computed(() => TONE[props.state] || 'muted')
const text = computed(() => props.label ?? (LABEL[props.state] || props.state))
const textClass = computed(() => ({
  good: props.size === 'lg' ? 'text-fg' : 'text-fg-2',
  warn: 'text-warn', bad: 'text-bad', muted: 'text-fg-3',
}[tone.value]))
</script>

<template>
  <component :is="size === 'lg' ? 'div' : 'span'">
    <span class="inline-flex items-center gap-2 min-w-0" :class="size === 'lg' ? 'gap-2.5' : ''">
      <span class="status-glyph" :class="tone" aria-hidden="true" />
      <span :class="[textClass, size === 'lg' ? 'text-[16px] font-medium leading-snug' : 'text-[12px] whitespace-nowrap']">
        {{ text }}
      </span>
    </span>
    <slot v-if="size === 'lg'" />
  </component>
</template>
