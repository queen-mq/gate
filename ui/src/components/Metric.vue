<script setup>
defineProps({
  label: String,
  value: [String, Number],
  unit: { type: String, default: null },
  tone: { type: String, default: 'plain' }, // plain | warn | bad
})
const tones = { plain: '', warn: 'text-warn', bad: 'text-bad' }
</script>

<template>
  <div>
    <div class="text-xs text-fg-2">{{ label }}</div>
    <div class="text-[26px] leading-tight font-semibold tracking-tight tabular-nums mt-1"
         :class="tones[tone]">
      {{ value }}<span v-if="unit" class="text-[13px] font-normal text-fg-3 ml-1">{{ unit }}</span>
    </div>
  </div>
</template>
