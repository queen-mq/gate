<script setup>
import { computed } from 'vue'

/*
  The one component allowed to speak in colour, and the only place that decides
  what each state looks like — the target list, the lane panel and the overview
  can never disagree about what "pacing" means.

  Dot + word, never a filled pill: a page with forty coloured capsules shouts
  everywhere and says nothing.

  The taxonomy is NOT the relay console's, and the difference is the whole
  point of this product. In a delivery console a retry is a failure. Here a
  DENIAL IS THE JOB: a lane sitting at its cap and refusing work is the system
  succeeding, and painting it red would train an operator to page themselves
  every time the limiter does what it exists to do.

  So the colours track a different question — "are we still in control?":
    flowing   under cap, nothing waiting              good
    pacing    at cap, backlog stable — working        good
    parked    denied, waiting out the lease           muted
    saturating backlog growing faster than the drain  warn
    blind     the cap is an assumption, not a number  warn
    breached  the vendor throttled us anyway          bad  — our numbers are wrong
*/
const props = defineProps({
  state: { type: String, required: true },
  label: { type: String, default: null },
  size: { type: String, default: 'md' }, // md | lg
})

const TONE = {
  flowing: 'good', pacing: 'good', admitting: 'good', ok: 'good', live: 'good',
  parked: 'muted', idle: 'muted', draining: 'muted',
  saturating: 'warn', blind: 'warn', degraded: 'warn', lagging: 'warn',
  breached: 'bad', throttled: 'bad', unreachable: 'bad', down: 'bad', blocked: 'bad',
}
const LABEL = {
  pacing: 'at cap, pacing',
  parked: 'parked until lease expiry',
  saturating: 'backlog growing',
  blind: 'cap is assumed',
  breached: 'throttled by the vendor',
}

const tone = computed(() => TONE[props.state] || 'muted')
const text = computed(() => props.label ?? (LABEL[props.state] || props.state))
const dotClass = computed(() => ({
  good: 'bg-good', warn: 'bg-warn', bad: 'bg-bad', muted: 'bg-line-2',
}[tone.value]))
const textClass = computed(() => ({
  good: 'text-fg-2', warn: 'text-warn', bad: 'text-bad', muted: 'text-fg-3',
}[tone.value]))
</script>

<template>
  <span class="inline-flex items-center" :class="size === 'lg' ? 'gap-2.5' : 'gap-1.5'">
    <span
      class="rounded-full shrink-0"
      :class="[dotClass, size === 'lg' ? 'w-[10px] h-[10px]' : 'w-[7px] h-[7px]']"
    />
    <span
      :class="[textClass, size === 'lg' ? 'text-[15px] font-medium' : 'text-[12.5px]']"
      class="whitespace-nowrap"
    >{{ text }}</span>
  </span>
</template>
