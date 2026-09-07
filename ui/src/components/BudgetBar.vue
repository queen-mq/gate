<script setup>
import { computed } from 'vue'

/*
  The console's signature control: one budget window, filled to what it has
  spent. It is the only chart here, and it is a bar rather than a time series
  because the question an operator asks is not "what was the shape of the last
  hour" but "how close am I, right now, to a number I do not control".

  Three deliberate choices:

  * The fill turns warm at 85%, not at 100%. Booking.com is the only vendor in
    the corpus that warns before its own ceiling, and it does it at 85% — so
    that is the threshold the industry itself considers "close", and the one an
    operator should feel before the vendor feels it.
  * A budget whose cap is ASSUMED is hatched, not solid. The bar would
    otherwise read as a measurement when it is arithmetic on a guess, and the
    single most expensive thing this console can do is make a guess look like a
    fact.
  * Over 100% is drawn, not clamped. It happens — a shared budget with feedback
    enforcement can overshoot by a window — and hiding it would remove the only
    on-screen evidence that the model and the vendor disagree.
*/
const props = defineProps({
  used: { type: Number, default: null },
  cap: { type: Number, required: true },
  assumed: { type: Boolean, default: false },
  height: { type: Number, default: 6 },
})

const known = computed(() => props.used !== null && props.used !== undefined && props.cap > 0)
const ratio = computed(() => (known.value ? props.used / props.cap : 0))
const width = computed(() => `${Math.min(100, Math.max(0, ratio.value * 100))}%`)
const over = computed(() => ratio.value > 1)

const fill = computed(() => {
  if (over.value) return 'bg-bad'
  if (ratio.value >= 0.85) return 'bg-warn'
  return 'bg-good'
})
</script>

<template>
  <div class="w-full">
    <div class="w-full rounded-full bg-surface-2 overflow-hidden"
         :style="{ height: `${height}px` }">
      <div v-if="known" class="h-full rounded-full transition-[width] duration-500 ease-spring"
           :class="[fill, assumed ? 'opacity-60' : '']"
           :style="{
             width,
             backgroundImage: assumed
               ? 'repeating-linear-gradient(135deg, transparent 0 3px, rgb(0 0 0 / 0.25) 3px 6px)'
               : 'none',
           }" />
    </div>
    <div v-if="!known" class="mt-1 text-[11px] text-fg-3">
      no single live value is available for this budget
    </div>
    <div v-if="over" class="mt-1 text-[11px] text-bad tabular-nums">
      over cap by {{ Math.round((ratio - 1) * 100) }}%
    </div>
  </div>
</template>
