<script setup>
import { computed } from 'vue'

/*
  BudgetBar answers "how close am I right now". This answers the question that
  follows it and that the bar physically cannot: "have I been here all
  afternoon, or did I touch it once". A window that has sat at 90% since lunch
  is a conversation to have with the vendor; one that spiked to 90% for a
  minute is the pacer doing its job, and a single bar renders the two
  identically.

  Inline SVG and not a chart library: the console is embedded in the binary, so
  a charting dependency is a megabyte every operator carries forever in order
  to draw sixty points.
*/
const props = defineProps({
  // Utilisation as a 0..1 fraction, oldest first.
  points: { type: Array, default: () => [] },
  width: { type: Number, default: 88 },
  height: { type: Number, default: 24 },
  assumed: { type: Boolean, default: false },
  /* Not every 0..1 series is a utilisation. A lane's share of admissions is
     just as high at 90% as it is healthy, and lending it the cap-proximity
     colours would say the opposite. */
  neutral: { type: Boolean, default: false },
})

const clean = computed(() => props.points.map((p) => (typeof p === 'number' && isFinite(p) ? p : 0)))

/*
  The vertical scale is pinned to the cap, never autoscaled to the data. An
  autoscaled sparkline draws a budget that never passed 3% exactly like one
  that spent the hour on its ceiling, which is the single comparison this page
  exists to make. It only grows past 1 when something actually went over.
*/
const top = computed(() => Math.max(1, ...clean.value))

const x = (i) => (clean.value.length < 2 ? props.width : (i / (clean.value.length - 1)) * props.width)
const y = (v) => props.height - 1.5 - (v / top.value) * (props.height - 3)

const line = computed(() =>
  clean.value.map((v, i) => `${i ? 'L' : 'M'}${x(i).toFixed(1)} ${y(v).toFixed(1)}`).join(' ')
)
const area = computed(() =>
  clean.value.length ? `${line.value} L${props.width} ${props.height} L0 ${props.height} Z` : ''
)

// Same thresholds as BudgetBar, and deliberately so: the bar and the trace sit
// next to each other in the budget list and must never disagree about whether
// the operator should be worried.
const last = computed(() => clean.value[clean.value.length - 1] ?? 0)
const tone = computed(() => {
  if (props.neutral) return 'text-fg-3'
  return last.value > 1 ? 'text-bad' : last.value >= 0.85 ? 'text-warn' : 'text-good'
})

// The cap only earns a rule when it has been crossed — otherwise it is the top
// edge of the box and drawing it twice adds nothing.
const capY = computed(() => (top.value > 1 ? y(1) : null))
</script>

<template>
  <svg v-if="clean.length > 1" :width="width" :height="height"
       :viewBox="`0 0 ${width} ${height}`" :class="tone" aria-hidden="true">
    <path :d="area" fill="currentColor" fill-opacity="0.13" />
    <path :d="line" fill="none" stroke="currentColor" stroke-width="1.4"
          stroke-linejoin="round" stroke-linecap="round"
          :stroke-dasharray="assumed ? '3 2' : undefined" />
    <line v-if="capY !== null" x1="0" :y1="capY" :x2="width" :y2="capY"
          stroke="currentColor" stroke-width="1" stroke-dasharray="2 2" stroke-opacity="0.5" />
  </svg>

  <!-- One point is not a history, and a flat line drawn from it would be a
       claim about a period nobody measured. -->
  <span v-else class="text-[11px] text-fg-3">no history</span>
</template>
