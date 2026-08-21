<script setup>
import { computed } from 'vue'

/*
  One counter, several ceilings.

  This is the picture the whole priority model is legible from, and it is a
  different picture from a budget bar. A budget bar answers "how close am I to a
  number"; this answers "how close am I to MY number, and whose number is above
  it".

  The bar is the counter — one kv row, one value. The marks are the per-path
  ceilings: `round(count_sub × share)`. A path refuses itself at its own mark
  and can never take anything above it, so **the gap between the tallest lower
  mark and the top is a reserve only the highest-priority path can reach** —
  held by the same row lock that does the counting, with no scheduler anywhere.

  v1 could not draw this, because it had nothing to draw: its lanes each held
  their own copy of the counter and DIVIDED the ceiling between them. Two lanes
  both told "you may use the ceiling" genuinely spent it twice — measured at
  93/s against a declared 50/s.

  A per-key budget has no single counter, so `value` is null and the bar says so
  rather than drawing a zero, which would read as "nothing spent".
*/
const props = defineProps({
  value: { type: Number, default: null },
  /* `{ path: ceiling }` */
  ceilings: { type: Object, default: () => ({}) },
  /* Draw this path's own mark solid; the others are hairlines. */
  highlight: { type: String, default: '' },
  assumed: { type: Boolean, default: false },
  height: { type: Number, default: 10 },
})

const top = computed(() => {
  const marks = Object.values(props.ceilings ?? {})
  return marks.length ? Math.max(...marks) : 0
})

const known = computed(() => props.value !== null && props.value !== undefined)
const ratio = computed(() => (top.value > 0 && known.value ? props.value / top.value : 0))
const width = computed(() => `${Math.min(100, Math.max(0, ratio.value * 100))}%`)

const fill = computed(() => {
  if (ratio.value > 1) return 'bg-bad'
  if (ratio.value >= 0.85) return 'bg-warn'
  return 'bg-good'
})

/* Sorted so the reserve is the gap after the last one. */
const marks = computed(() =>
  Object.entries(props.ceilings ?? {})
    .map(([path, at]) => ({ path, at, pct: top.value > 0 ? (at / top.value) * 100 : 0 }))
    .sort((a, b) => a.at - b.at),
)

/* The band only the top path can reach. Absent when every path may reach the
   ceiling, which is the ordinary case for a node one path crosses. */
const reserve = computed(() => {
  const m = marks.value
  if (m.length < 2) return null
  const below = m[m.length - 2]
  return { from: below.pct, width: 100 - below.pct, above: below.path }
})
</script>

<template>
  <div class="w-full">
    <div class="relative w-full rounded-full bg-surface-2 overflow-hidden"
         :style="{ height: `${height}px` }">
      <!-- The reserve, behind everything: a band, not a bar. -->
      <div v-if="reserve"
           class="absolute inset-y-0 bg-line-2/40"
           :style="{ left: `${reserve.from}%`, width: `${reserve.width}%` }" />

      <div v-if="known"
           class="h-full rounded-full transition-[width] duration-500 ease-spring relative"
           :class="[fill, assumed ? 'opacity-60' : '']"
           :style="{
             width,
             backgroundImage: assumed
               ? 'repeating-linear-gradient(135deg, transparent 0 3px, rgb(0 0 0 / 0.25) 3px 6px)'
               : 'none',
           }" />

      <!-- The ceilings. The last one IS the top of the bar and needs no line. -->
      <template v-for="(m, i) in marks" :key="m.path">
        <div v-if="i < marks.length - 1"
             class="absolute inset-y-0 w-px"
             :class="m.path === highlight ? 'bg-fg' : 'bg-line-2'"
             :style="{ left: `${m.pct}%` }"
             :title="`${m.path}: ${m.at}`" />
      </template>
    </div>

    <div v-if="!known" class="mt-1 text-[11px] text-fg-3">
      one counter per key — the number that matters is the worst live key, and
      finding it means enumerating a namespace
    </div>
    <div v-else-if="ratio > 1" class="mt-1 text-[11px] text-bad tabular-nums">
      over the ceiling by {{ Math.round((ratio - 1) * 100) }}%
    </div>
    <div v-else-if="reserve" class="mt-1 text-[11px] text-fg-3">
      the top {{ Math.round(reserve.width) }}% is a reserve
      <span class="font-mono">{{ reserve.above }}</span> cannot reach
    </div>
  </div>
</template>
