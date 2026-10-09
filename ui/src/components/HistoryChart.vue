<script setup>
import { computed, ref } from 'vue'
import { num, datetime } from '../lib/api.js'
const props = defineProps({ title: String, unit: String, rows: Array, lines: Array, events: Array })
const hover = ref(null)
const width = 360, height = 180, pad = 24
const max = computed(() => Math.max(1, ...(props.rows ?? []).flatMap(row => props.lines.map(l => row[l.key]).filter(Number.isFinite))))
const hasData = computed(() => (props.rows ?? []).some(row => props.lines.some(l => Number.isFinite(row[l.key]))))
const x = (i) => pad + i / Math.max(1, props.rows.length - 1) * (width - 2 * pad)
const y = (value) => height - pad - value / max.value * (height - 2 * pad)
function points(key) { return props.rows.map((row, i) => ({ value: row[key], i })).filter(p => Number.isFinite(p.value)) }
function path(key) {
  let continuous = false
  return props.rows.map((row, i) => {
    if (!Number.isFinite(row[key])) { continuous = false; return '' }
    const segment = `${continuous ? 'L' : 'M'}${x(i)},${y(row[key])}`
    continuous = true; return segment
  }).join(' ')
}
const marks = computed(() => (props.events ?? []).filter(e => e.at >= props.rows[0]?.t && e.at <= props.rows.at(-1)?.t).map(e => ({ ...e, x: pad + (e.at - props.rows[0].t) / Math.max(1, props.rows.at(-1).t - props.rows[0].t) * (width - 2 * pad) })))
function point(event) {
  const box = event.currentTarget.getBoundingClientRect()
  hover.value = Math.max(0, Math.min(props.rows.length - 1, Math.round(((event.clientX - box.left) / box.width * width - pad) / (width - 2 * pad) * (props.rows.length - 1))))
}
const selected = computed(() => hover.value === null ? null : props.rows[hover.value])
</script>
<template>
  <div class="history-chart">
    <div class="flex flex-wrap justify-between gap-2"><h3 class="text-sm font-semibold">{{ title }}</h3><span class="text-xs text-fg-3">{{ unit }}</span></div>
    <div class="flex flex-wrap gap-4 mt-2"><span v-for="line in lines" :key="line.key" class="text-xs text-fg-3"><i class="chart-dot" :style="{ background: line.color }" />{{ line.label }}</span></div>
    <div v-if="!hasData" class="h-[172px] flex items-center justify-center text-xs text-fg-3 text-center px-5">Waiting for measurements · missing data stays blank</div>
    <svg v-else :viewBox="`0 0 ${width} ${height}`" role="img" :aria-label="`${title}, ${unit}`" class="w-full mt-2" @pointermove="point" @pointerleave="hover = null">
      <line v-for="fraction in [0, .5, 1]" :key="fraction" :x1="pad" :x2="width - pad" :y1="y(max * fraction)" :y2="y(max * fraction)" stroke="var(--border)" />
      <text x="24" y="16" fill="currentColor" font-size="10" opacity=".5">{{ num(Math.ceil(max)) }}</text>
      <line v-for="(mark, i) in marks" :key="i" :x1="mark.x" :x2="mark.x" :y1="pad" :y2="height - pad" stroke="var(--warn)" stroke-dasharray="3 4"><title>{{ mark.change.label }} · {{ datetime(mark.at) }}</title></line>
      <g v-for="line in lines" :key="line.key"><path :d="path(line.key)" fill="none" :stroke="line.color" stroke-width="2" stroke-linejoin="round" /><circle v-for="point in points(line.key)" :key="point.i" :cx="x(point.i)" :cy="y(point.value)" r="1.7" :fill="line.color" /></g>
      <line v-if="selected" :x1="x(hover)" :x2="x(hover)" :y1="pad" :y2="height - pad" stroke="currentColor" opacity=".25" />
    </svg>
    <div class="flex justify-between text-[10px] text-fg-3"><time>{{ rows[0] ? new Date(rows[0].t).toLocaleTimeString([], { hour:'2-digit', minute:'2-digit' }) : '' }}</time><time>{{ rows.at(-1) ? new Date(rows.at(-1).t).toLocaleTimeString([], { hour:'2-digit', minute:'2-digit' }) : '' }}</time></div>
    <p class="chart-reading text-xs text-fg-3 mt-2">{{ selected ? datetime(selected.t) + ' · ' + lines.map(l => `${l.label}: ${Number.isFinite(selected[l.key]) ? num(selected[l.key]) : 'unknown'}`).join(' · ') : 'Move over the chart to inspect a sample.' }}</p>
  </div>
</template>
