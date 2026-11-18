<script setup>
import { computed, ref } from 'vue'
import { num, datetime } from '../lib/api.js'
import { yScale, timeTicks, duration } from '../lib/chart-axes.js'
// `duration`: values are seconds, ticks land on clock-friendly steps and read as "5m", "1h 30m".
const props = defineProps({ title: String, unit: String, rows: Array, lines: Array, events: Array, duration: Boolean })
const hover = ref(null)
const width = 360, height = 190, left = 46, right = 18, top = 18, bottom = 24
const plotW = width - left - right, plotH = height - top - bottom
const values = computed(() => (props.rows ?? []).flatMap(row => props.lines.map(l => row[l.key])).filter(Number.isFinite))
const hasData = computed(() => values.value.length > 0)
const scale = computed(() => yScale(values.value, props.duration))
const label = (value) => props.duration ? duration(value) : num(value)
const t0 = computed(() => props.rows?.[0]?.t ?? 0), t1 = computed(() => props.rows?.at(-1)?.t ?? 0)
const xt = (t) => left + (t - t0.value) / Math.max(1, t1.value - t0.value) * plotW
const x = (i) => left + i / Math.max(1, props.rows.length - 1) * plotW
const y = (value) => top + plotH - value / scale.value.max * plotH
const xTicks = computed(() => timeTicks(t0.value, t1.value))
const hhmm = (t) => new Date(t).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
function points(key) { return props.rows.map((row, i) => ({ value: row[key], i })).filter(p => Number.isFinite(p.value)) }
function path(key) {
  let continuous = false
  return props.rows.map((row, i) => {
    if (!Number.isFinite(row[key])) { continuous = false; return '' }
    const segment = `${continuous ? 'L' : 'M'}${x(i)},${y(row[key])}`
    continuous = true; return segment
  }).join(' ')
}
const marks = computed(() => (props.events ?? []).filter(e => e.at >= t0.value && e.at <= t1.value).map(e => ({ ...e, x: xt(e.at) })))
function point(event) {
  const box = event.currentTarget.getBoundingClientRect()
  hover.value = Math.max(0, Math.min(props.rows.length - 1, Math.round(((event.clientX - box.left) / box.width * width - left) / plotW * (props.rows.length - 1))))
}
const selected = computed(() => hover.value === null ? null : props.rows[hover.value])
</script>
<template>
  <div class="history-chart">
    <h3 class="text-sm font-semibold">{{ title }}</h3>
    <div class="flex flex-wrap gap-4 mt-2"><span v-for="line in lines" :key="line.key" class="text-xs text-fg-3"><i class="chart-dot" :style="{ background: line.color }" />{{ line.label }}</span></div>
    <div v-if="!hasData" class="h-[172px] flex items-center justify-center text-xs text-fg-3 text-center px-5">Waiting for measurements · missing data stays blank</div>
    <svg v-else :viewBox="`0 0 ${width} ${height}`" role="img" :aria-label="`${title}, ${unit}`" class="w-full mt-2" font-size="9.5" @pointermove="point" @pointerleave="hover = null">
      <text :x="left" y="9" fill="currentColor" opacity=".6">{{ unit }}</text>
      <g v-for="tick in scale.ticks" :key="tick">
        <line :x1="left" :x2="width - right" :y1="y(tick)" :y2="y(tick)" stroke="currentColor" :stroke-opacity="tick ? .1 : .3" :stroke-dasharray="tick ? '2 3' : null" />
        <text :x="left - 6" :y="y(tick)" text-anchor="end" dominant-baseline="middle" fill="currentColor" opacity=".6">{{ label(tick) }}</text>
      </g>
      <line :x1="left" :x2="left" :y1="top" :y2="top + plotH" stroke="currentColor" stroke-opacity=".3" />
      <g v-for="t in xTicks" :key="t">
        <line :x1="xt(t)" :x2="xt(t)" :y1="top + plotH" :y2="top + plotH + 4" stroke="currentColor" opacity=".35" />
        <text :x="xt(t)" :y="height - 4" text-anchor="middle" fill="currentColor" opacity=".6">{{ hhmm(t) }}</text>
      </g>
      <line v-for="(mark, i) in marks" :key="i" :x1="mark.x" :x2="mark.x" :y1="top" :y2="top + plotH" stroke="var(--warn)" stroke-dasharray="3 4"><title>{{ mark.change.label }} · {{ datetime(mark.at) }}</title></line>
      <g v-for="line in lines" :key="line.key"><path :d="path(line.key)" fill="none" :stroke="line.color" stroke-width="1.75" stroke-linejoin="round" /><circle v-for="p in points(line.key)" :key="p.i" :cx="x(p.i)" :cy="y(p.value)" r="1.5" :fill="line.color" /></g>
      <line v-if="selected" :x1="x(hover)" :x2="x(hover)" :y1="top" :y2="top + plotH" stroke="currentColor" opacity=".25" />
    </svg>
    <p class="chart-reading text-xs text-fg-3 mt-2">{{ selected ? datetime(selected.t) + ' · ' + lines.map(l => `${l.label}: ${Number.isFinite(selected[l.key]) ? label(selected[l.key]) : 'unknown'}`).join(' · ') : 'Move over the chart to inspect a sample.' }}</p>
  </div>
</template>
