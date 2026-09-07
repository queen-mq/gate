<script setup>
/*
  Every application on one axis: how close each one came to its own ceiling,
  minute by minute.

  The axis is utilisation and not admissions, because admissions cannot share
  one. A team at 150/s and a team at 20/s put on the same scale means the second
  is a flat line along the bottom whatever it does, and the flat line is the one
  about to be throttled. Normalising to each application's own limit is also the
  question a dashboard is asked: not how much left, but how close to refused.

  One line per application, and its value is the utilisation of its BUSIEST
  target that minute — the server picks it. An average across targets would
  draw a team with four quiet targets and one pinned at its cap as a
  comfortable 20%. What the average would have shown is volume, and volume is
  on the target pages; this chart answers one question and names the target
  responsible so the next click is obvious.
*/
import { computed, ref, watch } from 'vue'
import { api, pct, rate, datetime, clock, targetPath } from '../lib/api.js'
import { usePoll } from '../lib/poll.js'

const RANGES = [
  { key: '1h', minutes: 60, label: '1 hour' },
  { key: '6h', minutes: 360, label: '6 hours' },
  { key: '12h', minutes: 720, label: '12 hours' },
]
const range = ref(RANGES[1])

// `undefined` while the first request is in flight, `null` when nothing is
// being kept. The three states render differently and only one of them is a
// problem.
const data = ref(undefined)
const error = ref('')

async function load() {
  const minutes = range.value.minutes
  try {
    const r = await api.get(`/api/flow?minutes=${minutes}`)
    if (minutes !== range.value.minutes) return
    data.value = r?.minutes?.length ? r : null
    error.value = ''
  } catch (e) {
    if (minutes !== range.value.minutes) return
    error.value = e.message
  }
}
/* Slower than the live gauges on this page: the series is one point per
   minute, so refreshing it every four seconds would redraw the same picture
   fifteen times to add nothing. */
const refresh = usePoll(load, 15000)
watch(range, () => {
  data.value = undefined
  refresh()
})

const series = computed(() => data.value?.applications ?? [])
const minutes = computed(() => data.value?.minutes ?? [])
const durable = computed(() => data.value?.durable !== false)

/* Six colours, then it wraps. A deployment with more than six applications on
   one chart has outgrown a chart, and the honest fix is a filter rather than a
   seventh hue nobody can tell from the second. */
const colour = (i) => `var(--series-${(i % 6) + 1})`

const W = 760
const H = 220
const PAD_L = 34
const PAD_R = 12
const PAD_T = 12
const PAD_B = 22
const plotW = W - PAD_L - PAD_R
const plotH = H - PAD_T - PAD_B

const peak = computed(() =>
  series.value.reduce(
    (m, s) => Math.max(m, ...s.points.map((p) => p.utilisation ?? 0)),
    0
  )
)
/* The cap always stays on screen. A chart auto-scaled to a quiet hour puts 4%
   at the top of the plot and reads, at a glance, exactly like 100%. */
const top = computed(() => Math.max(1.05, peak.value * 1.08))

function px(i) {
  const n = minutes.value.length
  return n < 2 ? PAD_L + plotW / 2 : PAD_L + (i / (n - 1)) * plotW
}
function py(v) {
  return PAD_T + plotH - (Math.max(v ?? 0, 0) / top.value) * plotH
}

function path(s) {
  return s.points
    .map((p, i) => `${i ? 'L' : 'M'}${px(i).toFixed(1)} ${py(p.utilisation).toFixed(1)}`)
    .join(' ')
}

const xLabels = computed(() => {
  const n = minutes.value.length
  if (!n) return []
  const idx = [...new Set(n < 3 ? [0, n - 1] : [0, Math.floor((n - 1) / 2), n - 1])]
  return idx.map((i, k) => ({
    x: px(i),
    label: datetime(minutes.value[i]),
    anchor: k === 0 ? 'start' : k === idx.length - 1 ? 'end' : 'middle',
  }))
})

/* Hovering reads the chart at a minute; not hovering reads it at the last one.
   The legend shows the same fields either way, so the eye does not have to
   re-learn the row when the pointer lands. */
const hover = ref(null)
const at = computed(() => (hover.value ?? minutes.value.length - 1))

function onMove(ev) {
  const n = minutes.value.length
  if (!n) return
  const box = ev.currentTarget.getBoundingClientRect()
  const x = ((ev.clientX - box.left) / box.width) * W
  const i = Math.round(((x - PAD_L) / plotW) * (n - 1))
  hover.value = Math.min(n - 1, Math.max(0, i))
}

const rows = computed(() =>
  series.value
    .map((s, i) => ({
      application: s.application,
      colour: colour(i),
      point: s.points[at.value] ?? {},
    }))
    .sort((a, b) => (b.point.utilisation ?? 0) - (a.point.utilisation ?? 0))
)

function toneOf(u) {
  if (u > 1) return 'text-bad'
  if (u >= 0.85) return 'text-warn'
  return ''
}
</script>

<template>
  <section v-if="error || data !== null" class="mb-8">
    <div class="flex items-baseline justify-between mb-3 gap-4 flex-wrap">
      <div>
        <h2 class="section-title mb-0">Flow against the limit</h2>
        <p class="text-[12.5px] text-fg-3 mt-0.5">
          Each application against its own ceiling — its busiest target, minute by minute.
        </p>
      </div>
      <div class="flex items-center gap-1">
        <button
          v-for="r in RANGES" :key="r.key"
          class="px-2.5 py-1 rounded-md text-[12px] transition-colors"
          :class="r.key === range.key ? 'bg-surface-2 text-fg' : 'text-fg-3 hover:text-fg-2'"
          @click="range = r"
        >{{ r.key }}</button>
      </div>
    </div>

    <div v-if="error" class="card border-transparent bg-bad-dim px-5 py-4 text-[13.5px] text-bad">
      {{ error }}
    </div>

    <div v-else-if="data === undefined" class="card px-6 py-10"><div class="skeleton h-32 w-full" /></div>

    <div v-else class="card px-4 py-5 sm:px-6">
      <svg
        :viewBox="`0 0 ${W} ${H}`" class="w-full h-auto" role="img"
        aria-label="Utilisation of every application over time"
        @mousemove="onMove" @mouseleave="hover = null"
      >
        <!-- The cap solid, 85% dashed: the two thresholds every gauge in this
             console changes colour at, so a line crossing one here means the
             same thing it means there. -->
        <g class="text-line">
          <line :x1="PAD_L" :y1="py(0)" :x2="W - PAD_R" :y2="py(0)" stroke="currentColor" />
          <line :x1="PAD_L" :y1="py(0.85)" :x2="W - PAD_R" :y2="py(0.85)"
                stroke="currentColor" stroke-dasharray="3 3" />
          <line :x1="PAD_L" :y1="py(1)" :x2="W - PAD_R" :y2="py(1)" stroke="currentColor" />
        </g>
        <g class="text-fg-3" fill="currentColor" font-size="10" text-anchor="end">
          <text :x="PAD_L - 6" :y="py(0) + 3">0</text>
          <text :x="PAD_L - 6" :y="py(0.85) + 3">85%</text>
          <text :x="PAD_L - 6" :y="py(1) + 3">cap</text>
        </g>

        <path
          v-for="(s, i) in series" :key="s.application"
          :d="path(s)" fill="none" :stroke="colour(i)"
          stroke-width="1.6" stroke-linejoin="round" stroke-linecap="round"
        />

        <g v-if="hover !== null" class="text-fg-3">
          <line :x1="px(hover)" :y1="PAD_T" :x2="px(hover)" :y2="PAD_T + plotH"
                stroke="currentColor" stroke-dasharray="2 3" />
          <circle
            v-for="(s, i) in series" :key="s.application"
            :cx="px(hover)" :cy="py(s.points[hover]?.utilisation)" r="2.5" :fill="colour(i)"
          />
        </g>

        <g class="text-fg-3" fill="currentColor" font-size="10">
          <text v-for="(l, i) in xLabels" :key="i" :x="l.x" :y="H - 6" :text-anchor="l.anchor">
            {{ l.label }}
          </text>
        </g>
      </svg>

      <div class="mt-4 pt-4 border-t border-line">
        <div class="flex items-baseline justify-between mb-2">
          <span class="label mb-0">{{ hover === null ? 'Latest minute' : 'At this minute' }}</span>
          <span class="text-[11.5px] text-fg-3 tabular-nums">{{ clock(minutes[at]) }}</span>
        </div>
        <ul class="space-y-1.5">
          <li v-for="r in rows" :key="r.application"
              class="flex items-center gap-3 text-[13px]">
            <span class="w-2.5 h-2.5 rounded-full shrink-0" :style="{ background: r.colour }" />
            <span class="font-mono text-[12.5px] truncate">{{ r.application }}</span>
            <RouterLink
              v-if="r.point.target"
              :to="targetPath(r.application, String(r.point.target).split('.')[0])"
              class="chip text-fg-3 hover:text-fg transition-colors truncate max-w-[140px]"
            >{{ r.point.target }}</RouterLink>
            <!-- A minute this application did not appear in has no ceiling to
                 quote, because no target of its reported one. "0/s of 0/s"
                 would read as a limit of zero, which is the opposite of idle. -->
            <span class="ml-auto text-[12px] text-fg-3 tabular-nums hidden sm:inline">
              <template v-if="r.point.ceiling">
                {{ rate((r.point.admitted ?? 0) / 60) }}/s of {{ rate(r.point.ceiling / 60) }}/s
              </template>
              <template v-else>idle</template>
            </span>
            <span class="text-[13px] font-semibold tabular-nums w-14 text-right"
                  :class="toneOf(r.point.utilisation)">
              {{ pct(r.point.utilisation ?? 0) }}
            </span>
          </li>
        </ul>
      </div>

      <p class="hint mt-4">
        One line per application, at the utilisation of its busiest target that minute — so a
        team is measured by the target closest to being refused, not by its average.
        <template v-if="!durable">
          No database is configured, so this is one replica's memory: with more than one, each
          would draw a different picture.
        </template>
      </p>
    </div>
  </section>
</template>
