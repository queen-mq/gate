<script setup>
import { ref, computed, watch, useId, onMounted, onUnmounted } from 'vue'
import Icon from '../Icon.vue'
import StatusDot from '../StatusDot.vue'
import {
  graphModel,
  layoutTopology,
  flowingNodes,
} from '../../lib/dashboard.js'
import {
  num,
  pct,
  period,
  ceilingOf,
  graphPath,
  DEFAULT_APP,
} from '../../lib/api.js'

const props = defineProps({
  graph: { type: Object, required: true },
  stale: Boolean,
  highlightedPath: { type: String, default: '' },
  detailLink: Boolean,
})
const uid = useId().replace(/[^a-zA-Z0-9]/g, '')
const ids = Object.fromEntries(
  ['grid', 'top', 'left', 'right', 'glow', 'arrow'].map((k) => [
    k,
    `${uid}-${k}`,
  ]),
)
const url = (key) => `url(#${ids[key]})`
const spatial = ref(true)
const animated = ref(true)
try {
  spatial.value = localStorage.getItem('gate-spatial-view') !== 'flat'
  animated.value = localStorage.getItem('gate-spatial-motion') !== 'paused'
} catch {}
watch(spatial, (v) => {
  try {
    localStorage.setItem('gate-spatial-view', v ? 'spatial' : 'flat')
  } catch {}
})
watch(animated, (v) => {
  try {
    localStorage.setItem('gate-spatial-motion', v ? 'on' : 'paused')
  } catch {}
})
const reduced = ref(false)
const hidden = ref(document.hidden)
let motionQuery
let expiry
const activity = ref([])
const tilt = ref({ x: 0, y: 0 })
const selected = ref('')
const model = computed(() => graphModel(props.graph))
const layout = computed(() =>
  layoutTopology(model.value.nodes, model.value.edges),
)
const selectedNode = computed(
  () =>
    model.value.nodes.find((n) => n.id === selected.value) ??
    model.value.nodes.find((n) => !n.output && !n.ingressQueue) ??
    model.value.nodes[0],
)
watch(
  () => props.graph,
  (current, previous) => {
    clearTimeout(expiry)
    activity.value = flowingNodes(previous, current)
    // A stopped poll, hidden tab or failed refresh must not imply ongoing traffic.
    expiry = setTimeout(() => {
      activity.value = []
    }, 4500)
    if (
      previous?.application !== current.application ||
      previous?.graph !== current.graph
    )
      selected.value = ''
  },
)
watch(
  () => props.stale,
  (stale) => {
    if (stale) activity.value = []
  },
)
function motionChange(e) {
  reduced.value = e.matches
}
function visibilityChange() {
  hidden.value = document.hidden
  activity.value = []
}
onMounted(() => {
  motionQuery = window.matchMedia('(prefers-reduced-motion: reduce)')
  reduced.value = motionQuery.matches
  motionQuery.addEventListener('change', motionChange)
  document.addEventListener('visibilitychange', visibilityChange)
})
onUnmounted(() => {
  clearTimeout(expiry)
  motionQuery?.removeEventListener('change', motionChange)
  document.removeEventListener('visibilitychange', visibilityChange)
})
const moving = computed(
  () =>
    animated.value &&
    !reduced.value &&
    !hidden.value &&
    !props.stale &&
    props.graph.running,
)
function pointerMove(e) {
  if (!moving.value || e.pointerType === 'touch' || !spatial.value) return
  const bounds = e.currentTarget.getBoundingClientRect()
  tilt.value = {
    x: ((e.clientX - bounds.left - bounds.width / 2) / bounds.width) * 4,
    y: (-(e.clientY - bounds.top - bounds.height / 2) / bounds.height) * 3,
  }
}
const blocks = computed(() =>
  layout.value.nodes.map((n) => ({
    ...n,
    w: n.output ? 67 : 80,
    d: spatial.value ? 35 : 33,
    h: spatial.value ? (n.output ? 37 : n.ingressQueue ? 34 : 64) : 0,
  })),
)
const links = computed(() =>
  model.value.edges.map((e, i) => {
    const a = blocks.value.find((n) => n.id === e.from)
    const b = blocks.value.find((n) => n.id === e.to)
    const x1 = a.x + a.w,
      x2 = b.x - b.w,
      dx = Math.max(40, (x2 - x1) / 2)
    return {
      ...e,
      key: JSON.stringify([e.from, e.to]),
      d: `M${x1},${a.y} C${x1 + dx},${a.y} ${x2 - dx},${b.y} ${x2},${b.y}`,
      active: moving.value && activity.value.includes(a.id) && !b.breaker,
      highlighted:
        !props.highlightedPath || e.paths.includes(props.highlightedPath),
      duration: 3 + (i % 3) * 0.4,
    }
  }),
)
const top = (n) =>
  `${n.x},${n.y - n.h - n.d} ${n.x + n.w},${n.y - n.h} ${n.x},${n.y - n.h + n.d} ${n.x - n.w},${n.y - n.h}`
const left = (n) =>
  `${n.x - n.w},${n.y - n.h} ${n.x},${n.y - n.h + n.d} ${n.x},${n.y + n.d} ${n.x - n.w},${n.y}`
const right = (n) =>
  `${n.x},${n.y - n.h + n.d} ${n.x + n.w},${n.y - n.h} ${n.x + n.w},${n.y} ${n.x},${n.y + n.d}`
const color = (n) =>
  `var(--${n.tone === 'bad' ? 'bad' : n.tone === 'warn' || selectedNode.value?.id === n.id ? 'spatial-accent' : 'text-2'})`
const short = (name) => (name.length > 20 ? `${name.slice(0, 18)}…` : name)
const reading = (value) => (value == null ? '—' : num(value))
const selectedState = computed(() =>
  !props.graph.running
    ? 'down'
    : selectedNode.value?.breaker
      ? 'breached'
      : selectedNode.value?.budgets?.some((b) => b.confidence === 'assumed')
        ? 'blind'
        : selectedNode.value?.waiting_for_budget > 0
          ? 'pacing'
          : 'flowing',
)
function budgetLink(node, budget) {
  return graphPath(
    props.graph.application || DEFAULT_APP,
    props.graph.graph,
    `/nodes/${encodeURIComponent(node.node)}/budgets/${encodeURIComponent(budget.id)}`,
  )
}
</script>

<template>
  <section
    class="spatial-workbench"
    :class="{ 'is-stale': stale }"
    aria-label="Interactive traffic topology"
  >
    <div class="spatial-map card">
      <header class="spatial-heading">
        <div>
          <h2>
            Traffic topology
            <span class="spatial-tag"
              >{{ graph.paths?.length ?? 0 }} paths</span
            >
          </h2>
          <p>
            {{ graph.application || DEFAULT_APP }} <span>/</span>
            {{ graph.graph }}
          </p>
        </div>
        <div class="spatial-actions">
          <div
            class="segmented-control"
            role="group"
            aria-label="Topology projection"
          >
            <button
              :class="{ active: spatial }"
              :aria-pressed="spatial"
              @click="spatial = true"
            >
              3D</button
            ><button
              :class="{ active: !spatial }"
              :aria-pressed="!spatial"
              @click="spatial = false"
            >
              2D
            </button>
          </div>
          <button
            class="icon-button spatial-pause"
            :aria-label="
              animated
                ? 'Pause topology animations'
                : 'Resume topology animations'
            "
            :aria-pressed="!animated"
            :title="
              reduced
                ? 'Reduced motion is enabled on your device'
                : 'Toggle decorative motion'
            "
            @click="animated = !animated"
          >
            <Icon :name="animated ? 'pause' : 'chevron'" :size="15" />
          </button>
        </div>
      </header>
      <div v-if="!blocks.length" class="spatial-empty">
        No nodes in this graph.
      </div>
      <div
        v-else
        class="spatial-scroll"
        tabindex="0"
        aria-label="Scrollable topology. Select a node to inspect its budgets."
        @pointermove="pointerMove"
        @pointerleave="tilt = { x: 0, y: 0 }"
      >
        <svg
          :viewBox="`0 0 ${layout.width} ${layout.height}`"
          class="spatial-scene"
          :style="{
            width: `${layout.width}px`,
            maxWidth: '100%',
            minWidth: `${Math.max(620, layout.width * 0.75)}px`,
            transform:
              spatial && moving
                ? `perspective(1300px) rotateX(${tilt.y}deg) rotateY(${tilt.x}deg)`
                : 'none',
          }"
          aria-label="Graph nodes and declared message paths"
        >
          <defs>
            <pattern
              :id="ids.grid"
              width="80"
              height="40"
              patternUnits="userSpaceOnUse"
            >
              <path
                d="M0 0L80 40 M80 0L0 40"
                fill="none"
                stroke="var(--text-3)"
                stroke-width=".6"
                opacity=".08"
              />
            </pattern>
            <linearGradient :id="ids.top" x2="1" y2="1">
              <stop stop-color="var(--spatial-top)" />
              <stop offset="1" stop-color="var(--spatial-top-end)" />
            </linearGradient>
            <linearGradient :id="ids.left" x2="0" y2="1">
              <stop stop-color="var(--spatial-side)" />
              <stop offset="1" stop-color="var(--spatial-bottom)" />
            </linearGradient>
            <linearGradient :id="ids.right" x2="0" y2="1">
              <stop stop-color="var(--spatial-side-dark)" />
              <stop offset="1" stop-color="var(--spatial-bottom)" />
            </linearGradient>
            <filter
              :id="ids.glow"
              x="-200%"
              y="-200%"
              width="500%"
              height="500%"
            >
              <feGaussianBlur stdDeviation="3" />
            </filter>
            <marker
              :id="ids.arrow"
              viewBox="0 0 10 10"
              refX="9"
              refY="5"
              markerWidth="6"
              markerHeight="6"
              orient="auto-start-reverse"
            >
              <path d="M0 1L9 5L0 9" fill="var(--spatial-accent)" />
            </marker>
          </defs>
          <rect
            :width="layout.width"
            :height="layout.height"
            :fill="url('grid')"
          />
          <text x="30" y="31" class="spatial-map-note">
            {{ spatial ? 'SPATIAL VIEW' : 'FLAT VIEW' }} /
            {{ graph.nodes?.length ?? 0 }} NODES
          </text>
          <g
            v-for="edge in links"
            :key="edge.key"
            :opacity="edge.highlighted ? 1 : 0.15"
          >
            <path
              :d="edge.d"
              stroke="var(--spatial-accent)"
              stroke-width="9"
              opacity=".035"
              fill="none"
            />
            <path
              :d="edge.d"
              stroke="var(--spatial-accent)"
              stroke-width="1.2"
              opacity=".5"
              fill="none"
              :marker-end="url('arrow')"
            />
            <g v-if="edge.active">
              <g v-for="particle in 2" :key="particle">
                <circle r="5" fill="var(--brand)" :filter="url('glow')">
                  <animateMotion
                    :dur="`${edge.duration}s`"
                    :begin="`${(-particle * edge.duration) / 2}s`"
                    repeatCount="indefinite"
                    :path="edge.d"
                  />
                </circle>
                <circle r="2" fill="var(--brand)">
                  <animateMotion
                    :dur="`${edge.duration}s`"
                    :begin="`${(-particle * edge.duration) / 2}s`"
                    repeatCount="indefinite"
                    :path="edge.d"
                  />
                </circle>
              </g>
            </g>
          </g>
          <g
            v-for="node in blocks"
            :key="node.id"
            class="spatial-node"
            role="button"
            tabindex="0"
            :aria-pressed="selectedNode?.id === node.id"
            :aria-label="`${node.name}, ${node.kind}, ${node.usage == null ? 'budget usage unavailable' : pct(node.usage)}. Inspect node.`"
            :opacity="
              highlightedPath && !(node.paths ?? []).includes(highlightedPath)
                ? 0.3
                : 1
            "
            @click="selected = node.id"
            @keydown.enter.prevent="selected = node.id"
            @keydown.space.prevent="selected = node.id"
          >
            <title>{{ node.name }} · {{ node.kind }}</title>
            <ellipse
              :cx="node.x"
              :cy="node.y + 20"
              :rx="node.w + 6"
              ry="29"
              fill="var(--spatial-shadow)"
            />
            <template v-if="spatial">
              <polygon
                :points="top({ ...node, h: -8, w: node.w + 12, d: node.d + 7 })"
                fill="var(--spatial-bottom)"
                :stroke="color(node)"
                :stroke-opacity="selectedNode?.id === node.id ? 0.5 : 0.16"
              />
              <polygon
                :points="left(node)"
                :fill="url('left')"
                :stroke="color(node)"
                stroke-opacity=".25"
              />
              <polygon
                :points="right(node)"
                :fill="url('right')"
                :stroke="color(node)"
                stroke-opacity=".2"
              />
              <path
                v-for="layer in 3"
                :key="layer"
                :d="`M${node.x - node.w},${node.y - (layer * node.h) / 4} l${node.w},${node.d} l${node.w},${-node.d}`"
                fill="none"
                :stroke="color(node)"
                stroke-opacity=".15"
              />
              <polygon
                :points="top(node)"
                :fill="url('top')"
                :stroke="color(node)"
                :stroke-opacity="selectedNode?.id === node.id ? 1 : 0.65"
                :stroke-width="selectedNode?.id === node.id ? 1.7 : 1"
              />
              <polygon
                :points="top({ ...node, w: node.w - 9, d: node.d - 5 })"
                fill="none"
                :stroke="color(node)"
                stroke-opacity=".15"
              />
            </template>
            <rect
              v-else
              :x="node.x - node.w"
              :y="node.y - 33"
              :width="node.w * 2"
              height="66"
              rx="10"
              :fill="url('top')"
              :stroke="color(node)"
              :stroke-width="selectedNode?.id === node.id ? 2 : 1"
            />
            <g
              :transform="`translate(${node.x}, ${node.y - node.h}) ${spatial ? 'scale(1 .6)' : ''}`"
              fill="none"
              :stroke="color(node)"
              stroke-width="2"
              stroke-linecap="round"
              stroke-linejoin="round"
            >
              <path
                v-if="node.output"
                d="M-17-12H3V12H-17Z M-3 0H22 M15-7L22 0L15 7"
              />
              <path
                v-else-if="node.ingressQueue"
                d="M-18-12H18V-4H-18Z M-18 4H18V12H-18Z M-12-8H-10 M-12 8H-10"
              />
              <path
                v-else
                d="M-20 17V-17H-9V17 M-3 17V-17H8V17 M16-7L24 0L16 7 M8 0H24"
                stroke-width="3"
              />
            </g>
            <text
              :x="node.x"
              :y="node.y - node.h - node.d - 23"
              text-anchor="middle"
              class="spatial-node-label"
            >
              {{ short(node.name) }}
            </text>
            <text
              :x="node.x"
              :y="node.y - node.h - node.d - 7"
              text-anchor="middle"
              class="spatial-node-role"
            >
              {{ node.kind }}
              <tspan v-if="node.ingressQueue && !node.output">
                · {{ node.breaker ? 'backoff' : pct(node.usage) }}
              </tspan>
            </text>
            <text
              v-if="!node.ingressQueue || node.output"
              :x="node.x"
              :y="node.y + node.d + 27"
              text-anchor="middle"
              class="spatial-node-reading"
              :fill="color(node)"
            >
              {{
                node.output
                  ? `${reading(node.waiting_for_workers)} for workers`
                  : node.breaker
                    ? 'Vendor backoff'
                    : node.usage == null
                      ? node.budgets.length
                        ? 'Usage unavailable'
                        : 'No budget'
                      : `${pct(node.usage)} capacity`
              }}
            </text>
            <g
              v-if="node.waiting_for_budget > 0"
              :transform="`translate(${node.x + node.w - 12},${node.y - node.h - 14})`"
            >
              <rect
                x="-25"
                y="-12"
                width="60"
                height="22"
                rx="11"
                fill="var(--warn-dim)"
                stroke="var(--warn)"
                stroke-opacity=".6"
              />
              <text
                x="5"
                y="3"
                text-anchor="middle"
                font-size="9"
                fill="var(--warn)"
              >
                {{ num(node.waiting_for_budget) }}
              </text>
            </g>
          </g>
        </svg>
      </div>
      <footer class="spatial-map-footer">
        <span
          ><i></i>{{ stale ? 'Last known topology' : 'Declared paths'
          }}<span class="spatial-footer-divider">/</span> Select a node to
          inspect</span
        ><span>Motion indicates recent relays, not speed</span>
      </footer>
      <p v-if="layout.cyclic" class="spatial-notice">
        This topology contains a cycle. Inspect the graph declaration.
      </p>
    </div>

    <aside
      v-if="selectedNode"
      class="spatial-inspector card"
      aria-label="Selected node details"
    >
      <div class="spatial-inspector-eyebrow">
        NODE INSPECTOR<Icon name="graph" :size="15" />
      </div>
      <div class="spatial-node-identity">
        <span class="spatial-node-icon"
          ><Icon
            :name="
              selectedNode.output
                ? 'logout'
                : selectedNode.ingressQueue
                  ? 'lane'
                  : 'budget'
            "
            :size="23"
        /></span>
        <div>
          <h3>{{ selectedNode.name }}</h3>
          <p>{{ selectedNode.kind }}</p>
        </div>
      </div>
      <div class="spatial-node-status">
        <StatusDot v-if="!stale" :state="selectedState" /><span
          v-else
          class="text-warn"
          >Refresh failed · last known values</span
        >
      </div>
      <template v-if="selectedNode.budget">
        <div class="spatial-budget-title">
          <span>Budget utilization</span
          ><strong>{{ pct(selectedNode.usage) }}</strong>
        </div>
        <div
          class="spatial-meter"
          role="meter"
          aria-label="Selected node budget utilization"
          :aria-valuenow="Math.min(100, Math.round(selectedNode.usage * 100))"
          aria-valuemin="0"
          aria-valuemax="100"
          :aria-valuetext="pct(selectedNode.usage)"
        >
          <i
            v-for="i in 26"
            :key="i"
            :class="{ used: i <= Math.ceil(selectedNode.usage * 26) }"
          />
        </div>
        <div class="spatial-meter-scale">
          <span>0</span
          ><span
            >{{ num(ceilingOf(selectedNode.budget)) }} cost /
            {{ period(selectedNode.budget.windowSubSeconds) }}</span
          >
        </div>
        <RouterLink
          :to="budgetLink(selectedNode, selectedNode.budget)"
          class="spatial-budget-link"
          >{{ selectedNode.budget.id }}<Icon name="chevron" :size="12"
        /></RouterLink>
      </template>
      <p v-else class="spatial-inspector-note">
        {{
          selectedNode.output
            ? 'Admitted work, ready for your consumers.'
            : selectedNode.budgets?.length
              ? 'No aggregate budget reading is available.'
              : 'No budget declared on this node.'
        }}
      </p>
      <p v-if="selectedNode.unknownBudgets" class="spatial-inspector-note">
        {{ selectedNode.unknownBudgets }} budget{{
          selectedNode.unknownBudgets === 1 ? '' : 's'
        }}
        require a scope, operation, or a readable counter.
      </p>
      <dl class="spatial-node-metrics">
        <div v-if="!selectedNode.output">
          <dt>Admitted · stage total</dt>
          <dd>{{ reading(selectedNode.admitted) }}</dd>
        </div>
        <div v-if="!selectedNode.output">
          <dt>Waiting for budget</dt>
          <dd>{{ reading(selectedNode.waiting_for_budget) }}</dd>
        </div>
        <div>
          <dt>Waiting for workers</dt>
          <dd>{{ reading(selectedNode.waiting_for_workers) }}</dd>
        </div>
        <div>
          <dt>Paths</dt>
          <dd>{{ selectedNode.paths?.length ?? 0 }}</dd>
        </div>
      </dl>
      <div v-if="selectedNode.breaker" class="spatial-notice">
        Vendor backoff · {{ period(selectedNode.breaker.retryAfterSeconds) }}
      </div>
      <div class="spatial-inspector-paths">
        <span v-for="path in selectedNode.paths" :key="path" class="chip">{{
          path
        }}</span>
      </div>
      <RouterLink
        v-if="detailLink"
        :to="graphPath(graph.application, graph.graph)"
        class="spatial-detail-link"
        >Open graph details<Icon name="chevron" :size="14"
      /></RouterLink>
    </aside>
  </section>
</template>
