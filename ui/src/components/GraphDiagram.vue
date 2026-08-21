<script setup>
/*
  The drawing this feature came from, rendered from what is actually running.

  Laid out in one SVG rather than in measured HTML boxes: the layout is a
  function of the topology (a node sits one column right of everything that
  relays into it), so it can be computed rather than observed, and a diagram that
  is computed cannot disagree with itself while the page reflows.

  What the picture has to say, in order of importance:
    * which node is the bottleneck — the fill of its worst budget;
    * where the work is waiting — the number on the node, and the lag on the edge;
    * which way it flows, and in what order — the arrow, and the priority on it.

  The dashed retro edge is gone with the feature it drew: a throttle is reported
  to `POST .../backoff` now, which spends the node's window rather than sending
  an item back to the door it came in at.
*/
import { computed } from 'vue'

const props = defineProps({
  nodes: { type: Array, default: () => [] },   // /api/.../graphs/:name → nodes[]
  edges: { type: Array, default: () => [] },
})

const W = 210
const H = 92
const GAP_X = 96
const GAP_Y = 26
const PAD = 16

/* A node sits one column to the right of everything that relays into it, so the
   picture reads left to right in the order the work is actually paced. */
const layout = computed(() => {
  const byName = new Map(props.nodes.map((n) => [n.name, n]))
  const preds = new Map(props.nodes.map((n) => [n.name, []]))
  for (const e of props.edges) {
    if (byName.has(e.from) && byName.has(e.to)) preds.get(e.to).push(e.from)
  }
  const depth = new Map()
  const seen = new Set()
  const of = (name) => {
    if (depth.has(name)) return depth.get(name)
    if (seen.has(name)) return 0 // a cycle cannot be declared; do not hang if one is
    seen.add(name)
    const d = (preds.get(name) ?? []).reduce((m, p) => Math.max(m, of(p) + 1), 0)
    depth.set(name, d)
    return d
  }
  for (const n of props.nodes) of(n.name)

  const columns = new Map()
  for (const n of props.nodes) {
    const d = depth.get(n.name) ?? 0
    if (!columns.has(d)) columns.set(d, [])
    columns.get(d).push(n)
  }
  const placed = new Map()
  let height = 0
  for (const [d, list] of [...columns.entries()].sort(([a], [b]) => a - b)) {
    const colHeight = list.length * H + (list.length - 1) * GAP_Y
    height = Math.max(height, colHeight)
    list.forEach((n, i) => {
      placed.set(n.name, { node: n, x: PAD + d * (W + GAP_X), y: i * (H + GAP_Y), col: d })
    })
  }
  // Centre each column against the tallest one.
  for (const [d, list] of columns.entries()) {
    const colHeight = list.length * H + (list.length - 1) * GAP_Y
    const offset = (height - colHeight) / 2
    for (const n of list) placed.get(n.name).y += PAD + offset
  }
  const width = PAD * 2 + (columns.size - 1) * (W + GAP_X) + W
  return { placed, width: Math.max(width, W + PAD * 2), height: height + PAD * 2 + 34 }
})

const boxes = computed(() => [...layout.value.placed.values()])

function worst(node) {
  let top = null
  for (const b of node.budgets ?? []) {
    if (!top || (b.utilisation ?? 0) > (top.utilisation ?? 0)) top = b
  }
  return top
}

/* Bezier from the right edge of the source to the left edge of the destination. */
const links = computed(() =>
  props.edges
    .map((e) => {
      const a = layout.value.placed.get(e.from)
      const b = layout.value.placed.get(e.to)
      if (!a || !b) return null
      const x1 = a.x + W
      const y1 = a.y + H / 2
      const x2 = b.x
      const y2 = b.y + H / 2
      const dx = Math.max(40, (x2 - x1) / 2)
      return {
        ...e,
        d: `M${x1},${y1} C${x1 + dx},${y1} ${x2 - dx},${y2} ${x2},${y2}`,
        lx: (x1 + x2) / 2,
        ly: (y1 + y2) / 2 - 8,
      }
    })
    .filter(Boolean),
)

function fill(u) {
  if (u === null || u === undefined) return 'var(--color-line-2)'
  if (u > 1) return 'var(--color-bad)'
  if (u >= 0.85) return 'var(--color-warn)'
  return 'var(--color-good)'
}
</script>

<template>
  <div class="overflow-x-auto -mx-1 px-1">
    <svg
      :viewBox="`0 0 ${layout.width} ${layout.height}`"
      :style="{ width: `${layout.width}px`, maxWidth: '100%', height: 'auto' }"
      class="min-w-[520px]"
      role="img"
      aria-label="the graph, as it is running"
    >
      <defs>
        <marker id="gd-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7"
                markerHeight="7" orient="auto-start-reverse">
          <path d="M0,1 L9,5 L0,9 z" fill="var(--color-line-2)" />
        </marker>
      </defs>

      <g v-for="l in links" :key="`${l.from}-${l.to}`">
        <path :d="l.d" fill="none" stroke="var(--color-line-2)" stroke-width="1.4"
              marker-end="url(#gd-arrow)" />
        <text :x="l.lx" :y="l.ly" text-anchor="middle" font-size="10.5"
              font-family="var(--font-mono)" fill="var(--color-fg-3)">
          p{{ l.priority ?? 0 }}<tspan v-if="l.lag"> · {{ l.lag }} waiting</tspan>
        </text>
      </g>

      <g v-for="b in boxes" :key="b.node.name">
        <rect :x="b.x" :y="b.y" :width="W" :height="H" rx="10"
              fill="var(--color-surface)" stroke="var(--color-line)" />
        <text :x="b.x + 14" :y="b.y + 24" font-size="13" font-family="var(--font-mono)"
              fill="var(--color-fg)">{{ b.node.name }}</text>
        <!-- Role and shape on one line, so nothing collides with the depths at
             the foot of the box however long a node's name is. -->
        <text :x="b.x + W - 14" :y="b.y + 24" text-anchor="end" font-size="10"
              fill="var(--color-fg-3)" font-family="var(--font-mono)">
          {{ [b.node.entry ? 'entry' : null, b.node.consume ? 'terminal' : null,
              (b.node.paths ?? []).length > 1 ? `${b.node.paths.length} paths` : null]
              .filter(Boolean).join(' · ') }}
        </text>


        <!-- The worst budget, because a node is as spent as the counter closest
             to refusing; a node with none is a scheduler and says so. -->
        <template v-if="worst(b.node)">
          <text :x="b.x + 14" :y="b.y + 44" font-size="10.5" fill="var(--color-fg-2)"
                font-family="var(--font-mono)">
            {{ worst(b.node).id }}
          </text>
          <text :x="b.x + W - 14" :y="b.y + 44" text-anchor="end" font-size="10.5"
                fill="var(--color-fg-2)" font-family="var(--font-mono)">
            {{ Math.round((worst(b.node).utilisation ?? 0) * 100) }}%
          </text>
          <rect :x="b.x + 14" :y="b.y + 52" :width="W - 28" height="5" rx="2.5"
                fill="var(--color-surface-2)" />
          <rect :x="b.x + 14" :y="b.y + 52"
                :width="Math.max(0, Math.min(1, worst(b.node).utilisation ?? 0)) * (W - 28)"
                height="5" rx="2.5" :fill="fill(worst(b.node).utilisation)" />
        </template>
        <text v-else :x="b.x + 14" :y="b.y + 48" font-size="10.5" fill="var(--color-fg-3)"
              font-family="var(--font-mono)">no budget declared</text>

        <text :x="b.x + 14" :y="b.y + H - 14" font-size="10.5" fill="var(--color-fg-3)"
              font-family="var(--font-mono)">
          {{ b.node.waiting_for_budget ?? 0 }} for budget · {{ b.node.waiting_for_workers ?? 0 }} for workers
        </text>

      </g>
    </svg>
  </div>
</template>
