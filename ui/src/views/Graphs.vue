<script setup>
/*
  One row per graph. A graph is the only object: a one-node graph is what a
  target was, and a several-node one composes limits that would otherwise have to
  be checked at one instant or not at all.
*/
import { ref, computed } from 'vue'
import PageHeader from '../components/PageHeader.vue'
import Icon from '../components/Icon.vue'
import { api, num, isAdmin, graphPath as pathOf, DEFAULT_APP } from '../lib/api.js'
import { usePoll } from '../lib/poll.js'

const graphs = ref(null)
const error = ref('')

async function load() {
  try {
    graphs.value = await api.get('/api/graphs')
    error.value = ''
  } catch (e) {
    error.value = e.message
  }
}
usePoll(load)

function graphPath(g) {
  return pathOf(g.application || DEFAULT_APP, g.name)
}

/* Nodes in the order work moves through them, not in the order a map iterated:
   a row that reads `ip › messages › photos` describes a path that does not
   exist. One column right of everything that relays into it, same rule as the
   diagram. */
function inFlowOrder(g) {
  const preds = new Map((g.nodes ?? []).map((n) => [n.name, []]))
  for (const e of g.edges ?? []) {
    if (preds.has(e.to)) preds.get(e.to).push(e.from)
  }
  const depth = new Map()
  const seen = new Set()
  const of = (name) => {
    if (depth.has(name)) return depth.get(name)
    if (seen.has(name)) return 0
    seen.add(name)
    const d = (preds.get(name) ?? []).reduce((m, p) => Math.max(m, of(p) + 1), 0)
    depth.set(name, d)
    return d
  }
  return [...(g.nodes ?? [])].sort(
    (a, b) => of(a.name) - of(b.name) || a.name.localeCompare(b.name),
  )
}

const rows = computed(() =>
  (graphs.value ?? []).map((g) => ({
    ...g,
    nodes: inFlowOrder(g),

    waiting: (g.nodes ?? []).reduce((n, x) => n + (x.waiting_for_budget ?? 0), 0),
    admitted: (g.nodes ?? []).reduce((n, x) => n + (x.admitted ?? 0), 0),
    denied: (g.nodes ?? []).reduce((n, x) => n + (x.denied ?? 0), 0),
    down: (g.nodes ?? []).filter((x) => !x.running).length,
  })),
)
</script>

<template>
  <div>
    <PageHeader
      title="Graphs"
      sub="A graph is a set of declared paths: work enters at an ingress node, is charged against every
           node it traverses, and leaves on an egress queue your own consumers pop. The severe limit lives
           at the end, where it is enforced last and exactly."
    >
      <template #actions>
        <RouterLink v-if="isAdmin" to="/graphs/new" class="btn btn-primary">
          <Icon name="plus" :size="14" /> New graph
        </RouterLink>
      </template>
    </PageHeader>

    <p v-if="error" class="mb-4 text-[13px] text-bad">{{ error }}</p>

    <div v-if="rows.length === 0 && graphs !== null"
         class="rounded-xl border border-line bg-surface p-8 text-center">
      <p class="text-[13.5px] text-fg-2">No graph is declared.</p>
      <p class="text-[12.5px] text-fg-3 mt-1.5">
        A graph is declared whole, by its owner:
        <code class="font-mono">PUT /v1/apps/:app/graphs/:name</code>.
      </p>
    </div>

    <div v-for="g in rows" :key="`${g.application}/${g.name}`"
         class="mb-3 rounded-xl border border-line bg-surface overflow-hidden">
      <RouterLink :to="graphPath(g)" class="block p-5 hover:bg-surface-2 transition-colors">
        <div class="flex items-baseline gap-3 flex-wrap">
          <span class="font-mono text-[15px] text-fg">{{ g.name }}</span>
          <span class="text-[12px] text-fg-3">{{ g.application }}</span>
          <span class="text-[12px] text-fg-3">v{{ g.version }}</span>
          <span v-if="g.down" class="text-[12px] text-bad">{{ g.down }} node(s) not running</span>
          <Icon name="chevron" :size="12" class="ml-auto text-fg-3" />
        </div>

        <div class="mt-3 flex flex-wrap items-center gap-1.5 font-mono text-[12px] text-fg-2">
          <template v-for="(n, i) in g.nodes" :key="n.name">
            <span class="px-2 py-0.5 rounded-md bg-surface-2"
                  :class="n.running ? '' : 'text-bad'">{{ n.name }}</span>
            <Icon v-if="i < g.nodes.length - 1" name="chevron" :size="10" class="text-fg-3" />
          </template>
        </div>

        <div class="mt-3 grid grid-cols-2 sm:grid-cols-4 gap-4 text-[12.5px]">
          <div>
            <div class="text-fg-3 text-[11px]">waiting for budget</div>
            <div class="tabular-nums">{{ num(g.waiting) }}</div>
          </div>
          <div>
            <div class="text-fg-3 text-[11px]">admitted</div>
            <div class="tabular-nums">{{ num(g.admitted) }}</div>
          </div>
          <div>
            <div class="text-fg-3 text-[11px]">denied</div>
            <div class="tabular-nums">{{ num(g.denied) }}</div>
          </div>
          <div>
            <div class="text-fg-3 text-[11px]">relayed</div>
            <div class="tabular-nums">{{ num(g.forwarded) }}</div>
          </div>
        </div>
      </RouterLink>
    </div>
  </div>
</template>
