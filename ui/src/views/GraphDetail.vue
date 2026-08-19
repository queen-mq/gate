<script setup>
/*
  The live graph: the diagram, then the nodes behind it.

  The diagram answers "where is the work and what is holding it"; the tables
  answer "and against which number". Both are polled, because every figure here
  is a window that is closing right now.
*/
import { ref, computed } from 'vue'
import PageHeader from '../components/PageHeader.vue'
import GraphDiagram from '../components/GraphDiagram.vue'
import BudgetBar from '../components/BudgetBar.vue'
import StatusDot from '../components/StatusDot.vue'
import { api, num, pct, period, targetPath, DEFAULT_APP } from '../lib/api.js'
import { usePoll } from '../lib/poll.js'

const props = defineProps({ app: String, name: String })

const graph = ref(null)
const error = ref('')

async function load() {
  try {
    graph.value = await api.get(
      `/api/apps/${encodeURIComponent(props.app || DEFAULT_APP)}/graphs/${encodeURIComponent(props.name)}`,
    )
    error.value = ''
  } catch (e) {
    error.value = e.message
  }
}
usePoll(load)

/* Edge lag belongs on the edge in the picture, so it is folded in here rather
   than looked up twice. */
const edges = computed(() => graph.value?.edges ?? [])
const nodes = computed(() => graph.value?.nodes ?? [])
const relays = computed(() => graph.value?.relays ?? [])

function nodeState(n) {
  if (!n.running) return 'down'
  if ((n.lanes ?? []).some((l) => l.throttled > 0)) return 'breached'
  if ((n.waiting_for_budget ?? 0) > 0) return 'pacing'
  return 'flowing'
}
</script>

<template>
  <div>
    <PageHeader
      :title="name"
      mono
      :crumbs="[{ label: 'Graphs', to: '/graphs' }]"
      :sub="`${app} · version ${graph?.version ?? '—'} · consumed at ${(graph?.consume ?? []).join(', ') || '—'}`"
    />

    <p v-if="error" class="mb-4 text-[13px] text-bad">{{ error }}</p>

    <p v-for="w in graph?.warnings ?? []" :key="w"
       class="mb-3 text-[12.5px] text-warn">{{ w }}</p>

    <section v-if="graph" class="mb-8 rounded-xl border border-line bg-surface p-5">
      <GraphDiagram :nodes="nodes" :edges="edges" :breach="graph.breach ?? []" />
    </section>

    <section v-for="n in nodes" :key="n.name"
             class="mb-4 rounded-xl border border-line bg-surface p-5">
      <div class="flex items-baseline gap-3 flex-wrap">
        <span class="font-mono text-[14.5px]">{{ n.name }}</span>
        <StatusDot :state="nodeState(n)" />
        <RouterLink :to="targetPath(app, n.target)"
                    class="text-[12px] text-link hover:underline ml-auto font-mono">
          {{ n.target }}
        </RouterLink>
      </div>

      <div class="mt-3 grid grid-cols-2 sm:grid-cols-4 gap-4 text-[12.5px]">
        <div>
          <div class="text-fg-3 text-[11px]">waiting for budget</div>
          <div class="tabular-nums">{{ num(n.waiting_for_budget) }}</div>
        </div>
        <div>
          <div class="text-fg-3 text-[11px]">waiting for workers</div>
          <div class="tabular-nums">{{ num(n.waiting_for_workers) }}</div>
        </div>
        <div>
          <div class="text-fg-3 text-[11px]">shards</div>
          <div class="tabular-nums">{{ n.shards }}<span v-if="n.shardBy" class="text-fg-3"> by {{ n.shardBy }}</span></div>
        </div>
        <div>
          <div class="text-fg-3 text-[11px]">cost max</div>
          <div class="tabular-nums">{{ n.cost?.max }}</div>
        </div>
      </div>

      <table v-if="(n.budgets ?? []).length" class="w-full mt-4 text-[12.5px]">
        <thead>
          <tr class="text-left text-[11px] text-fg-3">
            <th class="font-normal pb-1.5">budget</th>
            <th class="font-normal pb-1.5">window</th>
            <th class="font-normal pb-1.5">keys</th>
            <th class="font-normal pb-1.5 w-[38%]">spent</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="b in n.budgets" :key="b.id" class="border-t border-line">
            <td class="py-2 font-mono">{{ b.id }}</td>
            <td class="py-2 tabular-nums text-fg-2">{{ num(b.cap) }} / {{ period(b.periodSeconds) }}</td>
            <td class="py-2 tabular-nums text-fg-2">
              <span v-if="b.scope?.length">{{ num(b.keys) }}<span v-if="b.maxKeys" class="text-fg-3"> / {{ num(b.maxKeys) }}</span></span>
              <span v-else class="text-fg-3">—</span>
            </td>
            <td class="py-2">
              <div class="flex items-center gap-2">
                <BudgetBar :used="b.used" :cap="b.cap" :assumed="b.confidence === 'assumed'" />
                <span class="tabular-nums text-fg-2 w-10 text-right">{{ pct(b.utilisation) }}</span>
              </div>
            </td>
          </tr>
        </tbody>
      </table>
      <p v-else class="mt-3 text-[12.5px] text-fg-3">
        No budget of its own: this node exists to isolate a class and to carry a priority, and is checked
        against the budgets downstream.
      </p>

      <div v-if="(n.lanes ?? []).some((l) => l.retried || l.exhausted)"
           class="mt-3 text-[12px] text-fg-2">
        <span v-for="l in n.lanes" :key="l.name">
          <template v-if="l.retried || l.exhausted">
            {{ num(l.retried) }} sent back after a throttle,
            {{ num(l.exhausted) }} out of attempts
          </template>
        </span>
      </div>
    </section>

    <section v-if="relays.length" class="rounded-xl border border-line bg-surface p-5">
      <h2 class="text-[13px] font-medium mb-1">Relays</h2>
      <p class="text-[12px] text-fg-3 mb-3">
        One per destination, draining its upstreams in strict priority order and stopping while the
        destination's queue is deeper than its window — a shallow bottleneck is what makes priority at the
        entrance priority in fact.
      </p>
      <table class="w-full text-[12.5px]">
        <thead>
          <tr class="text-left text-[11px] text-fg-3">
            <th class="font-normal pb-1.5">into</th>
            <th class="font-normal pb-1.5">from</th>
            <th class="font-normal pb-1.5">window</th>
            <th class="font-normal pb-1.5">relayed</th>
            <th class="font-normal pb-1.5">unroutable</th>
            <th class="font-normal pb-1.5">already there</th>

          </tr>
        </thead>
        <tbody>
          <tr v-for="r in relays" :key="r.dest" class="border-t border-line">
            <td class="py-2 font-mono">{{ r.dest }}</td>
            <td class="py-2 font-mono text-fg-2">
              {{ r.sources.map((s) => `${s.node}(p${s.priority})`).join(', ') }}
            </td>
            <td class="py-2 tabular-nums text-fg-2">{{ num(r.window) }}</td>
            <td class="py-2 tabular-nums text-fg-2">{{ num(r.forwarded) }}</td>
            <td class="py-2 tabular-nums" :class="r.unroutable ? 'text-bad' : 'text-fg-3'">
              {{ num(r.unroutable) }}
            </td>
            <!-- Batches this relay found part-forwarded and settled one item at a time.
                 Should be zero; visible because a recovery nobody can see is a recovery
                 nobody knows ran. -->
            <td class="py-2 tabular-nums" :class="r.duplicates ? 'text-warn' : 'text-fg-3'">
              {{ num(r.duplicates) }}
            </td>

          </tr>
        </tbody>
      </table>
    </section>
  </div>
</template>
