<script setup>
/*
  One row per target, and the row answers "how close is it, and to what".
  The tightest budget wins the row: a target with a comfortable daily ceiling
  and a saturated ten-second one is a saturated target, and averaging the two
  would hide exactly the one that is about to refuse.

  Grouped by APPLICATION, because a target's identity is the pair and not the
  name: two teams may both own something they call `airbnb`, holding two
  credentials against two ceilings, and a flat list would put them next to each
  other looking like duplicates of one thing.
*/
import { ref, computed } from 'vue'
import PageHeader from '../components/PageHeader.vue'
import StatusDot from '../components/StatusDot.vue'
import BudgetBar from '../components/BudgetBar.vue'
import Icon from '../components/Icon.vue'
import { api, num, pct, period, isAdmin, targetPath, DEFAULT_APP, READ_ONLY_NOTE } from '../lib/api.js'
import { usePoll } from '../lib/poll.js'

const targets = ref(null)
const apps = ref([])
const error = ref('')
const q = ref('')
const app = ref('')

async function load() {
  try {
    const [ts, as] = await Promise.all([
      api.get('/api/targets'),
      // The application index carries its own totals, which is what makes a
      // group header worth having. It is not required to render the list, so
      // a build that does not serve it degrades to headers with counts.
      api.get('/api/apps').catch(() => []),
    ])
    targets.value = ts
    apps.value = Array.isArray(as) ? as : []
    error.value = ''
  } catch (e) {
    error.value = e.message
  }
}
usePoll(load)

const appNames = computed(() => {
  const names = new Set(apps.value.map((a) => a.application))
  for (const t of targets.value ?? []) names.add(t.application || DEFAULT_APP)
  return [...names].sort()
})

const shown = computed(() => {
  const list = targets.value ?? []
  const needle = q.value.trim().toLowerCase()
  return list.filter((t) => {
    if (app.value && (t.application || DEFAULT_APP) !== app.value) return false
    if (!needle) return true
    return (
      t.name.toLowerCase().includes(needle) ||
      (t.application || DEFAULT_APP).toLowerCase().includes(needle)
    )
  })
})

const groups = computed(() => {
  const by = new Map()
  for (const t of shown.value) {
    const a = t.application || DEFAULT_APP
    if (!by.has(a)) by.set(a, [])
    by.get(a).push(t)
  }
  return [...by.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([application, rows]) => ({
      application,
      rows,
      totals: apps.value.find((x) => x.application === application) ?? null,
    }))
})
</script>

<template>
  <div>
    <PageHeader
      title="Targets"
      sub="One target is one thing that limits us: a portal, an API, an account. Its budgets are what it publishes, its lanes are how we choose to spend them. It belongs to an application, and applications never share a ceiling."
    >
      <template #actions>
        <select v-model="app" class="input w-[190px]" aria-label="Filter by application">
          <option value="">All applications</option>
          <option v-for="a in appNames" :key="a" :value="a">{{ a }}</option>
        </select>
        <div class="relative">
          <Icon name="search" :size="14"
                class="absolute left-3 top-1/2 -translate-y-1/2 text-fg-3 pointer-events-none" />
          <input v-model="q" class="input w-[200px] pl-9" placeholder="Filter targets" />
        </div>
        <RouterLink v-if="isAdmin" to="/targets/new" class="btn btn-primary">
          <Icon name="plus" :size="14" /> New target
        </RouterLink>
      </template>
    </PageHeader>

    <div v-if="error" class="card border-transparent bg-bad-dim px-5 py-4 text-[13.5px] text-bad">
      {{ error }}
    </div>

    <div v-else-if="!targets" class="card px-6 py-8 space-y-3">
      <div class="skeleton h-4 w-1/3" />
      <div class="skeleton h-4 w-1/2" />
    </div>

    <div v-else-if="!shown.length" class="card px-6 py-12 text-center">
      <p class="text-[13.5px] text-fg-2">
        {{ q || app ? 'No target matches that filter.' : 'No target declared yet.' }}
      </p>
      <p v-if="!q && !app" class="text-[12.5px] text-fg-3 mt-1">
        Declare one here, or from a caller with
        <span class="kbd">PUT /v1/apps/{application}/targets/{name}</span>.
      </p>
      <RouterLink v-if="!q && !app && isAdmin" to="/targets/new" class="btn btn-primary mt-5">
        <Icon name="plus" :size="14" /> New target
      </RouterLink>
      <p v-else-if="!q && !app" class="text-[12px] text-fg-3 mt-4">{{ READ_ONLY_NOTE }}</p>
    </div>

    <div v-else class="space-y-8">
      <section v-for="g in groups" :key="g.application">
        <h2 class="section-title">
          <span class="font-mono">{{ g.application }}</span>
          <span class="section-count">
            {{ num(g.rows.length) }} target{{ g.rows.length === 1 ? '' : 's' }}
            <template v-if="g.totals">
              · {{ num(g.totals.admitted) }} admitted · {{ num(g.totals.denied) }} denied
            </template>
          </span>
        </h2>

        <div class="card divide-y divide-line">
          <RouterLink
            v-for="t in g.rows" :key="`${t.application}/${t.name}`"
            :to="targetPath(t.application, t.name)"
            class="flex items-center gap-5 px-5 py-4 hover:bg-surface-2 transition-colors group"
          >
            <div class="min-w-0 flex-[2]">
              <div class="flex items-center gap-2.5">
                <span class="font-medium text-[14px]">{{ t.name }}</span>
                <span class="chip">v{{ t.version }}</span>
                <StatusDot :state="t.assumed_budgets ? 'blind' : t.state" />
              </div>
              <div class="text-[12.5px] text-fg-2 mt-0.5 truncate">
                {{ (t.lanes ?? []).map((l) => l.name).join(' · ') }}
                <span class="text-fg-3">— {{ t.budgets_total }} budget{{ t.budgets_total === 1 ? '' : 's' }}</span>
              </div>
            </div>

            <!-- The tightest window, named, because "87%" of what is the question. -->
            <div class="hidden md:block flex-1 min-w-0">
              <div class="flex items-baseline gap-2 mb-1.5">
                <span class="font-mono text-[11.5px] text-fg-2 truncate">{{ t.worst_budget_id }}</span>
                <span class="chip shrink-0">{{ period(t.worst_period_seconds) }}</span>
              </div>
              <BudgetBar :used="t.worst_used" :cap="t.worst_cap" :assumed="t.worst_assumed" />
            </div>

            <div class="w-16 text-right shrink-0 tabular-nums">
              <div class="text-[15px] font-semibold">{{ pct(t.worst_cap ? t.worst_used / t.worst_cap : 0) }}</div>
              <div class="text-[11px] text-fg-3">of cap</div>
            </div>

            <!-- Counters, not rates: the list endpoint reports totals since the
                 gate started, and dividing them by an uptime the console does not
                 know would be a rate that is wrong in a way nobody could see. -->
            <div class="w-20 text-right shrink-0 tabular-nums hidden sm:block">
              <div class="text-[13px]">{{ num(t.admitted) }}</div>
              <div class="text-[11px] text-fg-3">{{ num(t.denied) }} denied</div>
            </div>

            <Icon name="chevron" :size="15" class="text-fg-3 group-hover:text-fg-2 transition-colors" />
          </RouterLink>
        </div>
      </section>
    </div>
  </div>
</template>
