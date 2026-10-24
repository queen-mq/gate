<script setup>
import { computed, nextTick, onMounted, onUnmounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'
import Icon from './Icon.vue'
import GateBrand from './GateBrand.vue'
import { api, authState, me } from '../lib/api.js'
import { usePoll } from '../lib/poll.js'
import { rail, wide, drawerOpen } from '../lib/sidebar.js'

const route = useRoute()

/*
  Navigation grouped like Queen's console. The primary object is the GRAPH — one document, its
  nodes, its budgets and the paths that cross them. "Targets" is the same list
  in the older vocabulary, because a target is a one-node graph and a link in a
  runbook does not stop existing when a model changes.
*/
const groups = [
  {
    label: null,
    items: [
      { to: '/', label: 'Overview', icon: 'gauge', key: 'overview' },
    ],
  },
  {
    label: 'Routing',
    items: [
      { to: '/targets', label: 'Targets', icon: 'target', key: 'targets' },
      { to: '/graphs', label: 'Graphs', icon: 'graph', key: 'graphs' },
      { to: '/budgets', label: 'Shared budgets', icon: 'budget', key: 'budgets' },
    ],
  },
  {
    label: 'Observability',
    items: [
      { to: '/traces', label: 'Traces', icon: 'trace', key: 'traces' },
    ],
  },
]
const isActive = (item) => route.meta?.nav === item.key

// ---------------------------------------------------------------------------
// The drawer, up to 1100px: it closes on navigation, on Escape and when the
// window grows past the breakpoint; focus moves into it and back to its button.
// ---------------------------------------------------------------------------
const closeDrawer = () => { drawerOpen.value = false }
watch(() => route.fullPath, closeDrawer)
watch(wide, closeDrawer)
watch(drawerOpen, async (open) => {
  await nextTick()
  if (open) document.querySelector('#gate-navigation a')?.focus()
  else document.querySelector('.sidebar-mobile-toggle')?.focus()
})
function onKey(e) {
  if (e.key === 'Escape' && drawerOpen.value) closeDrawer()
}
onMounted(() => document.addEventListener('keydown', onKey))
onUnmounted(() => document.removeEventListener('keydown', onKey))

// ---------------------------------------------------------------------------
// The broker, and the two warnings an operator must never have to go looking
// for, because both mean the numbers on every other page are softer than they
// look. Not yet known is not "down".
// ---------------------------------------------------------------------------
const overview = ref(null)
const overviewError = ref(false)
usePoll(async () => {
  if (authState.value !== 'ready') return
  try {
    overview.value = await api.get('/api/overview')
    overviewError.value = false
  } catch {
    overview.value = null
    overviewError.value = true
  }
}, 15000)

const brokerState = computed(() => {
  if (!overview.value) return overviewError.value ? 'unknown' : 'checking'
  return overview.value.queen?.reachable === true ? 'connected' : 'down'
})
const brokerGlyph = computed(() => ({ connected: 'good', down: 'bad' })[brokerState.value] || 'muted')
const brokerLabel = computed(() => ({
  checking: 'Checking broker',
  unknown: 'Broker status unknown',
  connected: `Queen ${overview.value?.queen?.version ?? ''}`.trim(),
  down: 'Broker unreachable',
}[brokerState.value]))

const warnings = computed(() => {
  const w = []
  const assumed = overview.value?.budgets_assumed ?? 0
  if (assumed)
    w.push(`${assumed} budget${assumed === 1 ? ' is' : 's are'} an assumption, not a published number.`)
  const stale = overview.value?.budgets_stale ?? 0
  if (stale)
    w.push(`${stale} budget${stale === 1 ? '' : 's'} cite a source older than ninety days.`)
  return w
})

// Where you are; who you are is the top bar's account menu, as in Queen.
const host = window.location.host

// ---------------------------------------------------------------------------
// The rail: its rows are icons, so a row's name shows beside it on hover or focus.
// ---------------------------------------------------------------------------
const tip = ref(null)
const showTip = (e, text, note = '') => {
  if (!rail.value) return
  const r = e.currentTarget.getBoundingClientRect()
  tip.value = { text, note, x: Math.round(r.right + 10), y: Math.round(r.top + r.height / 2) }
}
const hideTip = () => { tip.value = null }
watch(rail, hideTip)
watch(() => route.path, hideTip)
</script>

<template>
  <!-- Narrow screens: the sidebar is a drawer over the page. -->
  <Transition name="drawer-scrim">
    <div v-if="drawerOpen" class="sidebar-overlay" aria-hidden="true" @click="closeDrawer" />
  </Transition>

  <button
    type="button"
    class="sidebar-mobile-toggle"
    aria-controls="gate-navigation"
    :aria-expanded="drawerOpen"
    :aria-label="drawerOpen ? 'Close navigation' : 'Open navigation'"
    @click="drawerOpen = !drawerOpen"
  >
    <svg v-if="!drawerOpen" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.6"><path stroke-linecap="round" stroke-linejoin="round" d="M3.75 6.75h16.5M3.75 12h16.5m-16.5 5.25h16.5" /></svg>
    <svg v-else fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.6"><path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12" /></svg>
  </button>

  <aside id="gate-navigation" class="sidebar" :class="{ 'sidebar-mobile-open': drawerOpen, rail }"
         :inert="!wide && !drawerOpen">
    <!-- The wordmark, like Queen's and Traceflow's; the rail keeps the symbol. -->
    <RouterLink to="/" class="brand" aria-label="Gate overview">
      <span class="brand-mark" aria-hidden="true"><GateBrand :variant="rail ? 'symbol' : 'compact'" /></span>
    </RouterLink>

    <div class="sidebar-content">
      <nav class="nav-groups" aria-label="Main">
        <div v-for="(group, gi) in groups" :key="gi" class="nav-group">
          <template v-if="group.label">
            <div v-if="rail" class="nav-rule" aria-hidden="true" />
            <div v-else class="nav-label">{{ group.label }}</div>
          </template>
          <RouterLink
            v-for="item in group.items"
            :key="item.key"
            :to="item.to"
            class="nav-item"
            :class="{ 'nav-item-active': isActive(item) }"
            :aria-current="isActive(item) ? 'page' : undefined"
            @mouseenter="showTip($event, item.label)"
            @mouseleave="hideTip"
            @focus="showTip($event, item.label)"
            @blur="hideTip"
          >
            <Icon :name="item.icon" class="nav-icon" />
            <span class="nav-name" :class="{ 'sr-only': rail }">{{ item.label }}</span>
          </RouterLink>
        </div>
      </nav>

      <!-- What softens the numbers, where you are, and whether the broker answers. -->
      <div class="sidebar-foot">
        <RouterLink
          v-for="w in warnings" :key="w" to="/" class="cell-line"
          @mouseenter="showTip($event, 'Budgets', w)" @mouseleave="hideTip"
        >
          <span class="status-glyph warn" aria-hidden="true" />
          <span class="cell-words" :class="{ 'sr-only': rail }"><span class="is-warn">{{ w }}</span></span>
        </RouterLink>

        <div v-if="!rail" class="whoami" :title="`Gate at ${host}`">
          <b>{{ host }}</b>
          <span class="whoami-sub">Gate<template v-if="me?.role"> · {{ me.role }}</template></span>
        </div>

        <p
          class="cell-line" role="status" :title="rail ? undefined : overview?.queen?.url"
          @mouseenter="showTip($event, brokerLabel, overview?.queen?.url)" @mouseleave="hideTip"
        >
          <span class="status-glyph" :class="brokerGlyph" aria-hidden="true" />
          <span class="cell-words" :class="{ 'sr-only': rail }">
            <b :class="{ 'is-bad': brokerState === 'down' }">{{ brokerLabel }}</b>
          </span>
        </p>
      </div>
    </div>
  </aside>

  <!-- The rail's names, at once and beside the icon. -->
  <Teleport to="body">
    <div v-if="tip" class="rail-tip" role="tooltip" :style="{ left: `${tip.x}px`, top: `${tip.y}px` }">
      {{ tip.text }}<span v-if="tip.note" class="rail-tip-note">{{ tip.note }}</span>
    </div>
  </Teleport>
</template>
