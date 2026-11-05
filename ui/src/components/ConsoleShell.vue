<script setup>
// The frame of the console, as Queen's (queen-mq/queen: app/src/console/ConsoleShell.vue): a
// 64 px icon rail (the sections and your account) and one band (where you are, the broker, the
// AI agent, Refresh, the theme, the shortcuts). A page fills the rest.
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import GateBrand from './GateBrand.vue'
import Icon from './Icon.vue'
import LookChip from './LookChip.vue'
import QcPopover from './QcPopover.vue'
import { api, authState, me, isAdmin, READ_ONLY_NOTE } from '../lib/api.js'
import { aiPanel, openAiAgent } from '../lib/ai-session.js'
import { pageCrumb } from '../lib/crumbs.js'
import { refreshAll, usePoll } from '../lib/poll.js'

const route = useRoute()
const router = useRouter()
const clock = (ms) => new Intl.DateTimeFormat('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit' }).format(ms)

/*
  The sections, grouped like Queen's console. The primary object is the GRAPH — one document,
  its nodes, its budgets and the paths that cross them. "Targets" is the same list in the older
  vocabulary, because a target is a one-node graph and a link in a runbook does not stop
  existing when a model changes.
*/
const RAIL = [
  [{ id: 'overview', label: 'Overview', icon: 'gauge', key: 'O', to: '/' }],
  [
    { id: 'targets', label: 'Targets', icon: 'target', key: 'T', to: '/targets' },
    { id: 'graphs', label: 'Graphs', icon: 'graph', key: 'G', to: '/graphs' },
    { id: 'budgets', label: 'Shared budgets', icon: 'budget', key: 'B', to: '/budgets' },
  ],
  [{ id: 'traces', label: 'Traces', icon: 'trace', key: 'R', to: '/traces' }],
]
const here = computed(() => route.meta?.nav || null)

// ---------------------------------------------------------------------------
// The broker, and the two warnings an operator must never have to go looking for, because
// both mean the numbers on every other page are softer than they look. Not yet known is not
// "down". They mark the Overview icon, as what needs you marks Queen's.
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
const brokerLabel = computed(() => ({
  checking: 'Checking the broker',
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
// The Overview's mark: red when the broker is down, amber when a budget is softer than it looks.
const mark = computed(() => (brokerState.value === 'down' ? 'red' : warnings.value.length ? 'amber' : null))
const overviewNote = computed(() => (brokerState.value === 'down' ? 'Broker unreachable' : warnings.value.length ? warnings.value.join(' ') : ''))

// ---- the band: where you are; a page below a list names itself, its list one click back ----
const path = computed(() => {
  const title = pageCrumb.value?.title || route.meta.title || 'Gate'
  const parent = pageCrumb.value?.parent ?? null
  return parent ? [{ label: parent.label, to: parent.to }, { label: title }] : [{ label: title }]
})

// ---- Refresh now: every poll on screen at once ----
const refreshing = ref(false)
const refreshedAt = ref(0)
async function refresh() {
  if (refreshing.value) return
  refreshing.value = true
  const t0 = Date.now()
  try { await refreshAll() } catch { /* each read says its own failure */ }
  // long enough to be seen turning, even when the answers are instant
  setTimeout(() => { refreshing.value = false; refreshedAt.value = Date.now() }, Math.max(0, 500 - (Date.now() - t0)))
}
const refreshTitle = computed(() => (refreshedAt.value ? `Refresh now (R). Last refreshed at ${clock(refreshedAt.value)}.` : 'Refresh now (R). Every page also refreshes itself.'))

const askAgent = () => openAiAgent(route.params.app || route.query.app)

// ---- your account ----
const userOpen = ref(false)
const helpOpen = ref(false)
const who = computed(() => me.value?.email || me.value?.actor || 'signed in')
const initials = computed(() => {
  const n = me.value?.email || me.value?.actor || ''
  const parts = n.replace(/@.*/, '').split(/[\s._-]+/).filter(Boolean)
  return (parts[0]?.[0] || '?').toUpperCase() + (parts[1]?.[0] || '').toUpperCase()
})
const signingOut = ref(false)
const signOutError = ref('')
async function signOut() {
  if (signingOut.value) return
  signingOut.value = true
  signOutError.value = ''
  try {
    await api.post('/api/auth/logout', null)
    window.location.assign('/')
  } catch (e) {
    // A 401 already switches the shell to SignIn. Other failures leave the
    // session intact and say so here, in the menu that was used.
    if (authState.value !== 'login') signOutError.value = e.message
  } finally {
    signingOut.value = false
  }
}

// ---- keyboard: G then a letter, R, ? ----
let gAt = 0
function typing(e) {
  const t = e.target
  return t && (t.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName))
}
function onKey(e) {
  if (e.metaKey || e.ctrlKey || e.altKey || typing(e) || e.defaultPrevented || aiPanel.open) return
  const k = e.key.toLowerCase()
  if (gAt && Date.now() - gAt < 1200) {
    gAt = 0
    const item = RAIL.flat().find((i) => i.key.toLowerCase() === k)
    if (item) { e.preventDefault(); router.push(item.to) }
    return
  }
  if (k === 'g') { gAt = Date.now(); return }
  if (k === 'r') { refresh(); return }
  if (e.key === '?') { helpOpen.value = !helpOpen.value }
}
onMounted(() => document.addEventListener('keydown', onKey))
onBeforeUnmount(() => document.removeEventListener('keydown', onKey))
</script>

<template>
  <div class="qn-shell">
    <nav class="qn-rail" aria-label="Sections">
      <RouterLink to="/" class="qn-logo" aria-label="Gate: overview"><GateBrand variant="symbol" class="qn-logo-mark" /></RouterLink>
      <template v-for="(group, gi) in RAIL" :key="gi">
        <div v-if="gi" class="qn-sep" />
        <RouterLink
          v-for="item in group"
          :key="item.id"
          :to="item.to"
          class="qn-ri"
          :class="{ on: here === item.id }"
          :aria-current="here === item.id ? 'page' : undefined"
          :aria-label="item.id === 'overview' && overviewNote ? `Overview: ${overviewNote}` : item.label"
        >
          <Icon :name="item.icon" :size="18" class="qn-ico" />
          <span v-if="item.id === 'overview' && mark" class="qn-ri-mark qn-mark" :class="mark" aria-hidden="true" />
          <span class="qn-tip">
            {{ item.label }}
            <span v-if="item.id === 'overview' && overviewNote" class="qn-t3 qn-tip-note">{{ overviewNote }}</span>
            <span class="qn-kbd">G {{ item.key }}</span>
          </span>
        </RouterLink>
      </template>
      <div style="flex: 1 1 0" />
      <QcPopover v-model:open="userOpen" side="right-end" :width="290" label="Your account">
        <template #trigger="{ toggle }">
          <button type="button" class="qn-ri" aria-haspopup="dialog" :aria-expanded="userOpen" aria-label="Your account" @click="toggle">
            <span class="qn-avatar">{{ initials }}</span>
            <span v-if="!userOpen" class="qn-tip">{{ who }}</span>
          </button>
        </template>
        <div style="padding: 8px 6px 4px">
          <p style="font-weight: 500; overflow-wrap: anywhere">{{ who }}</p>
          <p class="qn-t3" style="font-size: 12px; margin-top: 2px; overflow-wrap: anywhere">{{ isAdmin ? me?.role : READ_ONLY_NOTE }}</p>
          <p v-if="signOutError" class="red" style="font-size: 12px; margin-top: 6px" role="alert">Could not sign out: {{ signOutError }}</p>
          <!-- Only a Google session has a way out; any other actor is the deployment's. -->
          <template v-if="me?.actor === 'google'">
            <div class="qn-popsep" style="margin-top: 12px" />
            <button type="button" class="qn-opt" :disabled="signingOut" @click="signOut">{{ signingOut ? 'Signing out…' : 'Sign out' }}</button>
          </template>
        </div>
      </QcPopover>
    </nav>

    <div class="qn-body">
      <header class="qn-band">
        <nav aria-label="You are here" class="qn-path">
          <template v-for="(p, i) in path" :key="i">
            <span v-if="i" class="qn-t3" aria-hidden="true">/</span>
            <RouterLink v-if="p.to && i < path.length - 1" :to="p.to" class="qn-t2">{{ p.label }}</RouterLink>
            <span v-else :aria-current="i === path.length - 1 ? 'page' : undefined" :title="p.label" style="font-weight: 500">{{ p.label }}</span>
          </template>
        </nav>
        <div class="qn-gap" />
        <div class="qn-tools">
          <!-- The broker, as Queen's band names its cluster. -->
          <span class="qn-chip qn-broker" role="status" :title="overview?.queen?.url || brokerLabel">
            <span v-if="brokerState === 'down'" class="qn-mark red" aria-hidden="true" />
            <span :class="{ 'qn-t3': brokerState !== 'connected' && brokerState !== 'down' }">{{ brokerLabel }}</span>
          </span>
          <button
            type="button" class="qn-chip qn-sq" :class="{ on: aiPanel.open }" title="AI agent" aria-label="AI agent"
            aria-haspopup="dialog" aria-controls="gate-ai-chat" :aria-expanded="aiPanel.open" @click="askAgent"
          ><Icon name="sparkles" :size="16" class="qn-ico" /></button>
          <button
            type="button" class="qn-chip qn-sq qn-refresh" :class="{ busy: refreshing }"
            :aria-busy="refreshing" :aria-label="refreshing ? 'Refreshing' : 'Refresh now'" :title="refreshTitle" @click="refresh"
          >
            <svg class="qn-ico" width="16" height="16" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.7" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182m0-4.991v4.99" /></svg>
          </button>
          <LookChip />
          <QcPopover v-model:open="helpOpen" class="qn-helpwrap" side="bottom-end" :width="300" label="Keyboard shortcuts" :focus-first="false">
            <template #trigger="{ toggle }">
              <button type="button" class="qn-chip qn-sq qn-keys" aria-haspopup="dialog" :aria-expanded="helpOpen" aria-label="Keyboard shortcuts" @click="toggle">?</button>
            </template>
            <div class="qn-help">
              <span>Go to a section</span><span class="qn-kbd">G, then its letter</span>
              <span>Refresh now</span><span class="qn-kbd">R</span>
              <span>Close</span><span class="qn-kbd">Esc</span>
            </div>
          </QcPopover>
        </div>
      </header>
      <p v-if="brokerState === 'down'" class="qn-banner" role="alert">
        <span class="qn-mark red" aria-hidden="true" />
        <span><strong style="font-weight: 500">Gate cannot reach the broker.</strong> The numbers here may be from before it stopped answering.</span>
      </p>
      <main class="qn-page">
        <slot />
      </main>
    </div>
  </div>
</template>

<style scoped>
/* The body does not scroll: the console scrolls inside itself. */
.qn-shell { display: flex; height: 100vh; overflow: hidden; }
.qn-rail {
  height: 100vh; flex: 0 0 var(--rail-w);
  display: flex; flex-direction: column; align-items: center; gap: 4px;
  padding: 0 0 14px; border-right: 1px solid var(--line); z-index: 31;
}
/* The logo's cell is the band's height, so the mark sits on the band's centre line. */
.qn-logo { display: flex; align-items: center; justify-content: center; align-self: stretch; height: 52px; flex: 0 0 52px; margin-bottom: 10px; color: var(--text); }
.qn-logo:hover { text-decoration: none; }
.qn-logo-mark { width: 28px; }
.qn-ri {
  position: relative; display: flex; align-items: center; justify-content: center;
  width: 44px; height: 44px; border-radius: calc(var(--r-ctl) + 2px); color: var(--text2);
}
.qn-ri:hover { background: var(--bg2); color: var(--text); text-decoration: none; }
.qn-ri.on { background: var(--bg3); color: var(--text); }
/* Gate's icons are drawn on a 24-unit grid: at 18 px this stroke is the console's 1.4. */
.qn-ri .qn-ico { stroke-width: 1.85; }
.qn-ri-mark { position: absolute; top: 9px; right: 9px; width: 7px; height: 7px; }
.qn-sep { width: 24px; height: 1px; background: var(--line); margin: 6px 0; }
:global(.qn.tone .qn-sep) { background: var(--bg3); }
.qn-tip {
  position: absolute; left: 54px; top: 50%; transform: translateY(-50%);
  display: flex; align-items: center; gap: 10px; white-space: nowrap;
  padding: 5px 8px; border: 1px solid var(--line); border-radius: var(--r-ctl);
  background: var(--pop); color: var(--text); font-size: 12px;
  opacity: 0; pointer-events: none; z-index: 60;
}
.qn-tip-note { max-width: 320px; white-space: normal; }
.qn-ri:hover .qn-tip, .qn-ri:focus-visible .qn-tip { opacity: 1; }
:global(.qn.tone .qn-tip) { border-color: transparent; }
.qn-avatar { width: 30px; height: 30px; border-radius: 50%; background: var(--bg3); display: flex; align-items: center; justify-content: center; font-size: 11px; font-weight: 500; color: var(--text2); }
.qn-body { flex: 1 1 0; min-width: 0; height: 100vh; overflow-y: auto; display: flex; flex-direction: column; }
.qn-band {
  position: sticky; top: 0; z-index: 25; background: var(--bg); flex: 0 0 auto;
  display: flex; align-items: center; flex-wrap: wrap; gap: 10px 10px;
  min-height: 52px; padding: 8px 24px; border-bottom: 1px solid var(--line);
}
.qn-path { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; min-width: 0; }
.qn-path > span[aria-current] { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 48ch; }
.qn-gap { flex: 1 1 0; }
.qn-tools { display: flex; align-items: center; gap: 10px; }
.qn-broker { cursor: default; }
.qn-broker:hover { background: transparent; }
:global(.qn.tone .qn-broker:hover) { background: var(--ctl); }
.qn-sq { width: 32px; justify-content: center; padding: 0; }
.qn-sq .qn-ico { stroke-width: 1.7; }
.qn-refresh.busy .qn-ico { animation: qn-turn .8s linear infinite; }
@keyframes qn-turn { to { transform: rotate(360deg); } }
@media (prefers-reduced-motion: reduce) { .qn-refresh.busy .qn-ico { animation: none; opacity: .45; } }
.qn-help { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 8px 16px; padding: 8px 8px 6px; }
.qn-page { flex: 1 1 auto; min-width: 0; display: flex; flex-direction: column; }
.qn-page > :deep(*) { width: 100%; }
.qn-banner { flex: 0 0 auto; display: flex; align-items: center; gap: 10px; padding: 9px 24px; border-bottom: 1px solid var(--line); background: var(--bg2); }
@media (max-width: 1180px) { .qn-broker { display: none; } }
@media (max-width: 640px) {
  .qn-shell { flex-direction: column; }
  .qn-rail { height: auto; flex: 0 0 auto; flex-direction: row; overflow-x: auto; padding: 6px 8px; border-right: 0; border-bottom: 1px solid var(--line); }
  .qn-body { height: auto; flex: 1 1 0; min-height: 0; }
  /* The rail is a top bar of icons that share the width. The logo goes (the
     Overview icon is the way home). */
  .qn-rail { gap: 2px; }
  .qn-logo { display: none; }
  .qn-ri { width: auto; height: 40px; flex: 0 1 40px; min-width: 28px; }
  .qn-sep { width: 1px; height: 24px; margin: 0 2px; flex: 0 0 1px; }
  .qn-tip { display: none; }
  /* Two rows, the same on every page: where you are, then the tools. A long name is cut,
     never pushed out. */
  .qn-band { position: static; padding: 8px 16px; gap: 8px; }
  .qn-path { flex: 1 1 0; flex-wrap: nowrap; overflow: hidden; }
  .qn-path > * { flex: 0 1 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .qn-path > [aria-hidden] { flex: none; }
  .qn-gap { display: none; }
  .qn-tools { flex: 1 1 100%; min-width: 0; gap: 8px; }
  /* keyboard shortcuts: nothing to press on a phone */
  .qn-helpwrap { display: none; }
}
</style>
