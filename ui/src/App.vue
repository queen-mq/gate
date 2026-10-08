<script setup>
import { ref, onMounted, onUnmounted, computed, watch, nextTick } from 'vue'
import { useRoute } from 'vue-router'
import Icon from './components/Icon.vue'
import GateBrand from './components/GateBrand.vue'
import SignIn from './views/SignIn.vue'
import { api, authState, authError, me, fetchMe, isAdmin, READ_ONLY_NOTE } from './lib/api.js'
import { usePoll } from './lib/poll.js'

const route = useRoute()
const overview = ref(null)
const overviewError = ref(false)
const dark = ref(document.documentElement.classList.contains('dark'))
const mobileNav = ref(false)
const drawerQuery = window.matchMedia('(max-width: 1023px)')
const mobileViewport = ref(drawerQuery.matches)
const rail = ref(false)
try { rail.value = localStorage.getItem('gate-sidebar-rail') === 'true' } catch {}
const signingOut = ref(false)
const signOutError = ref('')

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

function toggleTheme() {
  dark.value = !dark.value
  document.documentElement.classList.toggle('dark', dark.value)
  try { localStorage.setItem('gate-theme', dark.value ? 'dark' : 'light') } catch {}
}

function toggleRail() {
  rail.value = !rail.value
  try { localStorage.setItem('gate-sidebar-rail', String(rail.value)) } catch {}
}

function onKey(e) {
  if (e.key === 'Escape' && mobileNav.value) {
    mobileNav.value = false
  }
}
function onViewportChange(e) {
  mobileViewport.value = e.matches
  mobileNav.value = false
}
watch(() => route.fullPath, () => { mobileNav.value = false })
watch(mobileNav, async (open) => {
  document.body.style.overflow = open ? 'hidden' : ''
  await nextTick()
  if (open && mobileNav.value) document.querySelector('#gate-navigation button')?.focus()
  else if (!open) document.querySelector('[aria-controls="gate-navigation"]')?.focus()
})
watch(authState, (state) => {
  if (state !== 'ready') {
    mobileNav.value = false
    overview.value = null
    overviewError.value = false
  }
})

async function signOut() {
  if (signingOut.value) return
  signingOut.value = true
  signOutError.value = ''
  try {
    await api.post('/api/auth/logout', null)
    window.location.assign('/')
  } catch (e) {
    // A 401 already switches the shell to SignIn. Other failures leave the
    // current session intact and should be visible rather than becoming an
    // unhandled event promise in the console.
    if (authState.value !== 'login') {
      signOutError.value = e.message
      if (rail.value) toggleRail()
    }
  } finally {
    signingOut.value = false
  }
}

async function load() {
  if (authState.value !== 'ready') return
  try {
    overview.value = await api.get('/api/overview')
    overviewError.value = false
  } catch {
    overview.value = null
    overviewError.value = true
  }
}

async function retryAuth() {
  authState.value = 'unknown'
  await fetchMe()
  load()
}

const refresh = usePoll(load, 15000)
onMounted(async () => {
  document.addEventListener('keydown', onKey)
  drawerQuery.addEventListener('change', onViewportChange)
  await fetchMe()
  refresh()
})
onUnmounted(() => {
  document.removeEventListener('keydown', onKey)
  drawerQuery.removeEventListener('change', onViewportChange)
  document.body.style.overflow = ''
})

const activeNav = computed(() => route.meta?.nav)
const currentPage = computed(() => groups.flatMap((g) => g.items).find((i) => i.key === activeNav.value)?.label || 'Console')
const brokerState = computed(() => {
  if (!overview.value) return overviewError.value ? 'unknown' : 'checking'
  return overview.value.queen?.reachable === true ? 'connected' : 'down'
})
const brokerLabel = computed(() => ({
  checking: 'Checking broker',
  unknown: 'Broker status unknown',
  connected: `Queen ${overview.value?.queen?.version ?? ''}`.trim(),
  down: 'Broker unreachable',
}[brokerState.value]))

/*
  The sidebar carries the two warnings an operator must never have to go
  looking for, because both mean the numbers on every other page are softer
  than they look.
*/
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
</script>

<template>
  <div v-if="authState === 'unknown'" class="min-h-screen flex flex-col items-center justify-center gap-5" role="status">
    <GateBrand variant="symbol" />
    <span class="text-xs text-fg-3">Loading console…</span>
  </div>

  <SignIn v-else-if="authState === 'login'" :dark="dark" @toggle-theme="toggleTheme" />

  <div v-else-if="authState === 'error'" class="min-h-screen grid place-items-center px-6">
    <div class="card w-full max-w-[440px] px-7 py-8 text-center">
      <GateBrand variant="symbol" class="mx-auto mb-6" />
      <h1 class="text-[20px] font-semibold tracking-tight">Console unavailable</h1>
      <p class="text-[13px] text-fg-2 mt-2 leading-relaxed">
        Gate could not establish whether this session is signed in.
      </p>
      <p class="mt-4 px-3 py-2.5 rounded-md bg-bad-dim text-[12px] text-bad break-words" role="alert">
        {{ authError }}
      </p>
      <button type="button" class="btn btn-primary mt-6 mx-auto" @click="retryAuth">Try again</button>
    </div>
  </div>

  <div v-else class="console-shell min-h-screen" :class="{ 'is-rail': rail }">
    <aside id="gate-navigation" class="gate-sidebar" :class="{ 'is-open': mobileNav, 'is-rail': rail }"
           :inert="mobileViewport && !mobileNav">
      <div class="sidebar-brand">
        <RouterLink to="/" aria-label="Gate overview" @click="mobileNav = false">
          <GateBrand class="brand-wordmark" />
          <GateBrand variant="symbol" class="brand-symbol" />
        </RouterLink>
        <button type="button" class="icon-button lg:hidden" aria-label="Close navigation" @click="mobileNav = false">
          <Icon name="x" :size="16" />
        </button>
      </div>

      <nav class="sidebar-nav" aria-label="Main navigation">
        <div v-for="(g, index) in groups" :key="index" class="nav-group">
          <div v-if="g.label" class="nav-label">{{ g.label }}</div>
          <RouterLink v-for="item in g.items" :key="item.key" :to="item.to"
                      class="nav-link" :class="{ 'is-active': activeNav === item.key }"
                      :aria-current="activeNav === item.key ? 'page' : undefined"
                      :aria-label="item.label" :title="rail ? item.label : undefined"
                      @click="mobileNav = false">
            <Icon :name="item.icon" :size="16" />
            <span class="nav-name">{{ item.label }}</span>
          </RouterLink>
        </div>
      </nav>

      <div class="sidebar-rail-foot">
        <RouterLink v-if="warnings.length" to="/" class="icon-button text-warn"
                    :title="warnings.join(' ')" :aria-label="warnings.join(' ')">
          <Icon name="alert" :size="16" />
        </RouterLink>
        <span v-if="me" class="icon-button text-[11px] font-medium" role="img"
              :title="`${me.role || 'unknown role'} · ${me.email || me.actor}`"
              :aria-label="`${me.role || 'unknown role'} · ${me.email || me.actor}`">
          {{ isAdmin ? 'A' : 'R' }}
        </span>
        <button v-if="me?.actor === 'google'" type="button" class="icon-button"
                :disabled="signingOut" title="Sign out" aria-label="Sign out" @click="signOut">
          <Icon name="logout" :size="16" />
        </button>
      </div>

      <div class="sidebar-foot space-y-3">
        <p v-for="w in warnings" :key="w" class="flex gap-2 text-[11px] leading-relaxed text-warn">
          <Icon name="alert" :size="12" class="mt-0.5 shrink-0" />{{ w }}
        </p>
        <div v-if="me" class="space-y-2">
          <div class="flex items-center justify-between gap-2 text-[11px]">
            <span class="text-fg-2 capitalize">{{ me.role || 'unknown role' }}</span>
            <button v-if="me.actor === 'google'" type="button" :disabled="signingOut"
                    class="text-fg-3 hover:text-fg transition-colors disabled:opacity-50"
                    @click="signOut">{{ signingOut ? 'Signing out…' : 'Sign out' }}</button>
          </div>
          <p class="text-[12px] text-fg-3 truncate" :title="me.email || me.actor">{{ me.email || me.actor }}</p>
          <p v-if="signOutError" class="text-[11px] leading-snug text-bad" role="alert">
            Could not sign out: {{ signOutError }}
          </p>
          <p v-if="!isAdmin" class="text-[11px] leading-relaxed text-fg-3">{{ READ_ONLY_NOTE }}</p>
        </div>
      </div>
    </aside>

    <div v-if="mobileNav" class="nav-scrim lg:hidden" aria-hidden="true" @click="mobileNav = false" />

    <div class="console-page" :inert="mobileNav">
      <header class="console-topbar">
        <button type="button" class="icon-button lg:hidden" aria-label="Open navigation"
                aria-controls="gate-navigation" :aria-expanded="mobileNav" @click="mobileNav = !mobileNav">
          <Icon name="menu" :size="17" />
        </button>
        <GateBrand variant="symbol" class="lg:hidden w-6" />
        <button type="button" class="icon-button hidden lg:inline-grid"
                :aria-label="rail ? 'Expand sidebar' : 'Collapse sidebar'"
                :title="rail ? 'Expand sidebar' : 'Collapse sidebar'" :aria-expanded="!rail"
                aria-controls="gate-navigation" @click="toggleRail">
          <Icon name="sidebar" :size="17" />
        </button>
        <div class="flex items-center gap-2 text-[12px] min-w-0">
          <span class="text-fg-3 hidden sm:inline">Console</span>
          <Icon name="chevron" :size="10" class="text-fg-3 hidden sm:block" />
          <span class="text-fg-2 truncate">{{ currentPage }}</span>
        </div>
        <div class="ml-auto flex items-center gap-4">
          <span class="flex items-center gap-2 text-[11px] text-fg-3" :title="overview?.queen?.url"
                role="status">
            <span class="status-glyph" :class="brokerState === 'connected' ? 'good' : brokerState === 'down' ? 'bad' : 'muted'" />
            <span class="sr-only sm:not-sr-only" :class="brokerState === 'down' ? 'text-bad' : ''">{{ brokerLabel }}</span>
          </span>
          <button type="button" class="icon-button"
                  :aria-label="dark ? 'Switch to light theme' : 'Switch to dark theme'"
                  :title="dark ? 'Switch to light theme' : 'Switch to dark theme'" @click="toggleTheme">
            <Icon :name="dark ? 'sun' : 'moon'" :size="16" />
          </button>
        </div>
      </header>

      <main class="console-main">
        <div class="console-content">
          <RouterView v-slot="{ Component }">
            <component :is="Component" class="animate-in" />
          </RouterView>
        </div>
      </main>
    </div>
  </div>
</template>
