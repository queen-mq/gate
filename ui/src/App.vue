<script setup>
import { ref, onMounted, computed } from 'vue'
import { useRoute } from 'vue-router'
import Icon from './components/Icon.vue'
import SignIn from './views/SignIn.vue'
import { api, authState, me, fetchMe, isAdmin, LOGOUT_URL, READ_ONLY_NOTE } from './lib/api.js'

const route = useRoute()
const overview = ref(null)
const dark = ref(document.documentElement.classList.contains('dark'))
const mobileNav = ref(false)

/*
  Navigation grouped by intent: "Monitor" is what an operator opens when a
  portal starts refusing, "Configure" what they touch when a vendor changes a
  number. The primary object is the TARGET — one thing that limits us, with its
  budgets and its lanes.
*/
const groups = [
  {
    label: 'Monitor',
    items: [
      { to: '/', label: 'Overview', icon: 'gauge', key: 'overview' },
      { to: '/targets', label: 'Targets', icon: 'target', key: 'targets' },
      { to: '/budgets', label: 'Shared budgets', icon: 'budget', key: 'budgets' },
      { to: '/traces', label: 'Traces', icon: 'trace', key: 'traces' },
    ],
  },
]

function toggleTheme() {
  dark.value = !dark.value
  document.documentElement.classList.toggle('dark', dark.value)
  localStorage.setItem('gate-theme', dark.value ? 'dark' : 'light')
}

async function load() {
  if (authState.value !== 'ready') return
  try {
    overview.value = await api.get('/api/overview')
  } catch {
    overview.value = null
  }
}
onMounted(async () => {
  await fetchMe()
  load()
  setInterval(() => !document.hidden && load(), 15000)
})

const brokerOk = computed(() => overview.value?.queen?.reachable === true)
const activeNav = computed(() => route.meta?.nav)

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
    w.push(`${stale} budget${stale === 1 ? '' : 's'} cite a source older than a year.`)
  return w
})
</script>

<template>
  <div v-if="authState === 'unknown'" class="min-h-screen grid place-items-center">
    <div class="skeleton h-5 w-40" />
  </div>

  <!-- Signed out: the sign-in page INSTEAD of the shell, not inside it. A
       sidebar whose every page answers "sign in required" is a dashboard that
       looks broken rather than closed. -->
  <SignIn v-else-if="authState === 'login'" />

  <div v-else class="min-h-screen">
    <!-- ------------------------------------------------------- sidebar -->
    <aside
      class="fixed inset-y-0 left-0 z-40 w-[248px] bg-bg border-r border-line
             flex flex-col transition-transform duration-200 ease-spring lg:translate-x-0"
      :class="mobileNav ? 'translate-x-0 bg-surface shadow-2xl' : '-translate-x-full'"
    >
      <div class="h-[60px] px-5 flex items-center shrink-0">
        <RouterLink to="/" class="flex items-center gap-2.5" @click="mobileNav = false">
          <span class="w-[22px] h-[22px] rounded-md bg-fg text-bg grid place-items-center
                       shrink-0" aria-hidden="true">
            <!-- Two posts and a bar: a gate, which is what the thing does —
                 it does not slow traffic down, it decides what goes through. -->
            <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                 stroke-width="2.4" stroke-linecap="round">
              <path d="M5 4v16" /><path d="M19 4v16" /><path d="M9.5 12h5" />
            </svg>
          </span>
          <span class="font-semibold tracking-tight text-[14.5px]">Gate</span>
        </RouterLink>
      </div>

      <nav class="flex-1 px-3 pt-2 pb-4 overflow-y-auto">
        <div v-for="g in groups" :key="g.label" class="mb-6">
          <div class="px-2.5 mb-1.5 text-[10.5px] font-semibold uppercase tracking-[0.08em] text-fg-3">
            {{ g.label }}
          </div>
          <RouterLink
            v-for="item in g.items" :key="item.key" :to="item.to"
            class="flex items-center gap-2.5 h-[34px] px-2.5 rounded-lg text-[13.5px]
                   transition-colors duration-100 mb-px"
            :class="activeNav === item.key
              ? 'bg-surface-2 text-fg font-medium'
              : 'text-fg-2 hover:text-fg hover:bg-surface-2'"
            @click="mobileNav = false"
          >
            <Icon :name="item.icon" :size="15.5"
                  :class="activeNav === item.key ? 'text-fg' : 'text-fg-3'" />
            {{ item.label }}
          </RouterLink>
        </div>
      </nav>

      <div class="px-5 py-4 border-t border-line space-y-3 shrink-0">
        <p v-for="w in warnings" :key="w"
           class="flex gap-1.5 text-[11px] leading-snug text-warn">
          <Icon name="alert" :size="12" class="mt-px shrink-0" />{{ w }}
        </p>

        <!-- Who is signed in, what they may do, and the way out. The role is
             not decoration: it decides whether every editor in this console is
             enabled, so it belongs where the identity is and not buried on the
             page that refuses to save. -->
        <div v-if="me" class="space-y-2">
          <div class="flex items-center gap-2.5 min-w-0">
            <span class="w-[26px] h-[26px] rounded-full bg-surface-2 border border-line grid
                         place-items-center text-[11px] font-semibold uppercase shrink-0">
              {{ (me.email || me.actor || '?')[0] }}
            </span>
            <span class="min-w-0 flex-1 leading-tight">
              <span class="block text-[12px] font-medium truncate" :title="me.email || me.actor">
                {{ me.email || me.actor }}
              </span>
              <span class="block text-[11px] text-fg-3 truncate">{{ me.role || 'unknown role' }}</span>
            </span>
            <a v-if="me.email" :href="LOGOUT_URL" class="w-7 h-7 grid place-items-center rounded-md
                      text-fg-3 hover:text-fg hover:bg-surface-2 transition-colors" title="Sign out">
              <Icon name="x" :size="13" />
            </a>
          </div>
          <p v-if="!isAdmin" class="text-[11px] leading-snug text-fg-3">{{ READ_ONLY_NOTE }}</p>
        </div>

        <div class="flex items-center justify-between">
          <span class="flex items-center gap-1.5 font-mono text-[11px] text-fg-3"
                :title="overview?.queen?.url">
            <span class="w-[6px] h-[6px] rounded-full" :class="brokerOk ? 'bg-good' : 'bg-bad'" />
            {{ brokerOk ? `Queen ${overview?.queen?.version ?? ''}` : 'broker unreachable' }}
          </span>
          <button class="w-7 h-7 grid place-items-center rounded-md text-fg-3
                         hover:text-fg hover:bg-surface-2 transition-colors"
                  :title="dark ? 'Switch to light' : 'Switch to dark'" @click="toggleTheme">
            <Icon :name="dark ? 'sun' : 'moon'" :size="14" />
          </button>
        </div>
      </div>
    </aside>

    <div v-if="mobileNav" class="fixed inset-0 z-30 bg-black/40 lg:hidden" @click="mobileNav = false" />

    <!-- --------------------------------------------------------- page -->
    <div class="lg:pl-[248px]">
      <header class="lg:hidden h-[52px] px-4 flex items-center gap-3 border-b border-line bg-bg
                     sticky top-0 z-20">
        <button class="btn btn-sm" @click="mobileNav = true">Menu</button>
        <span class="font-semibold tracking-tight text-[14px]">Gate</span>
      </header>

      <main class="px-6 lg:px-12 py-10">
        <div class="max-w-[1120px] mx-auto">
          <RouterView v-slot="{ Component }">
            <component :is="Component" class="animate-in" />
          </RouterView>
        </div>
      </main>
    </div>
  </div>
</template>
