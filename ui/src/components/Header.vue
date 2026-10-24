<script setup>
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import Icon from './Icon.vue'
import ThemeMenu from './ThemeMenu.vue'
import UserMenu from './UserMenu.vue'
import { aiPanel, openAiAgent } from '../lib/ai-session.js'
import { pageCrumb } from '../lib/crumbs.js'
import { refreshAll } from '../lib/poll.js'
import { collapsed, toggleSidebar, wide } from '../lib/sidebar.js'

const theme = defineModel('theme', { type: String, required: true })
const route = useRoute()

// A page below a list names itself; its list is one click back.
const pageTitle = computed(() => pageCrumb.value?.title || route.meta.title || 'Gate')
const parent = computed(() => pageCrumb.value?.parent ?? null)

const handleKeydown = (e) => {
  if ((e.metaKey || e.ctrlKey) && e.key === '\\' && wide.value) {
    e.preventDefault()
    toggleSidebar()
  }
}
// Every poll on screen at once, as Queen's refresh button does.
const isRefreshing = ref(false)
const handleRefresh = async () => {
  isRefreshing.value = true
  await refreshAll()
  setTimeout(() => { isRefreshing.value = false }, 500)
}

onMounted(() => document.addEventListener('keydown', handleKeydown))
onUnmounted(() => document.removeEventListener('keydown', handleKeydown))
</script>

<template>
  <div class="topbar-wrap">
    <header class="topbar">
      <!-- The page on the left; the AI agent, the theme, refresh and the account
           on the right, in Queen's order. -->
      <div class="topbar-side">
        <!-- The sidebar's width: the full column or its rail of icons. A wide
             screen only; below that the sidebar is a drawer with its own button
             in this same corner. -->
        <button
          type="button"
          class="top-btn nav-toggle"
          aria-controls="gate-navigation"
          :title="collapsed ? 'Expand sidebar (⌘\\)' : 'Collapse sidebar (⌘\\)'"
          :aria-label="collapsed ? 'Expand sidebar' : 'Collapse sidebar'"
          :aria-expanded="!collapsed"
          @click="toggleSidebar()"
        >
          <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
            <rect x="3" y="4.5" width="18" height="15" rx="2" />
            <path d="M9 4.5v15" />
          </svg>
        </button>

        <div class="crumbs">
          <template v-if="parent">
            <RouterLink class="crumb-link" :to="parent.to">{{ parent.label }}</RouterLink>
            <span class="sep">/</span>
          </template>
          <span class="here" :title="pageTitle">{{ pageTitle }}</span>
        </div>
      </div>

      <div class="topbar-side topbar-end">
        <button type="button" class="top-btn" :class="{ active: aiPanel.open }" title="AI agent" aria-label="AI agent"
                aria-haspopup="dialog" aria-controls="gate-ai-chat" :aria-expanded="aiPanel.open"
                @click="openAiAgent(route.params.app || route.query.app)">
          <Icon name="sparkles" />
        </button>

        <ThemeMenu v-model="theme" />

        <button type="button" class="top-btn" :disabled="isRefreshing" title="Refresh" aria-label="Refresh" @click="handleRefresh">
          <svg :class="{ 'animate-spin': isRefreshing }" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.6" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0l3.181 3.183a8.25 8.25 0 0013.803-3.7M4.031 9.865a8.25 8.25 0 0113.803-3.7l3.181 3.182m0-4.991v4.99" /></svg>
        </button>

        <UserMenu />
      </div>
    </header>
  </div>
</template>
