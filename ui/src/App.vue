<script setup>
import { onMounted, onUnmounted, watch } from 'vue'
import ConsoleShell from './components/ConsoleShell.vue'
import GateBrand from './components/GateBrand.vue'
import SignIn from './views/SignIn.vue'
import AiAgentChat from './components/AiAgentChat.vue'
import { authState, authError, fetchMe } from './lib/api.js'
import { aiPanel, closeAiAgent, resetAiSession } from './lib/ai-session.js'

watch(() => aiPanel.open, (chatOpen) => {
  document.body.style.overflow = chatOpen ? 'hidden' : ''
})
watch(authState, (state) => {
  if (state !== 'ready') {
    closeAiAgent()
    resetAiSession()
  }
})

async function retryAuth() {
  authState.value = 'unknown'
  await fetchMe()
}

onMounted(fetchMe)
onUnmounted(() => { document.body.style.overflow = '' })
</script>

<template>
  <!-- Boot gate: nothing renders until /api/me says who this is. -->
  <div v-if="authState === 'unknown'" class="boot-screen" role="status">
    <span class="boot-mark" aria-hidden="true"><GateBrand variant="symbol" /></span>
    <p>Starting session…</p>
  </div>

  <SignIn v-else-if="authState === 'login'" />

  <div v-else-if="authState === 'error'" class="boot-screen boot-error">
    <span class="boot-mark" aria-hidden="true"><GateBrand variant="symbol" /></span>
    <h2>Cannot start the console</h2>
    <p>Gate could not establish whether this session is signed in.</p>
    <p class="text-bad break-words" role="alert">{{ authError }}</p>
    <div class="boot-actions">
      <button type="button" class="btn" @click="retryAuth">Try again</button>
    </div>
  </div>

  <!-- Every page is drawn in the console's frame: the rail and the band (ConsoleShell). -->
  <template v-else>
    <ConsoleShell>
      <!-- The Overview takes the dashboard container, as in Queen and Traceflow. -->
      <div class="view-container" :class="{ dash: $route.meta.nav === 'overview' }">
        <RouterView v-slot="{ Component }">
          <component :is="Component" class="animate-in" />
        </RouterView>
      </div>
    </ConsoleShell>
    <AiAgentChat />
  </template>
</template>
