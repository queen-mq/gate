<template>
  <!-- The session: who is signed in, and the way out. The last thing on the top
       bar, where an account lives in every console (Queen's UserMenu.vue). -->
  <div v-if="me" ref="root" class="user-picker" @focusout="onFocusOut" @keydown.escape.stop.prevent="closeMenu(true)">
    <button
      ref="trigger"
      type="button"
      class="top-btn user-trigger"
      :class="{ active: open }"
      :title="`Signed in as ${who}`"
      :aria-label="`Account: ${who}`"
      aria-haspopup="menu"
      aria-controls="user-menu"
      :aria-expanded="open"
      @click="open ? closeMenu(true) : openMenu()"
      @keydown.down.prevent="openMenu()"
    >
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" aria-hidden="true">
        <circle cx="12" cy="8.5" r="3.6" />
        <path stroke-linecap="round" d="M4.8 20c.6-3.7 3.6-6 7.2-6s6.6 2.3 7.2 6" />
      </svg>
      <span class="user-name">{{ name }}</span>
    </button>

    <div v-if="open" id="user-menu" class="user-menu" role="menu" aria-label="Account">
      <div class="user-who">
        <span>Signed in as</span>
        <b :title="who">{{ who }}</b>
        <span>{{ isAdmin ? me.role : READ_ONLY_NOTE }}</span>
        <span v-if="signOutError" class="is-bad" role="alert">Could not sign out: {{ signOutError }}</span>
      </div>
      <!-- Only a Google session has a way out; any other actor is the deployment's. -->
      <button v-if="me.actor === 'google'" ref="out" type="button" class="user-item" role="menuitem" :disabled="signingOut" @click="signOut">
        {{ signingOut ? 'Signing out…' : 'Sign out' }}
      </button>
    </div>
  </div>
</template>

<script setup>
import { computed, nextTick, onMounted, onUnmounted, ref } from 'vue'
import { api, authState, me, isAdmin, READ_ONLY_NOTE } from '../lib/api.js'

const who = computed(() => me.value?.email || me.value?.actor || 'signed in')
// On the bar, the part of the address a person goes by; the menu has all of it.
const name = computed(() => (me.value?.email ? me.value.email.split('@')[0] : 'Account'))

const root = ref(null)
const trigger = ref(null)
const out = ref(null)
const open = ref(false)

async function openMenu() {
  open.value = true
  await nextTick()
  if (open.value) out.value?.focus()
}

function closeMenu(restoreFocus = false) {
  open.value = false
  if (restoreFocus) trigger.value?.focus()
}

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

function onFocusOut(event) {
  if (!root.value?.contains(event.relatedTarget)) closeMenu()
}

function onPointerDown(event) {
  if (open.value && !root.value?.contains(event.target)) closeMenu()
}

onMounted(() => document.addEventListener('pointerdown', onPointerDown))
onUnmounted(() => document.removeEventListener('pointerdown', onPointerDown))
</script>
