<script setup>
/*
  The screen a signed-out operator lands on.

  It has to exist as a PAGE rather than as a redirect because the console shell
  is deliberately exempt from the session check on the public listener: the SPA
  must be allowed to load so it can ask `/api/me` who it is. The server does
  redirect a browser that asks for HTML on any other path — but the shell is not
  any other path, so without this the console loaded, every fetch answered 401,
  and the operator got a dashboard full of "sign in required" with nowhere to
  click. That is what stage showed the first time it was opened.

  `next` carries the hash route back, so a link to a specific target survives
  the round trip through Google instead of dumping everyone on the overview.
*/
import { computed } from 'vue'
import GateBrand from '../components/GateBrand.vue'
import Icon from '../components/Icon.vue'

defineProps({ dark: Boolean })
const emit = defineEmits(['toggle-theme'])

const loginUrl = computed(() => {
  const here = window.location.hash?.slice(1) || '/'
  // The server's `next` is a PATH it redirects to, and every console route
  // lives under the hash — so what goes back is `/#/targets`, not `/targets`,
  // which would 404 into the shell's own fallback.
  return `/api/auth/google/login?next=${encodeURIComponent('/#' + here)}`
})
</script>

<template>
  <div class="min-h-screen grid place-items-center px-6 py-16 relative">
    <button type="button" class="icon-button absolute right-5 top-4"
            :aria-label="dark ? 'Switch to light theme' : 'Switch to dark theme'"
            :title="dark ? 'Switch to light theme' : 'Switch to dark theme'" @click="emit('toggle-theme')">
      <Icon :name="dark ? 'sun' : 'moon'" :size="16" />
    </button>
    <div class="w-full max-w-[380px] text-center">
      <GateBrand variant="full" class="mx-auto mb-8" />
      <div class="card px-6 py-7 sm:px-8">
        <h1 class="text-[20px] font-semibold tracking-tight">Sign in to Gate</h1>
        <p class="text-[13px] text-fg-2 mt-3 leading-relaxed">
          The ceilings this console shows are the ones being enforced right now.
          Sign in with your work account.
        </p>

        <a
          :href="loginUrl"
          class="btn btn-primary mt-6 h-10 w-full text-[13px]"
        >
          <!-- Google's mark, inlined: the console embeds every asset it serves,
               and a sign-in button that waits on a CDN is a sign-in button that
               is sometimes blank. -->
          <svg width="16" height="16" viewBox="0 0 48 48" aria-hidden="true">
            <path fill="#EA4335" d="M24 9.5c3.5 0 6.6 1.2 9 3.6l6.7-6.7C35.6 2.6 30.2 0 24 0 14.6 0 6.5 5.4 2.6 13.2l7.8 6.1C12.3 13.2 17.7 9.5 24 9.5z"/>
            <path fill="#4285F4" d="M46.1 24.6c0-1.6-.1-3.2-.4-4.6H24v9.1h12.4c-.5 2.9-2.2 5.3-4.6 6.9l7.1 5.5c4.2-3.9 6.6-9.6 6.6-16.9z"/>
            <path fill="#FBBC05" d="M10.4 28.7c-.5-1.4-.8-2.9-.8-4.7s.3-3.3.8-4.7l-7.8-6.1C.9 16.6 0 20.2 0 24s.9 7.4 2.6 10.8l7.8-6.1z"/>
            <path fill="#34A853" d="M24 48c6.2 0 11.5-2 15.3-5.5l-7.1-5.5c-2 1.3-4.6 2.1-8.2 2.1-6.3 0-11.7-3.7-13.6-9.4l-7.8 6.1C6.5 42.6 14.6 48 24 48z"/>
          </svg>
          Continue with Google
        </a>
      </div>

      <!-- The two things worth knowing BEFORE signing in, because the second
           one is otherwise discovered as a 403 on a button that looked live. -->
      <p class="text-[11.5px] text-fg-3 leading-relaxed mt-5 px-2 text-center">
        Access is limited to the domains this deployment allows. Everyone who signs in can read;
        changing a ceiling needs an account on the admin list.
      </p>
    </div>
  </div>
</template>
