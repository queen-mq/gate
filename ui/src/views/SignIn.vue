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

const loginUrl = computed(() => {
  const here = window.location.hash?.slice(1) || '/'
  // The server's `next` is a PATH it redirects to, and every console route
  // lives under the hash — so what goes back is `/#/targets`, not `/targets`,
  // which would 404 into the shell's own fallback.
  return `/api/auth/google/login?next=${encodeURIComponent('/#' + here)}`
})
</script>

<template>
  <div class="min-h-screen grid place-items-center px-6">
    <div class="w-full max-w-[380px] text-center">
      <span class="w-11 h-11 rounded-xl bg-fg text-bg grid place-items-center mx-auto"
            aria-hidden="true">
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor"
             stroke-width="2.4" stroke-linecap="round">
          <path d="M5 4v16" /><path d="M19 4v16" /><path d="M9.5 12h5" />
        </svg>
      </span>

      <h1 class="text-[22px] font-semibold tracking-[-0.02em] mt-5">Gate</h1>
      <p class="text-[13.5px] text-fg-2 mt-2 leading-relaxed">
        The ceilings this console shows are the ones being enforced right now.
        Sign in with your work account.
      </p>

      <a
        :href="loginUrl"
        class="mt-7 h-10 px-4 rounded-lg bg-fg text-bg text-[13.5px] font-medium
               inline-flex items-center justify-center gap-2.5 w-full
               hover:opacity-90 transition-opacity"
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

      <!-- The two things worth knowing BEFORE signing in, because the second
           one is otherwise discovered as a 403 on a button that looked live. -->
      <p class="hint mt-5 text-center">
        Access is limited to the domains this deployment allows. Everyone who signs in can read;
        changing a ceiling needs an account on the admin list.
      </p>
    </div>
  </div>
</template>
