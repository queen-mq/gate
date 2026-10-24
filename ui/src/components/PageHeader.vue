<script setup>
import { onUnmounted, watchEffect } from 'vue'
import { pageCrumb } from '../lib/crumbs.js'

/*
  The head of every page, Queen's anatomy (shell.css):

    Title  ·························· live · actions

  The way back is the top bar's crumb, not a row of its own. Gate's pages
  explain themselves, so the explanation follows the head as one quiet line.
*/
const props = defineProps({
  title: String,
  sub: String,
  crumbs: { type: Array, default: () => [] },
  mono: Boolean,
  /** "every 4s", "refresh failed" — only on a page that really polls. */
  live: { type: String, default: '' },
})

watchEffect(() => {
  pageCrumb.value = { title: props.title, parent: props.crumbs.at(-1) ?? null }
})
onUnmounted(() => { pageCrumb.value = null })
</script>

<template>
  <header class="page-head">
    <div class="page-id">
      <h1 :class="{ 'font-mono': mono }">{{ title }}</h1>
    </div>
    <div v-if="live || $slots.actions" class="page-controls">
      <span v-if="live" class="live-tick">
        <span class="live-dot" aria-hidden="true" />
        <span>live · {{ live }}</span>
      </span>
      <slot name="actions" />
    </div>
  </header>
  <p v-if="sub" class="page-lede">{{ sub }}</p>
</template>
