<script setup>
import Icon from './Icon.vue'
defineProps({
  title: String,
  sub: String,
  crumbs: { type: Array, default: () => [] },
  mono: Boolean,
})
</script>

<template>
  <header class="mb-8">
    <nav v-if="crumbs.length" class="flex items-center gap-1.5 text-[12.5px] text-fg-2 mb-3">
      <template v-for="(c, i) in crumbs" :key="i">
        <RouterLink :to="c.to" class="hover:text-fg transition-colors">{{ c.label }}</RouterLink>
        <Icon name="chevron" :size="11" class="text-fg-3" />
      </template>
    </nav>

    <div class="flex items-start gap-4 flex-wrap">
      <div class="min-w-0">
        <h1 class="font-semibold tracking-[-0.02em] leading-tight"
            :class="mono ? 'font-mono text-[24px]' : 'text-[28px]'">
          {{ title }}
        </h1>
        <p v-if="sub" class="text-[13.5px] text-fg-2 mt-1.5 max-w-[68ch] leading-relaxed">{{ sub }}</p>
      </div>
      <div class="ml-auto flex items-center gap-2 flex-wrap pt-1">
        <slot name="actions" />
      </div>
    </div>
  </header>
</template>
