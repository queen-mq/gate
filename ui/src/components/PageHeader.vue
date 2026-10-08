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
  <header class="mb-6">
    <nav v-if="crumbs.length" class="flex items-center gap-1.5 text-[12px] text-fg-3 mb-3" aria-label="Breadcrumb">
      <template v-for="(c, i) in crumbs" :key="i">
        <RouterLink :to="c.to" class="hover:text-fg transition-colors">{{ c.label }}</RouterLink>
        <Icon name="chevron" :size="11" class="text-fg-3" />
      </template>
    </nav>

    <div class="flex items-start gap-4 flex-wrap">
      <div class="min-w-0 flex-1 basis-[280px]">
        <h1 class="font-semibold tracking-[-0.02em] leading-tight"
            :class="mono ? 'font-mono text-[20px] break-all' : 'text-[22px]'">
          {{ title }}
        </h1>
        <p v-if="sub" class="text-[12.5px] text-fg-3 mt-2 max-w-[76ch] leading-relaxed">{{ sub }}</p>
      </div>
      <div v-if="$slots.actions" class="flex items-center gap-2 flex-wrap pt-0.5 max-w-full">
        <slot name="actions" />
      </div>
    </div>
  </header>
</template>
