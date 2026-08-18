<script setup>
import { onMounted, onUnmounted, ref } from 'vue'
import Icon from './Icon.vue'

defineProps({
  title: String,
  hint: String,
  confirm: { type: String, default: 'Save' },
  danger: Boolean,
  busy: Boolean,
})
const emit = defineEmits(['close', 'submit'])
const panel = ref(null)

function onKey(e) {
  if (e.key === 'Escape') emit('close')
}
onMounted(() => {
  document.addEventListener('keydown', onKey)
  panel.value?.querySelector('input, select, textarea')?.focus()
})
onUnmounted(() => document.removeEventListener('keydown', onKey))
</script>

<template>
  <!-- Teleported to <body> because the view wrapper animates a transform, and
       an ancestor with a transform becomes the containing block for
       position:fixed — the overlay would centre on the (tall) content column
       instead of the viewport, pushing the modal's head off screen. -->
  <Teleport to="body">
  <div class="fixed inset-0 z-50 grid place-items-center p-5 bg-black/50" @click.self="emit('close')">
    <form
      ref="panel"
      class="w-full max-w-[460px] max-h-[88vh] overflow-y-auto bg-surface border border-line
             rounded-2xl shadow-2xl animate-in"
      @submit.prevent="emit('submit')"
    >
      <header class="flex items-start justify-between px-6 pt-5 pb-0">
        <div>
          <h3 class="text-[16px] font-semibold tracking-tight">{{ title }}</h3>
          <p v-if="hint" class="hint mt-1.5 max-w-[62ch]">{{ hint }}</p>
        </div>
        <button type="button" class="btn btn-sm -mr-1.5 border-transparent" aria-label="Close"
                @click="emit('close')">
          <Icon name="x" :size="14" />
        </button>
      </header>

      <div class="px-6 py-5 space-y-4">
        <slot />
      </div>

      <!-- Sticky because the panel itself scrolls: a long body would otherwise
           push Cancel and Confirm off the bottom, and a dialog whose only exit
           is the Escape key is a trap. -->
      <footer class="sticky bottom-0 flex justify-end gap-2 px-6 py-4 border-t border-line bg-surface">
        <button type="button" class="btn" @click="emit('close')">Cancel</button>
        <button type="submit" class="btn" :class="danger ? 'btn-danger' : 'btn-primary'" :disabled="busy">
          {{ busy ? 'Working…' : confirm }}
        </button>
      </footer>
    </form>
  </div>
  </Teleport>
</template>
