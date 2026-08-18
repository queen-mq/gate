<script setup>
import Tooltip from './Tooltip.vue'

/*
  Label, explanation and the sentence that says what is wrong with this
  particular input, in one place — so no control in the editor can end up with
  a tooltip and no error slot, or an error that lands at the top of the page
  instead of next to the field it came from.

  The error is rendered warm rather than red only when it is a warning; a
  refusal is a refusal. Nothing here ever describes a lane refusing work — that
  is the limiter succeeding, and it has no business in a validation colour.
*/
defineProps({
  label: String,
  help: String,
  error: { type: String, default: null },
  hint: { type: String, default: null },
  // The control's id, so the <label> actually points at it.
  for: { type: String, default: null },
})
</script>

<template>
  <div class="min-w-0">
    <label v-if="label" class="label flex items-center gap-1.5" :for="$props.for">
      <span>{{ label }}</span>
      <Tooltip v-if="help" :text="help" />
    </label>
    <slot />
    <p v-if="error" class="mt-1.5 text-[11.5px] leading-snug text-bad">{{ error }}</p>
    <p v-else-if="hint" class="mt-1.5 text-[11.5px] leading-snug text-fg-3">{{ hint }}</p>
  </div>
</template>
