<script setup>
import compactLight from '../assets/brand/gate-logo-compatto-nero.svg'
import compactDark from '../assets/brand/gate-logo-compatto-bianco.svg'
import fullLight from '../assets/brand/gate-logo-nero.svg'
import fullDark from '../assets/brand/gate-logo-bianco.svg'
import symbolLight from '../assets/brand/gate-simbolo-nero.svg'
import symbolDark from '../assets/brand/gate-simbolo-bianco.svg'

defineProps({
  variant: { type: String, default: 'compact' },
})

const marks = {
  compact: { light: compactLight, dark: compactDark, width: 538, height: 268 },
  full: { light: fullLight, dark: fullDark, width: 1515, height: 775 },
  symbol: { light: symbolLight, dark: symbolDark, width: 160, height: 160 },
}
</script>

<template>
  <span class="gate-brand" :class="`gate-brand-${variant}`">
    <img class="gate-brand-light" :src="marks[variant].light" alt="Gate"
         :width="marks[variant].width" :height="marks[variant].height" />
    <img class="gate-brand-dark" :src="marks[variant].dark" alt="Gate"
         :width="marks[variant].width" :height="marks[variant].height" />
  </span>
</template>
