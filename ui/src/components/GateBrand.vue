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
  compact: { light: compactLight, dark: compactDark },
  full: { light: fullLight, dark: fullDark },
  symbol: { light: symbolLight, dark: symbolDark },
}
</script>

<template>
  <span class="gate-brand" :class="`gate-brand-${variant}`">
    <img class="gate-brand-light" :src="marks[variant].light" alt="Gate"
         :width="variant === 'symbol' ? 160 : 740" :height="variant === 'symbol' ? 160 : 280" />
    <img class="gate-brand-dark" :src="marks[variant].dark" alt="Gate"
         :width="variant === 'symbol' ? 160 : 740" :height="variant === 'symbol' ? 160 : 280" />
  </span>
</template>
