<script setup>
import { computed } from 'vue'
import avatarGood from '../assets/status-ok.webp'
import avatarWarn from '../assets/status-warning.webp'
import avatarBad from '../assets/status-critical.webp'

/*
  The one component allowed to speak in colour, and the only place that decides
  what each state looks like — the target list, the lane panel and the overview
  can never disagree about what "pacing" means.

  Dot + word, never a filled pill: a page with forty coloured capsules shouts
  everywhere and says nothing.

  The taxonomy is NOT the relay console's, and the difference is the whole
  point of this product. In a delivery console a retry is a failure. Here a
  DENIAL IS THE JOB: a lane sitting at its cap and refusing work is the system
  succeeding, and painting it red would train an operator to page themselves
  every time the limiter does what it exists to do.

  So the colours track a different question — "are we still in control?":
    flowing   under cap, nothing waiting              good
    pacing    at cap, backlog stable — working        good
    parked    denied, waiting out the lease           muted
    saturating backlog growing faster than the drain  warn
    blind     the cap is an assumption, not a number  warn
    breached  the vendor throttled us anyway          bad  — our numbers are wrong
*/
const props = defineProps({
  state: { type: String, required: true },
  label: { type: String, default: null },
  size: { type: String, default: 'md' }, // md | lg
  /* Opt-in, and only honoured at `lg`. The avatar is a headline device: one
     per page, in the hero, where there is a single status to be had. In a list
     of forty rows it would be the forty coloured capsules the dot exists to
     avoid — louder, and forty times heavier. */
  avatar: { type: Boolean, default: false },
})

const TONE = {
  flowing: 'good', pacing: 'good', admitting: 'good', ok: 'good', live: 'good',
  parked: 'muted', idle: 'muted', draining: 'muted',
  saturating: 'warn', blind: 'warn', degraded: 'warn', lagging: 'warn',
  breached: 'bad', throttled: 'bad', unreachable: 'bad', down: 'bad', blocked: 'bad',
}
const LABEL = {
  pacing: 'at cap, pacing',
  parked: 'parked until lease expiry',
  saturating: 'backlog growing',
  blind: 'cap is assumed',
  breached: 'throttled by the vendor',
}
/*
  Three drawings for four tones, so `muted` borrows the calm one and is drained
  of its colour. That is the honest reading: parked is not a good state and not
  a bad one, it is nothing happening. Hiding the avatar on `muted` instead
  would reflow the hero every time a lane sits out its lease — a page that
  jumps whenever the limiter works normally.
*/
const AVATAR = { good: avatarGood, warn: avatarWarn, bad: avatarBad, muted: avatarGood }

const tone = computed(() => TONE[props.state] || 'muted')
const text = computed(() => props.label ?? (LABEL[props.state] || props.state))
const dotClass = computed(() => ({
  good: 'bg-good', warn: 'bg-warn', bad: 'bg-bad', muted: 'bg-line-2',
}[tone.value]))
const textClass = computed(() => ({
  good: 'text-fg-2', warn: 'text-warn', bad: 'text-bad', muted: 'text-fg-3',
}[tone.value]))
const showAvatar = computed(() => props.avatar && props.size === 'lg')
const avatarSrc = computed(() => AVATAR[tone.value])
</script>

<template>
  <!-- The wrappers become block elements only when the avatar is drawn: the
       slot below carries the hero's sentence, and a <p> inside a <span> is not
       HTML. Without an avatar this is `display: contents` and the dot renders
       exactly as it did before — the five list call sites are untouched. -->
  <component :is="showAvatar ? 'div' : 'span'"
             class="inline-flex items-center" :class="showAvatar && 'gap-4'">
    <img
      v-if="showAvatar" :src="avatarSrc" alt="" aria-hidden="true"
      width="56" height="56"
      class="w-14 h-14 shrink-0 rounded-xl border border-line object-cover"
      :class="tone === 'muted' && 'grayscale opacity-60'"
    />
    <component :is="showAvatar ? 'div' : 'span'" :class="showAvatar ? 'min-w-0' : 'contents'">
      <span class="inline-flex items-center" :class="size === 'lg' ? 'gap-2.5' : 'gap-1.5'">
        <span
          class="rounded-full shrink-0"
          :class="[dotClass, size === 'lg' ? 'w-[10px] h-[10px]' : 'w-[7px] h-[7px]']"
        />
        <span
          :class="[textClass, size === 'lg' ? 'text-[15px] font-medium' : 'text-[12.5px]']"
          class="whitespace-nowrap"
        >{{ text }}</span>
      </span>
      <!-- The sub-sentence belongs to the caller, but its box belongs here:
           beside the avatar, not under it. -->
      <slot v-if="showAvatar" />
    </component>
  </component>
</template>
