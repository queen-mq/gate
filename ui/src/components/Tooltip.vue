<script setup>
import { ref, computed, onUnmounted, nextTick } from 'vue'

/*
  A hairline `?` next to a label, and a paragraph explaining what the field is
  for. No library: the console ships inside the binary, and a positioning
  package is a hundred kilobytes an operator carries forever in order to place a
  box under a seven-pixel circle.

  Three things it has to get right, and they are the reasons it is not a
  `title=` attribute:

  * KEYBOARD. The marker is a real button, so the panel opens on focus and not
    only under a mouse. Escape closes it, and closing returns nothing — the
    marker never takes the form's tab order hostage.
  * CLIPPING. The panel is teleported to <body> and positioned from the
    marker's rect. The editor scrolls, and a scroll container computes
    `overflow-x` to `auto` the moment `overflow-y` is set, so an absolutely
    positioned panel inside it would be cut off at the card edge — silently,
    and only for the fields near the right margin.
  * PHONES. There is no hover on a touch screen, so a tap toggles it, and the
    panel is clamped to the viewport rather than given a fixed offset.
*/
defineProps({
  text: { type: String, required: true },
  // A field whose whole label is the affordance reads better than a `?` in a
  // dense row of table headers; the panel and its behaviour are identical.
  label: { type: String, default: null },
})

const PANEL = 320
const MARGIN = 12

// One per instance, so `aria-describedby` points at this panel and not at the
// forty others the editor renders.
const panelId = `tip-${Math.random().toString(36).slice(2, 9)}`

const marker = ref(null)
const hovered = ref(false)
const focused = ref(false)
const pinned = ref(false)
// Off screen until measured: `open` flips synchronously in the handler while
// place() waits a tick for the DOM, and the frame in between would otherwise
// paint an unsized panel in the top-left corner of every page.
const pos = ref({ left: -9999, top: -9999, bottom: null, width: PANEL })

const open = computed(() => hovered.value || focused.value || pinned.value)

async function place() {
  await nextTick()
  const el = marker.value
  if (!el) return
  const r = el.getBoundingClientRect()
  const width = Math.min(PANEL, window.innerWidth - MARGIN * 2)
  // Left-aligned to the marker, then pulled back inside the viewport. On a
  // phone that collapses to "the full width of the screen minus the margins",
  // which is the only readable answer at 375px.
  const left = Math.max(MARGIN, Math.min(r.left, window.innerWidth - width - MARGIN))
  // Below unless there is no room below and there is room above: a panel that
  // runs off the bottom of a long form is a panel nobody reads.
  const below = window.innerHeight - r.bottom
  const placement = below < 160 && r.top > below ? 'above' : 'below'
  pos.value = {
    left,
    width,
    top: placement === 'below' ? r.bottom + 8 : null,
    bottom: placement === 'above' ? window.innerHeight - r.top + 8 : null,
  }
}

/* Built here rather than in the template: `window` is not in the expression
   allowlist a Vue template can see, and the flip-to-above case needs it. */
const panelStyle = computed(() => ({
  left: `${pos.value.left}px`,
  width: `${pos.value.width}px`,
  top: pos.value.top === null ? 'auto' : `${pos.value.top}px`,
  bottom: pos.value.bottom === null ? 'auto' : `${pos.value.bottom}px`,
}))

function show() {
  place()
}

function onKey(e) {
  if (e.key !== 'Escape') return
  if (!open.value) return
  // Swallowed, because the editor lives in a page and a modal both, and Escape
  // that closes the tooltip must not also close what is behind it.
  e.stopPropagation()
  pinned.value = false
  hovered.value = false
  marker.value?.blur()
}

document.addEventListener('keydown', onKey, true)
onUnmounted(() => document.removeEventListener('keydown', onKey, true))
</script>

<template>
  <span class="inline-flex items-center align-middle">
    <button
      ref="marker" type="button"
      class="inline-flex items-center justify-center shrink-0 select-none
             text-fg-3 hover:text-fg-2 transition-colors"
      :class="label
        ? 'gap-1 text-inherit underline decoration-dotted decoration-line-2 underline-offset-[3px]'
        : 'w-[14px] h-[14px] rounded-full border border-line-2 text-[9.5px] font-semibold leading-none'"
      :aria-expanded="open"
      :aria-describedby="open ? panelId : undefined"
      :aria-label="label ? undefined : 'What is this field for?'"
      @mouseenter="hovered = true; show()"
      @mouseleave="hovered = false"
      @focus="focused = true; show()"
      @blur="focused = false"
      @click.prevent.stop="pinned = !pinned; pinned && show()"
    >
      <template v-if="label">{{ label }}</template>
      <template v-else>?</template>
    </button>

    <Teleport to="body">
      <span
        v-if="open" :id="panelId" role="tooltip"
        class="fixed z-[60] block rounded-lg border border-line bg-surface shadow-2xl
               px-3.5 py-3 text-[12.5px] leading-relaxed text-fg-2 animate-in"
        :style="panelStyle"
      >{{ text }}</span>
    </Teleport>
  </span>
</template>
