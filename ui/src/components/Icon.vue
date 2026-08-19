<script setup>
// Inline stroke icons: the console ships inside a binary, so every glyph is a
// kilobyte an operator carries forever, and the set needed here is small.
const paths = {
  gauge: ['M12 13l4-4', 'M20.2 15a8.5 8.5 0 1 0-16.4 0'],
  target: ['M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18z', 'M12 8a4 4 0 1 0 0 8 4 4 0 0 0 0-8z', 'M12 11.5v1'],
  budget: ['M4 19V5', 'M4 15h4V9h4V5h4v14', 'M20 19H4'],
  lane: ['M5 4v16', 'M19 4v16', 'M12 6v3', 'M12 13v5'],
  breach: ['M13 3l-8 10h6l-2 8 8-10h-6z'],
  trace: ['M4.5 6h.01', 'M4.5 12h.01', 'M4.5 18h.01', 'M9 6h11', 'M9 12h11', 'M9 18h11'],
  chart: ['M4 5v14', 'M20 19H4', 'M7 15l3.5-5 3 3L18 7'],
  edit: ['M4 20h4L18.5 9.5l-4-4L4 16v4z', 'M13.5 6.5l4 4'],
  key: ['M14.5 9.5a3.5 3.5 0 1 1-3.4-3.5', 'M14 10l6-6', 'M17.5 6.5l1.5 1.5', 'M20 4l1.5 1.5'],
  search: ['M10.5 4a6.5 6.5 0 1 0 0 13 6.5 6.5 0 0 0 0-13z', 'M15.5 15.5L20 20'],
  sun: ['M12 4v1.5', 'M12 18.5V20', 'M4 12h1.5', 'M18.5 12H20', 'M6.3 6.3l1 1', 'M16.7 16.7l1 1', 'M17.7 6.3l-1 1', 'M7.3 16.7l-1 1'],
  moon: ['M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z'],
  plus: ['M12 5v14', 'M5 12h14'],
  chevron: ['M9 6l6 6-6 6'],
  down: ['M6 9l6 6 6-6'],
  pause: ['M9 5v14', 'M15 5v14'],
  alert: ['M12 8.5v5', 'M12 16.8v.4', 'M12 3l9.5 17.5h-19z'],
  check: ['M4 12.5l5 5L20 6.5'],
  copy: ['M9 9h11v11H9z', 'M5 15V4h11'],
  x: ['M5 5l14 14', 'M19 5L5 19'],
  clock: ['M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18z', 'M12 8v4.5l3 2'],
  link: ['M10 13a5 5 0 0 0 7 0l3-3a5 5 0 0 0-7-7l-1 1', 'M14 11a5 5 0 0 0-7 0l-3 3a5 5 0 0 0 7 7l1-1'],
  // Two nodes and an edge: the graph, which is a path work takes rather than a
  // place it sits.
  graph: ['M6 7.5a2.5 2.5 0 1 0 0-5 2.5 2.5 0 0 0 0 5z', 'M18 21.5a2.5 2.5 0 1 0 0-5 2.5 2.5 0 0 0 0 5z', 'M6 7.5v4a4 4 0 0 0 4 4h4a4 4 0 0 1 4 4v-.5'],

}
defineProps({
  name: { type: String, required: true },
  size: { type: [Number, String], default: 16 },
})
</script>

<template>
  <svg
    :width="size" :height="size" viewBox="0 0 24 24" fill="none"
    stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"
    aria-hidden="true" class="shrink-0"
  >
    <path v-for="(d, i) in paths[name] || []" :key="i" :d="d" />
  </svg>
</template>
