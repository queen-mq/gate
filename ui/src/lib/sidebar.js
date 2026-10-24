// The sidebar's width on a wide screen: the full column, or a rail of icons.
// Queen's composables/useSidebar.js: the choice is remembered on this device
// like the theme, and only means something above the drawer breakpoint — up
// to 1100px the sidebar is a drawer over the page and always opens in full.

import { computed, ref } from 'vue'

const KEY = 'gate-sidebar'
const LEGACY_KEY = 'gate-sidebar-rail' // 'true' meant the rail before the shell was shared

// Storage and matchMedia both throw in hardened contexts (Safari private mode,
// storage access denied); a sidebar width is never worth an exception.
const readStored = () => {
  try {
    return localStorage.getItem(KEY) ?? (localStorage.getItem(LEGACY_KEY) === 'true' ? 'rail' : null)
  } catch {
    return null
  }
}

// Sidebar.vue's DRAWER_MAX and the 1100px media queries in shell.css.
export const DRAWER_MAX = 1100
const wideQuery = (() => {
  try {
    return matchMedia(`(min-width: ${DRAWER_MAX + 1}px)`)
  } catch {
    return null
  }
})()

export const collapsed = ref(readStored() === 'rail')
/** Above the drawer breakpoint, where the rail exists at all. */
export const wide = ref(wideQuery ? wideQuery.matches : true)
wideQuery?.addEventListener?.('change', (e) => { wide.value = e.matches })

/** True when the sidebar is drawn as the icon rail right now. */
export const rail = computed(() => collapsed.value && wide.value)

export const toggleSidebar = () => {
  collapsed.value = !collapsed.value
  try { localStorage.setItem(KEY, collapsed.value ? 'rail' : 'full') } catch {}
}

/** The drawer, below the breakpoint: Sidebar opens it, App makes the page inert behind it. */
export const drawerOpen = ref(false)
