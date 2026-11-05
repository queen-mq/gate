// The console's look, as Queen's (queen-mq/queen: app/src/console/useLook.js): the theme (dark,
// soft, light, or the system's) and how areas are told apart (lines or tone), remembered in this
// browser. First visit: soft with tone areas, the console's default. Only an explicit Light
// chosen in the previous design (`gate-theme`) carries over; nothing else does.
//
// The classes go on <body> (`qn` and the theme, `tone`), where src/console.css reads them, so a
// dialog or a tooltip teleported to <body> takes the same tokens. Not on <html>: `.qn` sets a
// 13px font, which would shrink every rem. <html> keeps `dark` (for dark and soft: the logo's
// white artwork and Tailwind's `dark:` variant) and `color-scheme` for the native controls, and
// the page's tone, so overscroll shows no seam.
// Keep the pre-paint copy in index.html in sync with resolve() and apply().

import { computed, ref, watch } from 'vue'

export const LOOK_STORAGE_KEY = 'gate-look'
const OLD_THEME_KEY = 'gate-theme'

const THEMES = ['dark', 'soft', 'light', 'system']
const LIGHT_QUERY = '(prefers-color-scheme: light)'
// The pages' tone, as console.css's --bg, for <html> before the stylesheet has loaded.
const PAGE = { dark: '#0d0c0b', soft: '#202020', light: '#f6f5f3' }

export const THEME_OPTIONS = [
  { value: 'dark', label: 'Dark' },
  { value: 'soft', label: 'Soft' },
  { value: 'light', label: 'Light' },
  { value: 'system', label: 'System' },
]
export const AREA_OPTIONS = [
  { value: 'lines', label: 'Lines' },
  { value: 'tone', label: 'Tone' },
]

const storage = () => {
  try { return typeof localStorage !== 'undefined' ? localStorage : null } catch { return null }
}
const readSaved = () => {
  try { return JSON.parse(storage()?.getItem(LOOK_STORAGE_KEY) || '{}') || {} } catch { return {} }
}
const readOldTheme = () => {
  try { return storage()?.getItem(OLD_THEME_KEY) || null } catch { return null }
}
const mediaQuery = () => {
  try { return typeof matchMedia === 'function' ? matchMedia(LIGHT_QUERY) : null } catch { return null }
}

/** The stored look, or the first visit's. */
export const startLook = (saved, oldTheme) => ({
  theme: THEMES.includes(saved?.theme) ? saved.theme : (oldTheme === 'light' ? 'light' : 'soft'),
  areas: saved?.areas === 'lines' ? 'lines' : 'tone',
})
/** What a pick paints: System follows the OS, light or dark. */
export const resolve = (pick, systemLight) => (pick === 'system' ? (systemLight ? 'light' : 'dark') : pick)

const first = startLook(readSaved(), readOldTheme())
export const themePick = ref(first.theme)
export const areas = ref(first.areas)
const systemLight = ref(mediaQuery()?.matches === true)

/** The painted theme: dark, soft or light. */
export const theme = computed(() => resolve(themePick.value, systemLight.value))
export const isLight = computed(() => theme.value === 'light')

const apply = () => {
  if (typeof document === 'undefined') return
  const body = document.body
  const t = theme.value
  if (body) {
    body.classList.add('qn')
    for (const name of ['dark', 'soft', 'light']) body.classList.toggle(name, t === name)
    body.classList.toggle('tone', areas.value === 'tone')
  }
  const root = document.documentElement
  root.classList.toggle('dark', t !== 'light')
  root.style.colorScheme = t === 'light' ? 'light' : 'dark'
  root.style.background = PAGE[t]
}

const save = () => {
  try { storage()?.setItem(LOOK_STORAGE_KEY, JSON.stringify({ theme: themePick.value, areas: areas.value })) } catch { /* private window */ }
}

let started = false

/** Paint the look and follow it: the OS while the pick is System, and the other tabs. */
export const initTheme = () => {
  apply()
  if (started) return theme.value
  started = true
  watch([theme, areas], apply)
  watch([themePick, areas], save)
  mediaQuery()?.addEventListener?.('change', (event) => { systemLight.value = event.matches })
  if (typeof window !== 'undefined') {
    window.addEventListener('storage', (event) => {
      if (event.key !== LOOK_STORAGE_KEY && event.key !== null) return
      const next = startLook(readSaved(), readOldTheme())
      themePick.value = next.theme
      areas.value = next.areas
    })
  }
  return theme.value
}
