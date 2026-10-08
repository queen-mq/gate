import { ref, watch, onMounted, onUnmounted } from 'vue'

export const normalizeTheme = value => ['light', 'dark', 'system'].includes(value) ? value : 'system'
export const isDarkTheme = (theme, systemDark) => theme === 'dark' || (normalizeTheme(theme) === 'system' && systemDark)

export function useTheme() {
  const media = window.matchMedia('(prefers-color-scheme: dark)')
  const systemDark = ref(media.matches)
  let stored
  try { stored = localStorage.getItem('gate-theme') } catch {}
  const theme = ref(normalizeTheme(stored))

  watch([theme, systemDark], ([choice, system]) => {
    const dark = isDarkTheme(choice, system)
    document.documentElement.classList.toggle('dark', dark)
    document.documentElement.dataset.theme = choice
    document.documentElement.style.colorScheme = dark ? 'dark' : 'light'
  }, { immediate: true })
  watch(theme, choice => {
    try { localStorage.setItem('gate-theme', choice) } catch {}
  })

  function systemChanged(event) { systemDark.value = event.matches }
  function storageChanged(event) {
    if (event.key === 'gate-theme' || event.key === null) theme.value = normalizeTheme(event.newValue)
  }
  onMounted(() => {
    // Close the gap between setup and mounting if the OS changed meanwhile.
    systemDark.value = media.matches
    media.addEventListener('change', systemChanged)
    window.addEventListener('storage', storageChanged)
  })
  onUnmounted(() => {
    media.removeEventListener('change', systemChanged)
    window.removeEventListener('storage', storageChanged)
  })
  return { theme }
}
