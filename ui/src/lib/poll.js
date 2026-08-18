import { onMounted, onUnmounted } from 'vue'

/*
  A limiter console is a live instrument: the numbers on screen are a window
  that is closing right now, and a lane that is parked until its lease expires.
  Every view polls, pausing while the tab is hidden so an open console does not
  hammer the control API from a background tab all day.

  The default is faster than the relay console's: the shortest window a target
  can declare is one second, and a gauge that refreshes every eight would show
  a saturated budget as an empty one for most of its life.
*/
export function usePoll(fn, ms = 4000) {
  let timer = null

  function start() {
    stop()
    timer = setInterval(() => {
      if (!document.hidden) fn()
    }, ms)
  }
  function stop() {
    if (timer) clearInterval(timer)
    timer = null
  }
  function onVisibility() {
    // Refresh immediately on return, rather than waiting out the interval.
    if (!document.hidden) fn()
  }

  onMounted(() => {
    fn()
    start()
    document.addEventListener('visibilitychange', onVisibility)
  })
  onUnmounted(() => {
    stop()
    document.removeEventListener('visibilitychange', onVisibility)
  })
}
