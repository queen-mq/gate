import { onMounted, onUnmounted } from 'vue'

/*
  Collapse concurrent refresh requests into one active call and, at most, one
  follow-up. A slow control-plane response must not let the interval build a
  queue of requests whose replies can land out of order.
*/
export function singleFlight(fn) {
  let running = null
  let pending = false

  async function drain() {
    do {
      pending = false
      await fn()
    } while (pending)
  }

  return function run() {
    if (running) {
      pending = true
      return running
    }

    // Start on the next microtask so `running` is set before `fn` can call the
    // returned function recursively.
    running = Promise.resolve()
      .then(drain)
      .finally(() => { running = null })
    return running
  }
}

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
  let mounted = false
  const refresh = singleFlight(async () => {
    if (mounted) await fn()
  })

  function start() {
    stop()
    timer = setInterval(() => {
      if (!document.hidden) refresh()
    }, ms)
  }
  function stop() {
    if (timer) clearInterval(timer)
    timer = null
  }
  function onVisibility() {
    // Refresh immediately on return, rather than waiting out the interval.
    if (!document.hidden) refresh()
  }

  onMounted(() => {
    mounted = true
    refresh()
    start()
    document.addEventListener('visibilitychange', onVisibility)
  })
  onUnmounted(() => {
    mounted = false
    stop()
    document.removeEventListener('visibilitychange', onVisibility)
  })

  // Route and range watchers use this same gate as the timer. That keeps a
  // user-triggered refresh from racing the periodic one.
  return refresh
}
