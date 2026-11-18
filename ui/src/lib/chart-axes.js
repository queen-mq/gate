/* Axis arithmetic for the history charts: round tick values, so a reader can
   tell what a line's height means without hovering. */

const TARGET_TICKS = 5
// Seconds that read as whole clock units: 30s, 5m, 1h …
const DURATION_STEPS = [1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 10800, 21600, 43200, 86400]
const TIME_STEPS_MIN = [1, 2, 5, 10, 15, 30, 60, 120, 180, 360, 720]
const MAX_TIME_TICKS = 4

function niceStep(raw, isDuration) {
  if (isDuration) return DURATION_STEPS.find(s => s >= raw) ?? Math.ceil(raw / 86400) * 86400
  const power = 10 ** Math.floor(Math.log10(raw))
  return [1, 2, 5, 10].map(m => m * power).find(s => s >= raw * (1 - 1e-9))
}

/* Ticks from 0 to a round maximum at or above every value. */
export function yScale(values, isDuration = false) {
  const highest = Math.max(0, ...values)
  if (highest === 0) return { max: 1, ticks: [0, 1] }
  let step = niceStep(highest / TARGET_TICKS, isDuration)
  // Whole counts never show half an item.
  if (values.every(Number.isInteger)) step = Math.max(1, step)
  const count = Math.ceil(highest / step - 1e-9)
  const ticks = Array.from({ length: count + 1 }, (_, i) => Number((i * step).toPrecision(12)))
  return { max: ticks.at(-1), ticks }
}

/* Clock-aligned instants between t0 and t1 (epoch ms), at most MAX_TIME_TICKS of them.
   shortcut: aligned on UTC multiples, so in a half-hour-offset zone hourly ticks land on :30. */
export function timeTicks(t0, t1) {
  if (!(t1 > t0)) return []
  const minutes = TIME_STEPS_MIN.find(m => (t1 - t0) / (m * 60_000) <= MAX_TIME_TICKS) ?? 1440
  const step = minutes * 60_000
  const ticks = []
  for (let t = Math.ceil(t0 / step) * step; t <= t1; t += step) ticks.push(t)
  return ticks
}

export function duration(seconds) {
  const s = Math.round(seconds)
  if (s < 60) return `${s}s`
  const h = Math.floor(s / 3600), m = Math.floor(s % 3600 / 60), rest = s % 60
  return [h && `${h}h`, m && `${m}m`, !h && rest && `${rest}s`].filter(Boolean).join(' ')
}
