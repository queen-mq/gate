import { api, toDate } from './api.js'

/*
  The roll-ups are the console's only memory. They come from `gate.rollups` in
  Postgres, where every replica's minutes are summed into one row, so the series
  is the deployment's and not the answering pod's.

  A build with no database configured serves its own ring instead and reaches
  back only as far as that process has been running. That is not a broken
  console — it is a console with one replica's memory, which is exactly what a
  local run has.

  `null` therefore means "there is nothing to read", an array means "this is
  it, possibly empty", and every caller renders the difference.
*/
async function series(key, minutes) {
  const r = await api
    .get(`/api/rollups?target=${encodeURIComponent(key)}&minutes=${minutes}`)
    .catch(() => null)
  const rows = Array.isArray(r) ? r : (r?.windows ?? null)
  return rows && rows.length ? rows : null
}

/*
  Always the scoped key: two teams may both own an `airbnb` and their minutes
  must never be added together. The table keys on the pair, so there is one
  series to ask for and no reconciling to do — and the `target` half is
  `{graph}.{node}`, because a node is what a target was.
*/
export async function fetchRollups(application, target, minutes = 120) {
  const rows = await series(`${application}/${target}`, minutes)
  if (!rows) return null
  // Sorted here rather than trusted: a series drawn from windows in the wrong
  // order is not a wrong chart, it is a convincing one.
  return rows.slice().sort((a, b) => windowMs(a) - windowMs(b))
}

export function windowMs(row) {
  const d = toDate(row?.t)
  return d ? d.getTime() : 0
}

/*
  Every field in a bucket is an INCREMENT: what happened inside that minute.
  The gate itself reports lifetime counters, and the meter differences them
  against the previous reading before it stores anything, so the differencing
  happens once, on the server, where the previous reading lives.

  This function used to difference them a second time here, back when the
  buckets really did hold running totals. That is worth naming rather than
  quietly deleting: a stale compensation is invisible on a chart — it draws a
  plausible line, just the wrong one. A minute at a steady 4,290 admissions
  rendered as 10, and nothing about the picture said so.

  There is also no first window to drop any more. Every row is a minute that
  stands on its own.
*/
export function perMinute(rows, path = null) {
  if (!rows) return null
  // The table's `lane` column is the PATH; the DDL did not change, because a
  // node IS what a target was and ninety days of history are worth more than a
  // tidy column name.
  const bucket = (row) => (path ? row?.lanes?.[path] : row?.total) ?? {}
  return rows.map((row) => {
    const b = bucket(row)
    return {
      t: row.t,
      admitted: b.admitted ?? 0,
      denied: b.denied ?? 0,
      calls: b.calls ?? 0,
      throttled: b.throttled ?? 0,
      cost_estimated: b.cost_estimated ?? 0,
      cost_actual: b.cost_actual ?? 0,
    }
  })
}

/*
  A budget's utilisation is not in the roll-up and cannot be: the roll-up
  counts a minute of the whole target, the budget counts its own window. So the
  series is reconstructed — the admissions that fall inside the budget's window
  over what the budget allows in that much time.

  Two limits come with reconstructing it, and the pages that draw it say so:

  * a window SHORTER than a minute is averaged across the minute, so a burst
    inside it is smoothed away — the live gauge is what sees those;
  * a window LONGER than a minute is a trailing sum, which is what a rolling
    budget is and only an approximation of a calendar-aligned one.
*/
export function budgetSeries(minutes, budget) {
  if (!minutes) return null
  // Roll-ups have node/path dimensions, but no scope value or operation. A
  // percentage for either kind of selective budget would use unrelated work in
  // its numerator, so leave it unknown instead of drawing a false zero/peak.
  if (budget?.scopeBy || budget?.whenOp?.length) return null
  // The SUB-window and its count, because that is what is enforced: a budget
  // declared over ten seconds and subdivided into ten is a one-second window of
  // a tenth, and drawing the declared pair would draw a ceiling nothing meets.
  const p = budget?.windowSubSeconds ?? 60
  const cap = budget?.countSub ?? budget?.count ?? 0
  const span = Math.max(1, Math.ceil(p / 60))
  const allowance = p > 0 ? cap * ((span * 60) / p) : 0
  const out = minutes.map((w, i) => {
    let sum = 0
    for (let k = Math.max(0, i - span + 1); k <= i; k++) {
      // Rows written before cost rollups existed legitimately contain zero in
      // that column. Costs are positive, so admitted is a safe legacy fallback.
      sum += (minutes[k].cost_estimated ?? 0) > 0
        ? minutes[k].cost_estimated
        : (minutes[k].admitted ?? 0)
    }
    return { ...w, utilisation: allowance > 0 ? sum / allowance : 0 }
  })
  // A trailing sum that has not filled yet is not a low utilisation, it is an
  // unknown one, and drawing it puts a ramp on the left of every long window.
  return span > 1 ? out.slice(span - 1) : out
}
