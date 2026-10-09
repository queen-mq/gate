// Missing measurements stay null: an outage is a gap, not zero traffic.
export function incomingRate(previous, current) {
  if (!previous?.value?.available || !current?.value?.available || previous.value.version !== current.value.version) return null
  const elapsed = current.at - previous.at
  if (elapsed < 30_000 || elapsed > 90_000) return null
  const a = previous.value.positions, b = current.value.positions
  if (!a || !b || !Object.keys(a).length || Object.keys(a).sort().join('\0') !== Object.keys(b).sort().join('\0')) return null
  let delta = 0
  for (const key of Object.keys(a)) {
    if (!Number.isSafeInteger(a[key]) || !Number.isSafeInteger(b[key]) || b[key] < a[key]) return null
    delta += b[key] - a[key]
  }
  return delta * 60_000 / elapsed
}

export function timelineSeries(data, node, since, until) {
  const samples = (data?.samples ?? []).filter(s => s.node === node).sort((a, b) => a.t - b.t)
  const byTime = new Map(samples.map(s => [s.t, s]))
  const relayed = new Map((data?.relayed?.[node] ?? []).map(r => [r.t, r.total?.admitted]))
  const rows = []
  for (let t = Math.ceil(since / 60_000) * 60_000; t <= until; t += 60_000) {
    const sample = byTime.get(t), next = byTime.get(t + 60_000)
    const known = sample?.value?.available
    rows.push({ t,
      incoming: incomingRate(sample, next),
      relayed: relayed.get(t) ?? null,
      before: known ? sample.value.before : null,
      workers: known ? sample.value.workers : null,
      oldest: known ? sample.value.oldestSeconds : null,
    })
  }
  return rows
}
