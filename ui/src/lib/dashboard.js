import { DEFAULT_APP, utilisation, traceRef } from './api.js'

export const targetKey = (t) =>
  JSON.stringify([t.application || DEFAULT_APP, t.name ?? t.graph])

export function targetUsage(target) {
  return Number.isFinite(target?.worst_used) && target?.worst_cap > 0
    ? Math.max(0, target.worst_used / target.worst_cap)
    : null
}

export function breachMatches(breach, target) {
  const ref = traceRef(breach)
  return (
    (ref.name === target.name || ref.name.startsWith(`${target.name}.`)) &&
    (!ref.scoped || ref.application === (target.application || DEFAULT_APP))
  )
}

export function worstBudget(budgets = []) {
  // Selective budgets have no comparable aggregate reading. Keep their existence
  // visible in the inspector without interpreting missing readings as zero.
  return budgets
    .filter(
      (b) => !b.scopeBy && !b.whenOp?.length && Number.isFinite(utilisation(b)),
    )
    .reduce(
      (best, b) => (!best || utilisation(b) > utilisation(best) ? b : best),
      null,
    )
}

export function graphEdges(paths = []) {
  const edges = new Map()
  const split = (hop) =>
    Array.isArray(hop)
      ? hop
      : String(hop ?? '').startsWith('[')
        ? String(hop)
            .slice(1, -1)
            .split(',')
            .map((n) => n.trim())
            .filter(Boolean)
        : [String(hop)]
  for (const path of paths) {
    const hops = path.hops ?? []
    for (let i = 0; i + 1 < hops.length; i++) {
      for (const from of split(hops[i]))
        for (const to of split(hops[i + 1])) {
          const key = JSON.stringify([from, to])
          if (!edges.has(key)) edges.set(key, { from, to, paths: [] })
          edges.get(key).paths.push(path.name)
        }
    }
  }
  return [...edges.values()]
}

export function graphModel(graph) {
  if (!graph) return { nodes: [], edges: [] }
  const nodes = (graph.nodes ?? []).map((n) => {
    const budget = worstBudget(n.budgets)
    const stages = (graph.stages ?? []).filter((s) => s.node === n.node)
    const tone =
      !graph.running || n.breaker || stages.some((s) => s.counters?.wedged > 0)
        ? 'bad'
        : (n.budgets ?? []).some((b) => b.confidence === 'assumed')
          ? 'warn'
          : (utilisation(budget) ?? 0) >= 0.85
            ? 'warn'
            : 'good'
    return {
      ...n,
      id: n.node,
      name: n.node,
      budget,
      usage: utilisation(budget),
      tone,
      kind: n.ingressQueue
        ? 'Ingress node'
        : n.egressQueue
          ? 'Terminal node'
          : 'Relay node',
      admitted:
        stages.length &&
        stages.every((s) => Number.isFinite(s.counters?.admitted))
          ? stages.reduce((sum, s) => sum + s.counters.admitted, 0)
          : null,
      unknownBudgets: (n.budgets ?? []).filter(
        (b) =>
          b.scopeBy || b.whenOp?.length || !Number.isFinite(utilisation(b)),
      ).length,
    }
  })
  const edges = graphEdges(graph.paths).filter(
    (e) =>
      nodes.some((n) => n.id === e.from) && nodes.some((n) => n.id === e.to),
  )
  for (const node of [...nodes]) {
    if (!node.egressQueue) continue
    const id = `@egress:${node.id}`
    nodes.push({
      ...node,
      id,
      name: node.egressQueue,
      kind: 'Egress queue',
      output: true,
      breaker: null,
      tone: graph.running ? 'good' : 'bad',
      sourceNode: node.id,
      budget: null,
      budgets: [],
      usage: null,
      unknownBudgets: 0,
      waiting_for_budget: null,
    })
    edges.push({ from: node.id, to: id, paths: node.paths ?? [], output: true })
  }
  return { nodes, edges }
}

// A bounded Kahn layout supports branches, joins, disconnected nodes and a
// malformed cycle without recursion. Large graphs scroll rather than shrinking
// every label into an unreadable dot.
export function layoutTopology(nodes, edges) {
  const byId = new Map(nodes.map((n) => [n.id, n]))
  const incoming = new Map(nodes.map((n) => [n.id, 0]))
  const outgoing = new Map(nodes.map((n) => [n.id, []]))
  for (const e of edges) {
    if (!byId.has(e.from) || !byId.has(e.to)) continue
    incoming.set(e.to, incoming.get(e.to) + 1)
    outgoing.get(e.from).push(e.to)
  }
  const queue = nodes.filter((n) => incoming.get(n.id) === 0).map((n) => n.id)
  const depth = new Map(nodes.map((n) => [n.id, 0]))
  const visited = new Set()
  for (let i = 0; i < queue.length; i++) {
    const id = queue[i]
    visited.add(id)
    for (const next of outgoing.get(id)) {
      depth.set(next, Math.max(depth.get(next), depth.get(id) + 1))
      incoming.set(next, incoming.get(next) - 1)
      if (incoming.get(next) === 0) queue.push(next)
    }
  }
  const columns = new Map()
  for (const node of nodes) {
    const column = visited.has(node.id) ? depth.get(node.id) : 0
    if (!columns.has(column)) columns.set(column, [])
    columns.get(column).push(node)
  }
  const rows = Math.max(1, ...[...columns.values()].map((c) => c.length))
  const height = Math.max(380, rows * 164 + 84)
  const count = Math.max(1, columns.size)
  const width = Math.max(720, (count - 1) * 280 + 300)
  const placed = []
  for (const [col, members] of columns)
    members.forEach((n, i) =>
      placed.push({
        ...n,
        x: count === 1 ? width / 2 : 150 + (col * (width - 300)) / (count - 1),
        y: (height - (members.length - 1) * 164) / 2 + i * 164 + 22,
        column: col,
      }),
    )
  return { nodes: placed, width, height, cyclic: visited.size !== nodes.length }
}

export function flowingNodes(previous, current) {
  if (
    !previous ||
    !current?.running ||
    targetKey(previous) !== targetKey(current) ||
    previous.version !== current.version
  )
    return []
  const key = (s) => JSON.stringify([s.node, s.path, s.hop, s.source])
  const before = new Map(
    (previous.stages ?? []).map((s) => [key(s), s.counters?.forwarded]),
  )
  const blocked = new Set(
    (current.nodes ?? []).filter((n) => n.breaker).map((n) => n.node),
  )
  return [
    ...new Set(
      (current.stages ?? [])
        .filter((s) => {
          const count = before.get(key(s))
          return (
            !blocked.has(s.node) &&
            Number.isFinite(count) &&
            Number.isFinite(s.counters?.forwarded) &&
            s.counters.forwarded > count
          )
        })
        .map((s) => s.node),
    ),
  ]
}
