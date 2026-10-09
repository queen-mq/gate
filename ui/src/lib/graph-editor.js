// Forms patch the original document so fields outside the edited control survive.
export const NAME_HINT = 'Lowercase letters, numbers and dashes; up to 63 characters.'
export const validName = (value) => typeof value === 'string' && /^[a-z0-9][a-z0-9-]{0,62}$/.test(value)
const object = (value) => value !== null && typeof value === 'object' && !Array.isArray(value)
const integer = (value, min = 1, max = Number.MAX_SAFE_INTEGER) => Number.isSafeInteger(value) && value >= min && value <= max
const nonempty = (value) => typeof value === 'string' && value.trim().length > 0
const payloadPath = (value) => typeof value === 'string' && value.startsWith('payload.') && value.split('.')[1] !== '_gate' && value.split('.').slice(1).every(Boolean)

export function optional(value, key, next) {
  const copy = { ...value }
  if (next === '' || next === undefined || next === null) delete copy[key]
  else copy[key] = next
  return copy
}

export function newBudget(index = 1) {
  return { id: `limit-${index}`, count: 100, timeMs: 1000, confidence: 'inferred' }
}

export function newNode() {
  return { ingress: true, budgets: [newBudget()], egress: '' }
}

export function newTarget(application = 'default') {
  return { application, graph: '', version: 1, nodes: { limit: newNode() }, paths: [{ name: 'main', nodes: ['limit'] }] }
}

export function trafficMode(doc, watching, bumpVersion = false) {
  if (!!doc.watch === watching) return doc
  const next = optional(doc, 'watch', watching ? { durationSeconds: 86400 } : undefined)
  if (watching) next.counters = doc.counters ?? { windowSeconds: 60 }
  else next.nodes = Object.fromEntries(Object.entries(doc.nodes).map(([name, node]) => [name, (node.budgets ?? []).length ? node : { ...node, budgets: [{ ...newBudget(), count: '' }] }]))
  if (bumpVersion) next.version = Number(doc.version) + 1
  return next
}

export function formIssue(doc) {
  if (!object(doc) || !object(doc.nodes) || !Array.isArray(doc.paths)) return 'The form needs a graph document with nodes and paths. Check its structure in JSON.'
  if (doc.watch !== undefined && !object(doc.watch)) return 'Watch must be an observation settings object. Correct it in JSON to use the form.'
  for (const node of Object.values(doc.nodes)) {
    if (!object(node) || (node.budgets !== undefined && (!Array.isArray(node.budgets) || node.budgets.some((b) => !object(b))))) return 'A node or budget has an invalid structure. Correct it in JSON to use the form.'
    if (node.ingress !== undefined && typeof node.ingress !== 'boolean' && !object(node.ingress)) return 'Ingress must be a boolean or an object. Correct it in JSON to use the form.'
    if (node.egress !== undefined && typeof node.egress !== 'string' && !object(node.egress)) return 'Egress must be a queue name or an object. Correct it in JSON to use the form.'
    if (node.cost !== undefined && typeof node.cost !== 'number' && !object(node.cost)) return 'Cost must be a number or a payload-path object. Correct it in JSON to use the form.'
    if ((node.budgets ?? []).some((b) => b.whenOp !== undefined && (!Array.isArray(b.whenOp) || b.whenOp.some((op) => typeof op !== 'string')))) return 'Operation filters must be lists of strings. Correct them in JSON to use the form.'
  }
  if (doc.paths.some((p) => !object(p) || !Array.isArray(p.nodes) || p.nodes.some((n) => typeof n !== 'string' && (!Array.isArray(n) || n.some((x) => typeof x !== 'string'))))) return 'Each path needs a sequence of node names. Correct it in JSON to use the form.'
  return ''
}

export function renameNode(doc, previous, next) {
  if (next === previous) return doc
  if (!validName(next)) throw new Error(NAME_HINT)
  if (Object.hasOwn(doc.nodes, next)) throw new Error('A node with this name already exists.')
  return {
    ...doc,
    nodes: Object.fromEntries(Object.entries(doc.nodes).map(([name, node]) => [name === previous ? next : name, node])),
    paths: doc.paths.map((path) => ({ ...path, nodes: path.nodes.map((hop) => Array.isArray(hop) ? hop.map((name) => name === previous ? next : name) : hop === previous ? next : hop) })),
  }
}

export function nodeIsUsed(doc, name) {
  return doc.paths.some((path) => path.nodes.some((hop) => Array.isArray(hop) ? hop.includes(name) : hop === name))
}

export function validateDocument(doc) {
  const errors = []
  const add = (path, message, step = 'limits') => errors.push({ path, message, step })
  const structure = formIssue(doc)
  if (structure) return [{ path: 'document', message: structure, step: 'identity' }]
  for (const [key, label] of [['application', 'Application'], ['graph', 'Target / graph name']]) {
    if (!validName(doc[key])) add(key, `${label}: ${NAME_HINT}`, 'identity')
  }
  if (!integer(doc.version, 1, 4294967295)) add('version', 'Version must be a positive integer.', 'identity')
  if (!Object.keys(doc.nodes).length) add('nodes', 'Add at least one node.', 'routing')
  if (!doc.paths.length) add('paths', 'Add at least one path.', 'routing')
  if (doc.maxAttempts !== undefined && !integer(doc.maxAttempts, 1, 20)) add('maxAttempts', 'Re-entry attempts must be between 1 and 20.')
  if (doc.counters !== undefined && doc.counters?.windowSeconds !== 60) add('counters', 'History counters use a 60-second window.')
  if (doc.watch && !integer(doc.watch.durationSeconds, 120, 30 * 86400)) add('watch.durationSeconds', 'Choose an observation period between two minutes and 30 days.')
  for (const [name, node] of Object.entries(doc.nodes)) {
    const at = `nodes.${name}`
    if (!validName(name)) add(at, `Node name: ${NAME_HINT}`, 'routing')
    if (`gate.${doc.application}.${doc.graph}.${name}.in`.length > 63) add(at, 'Shorten the application, target or node name so the generated queue name fits in 63 characters.', 'identity')
    if (object(node.ingress)) {
      if (node.ingress.queue !== undefined && !nonempty(node.ingress.queue)) add(`${at}.ingress`, 'Enter an existing ingress queue name.', 'routing')
      if (node.ingress.partitions !== undefined && !integer(node.ingress.partitions, 1, 4294967295)) add(`${at}.ingress.partitions`, 'Partitions must be a positive integer.', 'routing')
    }
    if (node.egress !== undefined && !nonempty(typeof node.egress === 'string' ? node.egress : node.egress.queue)) add(`${at}.egress`, 'Enter the queue your workers consume.', 'routing')
    const cost = node.cost ?? 1
    const maximum = object(cost) ? cost.max ?? cost.default ?? 1 : cost
    if (object(cost)) {
      if (!payloadPath(cost.path)) add(`${at}.cost`, 'Cost must use a payload path such as payload.cost.')
      if (!integer(cost.default ?? 1) || !integer(maximum) || maximum < (cost.default ?? 1)) add(`${at}.cost`, 'Cost default and maximum must be positive integers, with maximum at least the default.')
    } else if (!integer(cost)) add(`${at}.cost`, 'Cost per item must be a positive integer.')
    if (node.batch !== undefined && !integer(node.batch, 1, 1000)) add(`${at}.batch`, 'Batch size must be between 1 and 1000.')
    if (node.concurrency !== undefined && !integer(node.concurrency, 1, 4294967295)) add(`${at}.concurrency`, 'Concurrency must be a positive integer.')
    if (!doc.watch && !(node.budgets ?? []).length) add(`${at}.budgets`, 'Add at least one rate limit.')
    if (!doc.watch && !(node.budgets ?? []).some((b) => b.scopeBy === undefined && b.whenOp === undefined)) add(`${at}.budgets`, 'Keep at least one limit that applies to every item, without a scope or operation filter.')
    const ids = new Set()
    for (const [i, budget] of (node.budgets ?? []).entries()) {
      const path = `${at}.budgets[${i}]`
      const label = `${name} / ${budget.id ?? `b${i}`}`
      const id = budget.id ?? `b${i}`
      if (ids.has(id)) add(path, `${label}: each limit needs a unique ID.`)
      ids.add(id)
      if (!integer(budget.count)) add(`${path}.count`, `${label}: allowance must be a positive integer.`)
      if (!integer(budget.timeMs, 100)) add(`${path}.timeMs`, `${label}: the window must be at least 100 milliseconds.`)
      if (budget.subWindows !== undefined && (!integer(budget.subWindows, 1, 3600) || budget.subWindows > budget.count)) add(`${path}.subWindows`, `${label}: subdivisions must be between 1 and 3600 and cannot exceed the allowance.`)
      if (integer(budget.count) && integer(maximum) && Math.floor(budget.count / (budget.subWindows ?? 1)) < maximum) add(path, `${label}: an item's maximum cost must fit in each subdivision.`)
      if (!['inferred', 'documented', 'assumed'].includes(budget.confidence ?? 'inferred')) add(`${path}.confidence`, `${label}: select how this limit was established.`)
      if (budget.confidence === 'documented' && (!nonempty(budget.source) || !nonempty(budget.asOf))) add(`${path}.source`, `${label}: a documented limit needs its source and verification date.`)
      if (budget.scopeBy !== undefined && !payloadPath(budget.scopeBy)) add(`${path}.scopeBy`, `${label}: scope must use a payload path such as payload.accountId.`)
      if (budget.whenOp !== undefined && (!budget.whenOp.length || budget.whenOp.some((op) => !nonempty(op)))) add(`${path}.whenOp`, `${label}: add an operation pattern or clear the filter.`)
    }
  }
  const pathNames = new Set(), visited = new Set(), edges = new Map()
  for (const [i, path] of doc.paths.entries()) {
    const at = `paths[${i}]`
    if (!validName(path.name) || pathNames.has(path.name)) add(at, 'Each path needs a unique name with lowercase letters, numbers and dashes.', 'routing')
    pathNames.add(path.name)
    if (!integer(path.priority ?? 0, 0, 4294967295)) add(at, `${path.name}: priority must be a nonnegative integer.`, 'routing')
    if (path.share !== undefined && (typeof path.share !== 'number' || !Number.isFinite(path.share) || path.share <= 0 || path.share > 1)) add(at, `${path.name}: share must be greater than 0 and at most 1.`, 'routing')
    if (!path.nodes.length) add(at, `${path.name}: select at least one node.`, 'routing')
    for (const [j, hop] of path.nodes.entries()) {
      const names = Array.isArray(hop) ? hop : [hop]
      if (Array.isArray(hop) && (hop.length < 2 || j !== path.nodes.length - 1)) add(at, `${path.name}: fan-out needs at least two nodes and must be the last hop.`, 'routing')
      for (const name of names) {
        if (!Object.hasOwn(doc.nodes, name)) { add(at, `${path.name}: node ${name || '(empty)'} does not exist.`, 'routing'); continue }
        visited.add(name)
        const node = doc.nodes[name]
        if (j === 0 && (!node.ingress || node.ingress === false)) add(at, `${path.name}: its first node needs an ingress queue.`, 'routing')
        if (j === path.nodes.length - 1 && node.egress === undefined) add(at, `${path.name}: its last node needs an egress queue.`, 'routing')
        if (j > 0) for (const previous of [].concat(path.nodes[j - 1])) {
          if (!edges.has(previous)) edges.set(previous, new Set())
          edges.get(previous).add(name)
        }
      }
    }
  }
  for (const name of Object.keys(doc.nodes)) if (!visited.has(name)) add(`nodes.${name}`, `${name}: add this node to a path.`, 'routing')
  const active = new Set(), done = new Set()
  function cycle(name) {
    if (active.has(name)) return true
    if (done.has(name)) return false
    active.add(name)
    for (const next of edges.get(name) ?? []) if (cycle(next)) return true
    active.delete(name); done.add(name)
    return false
  }
  if ([...edges.keys()].some(cycle)) add('paths', 'Paths must not contain a cycle.', 'routing')
  return errors
}

// A name collision must lead to editing, never to an accidental replacement.
export async function ensureNewGraph(api, application, name) {
  try {
    await api.get(`/api/apps/${encodeURIComponent(application)}/graphs/${encodeURIComponent(name)}`)
  } catch (error) {
    if (error.status === 404) return
    throw error
  }
  throw new Error('This application already has a target / graph with this name. Choose another name or edit the existing one.')
}
