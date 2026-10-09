import { newTarget, newBudget } from './graph-editor.js'

export const templates = [
  { id: 'global', name: 'Global limit', description: 'One allowance for all traffic.' },
  { id: 'account', name: 'Per account', description: 'A global ceiling plus a separate allowance for each account.' },
  { id: 'operation', name: 'Per operation', description: 'A global ceiling plus a limit for selected operations.' },
  { id: 'shared', name: 'Shared budget', description: 'Share one allowance with other targets in this application.' },
]

export function templateTarget(id, application = 'default') {
  const doc = newTarget(application)
  doc.counters = { windowSeconds: 60 }
  // These are editable examples, never claimed provider quotas.
  const global = { ...newBudget(), id: 'global', confidence: 'assumed' }
  doc.nodes.limit.budgets = [global]
  if (id === 'account') doc.nodes.limit.budgets.push({ ...global, id: 'account', count: 10, scopeBy: 'payload.accountId' })
  if (id === 'operation') doc.nodes.limit.budgets.push({ ...global, id: 'operation', count: 10, whenOp: ['write.*'] })
  if (id === 'shared') global.sharedKey = 'provider-quota'
  return doc
}

// A clone must never start another reader on the source target's input queue.
// Keep topology and rules; require explicit destinations and a fresh identity.
export function cloneGraph(source) {
  const doc = structuredClone(source)
  doc.graph = ''
  doc.version = 1
  if (doc.watch) delete doc.watch.startedAt
  for (const node of Object.values(doc.nodes)) {
    if (node.ingress && typeof node.ingress === 'object') delete node.ingress.queue
    if (node.egress !== undefined) node.egress = typeof node.egress === 'object' ? { ...node.egress, queue: '' } : ''
  }
  return doc
}

export function integrationExamples(graph, node) {
  const root = `/v1/apps/${encodeURIComponent(graph.application)}/graphs/${encodeURIComponent(graph.graph)}`
  const incoming = node.ingressQueue
  const outgoing = node.egressQueue
  const group = node.egressGroup
  const data = { op: 'write.example', value: 'replace with your payload' }
  const spec = graph.spec?.nodes?.[node.node]
  function field(path, value) {
    const parts = path?.startsWith('payload.') ? path.slice(8).split('.') : []
    if (!parts.length || parts.some(k => ['__proto__', 'constructor', 'prototype'].includes(k))) return
    let target = data
    for (const key of parts.slice(0, -1)) {
      if (!target[key] || typeof target[key] !== 'object') target[key] = {}
      target = target[key]
    }
    target[parts.at(-1)] = value
  }
  for (const budget of spec?.budgets ?? []) field(budget.scopeBy, 'example-account')
  if (spec?.cost && typeof spec.cost === 'object') field(spec.cost.path, spec.cost.default ?? 1)
  const operation = spec?.budgets?.flatMap(b => b.whenOp ?? [])[0]
  if (operation) data.op = operation.replaceAll('*', 'example')
  const js = JSON.stringify
  const producer = incoming ? `import { Queen } from 'queen-mq';\n\nconst queen = new Queen(process.env.QUEEN_URL);\nconst payload = ${js(data, null, 2)};\n\nawait queen.queue(${js(incoming)})\n  .partition('account-123')\n  .push([{ data: payload }]);` : null
  const consumer = outgoing ? `import { Queen } from 'queen-mq';\n\nconst queen = new Queen(process.env.QUEEN_URL);\nawait queen.queue(${js(outgoing)})${group ? `\n  .group(${js(group)})` : ''}\n  .subscriptionMode('all')\n  .consume(async (msg) => {\n    // Perform the work; return only after it succeeds.\n    await handleWork(msg.data);\n  });` : null
  const body = js({ op: data.op, payload: data, partition: 'account-123' }, null, 2)
  // Single-quote shell escaping also covers quotes in example data.
  const shell = (text) => `'${text.replaceAll("'", "'\\''")}'`
  const http = node.httpPush ? `curl --fail-with-body "$GATE_URL${root}/nodes/${encodeURIComponent(node.node)}/push" \\\n  -H 'Content-Type: application/json' \\\n  --data ${shell(body)}` : null
  return { producer, consumer, http }
}
