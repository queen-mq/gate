import test from 'node:test'
import assert from 'node:assert/strict'
import { ensureNewGraph, formIssue, newTarget, nodeIsUsed, optional, renameNode, trafficMode, validateDocument } from './graph-editor.js'

function target() {
  const doc = newTarget('test-app')
  doc.graph = 'provider-api'
  doc.nodes.limit.egress = { queue: 'test-app.provider.out', group: 'workers' }
  return doc
}

test('the wizard draft becomes a valid one-node graph once its identity and output are set', () => {
  assert.deepEqual(validateDocument(target()), [])
  const empty = newTarget()
  assert.ok(validateDocument(empty).some((e) => e.path === 'graph' && e.step === 'identity'))
  assert.ok(validateDocument(empty).some((e) => e.path === 'nodes.limit.egress' && e.step === 'routing'))
})

test('node rename updates every path and fan-out without losing advanced configuration', () => {
  const doc = target()
  doc.nodes.limit.ingress = { queue: 'existing.in', http: false, shed: false }
  doc.nodes.limit.cost = { path: 'payload.cost', default: 1, max: 10 }
  doc.nodes.limit.budgets[0] = { ...doc.nodes.limit.budgets[0], sharedKey: 'provider-account', subWindows: 2, source: 'observations', asOf: '2026-10-08' }
  doc.nodes.other = { budgets: [{ count: 100, timeMs: 1000 }], egress: 'other.out' }
  doc.paths = [{ name: 'main', nodes: ['limit'] }, { name: 'copies', nodes: ['limit', ['other', 'limit']] }]
  const original = structuredClone(doc)
  const renamed = renameNode(doc, 'limit', 'provider')
  assert.deepEqual(doc, original)
  assert.deepEqual(renamed.nodes.provider, original.nodes.limit)
  assert.deepEqual(renamed.paths[0].nodes, ['provider'])
  assert.deepEqual(renamed.paths[1].nodes, ['provider', ['other', 'provider']])
  assert.ok(nodeIsUsed(renamed, 'provider'))
  assert.ok(nodeIsUsed(renamed, 'other'))
  assert.throws(() => renameNode(renamed, 'provider', 'other'), /already exists/)
})

test('editing an optional budget setting preserves false flags and untouched fields', () => {
  const original = { count: 100, timeMs: 1001, whenOp: ['messages.*'], sharedKey: 'account', confidence: 'inferred', source: 'measured' }
  const next = optional(original, 'source', '')
  assert.equal(next.timeMs, 1001)
  assert.deepEqual(next.whenOp, ['messages.*'])
  assert.equal(next.sharedKey, 'account')
  assert.equal(original.source, 'measured')
  assert.equal(optional({ http: true }, 'http', false).http, false)
})

test('documented provenance, counter identity, subdivisions and item costs are checked together', () => {
  const doc = target()
  doc.nodes.limit.cost = 4
  doc.nodes.limit.budgets = [{ id: 'same', count: 5, timeMs: 1000, subWindows: 2, confidence: 'documented' }, { id: 'same', count: 100, timeMs: 1000, scopeBy: 'accountId', whenOp: [] }]
  const messages = validateDocument(doc).map((e) => e.message).join('\n')
  assert.match(messages, /source and verification date/)
  assert.match(messages, /unique ID/)
  assert.match(messages, /maximum cost/)
  assert.match(messages, /payload path/)
  assert.match(messages, /operation pattern/)
  doc.maxAttempts = 21
  assert.ok(validateDocument(doc).some((e) => e.path === 'maxAttempts'))
})

test('route checks catch cycles, missing nodes and non-terminal fan-outs', () => {
  const doc = target()
  doc.nodes.other = { ingress: true, budgets: [{ count: 100, timeMs: 1000 }], egress: 'other.out' }
  doc.paths = [{ name: 'forward', nodes: ['limit', 'other'] }, { name: 'return', nodes: ['other', 'limit'] }]
  assert.ok(validateDocument(doc).some((e) => /cycle/.test(e.message)))
  doc.paths = [{ name: 'invalid', nodes: ['limit', ['other', 'missing'], 'limit'] }]
  assert.ok(validateDocument(doc).some((e) => /last hop/.test(e.message)))
  assert.ok(validateDocument(doc).some((e) => /missing does not exist/.test(e.message)))
})

test('JSON with unsupported structures remains in JSON instead of entering an unsafe form', () => {
  for (const value of [null, [], { nodes: {}, paths: null }, { nodes: { n: { budgets: [null] } }, paths: [] }, { nodes: { n: { budgets: [], ingress: 'queue' } }, paths: [] }]) assert.ok(formIssue(value))
  const doc = target()
  doc.nodes.limit.futureField = { untouched: true }
  assert.equal(formIssue(doc), '')
  assert.deepEqual(renameNode(doc, 'limit', 'renamed').nodes.renamed.futureField, { untouched: true })
})

test('creation rejects existing graphs and API failures; only a 404 permits a new graph', async () => {
  const error = (status) => Object.assign(new Error('API failure'), { status })
  await assert.rejects(ensureNewGraph({ get: async () => ({ spec: target() }) }, 'test-app', 'provider-api'), /already has/)
  for (const status of [401, 403, 500]) await assert.rejects(ensureNewGraph({ get: async () => { throw error(status) } }, 'test-app', 'provider-api'), /API failure/)
  await ensureNewGraph({ get: async () => { throw error(404) } }, 'test-app', 'new-name')
})

test('watch accepts no limits, requires a bounded duration, and activation requires an explicit allowance', () => {
  const doc = trafficMode(target(), true)
  doc.nodes.limit.budgets = []
  assert.deepEqual(validateDocument(doc), [])
  assert.deepEqual(doc.counters, { windowSeconds: 60 })
  for (const value of [0, 60, 119, 86400 * 31, 120.5]) {
    doc.watch.durationSeconds = value
    assert.ok(validateDocument(doc).some((e) => e.path === 'watch.durationSeconds'))
  }
  doc.watch.durationSeconds = 86400
  const enforced = trafficMode(doc, false, true)
  assert.equal(enforced.version, doc.version + 1)
  assert.equal(enforced.watch, undefined)
  assert.equal(enforced.nodes.limit.budgets[0].count, '')
  assert.ok(validateDocument(enforced).some((e) => /allowance/.test(e.message)))
  assert.ok(doc.watch)
  assert.deepEqual(doc.nodes.limit.budgets, [])
})

test('changing traffic mode preserves existing limits, queues and cost configuration', () => {
  const original = target()
  original.nodes.limit.cost = { path: 'payload.cost', default: 2, max: 10 }
  const watching = trafficMode(original, true, true)
  watching.watch.startedAt = 123456
  assert.deepEqual(watching.nodes, original.nodes)
  assert.equal(watching.version, 2)
  assert.equal(trafficMode(watching, true, true), watching)
  assert.deepEqual(trafficMode(watching, false, true).nodes, original.nodes)
  assert.ok(formIssue({ ...watching, watch: true }))
})
