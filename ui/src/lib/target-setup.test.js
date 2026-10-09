import test from 'node:test'
import assert from 'node:assert/strict'
import { cloneGraph, templateTarget, integrationExamples } from './target-setup.js'
import { validateDocument } from './graph-editor.js'

test('templates keep an unconditional ceiling and valid configurable examples', () => {
  for (const id of ['global', 'account', 'operation', 'shared']) {
    const doc = templateTarget(id, 'tenant')
    doc.graph = 'target'; doc.nodes.limit.egress = 'worker-out'
    assert.deepEqual(validateDocument(doc), [])
    assert.ok(doc.nodes.limit.budgets.some(b => !b.scopeBy && !b.whenOp))
  }
})
test('cloning isolates routing and identity without losing shared rules or topology', () => {
  const original = templateTarget('shared', 'tenant')
  original.graph = 'original'; original.version = 9
  original.watch = { durationSeconds: 3600, startedAt: 123 }
  original.nodes.limit.ingress = { queue: 'original-in', http: false, partitions: 8 }
  original.nodes.limit.egress = { queue: 'original-out', group: 'workers' }
  const clone = cloneGraph(original)
  assert.equal(clone.graph, ''); assert.equal(clone.version, 1)
  assert.equal(clone.watch.startedAt, undefined)
  assert.equal(clone.nodes.limit.ingress.queue, undefined)
  assert.equal(clone.nodes.limit.egress.queue, '')
  assert.equal(clone.nodes.limit.egress.group, 'workers')
  assert.equal(clone.nodes.limit.budgets[0].sharedKey, 'provider-quota')
  assert.deepEqual(clone.paths, original.paths)
  assert.equal(original.nodes.limit.ingress.queue, 'original-in')
})
test('integration examples use resolved names and Gate HTTP wire format', () => {
  const examples = integrationExamples({ application: 'team', graph: 'api' }, { node: 'n', ingressQueue: 'actual.in', egressQueue: 'actual.out', egressGroup: 'actual-group', httpPush: true })
  assert.match(examples.producer, /queue\("actual.in"\)/)
  assert.match(examples.consumer, /group\("actual-group"\)/)
  assert.match(examples.http, /\/v1\/apps\/team\/graphs\/api\/nodes\/n\/push/)
  assert.match(examples.http, /"payload":/)
  assert.doesNotMatch(examples.http, /"items":/)
  const queueMode = integrationExamples({ application: 'a', graph: 'g' }, { node:'n', egressQueue:'out', httpPush:false })
  assert.equal(queueMode.http, null)
  assert.doesNotMatch(queueMode.consumer, /\.group/)
})

test('examples include nested scope and cost fields and valid shell continuations', () => {
  const graph = { application:'a', graph:'g', spec:{ nodes:{ n:{ budgets:[{ scopeBy:'payload.account.id', whenOp:['calendar.*'] }], cost:{ path:'payload.rooms', default:2 } } } } }
  const { producer, http } = integrationExamples(graph, { node:'n', ingressQueue:'in', httpPush:true })
  assert.match(producer, /"account": \{\s*"id": "example-account"/)
  assert.match(producer, /"rooms": 2/)
  assert.match(producer, /"op": "calendar.example"/)
  assert.doesNotMatch(http, /^\+/m)
})
