import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  targetKey,
  targetUsage,
  breachMatches,
  worstBudget,
  graphEdges,
  graphModel,
  layoutTopology,
  flowingNodes,
} from './dashboard.js'

test('target identity and node backoffs respect the owning application', () => {
  const target = { application: 'channel', name: 'airbnb' }
  assert.notEqual(
    targetKey(target),
    targetKey({ ...target, application: 'payments' }),
  )
  assert.equal(
    breachMatches({ application: 'channel', target: 'airbnb.ip' }, target),
    true,
  )
  assert.equal(
    breachMatches({ application: 'payments', target: 'airbnb.ip' }, target),
    false,
  )
  assert.equal(breachMatches({ target: 'channel/airbnb.ip' }, target), true)
  assert.equal(breachMatches({ target: 'airbnb-other.ip' }, target), false)
})

test('missing budget data remains unknown while an observed zero is zero', () => {
  assert.equal(targetUsage({ worst_used: 0, worst_cap: 100 }), 0)
  assert.equal(targetUsage({ worst_used: null, worst_cap: 100 }), null)
  assert.equal(targetUsage({ worst_used: 50, worst_cap: 0 }), null)
  assert.equal(
    worstBudget([
      { id: 'scoped', scopeBy: 'id', utilisation: 0.99 },
      { id: 'conditional', whenOp: ['delete'], utilisation: 1 },
      { id: 'missing', utilisation: null },
    ]),
    null,
  )
  assert.equal(
    worstBudget([
      { id: 'zero', utilisation: 0 },
      { id: 'busy', utilisation: 0.7 },
    ]).id,
    'busy',
  )
})

test('fan-out paths preserve branch edges and shared path memberships', () => {
  const edges = graphEdges([
    { name: 'p1', hops: ['a', '[b, c]', 'd'] },
    { name: 'p2', hops: ['a', ['b', 'c'], 'd'] },
  ])
  assert.equal(edges.length, 4)
  assert.deepEqual(edges.find((e) => e.from === 'a' && e.to === 'c').paths, [
    'p1',
    'p2',
  ])
  const nodes = ['a', 'b', 'c', 'd', 'disconnected'].map((id) => ({ id }))
  const layout = layoutTopology(nodes, edges)
  const node = (id) => layout.nodes.find((n) => n.id === id)
  assert.ok(node('a').x < node('b').x && node('b').x < node('d').x)
  assert.equal(node('b').x, node('c').x)
  assert.notEqual(node('b').y, node('c').y)
  assert.equal(layout.cyclic, false)
  assert.ok(
    layout.nodes.every(
      (n) =>
        n.x >= 0 && n.x <= layout.width && n.y >= 0 && n.y <= layout.height,
    ),
  )
})

test('empty, single-node and malformed cyclic layouts terminate with valid bounds', () => {
  assert.equal(layoutTopology([], []).nodes.length, 0)
  assert.equal(layoutTopology([{ id: 'only' }], []).nodes[0].x, 360)
  const cyclic = layoutTopology(
    [{ id: 'a' }, { id: 'b' }],
    [
      { from: 'a', to: 'b' },
      { from: 'b', to: 'a' },
    ],
  )
  assert.equal(cyclic.cyclic, true)
  assert.ok(Number.isFinite(cyclic.width) && Number.isFinite(cyclic.height))
})

test('graph models expose real egress queues without inventing measurements', () => {
  const result = graphModel({
    running: true,
    nodes: [
      {
        node: 'ip',
        ingressQueue: 'in',
        egressQueue: 'actual.out',
        breaker: { retryAfterSeconds: 30 },
        waiting_for_workers: 17,
        budgets: [{ id: 'scope', scopeBy: 'listing', utilisation: null }],
      },
    ],
    paths: [{ name: 'main', hops: ['ip'] }],
    stages: [],
  })
  assert.equal(result.nodes.length, 2)
  assert.equal(result.nodes[0].usage, null)
  assert.equal(result.nodes[0].admitted, null)
  assert.equal(result.nodes[1].name, 'actual.out')
  assert.equal(result.nodes[1].waiting_for_workers, 17)
  assert.equal(result.nodes[1].waiting_for_budget, null)
  assert.equal(result.nodes[1].breaker, null)
  assert.equal(result.edges[0].from, 'ip')
})

test('motion requires a recent counter increase on the same running graph', () => {
  const graph = (count, extras = {}) => ({
    application: 'channel',
    graph: 'airbnb',
    version: 1,
    running: true,
    nodes: [{ node: 'ip' }],
    stages: [
      {
        node: 'ip',
        path: 'p',
        hop: 0,
        source: 'in',
        counters: { forwarded: count },
      },
    ],
    ...extras,
  })
  assert.deepEqual(flowingNodes(null, graph(10)), [])
  assert.deepEqual(flowingNodes(graph(10), graph(20)), ['ip'])
  assert.deepEqual(flowingNodes(graph(20), graph(20)), [])
  assert.deepEqual(flowingNodes(graph(20), graph(2)), [])
  assert.deepEqual(flowingNodes(graph(10), graph(20, { running: false })), [])
  assert.deepEqual(
    flowingNodes(graph(10), graph(20, { application: 'another-team' })),
    [],
  )
  assert.deepEqual(flowingNodes(graph(10), graph(20, { version: 2 })), [])
  assert.deepEqual(
    flowingNodes(
      graph(10),
      graph(20, { nodes: [{ node: 'ip', breaker: {} }] }),
    ),
    [],
  )
})
