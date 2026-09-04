import assert from 'node:assert/strict'
import test from 'node:test'

import { singleFlight } from '../src/lib/poll.js'

const nextTurn = () => new Promise((resolve) => setImmediate(resolve))

test('singleFlight serialises calls and coalesces a pending refresh', async () => {
  let calls = 0
  let active = 0
  let peak = 0
  const releases = []
  const run = singleFlight(async () => {
    calls += 1
    active += 1
    peak = Math.max(peak, active)
    await new Promise((resolve) => releases.push(resolve))
    active -= 1
  })

  const first = run()
  await nextTurn()
  const second = run()
  const third = run()

  assert.equal(calls, 1)
  assert.equal(first, second)
  assert.equal(second, third)

  releases.shift()()
  await nextTurn()
  assert.equal(calls, 2)
  assert.equal(peak, 1)

  releases.shift()()
  await first
  assert.equal(active, 0)
})

test('singleFlight accepts a new refresh after a failed one', async () => {
  let calls = 0
  const run = singleFlight(async () => {
    calls += 1
    if (calls === 1) throw new Error('temporary failure')
  })

  await assert.rejects(run(), /temporary failure/)
  await assert.doesNotReject(run())
  assert.equal(calls, 2)
})
