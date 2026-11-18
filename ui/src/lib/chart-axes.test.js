import { test } from 'node:test'
import assert from 'node:assert/strict'
import { yScale, timeTicks, duration } from './chart-axes.js'

test('y scale rounds the maximum up to a round step', () => {
  assert.deepEqual(yScale([3, 216, 40]), { max: 250, ticks: [0, 50, 100, 150, 200, 250] })
})

test('y scale never shows fractions for whole counts', () => {
  assert.deepEqual(yScale([0, 2, 1]).ticks, [0, 1, 2])
  assert.deepEqual(yScale([0, 1]).ticks, [0, 1])
})

test('y scale keeps fractional steps for rates', () => {
  assert.deepEqual(yScale([0.3, 0.7]).ticks, [0, 0.2, 0.4, 0.6, 0.8])
})

test('y scale with no traffic still has an axis', () => {
  assert.deepEqual(yScale([0, 0]), { max: 1, ticks: [0, 1] })
})

test('duration scale steps on clock units', () => {
  assert.deepEqual(yScale([5007], true).ticks, [0, 1800, 3600, 5400])
})

test('time ticks align to the clock and stay few', () => {
  const t0 = Date.UTC(2026, 10, 18, 10, 41), t1 = Date.UTC(2026, 10, 18, 11, 40)
  const ticks = timeTicks(t0, t1)
  assert.equal(ticks.length, 4)
  assert.equal(ticks[0], Date.UTC(2026, 10, 18, 10, 45))
  assert.deepEqual(timeTicks(t1, t0), [])
})

test('duration labels', () => {
  assert.equal(duration(45), '45s')
  assert.equal(duration(90), '1m 30s')
  assert.equal(duration(5400), '1h 30m')
  assert.equal(duration(3600), '1h')
})
