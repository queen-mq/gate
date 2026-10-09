import test from 'node:test'
import assert from 'node:assert/strict'
import { incomingRate, timelineSeries } from './insights-series.js'
const sample = (t, positions, extra = {}) => ({ node:'n', t, at:t, value: { available:true, version:1, positions, before:3, workers:8, oldestSeconds:60, ...extra } })
test('incoming estimates require comparable, adjacent broker samples', () => {
  assert.equal(incomingRate(sample(0, { p:100 }), sample(60000,{ p:160 })),60)
  assert.equal(incomingRate(sample(0, { p:100 }), sample(60000,{ p:3 })),null)
  assert.equal(incomingRate(sample(0, { p:100 }), sample(120000,{ p:160 })),null)
  assert.equal(incomingRate(sample(0, { p:100 }), sample(60000,{ p:160, q:5 })),null)
  assert.equal(incomingRate(sample(0, { p:100 }), sample(60000,{ p:160 }, { available:false })),null)
  assert.equal(incomingRate(sample(0, { p:100 }), sample(60000,{ p:160 }, { version:2 })),null)
})
test('charts preserve missing ages and outage gaps while showing measured idle time', () => {
  const data = { samples:[sample(0,{ p:0 }), sample(60000,{ p:0 },{ oldestSeconds:null }),sample(180000,{ p:5 })], relayed:{ n:[{ t:0, total:{ admitted:7 } }] } }
  const rows = timelineSeries(data,'n',0,180000)
  assert.equal(rows[0].relayed,7); assert.equal(rows[0].incoming,0)
  assert.equal(rows[1].oldest,null); assert.equal(rows[2].before,null)
  assert.equal(rows[1].relayed,null); assert.equal(rows[2].incoming,null)
})
