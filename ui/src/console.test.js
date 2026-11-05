// src/console.css is Queen's console design, copied by `npm run sync:console`. Its first line
// holds the SHA-256 of the rest: a hand edit here would split the consoles.
import { createHash } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { test } from 'node:test'
import assert from 'node:assert/strict'

test('src/console.css is an unedited copy of Queen\'s console design', () => {
  const [first, ...rest] = readFileSync(new URL('./console.css', import.meta.url), 'utf8').split('\n')
  const recorded = /sha256:([0-9a-f]{64})/.exec(first)?.[1]
  assert.ok(recorded, 'the first line must record the sha256 written by npm run sync:console')
  assert.equal(createHash('sha256').update(rest.join('\n')).digest('hex'), recorded,
    'src/console.css was edited by hand: change app/src/console/console.css in Queen, then run npm run sync:console')
})
