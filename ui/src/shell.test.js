// src/shell.css is Queen's console shell, copied by `npm run sync:shell`. Its first
// line holds the SHA-256 of the rest: a hand edit here would split the three consoles.
import { createHash } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { test } from 'node:test'
import assert from 'node:assert/strict'

test('src/shell.css is an unedited copy of Queen\'s shell', () => {
  const [first, ...rest] = readFileSync(new URL('./shell.css', import.meta.url), 'utf8').split('\n')
  const recorded = /sha256:([0-9a-f]{64})/.exec(first)?.[1]
  assert.ok(recorded, 'the first line must record the sha256 written by npm run sync:shell')
  assert.equal(createHash('sha256').update(rest.join('\n')).digest('hex'), recorded,
    'src/shell.css was edited by hand: change app/src/shell.css in Queen, then run npm run sync:shell')
})
