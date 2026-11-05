#!/usr/bin/env node
// Copies Queen's console design (queen-mq/queen: app/src/console/console.css) to src/console.css,
// with one header line holding the SHA-256 of what follows it. The design is Queen's: change it
// there, then run this. A test fails when the copy is edited by hand.
//
//   npm run sync:console                     Queen's master
//   npm run sync:console -- <ref>            a branch, tag or commit of Queen (v2.7.1)
//   npm run sync:console -- --from <file>    a local checkout's app/src/console/console.css
//   npm run sync:console -- --check          exit 1 when Queen's master differs from the copy
import { createHash } from 'node:crypto'
import { readFile, writeFile } from 'node:fs/promises'

const TARGET = new URL('../src/console.css', import.meta.url)
const RAW = 'https://raw.githubusercontent.com/queen-mq/queen'
const PATH = 'app/src/console/console.css'

const sha256 = (text) => createHash('sha256').update(text).digest('hex')
const header = (body) =>
  `/* Queen's console design (queen-mq/queen: ${PATH}), copied by \`npm run sync:console\`. Edit it there. sha256:${sha256(body)} */\n`

async function upstream(ref) {
  const res = await fetch(`${RAW}/${encodeURIComponent(ref)}/${PATH}`)
  if (!res.ok) throw new Error(`GET ${res.url} answered ${res.status}`)
  return res.text()
}

async function main(args) {
  const from = args.indexOf('--from')
  if (args.includes('--check')) {
    const local = (await readFile(TARGET, 'utf8')).replace(/^.*\n/, '')
    if (local !== (await upstream('master'))) {
      console.error('src/console.css differs from Queen\'s master: run `npm run sync:console`.')
      process.exit(1)
    }
    console.log('src/console.css matches Queen\'s master.')
    return
  }
  const body = from >= 0 ? await readFile(args[from + 1], 'utf8') : await upstream(args[0] || 'master')
  if (!body.includes('THE NEW CONSOLE')) throw new Error('that is not the console design')
  await writeFile(TARGET, header(body) + body)
  console.log(`src/console.css written (sha256 ${sha256(body).slice(0, 12)}).`)
}

main(process.argv.slice(2)).catch((err) => {
  console.error(err.message)
  process.exit(1)
})
