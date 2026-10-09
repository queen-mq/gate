import test from 'node:test'
import assert from 'node:assert/strict'
import { aiSession, resetAiSession, stageAiDraft, readAiDraft, forgetAiDraft, completeAiDraft } from './ai-session.js'

test('review receives an isolated draft and cannot mutate the conversation', () => {
  resetAiSession('demo')
  const doc = { application: 'demo', graph: 'api', nodes: { limit: { budgets: [{ count: 100 }] } } }
  stageAiDraft(doc, 'review')
  doc.nodes.limit.budgets[0].count = 200
  const copy = readAiDraft('review')
  assert.equal(copy.nodes.limit.budgets[0].count, 100)
  copy.graph = 'changed'
  assert.equal(readAiDraft('review').graph, 'api')
  forgetAiDraft('review')
  assert.equal(readAiDraft('review'), null)
})

test('new conversations and sign-out discard previous drafts and messages', () => {
  stageAiDraft({ application: 'private-team' }, 'old')
  aiSession.messages.push({ role: 'user', content: 'private requirements' })
  aiSession.draft = { graph: 'old' }
  resetAiSession('other-team')
  assert.equal(aiSession.application, 'other-team')
  assert.deepEqual(aiSession.messages, [])
  assert.equal(aiSession.draft, null)
  assert.equal(aiSession.created, false)
  assert.equal(readAiDraft('old'), null)
})

test('saving a review records the edited target without replacing a newer draft', () => {
  resetAiSession('demo')
  aiSession.draft = { application: 'demo', graph: 'proposal' }
  stageAiDraft(aiSession.draft, 'edited')
  completeAiDraft('edited', { application: 'demo', graph: 'saved-name' })
  assert.equal(aiSession.created, true)
  assert.equal(aiSession.draft.graph, 'saved-name')
  assert.equal(readAiDraft('edited'), null)

  resetAiSession('demo')
  aiSession.draft = { graph: 'first' }
  stageAiDraft(aiSession.draft, 'stale')
  aiSession.draft = { graph: 'newer' }
  completeAiDraft('stale', { graph: 'first' })
  assert.equal(aiSession.created, false)
  assert.equal(aiSession.draft.graph, 'newer')
})
