import { reactive } from 'vue'

// In-memory only: a refresh or sign-out clears the conversation and its drafts.
export const aiSession = reactive({ application: 'default', messages: [], draft: null, assumptions: [], warnings: [], created: false })
export const aiPanel = reactive({ open: false })
const drafts = new Map()

export function openAiAgent(application) {
  if (!aiSession.messages.length && application) aiSession.application = application
  aiPanel.open = true
}

export function closeAiAgent() { aiPanel.open = false }

export function resetAiSession(application = 'default') {
  Object.assign(aiSession, { application, messages: [], draft: null, assumptions: [], warnings: [], created: false })
  drafts.clear()
}

export function stageAiDraft(doc, id = crypto.randomUUID()) {
  drafts.set(id, JSON.stringify(doc))
  while (drafts.size > 5) drafts.delete(drafts.keys().next().value)
  return id
}

export function readAiDraft(id) {
  const text = drafts.get(id)
  return text ? JSON.parse(text) : null
}

export function forgetAiDraft(id) { drafts.delete(id) }

export function completeAiDraft(id, savedDocument) {
  // A saved older review must not replace a subsequently refined draft.
  if (drafts.get(id) === JSON.stringify(aiSession.draft)) {
    aiSession.draft = JSON.parse(JSON.stringify(savedDocument))
    aiSession.created = true
  }
  forgetAiDraft(id)
}
