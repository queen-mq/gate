<script setup>
import { computed, nextTick, onUnmounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import Icon from './Icon.vue'
import { api, graphPath, isAdmin, READ_ONLY_NOTE } from '../lib/api.js'
import { aiSession as session, aiPanel, closeAiAgent, resetAiSession, stageAiDraft } from '../lib/ai-session.js'

const router = useRouter()
const dialog = ref(null)
const view = ref('conversation')
const status = ref(null)
const statusError = ref('')
const error = ref('')
const prompt = ref('')
const busy = ref(false)
const transcript = ref(null)
const promptField = ref(null)
const showJson = ref(false)
const providerName = computed(() => status.value?.provider || 'the configured AI provider')
let controller
let pendingId
let active = true
const applications = ref([])
const examples = [
  { label: 'A global limit', text: 'Create a target named provider-api with its own ingress, queue provider.jobs.out for workers, and a global limit of 100 requests per second.' },
  { label: 'A limit per account', text: 'Create a target named account-api with its own ingress and queue account.jobs.out. Allow 100 requests per second overall and 10 per second for each payload.accountId.' },
  { label: 'Observe before limiting', text: 'Create a target named traffic-study with its own ingress and queue traffic.jobs.out. I want to observe traffic in Watch mode for 24 hours before choosing limits.' },
]
const canSend = computed(() => isAdmin.value && status.value?.enabled && !busy.value && !session.created && prompt.value.trim() && session.messages.length < 21)
const nodes = computed(() => Object.entries(session.draft?.nodes ?? {}))
const json = computed(() => JSON.stringify(session.draft, null, 2))

async function loadStatus() {
  statusError.value = ''
  try { status.value = await api.get('/api/ai/status') }
  catch (e) { statusError.value = e.message }
}
watch(() => aiPanel.open, async (open) => {
  await nextTick()
  if (!active || open !== aiPanel.open) return
  if (!open) { dialog.value?.close(); return }
  dialog.value?.showModal()
  await loadStatus()
  try { applications.value = await api.get('/api/apps') ?? [] } catch {}
}, { immediate: true })
function rollbackPending() {
  if (pendingId && session.messages.at(-1)?.id === pendingId) session.messages.pop()
}
onUnmounted(() => { active = false; rollbackPending(); controller?.abort() })

async function scroll() {
  await nextTick()
  if (!aiPanel.open) return
  transcript.value?.scrollTo({ top: transcript.value.scrollHeight, behavior: 'smooth' })
}
function selectView(name, focus = false) {
  view.value = name
  if (focus) nextTick(() => dialog.value?.querySelector(`#ai-tab-${name}`)?.focus())
}
function tabKey(e) {
  if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(e.key)) return
  e.preventDefault()
  selectView(e.key === 'Home' ? 'conversation' : e.key === 'End' ? 'draft' : view.value === 'draft' ? 'conversation' : 'draft', true)
}
function startOver() {
  controller?.abort()
  resetAiSession(session.application)
  prompt.value = ''
  error.value = ''
  showJson.value = false
  view.value = 'conversation'
  nextTick(() => promptField.value?.focus())
}
function example(text) { prompt.value = text; promptField.value?.focus() }

async function send() {
  if (!canSend.value) return
  const content = prompt.value.trim()
  const messages = [...session.messages.map(({ role, content }) => ({ role, content })), { role: 'user', content }]
  busy.value = true
  error.value = ''
  prompt.value = ''
  pendingId = crypto.randomUUID()
  session.messages.push({ role: 'user', content, id: pendingId })
  controller = new AbortController()
  await scroll()
  try {
    const reply = await api.post('/api/ai/chat', { application: session.application, messages, draft: session.draft }, { signal: controller.signal })
    if (!active) return
    session.messages.push({ role: 'assistant', content: reply.message })
    // A follow-up question does not discard the last valid draft.
    if (reply.draft) {
      session.draft = reply.draft
      session.assumptions = reply.assumptions ?? []
      session.warnings = reply.warnings ?? []
    }
    await scroll()
  } catch (e) {
    // Restore a failed turn so Retry does not duplicate it in model context.
    rollbackPending()
    if (active) { prompt.value = content; error.value = e.name === 'AbortError' ? 'Request cancelled.' : e.message }
  } finally {
    busy.value = false
    if (active && aiPanel.open && view.value === 'conversation') promptField.value?.focus()
  }
}
function review() {
  if (!session.draft || session.created || busy.value || !isAdmin.value) return
  const id = stageAiDraft(session.draft)
  closeAiAgent()
  router.push({ path: '/graphs/new', query: { aiDraft: id } })
}
</script>

<template>
  <dialog id="gate-ai-chat" ref="dialog" class="ai-chat-dialog" aria-labelledby="ai-chat-title"
          @cancel.prevent="closeAiAgent" @close="closeAiAgent" @click="($event.target === dialog) && closeAiAgent()">
    <header class="ai-panel-header">
      <span class="ai-agent-avatar"><Icon name="sparkles" :size="18" /></span>
      <div class="min-w-0 flex-1"><h2 id="ai-chat-title">AI agent</h2><p>{{ status?.enabled ? providerName + ' · ' + status.model : 'Build a configuration with Gate' }}</p></div>
      <button type="button" class="icon-button" :disabled="busy || !session.messages.length" aria-label="New conversation" title="New conversation" @click="startOver"><Icon name="plus" :size="16" /></button>
      <button type="button" class="icon-button" aria-label="Close AI chat" title="Close chat" autofocus @click="closeAiAgent"><Icon name="x" :size="17" /></button>
    </header>
    <div class="ai-panel-tabs" role="tablist" aria-label="AI workspace" @keydown="tabKey">
      <button id="ai-tab-conversation" role="tab" :aria-selected="view === 'conversation'" aria-controls="ai-conversation" :tabindex="view === 'conversation' ? 0 : -1" @click="selectView('conversation')">Conversation</button>
      <button id="ai-tab-draft" role="tab" :aria-selected="view === 'draft'" aria-controls="ai-draft" :tabindex="view === 'draft' ? 0 : -1" @click="selectView('draft')">Draft <span v-if="session.draft" class="ai-tab-dot" /></button>
    </div>

    <p v-if="!isAdmin" class="ai-notice">{{ READ_ONLY_NOTE }}</p>
    <div v-if="statusError" class="ai-notice" role="alert">{{ statusError }} <button class="btn ml-3" @click="loadStatus">Retry</button></div>
    <div v-else-if="status && !status.enabled" class="ai-notice" role="status">
      <Icon name="key" :size="18" />
      <div><p class="font-medium">Connect {{ providerName }} to start</p><p class="text-fg-2 mt-1">An administrator needs to configure the server's {{ providerName }} API key. You can still create a target with the guided wizard.</p></div>
      <RouterLink to="/targets/new" class="btn shrink-0" @click="closeAiAgent">Open wizard <Icon name="chevron" :size="13" /></RouterLink>
    </div>

      <section v-show="view === 'conversation'" id="ai-conversation" class="ai-conversation" role="tabpanel" aria-labelledby="ai-tab-conversation">
        <div class="ai-chat-header">
          <p class="flex-1 text-xs text-fg-2">Describe your queues and limits.<br />Review the draft before creating it.</p>
          <label class="text-xs text-fg-3" for="ai-application">Application
            <input id="ai-application" v-model="session.application" list="ai-applications" class="input mt-1" :disabled="busy || session.messages.length > 0 || !isAdmin" maxlength="63" placeholder="default" />
            <datalist id="ai-applications"><option v-for="app in applications" :key="app.application" :value="app.application" /></datalist>
          </label>
        </div>

        <div ref="transcript" class="ai-transcript" role="log" aria-label="Messages" aria-live="polite" :aria-busy="busy">
          <div v-if="!session.messages.length" class="ai-welcome">
            <span class="ai-welcome-icon"><Icon name="sparkles" :size="27" /></span>
            <h3>What would you like to configure?</h3>
            <p>Tell me where work enters, where it should go, and how quickly it can run. We can also observe traffic before choosing limits.</p>
            <div class="ai-examples">
              <button v-for="item in examples" :key="item.label" class="ai-example" :disabled="!isAdmin || !status?.enabled" @click="example(item.text)"><span>{{ item.label }}</span><Icon name="chevron" :size="14" /></button>
            </div>
          </div>
          <article v-for="(message, i) in session.messages" :key="i" class="ai-message" :class="message.role">
            <div class="ai-message-author"><Icon v-if="message.role === 'assistant'" name="sparkles" :size="13" />{{ message.role === 'user' ? 'You' : 'Gate agent' }}</div>
            <p>{{ message.content }}</p>
          </article>
          <div v-if="busy" class="ai-thinking" role="status"><span class="ai-live-dot" /> Preparing and checking your configuration…</div>
          <button v-if="session.draft && !busy" class="btn w-full" @click="selectView('draft', true)"><Icon name="graph" :size="14" />{{ session.created ? 'View created target' : 'Review configuration draft' }}<Icon name="chevron" :size="13" /></button>
        </div>

        <form class="ai-composer" @submit.prevent="send">
          <p v-if="error" role="alert" class="text-bad text-xs mb-3">{{ error }}</p>
          <p v-if="session.created" class="text-xs text-fg-2 mb-3">Target created. Start a new conversation to configure another target.</p>
          <p v-if="session.messages.length >= 21" class="text-xs text-fg-2 mb-3">This conversation is full. Review your draft or start a new conversation.</p>
          <label for="ai-prompt" class="sr-only">Describe your configuration</label>
          <textarea id="ai-prompt" ref="promptField" v-model="prompt" rows="3" maxlength="8000" class="input" placeholder="For example: 100 requests/second globally and 10 per account…" :disabled="!isAdmin || !status?.enabled || busy || session.created" @keydown.enter.exact.prevent="!$event.isComposing && send()" />
          <div class="ai-composer-footer"><span>Enter to send · Shift + Enter for a new line</span><button type="submit" class="btn btn-primary" :disabled="!canSend"><Icon name="send" :size="14" />{{ busy ? 'Working…' : 'Send' }}</button></div>
          <p class="ai-data-note">Your messages and draft are sent to {{ providerName }}. Keep credentials out of the conversation. This conversation clears on refresh or sign-out.</p>
        </form>
      </section>

      <section v-show="view === 'draft'" id="ai-draft" class="ai-draft" role="tabpanel" aria-labelledby="ai-tab-draft">
        <div class="ai-draft-header"><h2 class="font-medium text-sm">Configuration draft</h2><span class="chip">{{ session.created ? 'Created' : session.draft ? 'Ready to review' : 'Not created' }}</span></div>
        <div v-if="!session.draft" class="ai-draft-empty"><Icon name="graph" :size="32" /><h3>Your configuration will appear here</h3><p>The agent asks for missing details, checks the configuration with Gate, and prepares it for review.</p><ol><li>Describe the target</li><li>Refine queues and limits</li><li>Review and create</li></ol></div>
        <template v-else>
          <div class="ai-draft-body">
            <p class="text-xs text-fg-3">{{ session.draft.application }}</p>
            <h3 class="font-mono text-lg mt-1 break-all">{{ session.draft.graph }}</h3>
            <span class="chip mt-3">{{ session.draft.watch ? 'Watch · ' + Math.round(session.draft.watch.durationSeconds / 60) + ' minutes' : 'Enforce limits' }}</span>
            <div v-for="[name, node] in nodes" :key="name" class="ai-node">
              <p class="font-medium text-sm">{{ name }}</p>
              <p v-if="node.ingress" class="text-xs text-fg-2 mt-2 break-all">In: {{ node.ingress.queue || 'Gate-owned queue' }}</p>
              <p v-if="node.egress" class="text-xs text-fg-2 mt-1 break-all">Out: {{ node.egress.queue || node.egress }}</p>
              <p v-for="(budget, i) in node.budgets" :key="i" class="text-xs mt-2">{{ budget.count }} units / {{ budget.timeMs / 1000 }}s <span class="text-fg-3">{{ budget.scopeBy ? 'per ' + budget.scopeBy : 'global' }}{{ budget.whenOp ? ' · ' + budget.whenOp.join(', ') : '' }}</span></p>
            </div>
            <div v-if="session.assumptions.length" class="ai-draft-notes"><h4>Assumptions to review</h4><ul><li v-for="(item, i) in session.assumptions" :key="i">{{ item }}</li></ul></div>
            <details v-if="session.warnings.length" class="ai-draft-notes"><summary>Gate notices ({{ session.warnings.length }})</summary><ul><li v-for="(item, i) in session.warnings" :key="i">{{ item }}</li></ul></details>
            <button class="ai-json-toggle" :aria-expanded="showJson" @click="showJson = !showJson"><Icon name="down" :size="12" />{{ showJson ? 'Hide' : 'View' }} JSON</button>
            <pre v-if="showJson" class="ai-json">{{ json }}</pre>
          </div>
          <div v-if="session.created" class="ai-draft-footer"><RouterLink class="btn btn-primary w-full" :to="graphPath(session.draft.application, session.draft.graph)" @click="closeAiAgent">Open created target <Icon name="chevron" :size="14" /></RouterLink><p>Saved through the editor. Start a new conversation to create another target.</p></div>
          <div v-else class="ai-draft-footer"><button class="btn btn-primary w-full" :disabled="busy || !isAdmin" @click="review">Review in editor <Icon name="chevron" :size="14" /></button><p>Nothing has been saved. Queue availability and ownership are checked when you create the graph.</p></div>
        </template>
      </section>
  </dialog>
</template>
