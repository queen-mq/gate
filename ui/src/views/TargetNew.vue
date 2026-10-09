<script setup>
import { computed, onMounted, ref, watch, nextTick } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import PageHeader from '../components/PageHeader.vue'
import Field from '../components/Field.vue'
import Icon from '../components/Icon.vue'
import NodeFields from '../components/editor/NodeFields.vue'
import WatchSettings from '../components/editor/WatchSettings.vue'
import { api, DEFAULT_APP, graphPath, isAdmin, READ_ONLY_NOTE, window as windowMs } from '../lib/api.js'
import { templates, templateTarget } from '../lib/target-setup.js'
import { ensureNewGraph, NAME_HINT, trafficMode, validateDocument } from '../lib/graph-editor.js'

const route = useRoute(), router = useRouter()
const draft = ref(templateTarget('global', typeof route.query.app === 'string' ? route.query.app : DEFAULT_APP))
const steps = computed(() => [{ key: 'identity', title: 'Identity', hint: 'Choose what you want to monitor or limit.' }, { key: 'routing', title: 'Queues', hint: 'Connect incoming work to your workers.' }, { key: 'limits', title: draft.value.watch ? 'Observation' : 'Rate limits', hint: draft.value.watch ? 'Choose how long to observe before reviewing limits.' : 'Set the allowances every item must meet.' }, { key: 'review', title: 'Review', hint: 'Check the target before creating it.' }])
const template = ref('global')
function chooseTemplate(id) {
  template.value = id
  const next = templateTarget(id, draft.value.application)
  draft.value = { ...draft.value, nodes: { limit: { ...node.value, budgets: next.nodes.limit.budgets } } }
  limitsTouched.value = true
}
const limitsTouched = ref(false)
const step = ref(0), attempted = ref(false), busy = ref(false), error = ref(''), created = ref(false)
const warnings = ref([]), apps = ref([]), showJson = ref(false), heading = ref(null)
const node = computed(() => draft.value.nodes.limit)
const checks = computed(() => validateDocument(draft.value))
const visibleChecks = computed(() => checks.value.filter((check) => step.value === 3 || check.step === steps.value[step.value].key))
const outputQueue = computed(() => typeof node.value.egress === 'string' ? node.value.egress : node.value.egress?.queue)
const inputQueue = computed(() => typeof node.value.ingress === 'object' && node.value.ingress.queue !== undefined ? node.value.ingress.queue : `gate.${draft.value.application}.${draft.value.graph}.limit.ingress`)
onMounted(async () => {
  try { apps.value = (await api.get('/api/apps')) ?? [] } catch { /* Suggestions are optional. */ }
})
watch(step, async () => { attempted.value = false; error.value = ''; await nextTick(); heading.value?.focus() })
function updateNode(value) { if (step.value === 2) limitsTouched.value = true; draft.value = { ...draft.value, nodes: { ...draft.value.nodes, limit: value } } }
function chooseMode(event) {
  const watching = event.target.value === 'watch'
  draft.value = trafficMode(draft.value, watching)
  if (watching && !limitsTouched.value) updateNode({ ...node.value, budgets: [] })
}
function next() {
  if (!isAdmin.value || busy.value || created.value) return
  attempted.value = true
  if (!visibleChecks.value.length && step.value < 3) step.value++
}
async function create() {
  if (!isAdmin.value || busy.value || created.value || step.value !== 3) return
  attempted.value = true
  if (checks.value.length) return
  error.value = ''
  busy.value = true
  try {
    const { application, graph } = draft.value
    await ensureNewGraph(api, application, graph)
    const response = await api.put(`/v1/apps/${encodeURIComponent(application)}/graphs/${encodeURIComponent(graph)}`, draft.value)
    warnings.value = response?.warnings ?? []
    created.value = true
    if (!warnings.value.length) router.push({ path: graphPath(application, graph), query: { setup: '1' } })
  } catch (failure) { error.value = failure.message }
  finally { busy.value = false }
}
</script>

<template>
  <div class="target-wizard">
    <PageHeader title="New target" sub="Set up a target one step at a time: choose its application, connect its queues, then watch traffic or enforce rate limits." :crumbs="[{ to: '/targets', label: 'Targets' }]">
      <template #actions><RouterLink to="/targets" class="btn">Cancel</RouterLink></template>
    </PageHeader>
    <p v-if="!isAdmin" class="text-fg-3 mb-5">{{ READ_ONLY_NOTE }}</p>
    <nav aria-label="Target creation progress" class="wizard-steps">
      <button v-for="(item, i) in steps" :key="item.key" type="button" :class="{ current: step === i, complete: step > i }" :aria-current="step === i ? 'step' : undefined" :disabled="i > step || busy || created" @click="step = i">
        <span class="wizard-step-number"><Icon v-if="i < step" name="check" :size="13" /><template v-else>{{ i + 1 }}</template></span>
        <span>{{ item.title }}</span>
      </button>
    </nav>
    <div v-if="created" class="card editor-section mb-5" role="status">
      <h2 class="editor-section-title">Target created</h2>
      <p class="hint mt-2">{{ draft.application }} / {{ draft.graph }} is ready.</p>
      <ul v-if="warnings.length" class="mt-3 list-disc pl-5 text-warn space-y-1"><li v-for="(warning, i) in warnings" :key="i">{{ warning }}</li></ul>
      <RouterLink :to="{ path: graphPath(draft.application, draft.graph), query: { setup: '1' } }" class="btn btn-primary mt-4">Open target <Icon name="chevron" :size="13" /></RouterLink>
    </div>
    <div v-if="error" role="alert" class="editor-errors mb-5"><p v-for="(line, i) in error.split('; ')" :key="i">{{ line }}</p></div>
    <div v-if="attempted && visibleChecks.length" role="alert" class="editor-errors mb-5"><p class="font-semibold mb-2">Complete these fields to continue</p><ul class="list-disc pl-5 space-y-1"><li v-for="(check, i) in visibleChecks" :key="i">{{ check.message }}</li></ul></div>

    <form class="card editor-section" @submit.prevent="step === 3 ? create() : next()">
      <div class="mb-6">
        <p class="text-[11px] uppercase tracking-wider text-fg-3 mb-2">Step {{ step + 1 }} of 4</p>
        <h2 ref="heading" tabindex="-1" class="editor-section-title text-[18px] outline-none">{{ steps[step].title }}</h2>
        <p class="hint mt-1">{{ steps[step].hint }}</p>
      </div>
      <fieldset :disabled="!isAdmin || busy || created" class="min-w-0">
        <div v-if="step === 0" class="grid sm:grid-cols-2 gap-5">
          <div class="sm:col-span-2">
            <h3 class="label mb-3">Start from a template</h3>
            <div class="grid sm:grid-cols-2 gap-3" role="group" aria-label="Target template">
              <button v-for="item in templates" :key="item.id" type="button" class="template-option" :class="{ selected: template === item.id }" :aria-pressed="template === item.id" @click="chooseTemplate(item.id)">
                <strong>{{ item.name }}</strong><span>{{ item.description }}</span>
              </button>
            </div>
            <p class="hint mt-2">Example limits only. Review the amounts, account field, operation patterns and shared key before creating.</p>
          </div>
          <Field label="Application" for="wizard-application" :hint="`Select an existing application or enter a new name. ${NAME_HINT}`" :error="attempted ? visibleChecks.find(c => c.path === 'application')?.message : null">
            <input id="wizard-application" v-model="draft.application" class="input font-mono" list="wizard-applications" autocomplete="off" />
            <datalist id="wizard-applications"><option v-for="app in apps" :key="app.application" :value="app.application" /></datalist>
          </Field>
          <Field label="Target name" for="wizard-target" :hint="NAME_HINT" :error="attempted ? visibleChecks.find(c => c.path === 'graph')?.message : null">
            <input id="wizard-target" v-model="draft.graph" class="input font-mono" placeholder="provider-api" autocomplete="off" />
          </Field>
          <Field label="Traffic mode" for="wizard-mode" class="sm:col-span-2" hint="Watch traffic first, or start enforcing limits immediately.">
            <select id="wizard-mode" class="input" :value="draft.watch ? 'watch' : 'enforce'" @change="chooseMode"><option value="enforce">Enforce limits</option><option value="watch">Watch traffic</option></select>
          </Field>
          <p class="editor-note sm:col-span-2">A target belongs to one application. Its limits and queues are separate from other applications, even when their targets have the same name.</p>
        </div>
        <NodeFields v-else-if="step === 1" :model-value="node" :sections="['routing']" :watching="!!draft.watch" standalone :queue-prefix="`gate.${draft.application}.${draft.graph}.limit`" @update:model-value="updateNode" />
        <WatchSettings v-else-if="step === 2 && draft.watch" :model-value="draft.watch" @update:model-value="draft.watch = $event" />
        <NodeFields v-else-if="step === 2" :model-value="node" :sections="['limits']" @update:model-value="updateNode" />
        <div v-else class="space-y-5">
          <div class="wizard-review">
            <div><span>Traffic mode</span><strong>{{ draft.watch ? 'Watch traffic · no limits enforced' : 'Enforce limits' }}</strong></div>
            <div v-if="draft.watch"><span>Observation period</span><strong>{{ windowMs(draft.watch.durationSeconds * 1000) }}</strong></div>
            <div><span>Application</span><strong class="font-mono">{{ draft.application }}</strong></div>
            <div><span>Target</span><strong class="font-mono">{{ draft.graph }}</strong></div>
            <div><span>Incoming queue</span><strong class="font-mono break-all">{{ inputQueue }}</strong></div>
            <div><span>Outgoing queue</span><strong class="font-mono break-all">{{ outputQueue }}</strong></div>
          </div>
          <div v-if="!draft.watch" class="space-y-2">
            <h3 class="font-semibold">Rate limits</h3>
            <div v-for="(limit, i) in node.budgets" :key="i" class="editor-budget flex flex-wrap gap-x-4 gap-y-1 items-center">
              <strong class="font-mono text-xs">{{ limit.id ?? `b${i}` }}</strong><span>{{ limit.count }} cost units / {{ windowMs(limit.timeMs) }}</span><span class="chip">{{ limit.confidence ?? 'inferred' }}</span>
              <p v-if="limit.scopeBy || limit.whenOp || limit.sharedKey || limit.subWindows" class="w-full text-xs text-fg-3">{{ [limit.subWindows ? `${limit.subWindows} subdivisions` : '', limit.scopeBy ? `per ${limit.scopeBy}` : '', limit.whenOp?.length ? `operations: ${limit.whenOp.join(', ')}` : '', limit.sharedKey ? `shared: ${limit.sharedKey}` : ''].filter(Boolean).join(' · ') }}</p>
            </div>
          </div>
          <label class="flex items-center gap-2"><input type="checkbox" :checked="!!draft.counters || !!draft.watch" :disabled="!!draft.watch" @change="$event.target.checked ? draft.counters = { windowSeconds: 60 } : delete draft.counters" /> Enable one-minute history counters</label>
          <p class="editor-note">{{ draft.watch ? 'Creating this target starts observation and relays traffic without rate limits. The timer never activates limits automatically.' : 'Creating this target provisions its queues and starts enforcing its limits.' }} Your workers consume the outgoing queue.</p>
          <button type="button" class="editor-text-button" :aria-expanded="showJson" @click="showJson = !showJson">{{ showJson ? 'Hide' : 'Show' }} generated JSON</button>
          <pre v-if="showJson" class="editor-json-preview" aria-label="Generated target JSON">{{ JSON.stringify(draft, null, 2) }}</pre>
        </div>
      </fieldset>
      <footer class="wizard-footer">
        <button v-if="step > 0" type="button" class="btn" :disabled="busy || created" @click="step--">Back</button>
        <RouterLink v-else to="/graphs/new" class="editor-text-button">Need several nodes? Create a graph</RouterLink>
        <button type="submit" class="btn btn-primary ml-auto" :disabled="!isAdmin || busy || created"><Icon v-if="step === 3" name="check" :size="14" />{{ busy ? 'Creating…' : created ? 'Created' : step === 3 ? 'Create target' : 'Continue' }}<Icon v-if="step < 3" name="chevron" :size="13" /></button>
      </footer>
    </form>
  </div>
</template>
