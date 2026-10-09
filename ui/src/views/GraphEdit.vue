<script setup>
// JSON is the shared draft. The form patches it without discarding other fields.
import { ref, computed, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import PageHeader from '../components/PageHeader.vue'
import Icon from '../components/Icon.vue'
import { cloneGraph } from '../lib/target-setup.js'
import GraphForm from '../components/editor/GraphForm.vue'
import { api, isAdmin, READ_ONLY_NOTE, graphApi, graphPath, DEFAULT_APP } from '../lib/api.js'
import { ensureNewGraph, formIssue, newTarget, trafficMode, validateDocument } from '../lib/graph-editor.js'

const props = defineProps({ app: String, name: String })
const router = useRouter()
const route = useRoute()
const application = computed(() => props.app || DEFAULT_APP)
const editing = computed(() => !!props.name)

const text = ref('')
const loading = ref(true)
const busy = ref(false)
const error = ref('')
const conflict = ref('')
const warnings = ref([])
const migration = ref([])
const mode = ref('form')
const attempted = ref(false)
const declared = ref(false)

/* A load that failed leaves nothing safe to declare. Kept apart from `error`,
   which a REFUSED SAVE also sets and which must not disable the button that
   would let the operator try again. */
const loadFailed = ref(false)
let loadToken = 0

async function loadDocument() {
  const token = ++loadToken
  const app = application.value
  const name = props.name
  loading.value = true
  loadFailed.value = false
  error.value = ''
  conflict.value = ''
  warnings.value = []
  migration.value = []
  attempted.value = false
  declared.value = false
  mode.value = 'form'

  if (!name && !route.query.clone) {
    text.value = JSON.stringify(newTarget(app), null, 2)
    loading.value = false
    return
  }
  try {
    const live = await api.get(graphApi(name ? app : String(route.query.app || DEFAULT_APP), name || String(route.query.clone)))
    // Vue Router reuses this component when only route params change. An older
    // response must never replace the document belonging to the new URL.
    if (token !== loadToken) return
    /* `spec` is the stored document verbatim. Editing the VIEW instead would
       hand the server back its own computed fields, and `deny_unknown_fields`
       would refuse every one of them. */
    const original = live?.spec ?? {}
    const doc = !name ? cloneGraph(original) : original.watch && route.query.configureLimits === '1' ? trafficMode(original, false, true) : original
    text.value = JSON.stringify(doc, null, 2)
    if (formIssue(doc)) mode.value = 'json'
  } catch (e) {
    if (token !== loadToken) return
    /* Drop the previous graph's document with it. Leaving it in the textarea
       would put A's nodes, paths and budgets under B's title with Declare still
       enabled — and `as_graph` sets `doc.graph` from the URL, so the server
       would accept that write and replace B. */
    text.value = ''
    loadFailed.value = true
    error.value = e.message
  } finally {
    if (token === loadToken) loading.value = false
  }
}

// `onMounted` alone leaves the previous graph in the editor when navigating
// between two URLs backed by the same route record. Watching the identity also
// covers `/graphs/new` if RouterView elects to reuse the component instance.
watch(() => [props.app, props.name, route.query.configureLimits, route.query.clone, route.query.app], loadDocument, { immediate: true })

const parsed = computed(() => {
  try {
    return { ok: true, value: JSON.parse(text.value) }
  } catch (e) {
    return { ok: false, error: e.message }
  }
})

const graphName = computed(() => props.name || parsed.value.value?.graph || '')
const destinationApp = computed(() => editing.value ? application.value : parsed.value.value?.application ?? DEFAULT_APP)
const formDocument = computed(() => ({ ...parsed.value.value, application: destinationApp.value, graph: graphName.value }))
const formError = computed(() => parsed.value.ok ? formIssue(parsed.value.value) : 'Correct the JSON syntax to open the form. Your draft is kept as entered.')
const checks = computed(() => parsed.value.ok && !formError.value ? validateDocument(formDocument.value) : [])
function updateDocument(doc) { text.value = JSON.stringify(doc, null, 2) }

async function save() {
  if (!isAdmin.value || busy.value || loading.value || loadFailed.value || declared.value) return
  error.value = ''
  conflict.value = ''
  warnings.value = []
  migration.value = []
  attempted.value = true
  if (!parsed.value.ok) {
    error.value = `not JSON: ${parsed.value.error}`
    return
  }
  if (mode.value === 'form' && checks.value.length) return
  const name = graphName.value
  if (!name) {
    error.value = 'the document must name itself: add `"graph": "…"`.'
    return
  }
  busy.value = true
  try {
    const app = destinationApp.value
    if (!editing.value) await ensureNewGraph(api, app, name)
    const res = await api.put(
      `/v1/apps/${encodeURIComponent(app)}/graphs/${encodeURIComponent(name)}`,
      parsed.value.value,
    )
    warnings.value = res?.warnings ?? []
    migration.value = res?.migration ?? []
    // Keep caveats and migration notes visible before opening the saved graph.
    declared.value = true
    if (!migration.value.length && !warnings.value.length) {
      router.push({ path: graphPath(app, name), query: editing.value ? {} : { setup: '1' } })
    }
  } catch (e) {
    if (e.status === 409) conflict.value = e.message
    else error.value = e.message
  } finally {
    busy.value = false
  }
}
</script>

<template>
  <div>
    <p v-if="route.query.clone" class="editor-note mb-5">Cloning {{ route.query.clone }}. Choose a new name and outgoing queues. Incoming queues will be created for the clone. Shared budget keys are preserved: review them if the clone should have a separate allowance.</p>
    <PageHeader
      :title="editing ? name : 'New graph'"
      :mono="editing"
      :crumbs="[{ to: '/graphs', label: 'Graphs' }]"
      sub="Configure the nodes, limits and paths in a guided form, or edit the same document as JSON."
    >
      <template #actions>
        <RouterLink v-if="editing" :to="graphPath(application, name)" class="btn">Cancel</RouterLink>
        <RouterLink v-else to="/graphs" class="btn">Cancel</RouterLink>
        <button class="btn btn-primary" :disabled="!isAdmin || busy || loading || loadFailed || declared" @click="save">
          <Icon name="check" :size="14" /> {{ busy ? 'Saving…' : declared ? 'Saved' : editing ? 'Save changes' : 'Create graph' }}
        </button>
      </template>
    </PageHeader>

    <p v-if="!isAdmin" class="-mt-4 mb-6 text-[12px] text-fg-3">{{ READ_ONLY_NOTE }}</p>

    <!-- 422: this document is wrong. Every rule names the number, the
         consequence and the fix, joined with `; `. -->
    <div v-if="error" role="alert" class="card border-transparent bg-bad-dim px-5 py-4 mb-4 text-[13px] text-bad">
      <p v-for="(line, i) in String(error).split('; ')" :key="i" class="leading-relaxed">
        {{ line }}
      </p>
    </div>

    <!-- 409: this document is right, and applying it re-founds a counter or
         strands a queue. A different sentence, because it needs a different
         action. -->
    <div v-if="conflict" role="alert"
         class="card border-transparent bg-warn-dim px-5 py-4 mb-4 text-[13px] text-warn">
      {{ conflict }}
    </div>

    <div v-if="migration.length"
         class="card border-transparent bg-warn-dim px-5 py-4 mb-4 text-[13px] text-warn">
      <p class="font-medium">This was written for v1 and has been mapped.</p>
      <ul class="mt-2 space-y-1 text-[12.5px]">
        <li v-for="(w, i) in migration" :key="i">{{ w }}</li>
      </ul>
      <RouterLink :to="graphPath(destinationApp, graphName)" class="btn mt-3">
        Open the graph <Icon name="chevron" :size="13" />
      </RouterLink>
    </div>

    <div v-if="warnings.length"
         class="card border-transparent bg-warn-dim px-5 py-4 mb-4 text-[13px] text-warn">
      <p class="font-medium">Declared, with caveats:</p>
      <ul class="mt-2 space-y-1 text-[12.5px]">
        <li v-for="(w, i) in warnings" :key="i">{{ w }}</li>
      </ul>
      <RouterLink :to="graphPath(destinationApp, graphName)" class="btn mt-3">Open the graph <Icon name="chevron" :size="13" /></RouterLink>
    </div>

    <div v-if="loading" class="card px-6 py-8"><div class="skeleton h-5 w-1/3" /></div>

    <template v-else-if="!loadFailed">
      <div class="editor-toolbar">
        <div class="editor-tabs" role="group" aria-label="Editor view">
          <button type="button" :aria-pressed="mode === 'form'" :disabled="!!formError || busy" @click="mode = 'form'">Guided form</button>
          <button type="button" :aria-pressed="mode === 'json'" :disabled="busy" @click="mode = 'json'">JSON</button>
        </div>
        <span class="text-xs text-fg-3">One draft · changes stay with you when switching views</span>
      </div>
      <p v-if="formError" class="editor-note mb-4" role="status">{{ formError }}</p>
      <div v-if="attempted && mode === 'form' && checks.length" class="editor-errors mb-4" role="alert">
        <p class="font-semibold mb-2">Check these fields before saving</p>
        <ul class="list-disc pl-5 space-y-1"><li v-for="(check, i) in checks" :key="i">{{ check.message }}</li></ul>
      </div>
      <fieldset v-if="mode === 'form' && !formError" :disabled="!isAdmin || busy || declared" class="min-w-0">
        <GraphForm :model-value="formDocument" :locked-identity="editing" @update:model-value="updateDocument" />
      </fieldset>
      <div v-else class="card p-0 overflow-hidden">
        <textarea
          v-model="text"
          aria-label="Graph JSON document"
          :readonly="!isAdmin || busy || declared"
          spellcheck="false"
          class="w-full bg-transparent font-mono text-[12.5px] leading-relaxed p-5 outline-none resize-y"
          :class="parsed.ok ? '' : 'text-bad'"
          rows="34"
        />
        <div class="px-5 py-2.5 border-t border-line text-[11.5px]"
             :class="parsed.ok ? 'text-fg-3' : 'text-bad'">
          <template v-if="parsed.ok">
            {{ Object.keys(parsed.value?.nodes ?? {}).length }} node(s),
            {{ (parsed.value?.paths ?? []).length }} path(s)
          </template>
          <template v-else>not JSON: {{ parsed.error }}</template>
        </div>
      </div>
    </template>
  </div>
</template>
