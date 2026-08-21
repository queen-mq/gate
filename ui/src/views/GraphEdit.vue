<script setup>
/*
  The document, as a document.

  v1 had a form — four hundred and fifty lines of it, over a five-hundred-line
  model of the spec — and it was worth having while the schema was lanes, cap
  policies, alignments, shards and store kinds, because each of those was a
  choice with a wrong answer a form could steer away from. v2's document is
  smaller and more expressive at once: paths with shares, budgets with
  subdivision, a cost that is a payload path, a fan-out that is a nested array.
  A form over that is either a worse JSON editor or a subset of the schema, and
  a subset of the schema is a console that quietly cannot express what the API
  accepts.

  So: the text, and the server's own refusal above it. That refusal is the whole
  reason this is usable — every rule names the number, the consequence and the
  fix, and a 422 here reads better than any client-side validation could,
  because it is the same sentence the caller's CI gets.

  The one thing the editor does add is the difference between a 422 and a 409:
  "this document is wrong" and "this document is right but re-founds a counter"
  need different words above the same box.
*/
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import PageHeader from '../components/PageHeader.vue'
import Icon from '../components/Icon.vue'
import { api, isAdmin, READ_ONLY_NOTE, graphApi, graphPath, DEFAULT_APP } from '../lib/api.js'

const props = defineProps({ app: String, name: String })
const router = useRouter()
const application = computed(() => props.app || DEFAULT_APP)
const editing = computed(() => !!props.name)

const text = ref('')
const loading = ref(true)
const busy = ref(false)
const error = ref('')
const conflict = ref('')
const warnings = ref([])
const migration = ref([])

const STARTER = {
  version: 1,
  nodes: {
    providerx: {
      ingress: true,
      budgets: [{ id: 'providerx', count: 100, timeMs: 1000, confidence: 'inferred' }],
      egress: 'your-app.providerx.out',
    },
  },
  paths: [{ name: 'main', nodes: ['providerx'] }],
}

onMounted(async () => {
  if (!editing.value) {
    text.value = JSON.stringify(STARTER, null, 2)
    loading.value = false
    return
  }
  try {
    const live = await api.get(graphApi(application.value, props.name))
    /* `spec` is the stored document verbatim. Editing the VIEW instead would
       hand the server back its own computed fields, and `deny_unknown_fields`
       would refuse every one of them. */
    const doc = live?.spec ?? {}
    text.value = JSON.stringify(doc, null, 2)
  } catch (e) {
    error.value = e.message
  }
  loading.value = false
})

const parsed = computed(() => {
  try {
    return { ok: true, value: JSON.parse(text.value) }
  } catch (e) {
    return { ok: false, error: e.message }
  }
})

const graphName = computed(() => props.name || parsed.value.value?.graph || '')

async function save() {
  error.value = ''
  conflict.value = ''
  warnings.value = []
  migration.value = []
  if (!parsed.value.ok) {
    error.value = `not JSON: ${parsed.value.error}`
    return
  }
  const name = graphName.value
  if (!name) {
    error.value = 'the document must name itself: add `"graph": "…"`.'
    return
  }
  busy.value = true
  try {
    const res = await api.put(
      `/v1/apps/${encodeURIComponent(application.value)}/graphs/${encodeURIComponent(name)}`,
      parsed.value.value,
    )
    warnings.value = res?.warnings ?? []
    migration.value = res?.migration ?? []
    /* Warnings are trades, not mistakes, so they do not hold the page — but a
       MIGRATED document is a different thing: the caller wrote v1 and the
       server rewrote it, and they should read what changed before they leave. */
    if (!migration.value.length) {
      router.push(graphPath(application.value, name))
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
    <PageHeader
      :title="editing ? name : 'New graph'"
      :mono="editing"
      :crumbs="[{ to: '/graphs', label: 'Graphs' }]"
      sub="The whole document, every time. A 200 means validated, provisioned and stored."
    >
      <template #actions>
        <RouterLink v-if="editing" :to="graphPath(application, name)" class="btn">Cancel</RouterLink>
        <RouterLink v-else to="/graphs" class="btn">Cancel</RouterLink>
        <button class="btn btn-primary" :disabled="!isAdmin || busy || loading" @click="save">
          <Icon name="check" :size="14" /> {{ busy ? 'Declaring…' : 'Declare' }}
        </button>
      </template>
    </PageHeader>

    <p v-if="!isAdmin" class="-mt-4 mb-6 text-[12px] text-fg-3">{{ READ_ONLY_NOTE }}</p>

    <!-- 422: this document is wrong. Every rule names the number, the
         consequence and the fix, joined with `; `. -->
    <div v-if="error" class="card border-transparent bg-bad-dim px-5 py-4 mb-4 text-[13px] text-bad">
      <p v-for="(line, i) in String(error).split('; ')" :key="i" class="leading-relaxed">
        {{ line }}
      </p>
    </div>

    <!-- 409: this document is right, and applying it re-founds a counter or
         strands a queue. A different sentence, because it needs a different
         action. -->
    <div v-if="conflict"
         class="card border-transparent bg-warn-dim px-5 py-4 mb-4 text-[13px] text-warn">
      {{ conflict }}
    </div>

    <div v-if="migration.length"
         class="card border-transparent bg-warn-dim px-5 py-4 mb-4 text-[13px] text-warn">
      <p class="font-medium">This was written for v1 and has been mapped.</p>
      <ul class="mt-2 space-y-1 text-[12.5px]">
        <li v-for="(w, i) in migration" :key="i">{{ w }}</li>
      </ul>
      <RouterLink :to="graphPath(application, graphName)" class="btn mt-3">
        Open the graph <Icon name="chevron" :size="13" />
      </RouterLink>
    </div>

    <div v-if="warnings.length"
         class="card border-transparent bg-warn-dim px-5 py-4 mb-4 text-[13px] text-warn">
      <p class="font-medium">Declared, with caveats:</p>
      <ul class="mt-2 space-y-1 text-[12.5px]">
        <li v-for="(w, i) in warnings" :key="i">{{ w }}</li>
      </ul>
    </div>

    <div v-if="loading" class="card px-6 py-8"><div class="skeleton h-5 w-1/3" /></div>

    <div v-else class="card p-0 overflow-hidden">
      <textarea
        v-model="text"
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
  </div>
</template>
