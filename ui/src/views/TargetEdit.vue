<script setup>
/*
  Create and edit are the same page, because they are the same document — the
  only difference is whether the identity pair is already spent.

  Two rules govern everything below.

  * THE SERVER IS THE AUTHORITY. The console mirrors the cheap validations so a
    refusal that is going to happen is visible before the round trip, but it
    never refuses on its own reading: Apply always submits, and what comes back
    is shown verbatim. Those sentences were written by the thing that knows
    which rule broke; paraphrasing them would throw away the only part that says
    what to change.
  * A REFUSAL LANDS ON ITS FIELD. `[cost-fits] budget `ip-10s` caps at 200 but a
    single item may cost 400` is a sentence about one input, and it is shown
    both at the top of the page and under that input.
*/
import { ref, computed, watch } from 'vue'
import { useRouter } from 'vue-router'
import PageHeader from '../components/PageHeader.vue'
import TargetForm from '../components/TargetForm.vue'
import Icon from '../components/Icon.vue'
import { api, isAdmin, me, READ_ONLY_NOTE, targetApi, targetPath, DEFAULT_APP } from '../lib/api.js'
import { blankDraft, toDraft, toSpec, validateDraft, draftWarnings, mapServerProblems } from '../lib/spec.js'

const props = defineProps({ app: String, name: String })
const router = useRouter()

const editing = computed(() => !!props.name)
const draft = ref(blankDraft(props.app || DEFAULT_APP))
const loading = ref(editing.value)
const loadError = ref('')
const busy = ref(false)
// { status, message } exactly as it arrived.
const refusal = ref(null)
const serverFields = ref({})
const warnings = ref([])
// Set once the PUT has been accepted: the route to the target it produced.
const applied = ref(null)

/* Suggestions, never a constraint: a new application is founded by typing its
   name here, so the list must not become a select. It only exists to keep a
   typo from quietly founding one. */
const applications = ref([])
async function loadApps() {
  const r = await api.get('/api/apps').catch(() => null)
  const rows = Array.isArray(r) ? r : (r?.applications ?? [])
  applications.value = rows
    .map((a) => (typeof a === 'string' ? a : (a?.application ?? a?.name)))
    .filter(Boolean)
}
loadApps()

async function load() {
  if (!editing.value) {
    loading.value = false
    return
  }
  loading.value = true
  try {
    const t = await api.get(targetApi(props.app, props.name))
    // The DECLARED document, not the computed view of it: what is edited here
    // is what was PUT, so a round trip through this page changes nothing on its
    // own.
    draft.value = toDraft(t.spec)
    loadError.value = ''
  } catch (e) {
    loadError.value = e.message
  } finally {
    loading.value = false
  }
}
watch(() => [props.app, props.name], load, { immediate: true })

/*
  A form nobody has filled in yet is not a form full of mistakes. An existing
  target is a real document and its problems are real from the first render; a
  blank one earns its red only once somebody has tried to declare it — after
  which the mirror is live again, so a fix is confirmed as it is typed. The
  attempt costs no round trip either way.
*/
const attempted = ref(false)
const showProblems = computed(() => editing.value || attempted.value)

const rawFields = computed(() => validateDraft(draft.value))
const localFields = computed(() => (showProblems.value ? rawFields.value : {}))
/* The server's sentence wins where both have something to say about the same
   input: it is the one that actually refused. */
const fields = computed(() => ({ ...localFields.value, ...serverFields.value }))
const localProblems = computed(() => Object.entries(localFields.value))
const localWarnings = computed(() => draftWarnings(draft.value))

async function save() {
  attempted.value = true
  refusal.value = null
  serverFields.value = {}
  busy.value = true
  const spec = toSpec(draft.value)
  try {
    const r = await api.put(
      `/v1/apps/${encodeURIComponent(spec.application)}/targets/${encodeURIComponent(spec.name)}`,
      spec
    )
    warnings.value = r?.warnings ?? []
    // A PUT that succeeded can still have been a bad idea, and the server says
    // so instead of refusing. Those sentences would be lost in a navigation, so
    // the page stays put and hands over the last step.
    if (warnings.value.length) {
      applied.value = targetPath(spec.application, spec.name)
      window.scrollTo({ top: 0, behavior: 'smooth' })
    } else {
      router.push(targetPath(spec.application, spec.name))
    }
  } catch (e) {
    refusal.value = { status: e.status ?? 0, message: e.message }
    serverFields.value = mapServerProblems(e.message, draft.value, e.status)
    window.scrollTo({ top: 0, behavior: 'smooth' })
  } finally {
    busy.value = false
  }
}

function cancel() {
  router.push(editing.value ? targetPath(props.app, props.name) : '/targets')
}

/* 409 and 422 mean different things and need different words above the same
   form: one says this document is wrong, the other says this document is right
   but re-founds the counters. 403 is neither — it is the account. */
const HEADING = {
  409: 'This change re-founds the counters',
  422: 'The server refused this spec',
  403: 'This account cannot write',
}
</script>

<template>
  <div>
    <PageHeader
      :title="editing ? `Edit ${name}` : 'New target'"
      :crumbs="editing
        ? [{ to: '/targets', label: 'Targets' }, { to: targetPath(app, name), label: `${app}/${name}` }]
        : [{ to: '/targets', label: 'Targets' }]"
      :sub="editing
        ? 'Caps, floors, concurrency and provenance apply immediately. Anything that re-founds the counters — a window, an alignment, a scope, a store, the admitted partitioning, a removed lane — needs the version bumped in the same edit.'
        : 'A target is one thing that limits us. Declare what it publishes as budgets, and how we choose to spend them as lanes.'"
    />

    <!-- The one quiet sentence a viewer gets, instead of buttons that would be
         answered with a 403. -->
    <div v-if="!isAdmin" class="card px-5 py-4 mb-6 flex gap-3 text-[13px] text-fg-2">
      <Icon name="alert" :size="15" class="mt-px shrink-0 text-fg-3" />
      <p class="leading-relaxed">
        {{ READ_ONLY_NOTE }}<template v-if="me?.email"> ({{ me.email }})</template>. The form below is
        filled in and readable, and nothing in it can be saved.
      </p>
    </div>

    <div v-if="loadError" class="card border-transparent bg-bad-dim px-5 py-4 text-[13.5px] text-bad">
      {{ loadError }}
    </div>

    <div v-else-if="loading" class="space-y-4">
      <div class="card px-6 py-8"><div class="skeleton h-5 w-1/3" /></div>
      <div class="card px-6 py-8"><div class="skeleton h-5 w-1/2" /></div>
    </div>

    <!-- `invalid` does not bubble, so it is caught on the way down: the
         browser's own required-field check blocks the submit before save()
         runs, and without this the first Apply on a blank form would look like
         nothing at all happened. -->
    <form v-else @submit.prevent="save" @invalid.capture="attempted = true">
      <!-- ------------------------------------------- accepted anyway -->
      <div v-if="applied"
           class="card border-transparent bg-warn-dim px-5 py-4 mb-6 flex gap-3 text-warn">
        <Icon name="alert" :size="16" class="mt-px shrink-0" />
        <div class="flex-1 min-w-0">
          <b class="font-semibold text-[13px]">Applied, with warnings.</b>
          <p v-for="(w, i) in warnings" :key="i" class="text-[12.5px] leading-relaxed mt-1">{{ w }}</p>
        </div>
        <RouterLink :to="applied" class="btn btn-sm shrink-0">Open the target</RouterLink>
      </div>

      <!-- ------------------------------------------------ the refusal -->
      <div v-if="refusal"
           class="card border-transparent px-5 py-4 mb-6"
           :class="refusal.status === 409 ? 'bg-warn-dim text-warn' : 'bg-bad-dim text-bad'">
        <b class="font-semibold block text-[13px] mb-1.5">
          {{ HEADING[refusal.status] || 'The server refused this spec' }}
        </b>
        <!-- Verbatim, always. -->
        <p class="font-mono text-[12px] leading-relaxed break-words">{{ refusal.message }}</p>
        <p v-if="Object.keys(serverFields).length" class="text-[12px] mt-2 opacity-88">
          Each sentence above is also shown under the field it came from.
        </p>
      </div>

      <!-- ------------------------------------- what will be refused -->
      <div v-if="localProblems.length"
           class="card border-transparent bg-bad-dim px-5 py-4 mb-6 text-bad">
        <b class="font-semibold block text-[13px] mb-1.5">
          {{ localProblems.length }} thing{{ localProblems.length === 1 ? '' : 's' }} the server will refuse
        </b>
        <ul class="space-y-1">
          <li v-for="[path, msg] in localProblems" :key="path" class="text-[12.5px] leading-relaxed">
            <span class="font-mono opacity-88">{{ path }}</span> — {{ msg }}
          </li>
        </ul>
      </div>

      <!-- Accepted, and still worth saying. -->
      <div v-if="localWarnings.length"
           class="card border-transparent bg-warn-dim px-5 py-4 mb-6 text-warn">
        <b class="font-semibold block text-[13px] mb-1.5">Accepted, with a cost</b>
        <p v-for="(w, i) in localWarnings" :key="i" class="text-[12.5px] leading-relaxed mt-1">{{ w }}</p>
      </div>

      <TargetForm :draft="draft" :errors="fields" :disabled="!isAdmin || busy" :locked="editing"
                  :applications="applications" />

      <!-- Sticky, because this form is taller than a screen and an editor whose
           only exit is the back button is a trap. -->
      <div class="sticky bottom-0 mt-8 -mx-6 lg:-mx-12 px-6 lg:px-12 py-4 bg-bg border-t border-line
                  flex items-center gap-3">
        <p class="text-[12px] text-fg-3 leading-snug flex-1 min-w-0">
          <template v-if="isAdmin">
            The server validates this document and answers with the rule it broke.
          </template>
          <template v-else>{{ READ_ONLY_NOTE }}</template>
        </p>
        <button type="button" class="btn shrink-0" @click="cancel">Cancel</button>
        <button type="submit" class="btn btn-primary shrink-0" :disabled="!isAdmin || busy">
          {{ busy ? 'Working…' : editing ? 'Apply' : 'Declare target' }}
        </button>
      </div>
    </form>
  </div>
</template>
