<script setup>
/*
  The target document as a form, which is the only way it should ever have been
  edited.

  A textarea of JSON asks an operator to hold the whole schema in their head and
  gives them nothing back until the server refuses — and the fields that matter
  most here are precisely the ones whose consequences are invisible from the
  name. `alignment` has no default because guessing it is a factor-of-two
  overshoot. A `cost.max` above a cap is a lane blocked forever. Two lanes both
  claiming the ceiling enforce it twice. So every one of those carries a
  tooltip that says what goes wrong, and every one of them has its refusal land
  next to the input rather than in a paragraph at the top of the page.

  The JSON is still here, at the bottom, read-only: somebody about to change a
  production ceiling is entitled to see the exact bytes before they press the
  button. It is a disclosure, never the path.
*/
import { computed } from 'vue'
import Field from './Field.vue'
import Tooltip from './Tooltip.vue'
import Icon from './Icon.vue'
import { HELP, NOT_IN_THIS_FORM } from '../lib/help.js'
import {
  DIMS, CAP_KINDS, blankBudget, blankLane, toSpec, laneReservation, joinPeriod,
} from '../lib/spec.js'
import { num } from '../lib/api.js'

const props = defineProps({
  draft: { type: Object, required: true },
  // Field path -> sentence. Merged from the client-side mirror and whatever the
  // server said, so both arrive at the same input.
  errors: { type: Object, default: () => ({}) },
  disabled: Boolean,
  // Editing an existing target: the pair is its identity, so neither half of it
  // can be changed here — that would be a different target, not an edit.
  locked: Boolean,
  // The applications that already exist. There is no separate "create an
  // application" action anywhere in Gate: an application exists because a
  // target claims it, and that is the whole of the concept. Which makes a typo
  // in this field indistinguishable from a deliberate new team — it silently
  // gets its own queues, its own ceilings and its own sync scope — so the
  // existing names are offered as suggestions and a name that is not among
  // them says so before the button is pressed.
  applications: { type: Array, default: () => [] },
})

const spec = computed(() => toSpec(props.draft))

/* Three states worth distinguishing, because only one of them is a surprise:
   locked (identity, not editable), joining a team that exists, and founding a
   new one. The third is the one that happens by accident. */
const appHint = computed(() => {
  if (props.locked) return 'Part of the identity — a different application is a different target.'
  const a = String(props.draft.application ?? '').trim()
  if (!a) return null
  // `default` is where a target lands when nobody names an application, so a
  // blank form starts there and it is never the surprise this hint is for.
  if (a === 'default') return null
  if (props.applications.includes(a)) return null
  return `New application — "${a}" gets its own queues, its own ceilings, and its own sync scope. Nothing is shared with the others.`
})
const json = computed(() => JSON.stringify(spec.value, null, 2))

function addBudget() {
  props.draft.budgets.push(blankBudget())
}
function removeBudget(i) {
  props.draft.budgets.splice(i, 1)
}
function addLane() {
  props.draft.lanes.push(blankLane(props.draft.lanes.length === 0))
}
function removeLane(i) {
  props.draft.lanes.splice(i, 1)
}

/* Exactly one lane is the default, so choosing one un-chooses the rest. A pair
   of checkboxes that let you tick both would only ever produce a 422. */
function makeDefault(i) {
  props.draft.lanes.forEach((l, k) => {
    l.default = k === i
  })
}

function toggleDim(b, dim) {
  const at = b.scope.indexOf(dim)
  if (at === -1) b.scope.push(dim)
  else b.scope.splice(at, 1)
}

/* The rate a budget works out to, shown next to the window because that is the
   number the pacing and the lanes are actually divided against — and because
   "2000 per 10s" and "200/s" are not equally obvious to everybody. */
function rateOf(b) {
  const seconds = joinPeriod(b.periodValue, b.periodUnit)
  const cap = Number(b.cap) || 0
  if (!seconds || !cap) return null
  const r = cap / seconds
  return r >= 10 ? `${Math.round(r).toLocaleString()}/s` : `${r.toFixed(2).replace(/\.?0+$/, '')}/s`
}

const reserved = computed(() => props.draft.lanes.reduce((a, l) => a + laneReservation(l), 0))
const takers = computed(() =>
  props.draft.lanes.filter((l) => l.capKind === 'ceiling' || l.capKind === 'absolute').length
)

function err(path) {
  return props.errors[path] ?? null
}
</script>

<template>
  <div class="space-y-10">
    <!-- ------------------------------------------------------- identity -->
    <section>
      <h2 class="section-title">Target
        <span class="section-count">one thing that limits us</span>
      </h2>
      <div class="card px-5 py-5 grid sm:grid-cols-2 gap-5">
        <Field label="Application" :help="HELP.application" for="t-app" :error="err('application')"
               :hint="appHint">
          <input id="t-app" v-model="draft.application" class="input font-mono" list="t-app-known"
                 placeholder="channel-manager" :disabled="disabled || locked" required
                 autocomplete="off" spellcheck="false" />
          <datalist id="t-app-known">
            <option v-for="a in applications" :key="a" :value="a" />
          </datalist>
        </Field>

        <Field label="Name" :help="HELP.name" for="t-name" :error="err('name')"
               :hint="locked ? 'Renaming is a new target, not an edit.' : null">
          <input id="t-name" v-model="draft.name" class="input font-mono" placeholder="airbnb"
                 :disabled="disabled || locked" required />
        </Field>

        <Field label="Version" :help="HELP.version" for="t-version" :error="err('version')">
          <input id="t-version" v-model="draft.version" class="input tabular-nums" type="number"
                 min="1" step="1" :disabled="disabled" required />
        </Field>

        <Field label="Egress" :help="HELP.egress" for="t-egress" :error="err('egress')"
               hint="Optional. A label, not a limit.">
          <input id="t-egress" v-model="draft.egress" class="input font-mono"
                 placeholder="nat-pod-default" :disabled="disabled" />
        </Field>
      </div>
    </section>

    <!-- -------------------------------------------------------- budgets -->
    <section>
      <h2 class="section-title">
        <Tooltip :text="HELP.budgets" label="Budgets" />
        <span class="section-count">all of them must admit</span>
        <button v-if="!disabled" type="button" class="btn btn-sm ml-auto" @click="addBudget">
          <Icon name="plus" :size="12" /> Add budget
        </button>
      </h2>

      <p v-if="err('budgets')" class="mb-3 text-[12.5px] text-bad leading-relaxed">{{ err('budgets') }}</p>

      <div class="space-y-4">
        <div v-for="(b, i) in draft.budgets" :key="b._k" class="card px-5 py-5">
          <div class="flex items-center gap-3 mb-4">
            <span class="text-[11px] font-semibold uppercase tracking-[0.08em] text-fg-3">
              Budget {{ i + 1 }}
            </span>
            <span v-if="rateOf(b)" class="chip">{{ rateOf(b) }}</span>
            <span v-if="b.confidence === 'assumed'" class="chip text-warn">enforced at 70%</span>
            <button v-if="!disabled && draft.budgets.length > 1" type="button"
                    class="btn btn-sm btn-danger ml-auto" :aria-label="`Remove budget ${i + 1}`"
                    @click="removeBudget(i)">
              <Icon name="x" :size="12" /> Remove
            </button>
          </div>

          <div class="grid sm:grid-cols-2 lg:grid-cols-4 gap-5">
            <Field label="Id" :help="HELP.budgetId" :for="`b-id-${i}`" :error="err(`budgets.${i}.id`)">
              <input :id="`b-id-${i}`" v-model="b.id" class="input font-mono" placeholder="ip-10s"
                     :disabled="disabled" required />
            </Field>

            <Field label="Cap" :help="HELP.cap" :for="`b-cap-${i}`" :error="err(`budgets.${i}.cap`)"
                   hint="In cost units.">
              <input :id="`b-cap-${i}`" v-model="b.cap" class="input tabular-nums" type="number"
                     min="1" step="any" placeholder="2000" :disabled="disabled" required />
            </Field>

            <Field label="Window" :help="HELP.period" :for="`b-per-${i}`" :error="err(`budgets.${i}.period`)">
              <div class="flex gap-2">
                <input :id="`b-per-${i}`" v-model="b.periodValue" class="input tabular-nums" type="number"
                       min="1" step="1" placeholder="10" :disabled="disabled" required />
                <select v-model="b.periodUnit" class="input w-[86px] shrink-0" :disabled="disabled"
                        aria-label="Window unit">
                  <option value="s">seconds</option>
                  <option value="m">minutes</option>
                  <option value="h">hours</option>
                  <option value="d">days</option>
                </select>
              </div>
            </Field>

            <!-- The field this whole form was worth building for. -->
            <Field label="Alignment" :help="HELP.alignment" :for="`b-align-${i}`"
                   :error="err(`budgets.${i}.alignment`)"
                   :hint="b.alignment ? null : 'No default — choose.'">
              <select :id="`b-align-${i}`" v-model="b.alignment" class="input" :disabled="disabled" required>
                <option value="" disabled>choose one</option>
                <option value="rolling">rolling — never more than cap in any such window</option>
                <option value="calendar">calendar — resets on the clock boundary</option>
              </select>
            </Field>

            <Field label="Confidence" :help="HELP.confidence" :for="`b-conf-${i}`"
                   :error="err(`budgets.${i}.confidence`)">
              <select :id="`b-conf-${i}`" v-model="b.confidence" class="input" :disabled="disabled" required>
                <option value="" disabled>choose one</option>
                <option value="documented">documented — the vendor publishes it</option>
                <option value="inferred">inferred — our deduction from real sources</option>
                <option value="assumed">assumed — we do not know</option>
              </select>
            </Field>

            <Field label="Store" :help="HELP.store" :for="`b-store-${i}`" :error="err(`budgets.${i}.store`)">
              <select :id="`b-store-${i}`" v-model="b.store" class="input" :disabled="disabled">
                <option value="gate">gate — counters in the partition state</option>
                <option value="kv">kv — one row per key</option>
              </select>
            </Field>

            <Field label="Max keys" :help="HELP.maxKeys" :for="`b-keys-${i}`"
                   :error="err(`budgets.${i}.maxKeys`)"
                   :hint="b.scope.length ? 'Required: this budget has a scope.' : 'Only needed with a scope.'">
              <input :id="`b-keys-${i}`" v-model="b.maxKeys" class="input tabular-nums" type="number"
                     min="1" step="1" placeholder="5000" :disabled="disabled" :required="!!b.scope.length" />
            </Field>

            <Field label="Match on op" :help="HELP.matchOp" :for="`b-ops-${i}`"
                   :error="err(`budgets.${i}.matchOps`)"
                   hint="Comma separated. Empty selects everything.">
              <input :id="`b-ops-${i}`" v-model="b.matchOps" class="input font-mono"
                     placeholder="listing.*, messaging.send" :disabled="disabled" />
            </Field>
          </div>

          <div class="mt-5 grid sm:grid-cols-2 lg:grid-cols-4 gap-5">
            <Field label="Scope" :help="HELP.scope" :error="err(`budgets.${i}.scope`)"
                   class="sm:col-span-2"
                   :hint="b.scope.length ? `One counter per ${b.scope.join(' + ')}.` : 'One counter for the whole target.'">
              <div class="flex flex-wrap gap-1.5 pt-1">
                <button v-for="d in DIMS" :key="d" type="button" class="btn btn-sm font-mono"
                        :class="b.scope.includes(d) ? 'bg-surface-2 border-fg-3 text-fg' : 'text-fg-2'"
                        :disabled="disabled" :aria-pressed="b.scope.includes(d)"
                        @click="toggleDim(b, d)">
                  {{ d }}
                </button>
              </div>
            </Field>

            <Field label="Source" :help="HELP.source" :for="`b-src-${i}`"
                   :error="err(`budgets.${i}.source`)"
                   :hint="b.confidence === 'documented' ? 'Required for documented.' : null">
              <input :id="`b-src-${i}`" v-model="b.source" class="input"
                     placeholder="developer.withairbnb.com/…/rate-limits" :disabled="disabled"
                     :required="b.confidence === 'documented'" />
            </Field>

            <Field label="As of" :help="HELP.asOf" :for="`b-asof-${i}`"
                   :error="err(`budgets.${i}.asOf`)"
                   :hint="b.confidence === 'documented' ? 'Required for documented.' : null">
              <input :id="`b-asof-${i}`" v-model="b.asOf" class="input tabular-nums" type="date"
                     :disabled="disabled" :required="b.confidence === 'documented'" />
            </Field>
          </div>
        </div>
      </div>
    </section>

    <!-- ---------------------------------------------------------- lanes -->
    <section>
      <h2 class="section-title">
        <Tooltip :text="HELP.lanes" label="Lanes" />
        <span class="section-count">how the ceiling is divided</span>
        <button v-if="!disabled" type="button" class="btn btn-sm ml-auto" @click="addLane">
          <Icon name="plus" :size="12" /> Add lane
        </button>
      </h2>

      <p v-if="err('lanes')" class="mb-3 text-[12.5px] text-bad leading-relaxed">{{ err('lanes') }}</p>

      <div class="space-y-4">
        <div v-for="(l, i) in draft.lanes" :key="l._k" class="card px-5 py-5">
          <div class="flex items-center gap-3 mb-4">
            <span class="text-[11px] font-semibold uppercase tracking-[0.08em] text-fg-3">
              Lane {{ i + 1 }}
            </span>
            <span v-if="l.default" class="chip">default</span>
            <button v-if="!disabled && draft.lanes.length > 1" type="button"
                    class="btn btn-sm btn-danger ml-auto" :aria-label="`Remove lane ${i + 1}`"
                    @click="removeLane(i)">
              <Icon name="x" :size="12" /> Remove
            </button>
          </div>

          <div class="grid sm:grid-cols-2 lg:grid-cols-4 gap-5">
            <Field label="Name" :help="HELP.laneName" :for="`l-name-${i}`" :error="err(`lanes.${i}.name`)">
              <input :id="`l-name-${i}`" v-model="l.name" class="input font-mono" placeholder="bulk"
                     :disabled="disabled" required />
            </Field>

            <Field label="Cap policy" :help="HELP.laneCap" :for="`l-cap-${i}`" :error="err(`lanes.${i}.cap`)"
                   class="lg:col-span-2">
              <div class="flex gap-2">
                <select :id="`l-cap-${i}`" v-model="l.capKind" class="input" :disabled="disabled">
                  <option v-for="k in CAP_KINDS" :key="k" :value="k">{{ k }}</option>
                </select>
                <input v-if="l.capKind === 'absolute' || l.capKind === 'share'"
                       v-model="l.capValue" class="input w-[110px] shrink-0 tabular-nums" type="number"
                       :step="l.capKind === 'share' ? 0.05 : 1" min="0"
                       :max="l.capKind === 'share' ? 1 : undefined"
                       :placeholder="l.capKind === 'share' ? '0.25' : '50'"
                       :aria-label="l.capKind === 'share' ? 'Share of the ceiling' : 'Cost units per second'"
                       :disabled="disabled" required />
              </div>
            </Field>

            <Field label="Concurrency" :help="HELP.concurrency" :for="`l-conc-${i}`"
                   :error="err(`lanes.${i}.concurrency`)" hint="Consumers, not rate.">
              <input :id="`l-conc-${i}`" v-model="l.concurrency" class="input tabular-nums" type="number"
                     min="1" step="1" :disabled="disabled" required />
            </Field>

            <Field label="Floor" :help="HELP.floor" :for="`l-floor-${i}`" :error="err(`lanes.${i}.floor`)"
                   :hint="l.capKind === 'ceiling-minus-measured'
                     ? 'A fraction of the ceiling this lane always keeps.'
                     : 'Only used by ceiling-minus-measured.'">
              <input :id="`l-floor-${i}`" v-model="l.floor" class="input tabular-nums" type="number"
                     min="0" max="1" step="0.05"
                     :disabled="disabled || l.capKind !== 'ceiling-minus-measured'" />
            </Field>

            <Field label="Default lane" :help="HELP.defaultLane" :error="null"
                   hint="Items that name no lane land here.">
              <button type="button" class="btn w-full justify-center"
                      :class="l.default ? 'btn-primary' : ''" :disabled="disabled"
                      :aria-pressed="l.default" @click="makeDefault(i)">
                {{ l.default ? 'is the default' : 'make default' }}
              </button>
            </Field>
          </div>
        </div>
      </div>

      <!-- The arithmetic the validator does, shown while it can still be
           changed. Not a warning about denials: a lane at its cap refusing work
           is the limiter succeeding. This is about the ceiling being counted
           twice, which is the opposite failure. -->
      <p class="mt-3 text-[12px] text-fg-3 leading-relaxed">
        {{ (reserved * 100).toFixed(0) }}% of the ceiling is reserved by shares and floors;
        <template v-if="takers === 1">one lane takes the residual.</template>
        <template v-else-if="takers === 0">no lane takes the residual, so it goes unused.</template>
        <template v-else>{{ takers }} lanes claim the residual, and each would enforce it separately.</template>
      </p>
    </section>

    <!-- ------------------------------------------------- cost & pacing -->
    <section>
      <h2 class="section-title">Cost
        <span class="section-count">what one item spends</span>
      </h2>
      <div class="card px-5 py-5 grid sm:grid-cols-3 gap-5">
        <Field label="Cost field" :help="HELP.costField" for="c-field" :error="err('cost.field')">
          <input id="c-field" v-model="draft.cost.field" class="input font-mono" placeholder="httpCost"
                 :disabled="disabled" required />
        </Field>
        <Field label="Default cost" :help="HELP.costDefault" for="c-default" :error="err('cost.default')">
          <input id="c-default" v-model="draft.cost.default" class="input tabular-nums" type="number"
                 min="1" step="any" :disabled="disabled" required />
        </Field>
        <Field label="Max cost" :help="HELP.costMax" for="c-max" :error="err('cost.max')"
               hint="Validated against every cap.">
          <input id="c-max" v-model="draft.cost.max" class="input tabular-nums" type="number"
                 min="1" step="any" :disabled="disabled" required />
        </Field>
      </div>
    </section>

    <section>
      <h2 class="section-title">Pacing and admitted work
        <span class="section-count">the rhythm, and what happens after the gate</span>
      </h2>
      <div class="card px-5 py-5 grid sm:grid-cols-2 lg:grid-cols-4 gap-5">
        <Field label="Lease seconds" :help="HELP.leaseSeconds" for="p-lease"
               :error="err('pacing.leaseSeconds')">
          <input id="p-lease" v-model="draft.pacing.leaseSeconds" class="input tabular-nums" type="number"
                 min="1" step="1" :disabled="disabled" required />
        </Field>
        <Field label="Batch" :help="HELP.batch" for="p-batch" :error="err('pacing.batch')">
          <input id="p-batch" v-model="draft.pacing.batch" class="input tabular-nums" type="number"
                 min="1" step="1" :disabled="disabled" required />
        </Field>
        <Field label="Partition admitted by" :help="HELP.partitionBy" for="a-by"
               :error="err('admitted.partitionBy')">
          <select id="a-by" v-model="draft.admitted.partitionBy" class="input" :disabled="disabled">
            <option value="connection">connection</option>
            <option value="entity">entity — serialises per entity</option>
            <option value="none">none</option>
          </select>
        </Field>
        <Field label="Partitions" :help="HELP.partitions" for="a-parts"
               :error="err('admitted.partitions')" hint="The real parallelism ceiling.">
          <input id="a-parts" v-model="draft.admitted.partitions" class="input tabular-nums" type="number"
                 min="1" step="1" :disabled="disabled" required />
        </Field>
      </div>
    </section>

    <!-- ----------------------------------------------------------- JSON -->
    <section>
      <details class="card px-5 py-4 group">
        <summary class="cursor-pointer list-none flex items-center gap-2 text-[13px] font-medium select-none">
          <Icon name="chevron" :size="13"
                class="text-fg-3 transition-transform group-open:rotate-90" />
          View as JSON
          <span class="text-xs font-normal text-fg-3">
            {{ num(spec.budgets.length) }} budget{{ spec.budgets.length === 1 ? '' : 's' }} ·
            {{ num(spec.lanes.length) }} lane{{ spec.lanes.length === 1 ? '' : 's' }}
          </span>
        </summary>
        <p class="hint mt-3">
          Exactly the bytes that will be sent, read-only. There is no second serialisation:
          this is the document, rendered from the same fields above.
        </p>
        <pre class="mt-3 max-h-[340px] overflow-auto rounded-lg bg-surface-2 px-3.5 py-3
                    font-mono text-[11.5px] leading-relaxed whitespace-pre">{{ json }}</pre>

        <p class="hint mt-4">
          Declared in <span class="kbd">TARGET_SPEC.md</span> but not part of the document this
          server accepts, so there is nothing here to set:
        </p>
        <ul class="mt-1.5 space-y-1">
          <li v-for="[what, why] in NOT_IN_THIS_FORM" :key="what"
              class="text-[11.5px] text-fg-3 leading-relaxed">
            <span class="font-mono text-fg-2">{{ what }}</span> — {{ why }}
          </li>
        </ul>
      </details>
    </section>
  </div>
</template>
