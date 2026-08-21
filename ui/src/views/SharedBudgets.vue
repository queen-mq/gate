<script setup>
/*
  The budgets that cross nodes and graphs: an egress IP that several portals
  leave through, a per-account ceiling two products share.

  They get their own page for a different reason than they used to. In v1 they
  were the only limits a single gate could not see — its state was per partition
  — and the only ones enforced by a round trip to kv. Now EVERY budget is a kv
  counter, and what makes these special is only their key: one row for the
  application rather than one per node.

  Nothing is created here, and that is the design rather than an omission: a
  shared budget is what two nodes HAVE when both declare a budget with the same
  `sharedKey` inside one application. The key IS the sharing, and an "add"
  button here would be a second place to declare the same thing.

  Which makes the empty state the most important thing on this page: it is the
  only surface that can say how the thing comes to exist.
*/
import { ref } from 'vue'
import PageHeader from '../components/PageHeader.vue'
import BudgetBar from '../components/BudgetBar.vue'
import Icon from '../components/Icon.vue'
import {
  api, num, pct, period, window as windowMs, ago, utilisation, ceilingOf,
  targetKeyPath,
} from '../lib/api.js'
import { usePoll } from '../lib/poll.js'

// `undefined` while the first request is in flight, `null` when the endpoint is
// not answering at all. A shared budget is an optional thing to have declared,
// so its absence is a fact about the deployment and not an error to shout.
const budgets = ref(undefined)
const error = ref('')

async function load() {
  try {
    const r = await api.get('/api/budgets').catch(() => null)
    budgets.value = r === null ? null : (Array.isArray(r) ? r : (r?.budgets ?? []))
    error.value = ''
  } catch (e) {
    error.value = e.message
  }
}
usePoll(load)

/* The declared window, and — where they differ — the one it is actually
   enforced in. A budget declared over an hour and subdivided into sixty is a
   one-minute window of a sixtieth, and reporting only the declared pair would
   report a ceiling nothing ever meets. */
function windowOf(b) {
  return windowMs(b.timeMs)
}
function enforcedOf(b) {
  if (!b.subWindows || b.subWindows <= 1) return null
  return `${num(b.countSub)} / ${period(b.windowSubSeconds)}`
}
</script>

<template>
  <div>
    <PageHeader
      title="Shared budgets"
      sub="Ceilings that cross nodes and graphs — an egress IP, a shared account. One kv row for the whole application, which is what makes them shared."
    />

    <div v-if="error" class="card border-transparent bg-bad-dim px-5 py-4 text-[13.5px] text-bad">
      {{ error }}
    </div>

    <div v-else-if="budgets === undefined" class="card px-6 py-8"><div class="skeleton h-5 w-1/3" /></div>

    <div v-else-if="budgets === null" class="card px-6 py-14 text-center">
      <p class="text-[15px] font-medium">Shared budgets are not being served</p>
      <p class="hint mt-2 max-w-md mx-auto">
        This build answers for each node's own budgets. The shared ones are read from a different
        endpoint.
      </p>
    </div>

    <!-- Empty, and the only page that can explain how it stops being empty. -->
    <div v-else-if="!budgets.length" class="card px-6 py-10 sm:px-10">
      <p class="text-[15px] font-medium text-center">No shared budget declared.</p>
      <p class="text-[13px] text-fg-2 mt-2 max-w-[56ch] mx-auto text-center leading-relaxed">
        If every node leaves through its own identity, there is nothing to share — which is the
        cheapest way to be right.
      </p>

      <div class="mt-8 pt-7 border-t border-line max-w-[62ch] mx-auto">
        <p class="label">Declaring one</p>
        <p class="text-[13px] text-fg-2 leading-relaxed">
          A shared budget is not created on this page. It exists when two nodes declare a budget
          with the same <span class="font-mono text-[12.5px]">sharedKey</span>, inside the same
          application — that pair is the kv row they both spend against.
        </p>
        <ol class="mt-4 space-y-2.5 text-[13px] text-fg-2">
          <li class="flex gap-2.5">
            <span class="chip shrink-0 mt-px">1</span>
            <span>
              Open a graph whose node leaves through the shared identity and press <b>Edit</b>.
            </span>
          </li>
          <li class="flex gap-2.5">
            <span class="chip shrink-0 mt-px">2</span>
            <span>
              Give the budget a
              <span class="font-mono text-[12.5px]">sharedKey</span> that names the thing being
              shared — <span class="font-mono text-[12.5px]">egress-ip</span>, not
              <span class="font-mono text-[12.5px]">limit-2</span>.
            </span>
          </li>
          <li class="flex gap-2.5">
            <span class="chip shrink-0 mt-px">3</span>
            <span>
              Repeat on every other node that shares it, with the <b>same count, timeMs and
              subWindows</b>. A declare refuses a disagreement inside one document
              (<span class="font-mono text-[12.5px]">shared-conflict</span>) and this page reports
              one across two.
            </span>
          </li>
        </ol>
        <p class="hint mt-5">
          Worth knowing first: this is a FIXED window whose start is the first admission after the
          previous one expired, so a sliding observer can see up to twice the sub-window's count
          across one boundary. Subdivide it — <span class="font-mono text-[12.5px]">subWindows</span>
          — to make that number small.
        </p>
        <RouterLink to="/graphs" class="inline-flex items-center gap-1.5 mt-5 text-[13px] text-link">
          Go to graphs <Icon name="chevron" :size="14" />
        </RouterLink>
      </div>
    </div>

    <div v-else class="card divide-y divide-line">
      <div v-for="b in budgets" :key="`${b.application}/${b.id}`" class="px-5 py-4">
        <div class="flex items-center gap-3 flex-wrap">
          <span class="font-mono text-[13px] font-medium">{{ b.id }}</span>
          <span v-if="b.application" class="chip text-fg-3">{{ b.application }}</span>
          <span class="chip">{{ num(b.count) }} / {{ windowOf(b) }}</span>
          <span v-if="enforcedOf(b)" class="chip">{{ enforcedOf(b) }}</span>
          <span class="chip font-mono">{{ b.key }}</span>
          <span class="ml-auto text-[13px] font-semibold tabular-nums"
                :class="utilisation(b) > 1 ? 'text-bad' : utilisation(b) >= 0.85 ? 'text-warn' : ''">
            {{ pct(utilisation(b)) }}
          </span>
        </div>

        <div class="mt-2.5">
          <BudgetBar :used="b.used ?? 0" :cap="ceilingOf(b)"
                     :assumed="b.confidence === 'assumed'" :height="7" />
        </div>

        <div class="flex items-center gap-2 mt-2 text-[11.5px] text-fg-3 flex-wrap">
          <template v-if="b.members?.length">
            <span>{{ b.members.length === 1 ? 'declared by' : 'shared by' }}</span>
            <RouterLink v-for="m in b.members" :key="m" :to="targetKeyPath(m)"
                        class="chip hover:text-fg transition-colors">{{ m }}</RouterLink>
          </template>
          <!-- One member is not a shared budget yet. Saying so beats showing it
               as one and letting somebody believe a second target is being held
               back by it. -->
          <span v-if="b.members?.length === 1">
            — one row for the application, but nothing shares it yet
          </span>
          <span v-if="b.expiresAt" class="ml-auto">
            window rotates {{ ago(b.expiresAt) }}
          </span>
        </div>

        <!-- Two targets, one kv key, two different declarations: they already
             spend against the same counter, so only one of them can be
             describing a real ceiling. The console cannot tell which, so it
             names both rather than averaging them into a number that is
             nobody's. -->
        <div v-if="b.conflicts?.length"
             class="mt-3 rounded-lg bg-bad-dim px-3.5 py-3 text-[12.5px] text-bad">
          <p class="flex gap-2 font-medium">
            <Icon name="alert" :size="14" class="mt-px shrink-0" />
            Declared differently by {{ b.conflicts.length }} of its members
          </p>
          <p class="mt-1.5 leading-relaxed">
            They all spend against one key, so only one declaration can be true. The figure above is
            <span class="font-mono">{{ num(b.count) }} / {{ windowOf(b) }}</span>, which is simply
            the one loaded first.
          </p>
          <ul class="mt-2 space-y-1 font-mono text-[11.5px]">
            <li v-for="c in b.conflicts" :key="c.target">
              {{ c.target }} — {{ num(c.count) }} / {{ windowMs(c.timeMs) }},
              {{ c.subWindows }} sub-window(s)
            </li>
          </ul>
        </div>

        <p v-if="(b.windowSubSeconds ?? 0) > 2" class="mt-2 text-[11.5px] text-warn">
          a {{ period(b.windowSubSeconds) }} sub-window is a fixed window: a sliding observer can
          see up to {{ num(b.countSub * 2) }} across one boundary
        </p>
      </div>
    </div>
  </div>
</template>
