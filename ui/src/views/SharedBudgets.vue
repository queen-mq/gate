<script setup>
/*
  The budgets that belong to no target: an egress IP that several portals leave
  through, a per-account ceiling two products share. They get their own page
  because they are the only limits in the system a single gate cannot see — the
  gate's state is per partition, and two targets are two partitions — so they
  are also the only ones enforced by a round trip to queen.kv instead of by
  arithmetic in memory.

  Nothing is created here, and that is the design rather than an omission: a
  shared budget is not an object, it is what two targets HAVE when both declare
  a `store: kv` budget under the same id inside one application. The kv key is
  `{application}:{id}:{window}`, so the sharing IS the key, and an "add" button
  here would be a second place to declare the same thing.

  Which makes the empty state the most important thing on this page: it is the
  only surface that can say how the thing comes to exist.
*/
import { ref } from 'vue'
import PageHeader from '../components/PageHeader.vue'
import BudgetBar from '../components/BudgetBar.vue'
import Icon from '../components/Icon.vue'
import { api, num, pct, period, ago, utilisation, targetKeyPath, splitTargetKey } from '../lib/api.js'
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

// The shape is not fixed yet on this endpoint, and a period is the one field
// that reads as nonsense when it comes back under the other spelling.
function windowOf(b) {
  return period(b.periodSeconds ?? b.period_seconds)
}
</script>

<template>
  <div>
    <PageHeader
      title="Shared budgets"
      sub="Ceilings that cross targets — an egress IP, a shared account. They live on queen.kv because no single gate can see past its own partition."
    />

    <div v-if="error" class="card border-transparent bg-bad-dim px-5 py-4 text-[13.5px] text-bad">
      {{ error }}
    </div>

    <div v-else-if="budgets === undefined" class="card px-6 py-8"><div class="skeleton h-5 w-1/3" /></div>

    <div v-else-if="budgets === null" class="card px-6 py-14 text-center">
      <p class="text-[15px] font-medium">Cross-target budgets are not being served</p>
      <p class="hint mt-2 max-w-md mx-auto">
        This build answers for each target's own budgets, which are enforced in the gate. The shared
        ones are held in queen.kv and read from a different endpoint.
      </p>
    </div>

    <!-- Empty, and the only page that can explain how it stops being empty. -->
    <div v-else-if="!budgets.length" class="card px-6 py-10 sm:px-10">
      <p class="text-[15px] font-medium text-center">No shared budget declared.</p>
      <p class="text-[13px] text-fg-2 mt-2 max-w-[56ch] mx-auto text-center leading-relaxed">
        If every target leaves through its own identity, there is nothing to share — which is the
        cheapest way to be right.
      </p>

      <div class="mt-8 pt-7 border-t border-line max-w-[62ch] mx-auto">
        <p class="label">Declaring one</p>
        <p class="text-[13px] text-fg-2 leading-relaxed">
          A shared budget is not created on this page. It exists when two targets declare a budget
          with the <span class="font-mono text-[12.5px]">same id</span> and
          <span class="font-mono text-[12.5px]">store: kv</span>, inside the same application — that
          pair is the key they both spend against.
        </p>
        <ol class="mt-4 space-y-2.5 text-[13px] text-fg-2">
          <li class="flex gap-2.5">
            <span class="chip shrink-0 mt-px">1</span>
            <span>
              Open a target that leaves through the shared identity, press <b>Edit</b>, then
              <b>Add budget</b>.
            </span>
          </li>
          <li class="flex gap-2.5">
            <span class="chip shrink-0 mt-px">2</span>
            <span>
              Give it an id that names the thing being shared —
              <span class="font-mono text-[12.5px]">egress-ip</span>, not
              <span class="font-mono text-[12.5px]">limit-2</span> — and set <b>Store</b> to
              <span class="font-mono text-[12.5px]">kv</span>.
            </span>
          </li>
          <li class="flex gap-2.5">
            <span class="chip shrink-0 mt-px">3</span>
            <span>
              Repeat on every other target that shares it, with the <b>same id, cap and window</b>.
              They appear here as one budget with several members.
            </span>
          </li>
        </ol>
        <p class="hint mt-5">
          Two things worth knowing first. The spend leaves the gate cycle and becomes an out-of-band
          call, so it is not rolled back if the cycle aborts — the refund is issued inside the same
          cycle instead. And <span class="font-mono text-[12.5px]">rolling</span> on kv is really a
          fixed window, because the TTL is create-only, so it admits up to twice the cap at the
          boundary: declare these <span class="font-mono text-[12.5px]">calendar</span> unless you
          mean that.
        </p>
        <RouterLink to="/targets" class="inline-flex items-center gap-1.5 mt-5 text-[13px] text-link">
          Go to targets <Icon name="chevron" :size="14" />
        </RouterLink>
      </div>
    </div>

    <div v-else class="card divide-y divide-line">
      <div v-for="b in budgets" :key="`${b.application}/${b.id}`" class="px-5 py-4">
        <div class="flex items-center gap-3 flex-wrap">
          <span class="font-mono text-[13px] font-medium">{{ b.id }}</span>
          <span v-if="b.application" class="chip text-fg-3">{{ b.application }}</span>
          <span class="chip">{{ num(b.cap) }} / {{ windowOf(b) }}</span>
          <span v-if="b.alignment" class="chip">{{ b.alignment }}</span>
          <span v-if="b.enforcement" class="chip">{{ b.enforcement }}</span>
          <span class="ml-auto text-[13px] font-semibold tabular-nums"
                :class="utilisation(b) > 1 ? 'text-bad' : utilisation(b) >= 0.85 ? 'text-warn' : ''">
            {{ pct(utilisation(b)) }}
          </span>
        </div>

        <div class="mt-2.5">
          <BudgetBar :used="b.used" :cap="b.cap" :assumed="b.confidence === 'assumed'" :height="7" />
        </div>

        <div class="flex items-center gap-2 mt-2 text-[11.5px] text-fg-3 flex-wrap">
          <template v-if="b.members?.length">
            <span>{{ b.members.length === 1 ? 'declared by' : 'shared by' }}</span>
            <RouterLink v-for="m in b.members" :key="m" :to="targetKeyPath(m)"
                        class="chip hover:text-fg transition-colors">{{ splitTargetKey(m).name }}</RouterLink>
          </template>
          <!-- One member is not a shared budget yet. Saying so beats showing it
               as one and letting somebody believe a second target is being held
               back by it. -->
          <span v-if="b.members?.length === 1">
            — kv-held, but nothing shares it yet
          </span>
          <span v-if="b.local_lease" class="ml-auto">
            {{ num(b.local_lease) }} leased locally
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
            <span class="font-mono">{{ num(b.cap) }} / {{ windowOf(b) }}</span>, which is simply the
            one loaded first.
          </p>
          <ul class="mt-2 space-y-1 font-mono text-[11.5px]">
            <li v-for="c in b.conflicts" :key="c.target">
              {{ splitTargetKey(c.target).name }} — {{ num(c.cap) }} / {{ period(c.periodSeconds) }}, {{ c.alignment }}
            </li>
          </ul>
        </div>

        <p v-if="b.alignment === 'rolling'" class="mt-2 text-[11.5px] text-warn">
          rolling on kv is a fixed window — up to 2× at the boundary
        </p>
        <p v-else-if="b.last_breach_at" class="mt-2 text-[11.5px] text-bad">
          breached {{ ago(b.last_breach_at) }}
        </p>
      </div>
    </div>
  </div>
</template>
