<script setup>
/*
  One refusal per row: the node, the path, and the budget that held it — which is
  the only thing that settles an argument with a caller who swears their request
  was refused.

  The colour rule here is the whole product in one line: a denial is rendered in
  quiet grey. **It is the limiter working.** Only a throttle — the vendor
  refusing work we had admitted — is painted bad, because only a throttle means
  the number we enforce is wrong.
*/
import { ago, clock, pct, traceRef, traceRefPath } from '../lib/api.js'

defineProps({
  traces: { type: Array, default: () => [] },
  showTarget: Boolean,
})

const WORD = {
  ok: 'completed',
  denied: 'denied at the cap',
  throttled: 'throttled by the vendor',
}

function tone(t) {
  return t.outcome === 'throttled' ? 'text-bad' : t.outcome === 'ok' ? 'text-fg-3' : 'text-fg-2'
}
</script>

<template>
  <ul class="divide-y divide-line">
    <li v-for="(t, i) in traces" :key="i"
        class="flex items-center gap-3 px-5 h-[46px] text-[13px]">
      <span class="font-mono text-[11.5px] text-fg-3 tabular-nums w-[64px] shrink-0"
            :title="clock(t.at)">{{ ago(t.at) }}</span>

      <!-- A trace carries its application beside the target. Both halves are
           shown, because a row that said only `airbnb` would be ambiguous the
           day a second team declares one. -->
      <RouterLink
        v-if="showTarget"
        :to="traceRefPath(t)"
        class="chip shrink-0 max-w-[160px] truncate hover:text-fg transition-colors"
        :title="`${traceRef(t).application ?? ''}${traceRef(t).application ? '/' : ''}${traceRef(t).name}`"
      >
        {{ traceRef(t).name }}
        <span v-if="traceRef(t).scoped" class="text-fg-3 ml-1 hidden sm:inline">
          {{ traceRef(t).application }}
        </span>
      </RouterLink>
      <span class="font-mono text-[11.5px] text-fg-3 shrink-0 w-[70px] truncate">{{ t.path ?? t.lane }}</span>
      <span class="font-mono text-[12px] text-fg-2 shrink-0 max-w-[160px] truncate">{{ t.op }}</span>

      <span class="truncate flex-1" :class="tone(t)">
        {{ t.reason || WORD[t.outcome] || t.outcome }}
        <span v-if="t.budget_id" class="font-mono text-fg-3">· {{ t.budget_id }}</span>
      </span>

      <!-- A decision taken against a cap nobody published is worth flagging on
           the decision itself, not only on the budget it came from. -->
      <span v-if="t.cap_was_assumed" class="chip text-warn shrink-0">assumed cap</span>

      <span v-if="t.utilisation !== undefined"
            class="font-mono text-[11.5px] tabular-nums w-[46px] text-right shrink-0"
            :class="t.utilisation > 1 ? 'text-bad' : t.utilisation >= 0.85 ? 'text-warn' : 'text-fg-3'">
        {{ pct(t.utilisation) }}
      </span>
      <span v-else-if="t.node"
            class="font-mono text-[11.5px] text-fg-3 truncate w-[64px] text-right shrink-0">
        {{ t.node }}
      </span>
    </li>
  </ul>
</template>
