/*
  The target document, as a form.

  Three jobs, kept out of the components so each of them is readable on its own:

    1. DRAFT <-> WIRE. The form edits strings and split-out units; the server
       takes `periodSeconds` and a cap policy encoded in one string. Neither
       shape should leak into the other.
    2. THE MIRROR. The cheap rules from the server's validator, run as the
       operator types, so a refusal that was going to happen is on screen before
       the round trip. It is a mirror and nothing more: the server is the
       authority, the console never refuses a submit on its own reading, and
       whatever comes back is shown verbatim.
    3. LANDING THE REFUSAL. The server answers with `[rule] sentence; [rule]
       sentence`. The sentences are written for a human and are shown as they
       are — but the rule name says which field produced them, so it also gets
       attached to that input.
*/

/* Above this a scoped budget's counters do not belong in the gate's state
   document: it is re-read in full every cycle. Mirrors GATE_MAX_KEYS. */
export const GATE_MAX_KEYS = 5000

export const DIMS = ['host', 'entity', 'account', 'connection', 'tenant']
export const CAP_KINDS = ['ceiling', 'ceiling-minus-measured', 'absolute', 'share']

const UNITS = [
  ['d', 86400],
  ['h', 3600],
  ['m', 60],
  ['s', 1],
]

export function splitPeriod(seconds) {
  const s = Number(seconds) || 0
  for (const [unit, size] of UNITS) {
    if (s >= size && s % size === 0) return { value: s / size, unit }
  }
  return { value: s, unit: 's' }
}

export function joinPeriod(value, unit) {
  const size = UNITS.find(([u]) => u === unit)?.[1] ?? 1
  return Math.round(Number(value) || 0) * size
}

/* ------------------------------------------------------------- new drafts */

/* A row identity that survives being reordered or having a sibling removed.
   Keying the v-for on the array index instead would move every input's value up
   one row when a budget in the middle is deleted, and the operator would be
   looking at a document they did not write. Never serialised: `toSpec` builds
   the wire object field by field. */
let rowKey = 0
export const nextRowKey = () => `r${++rowKey}`

export function blankBudget() {
  return {
    _k: nextRowKey(),
    id: '',
    cap: '',
    periodValue: '',
    periodUnit: 's',
    // Deliberately empty. `alignment` has no default in the spec and it must
    // not acquire one here either: a pre-selected `rolling` is exactly the
    // silent guess the field exists to prevent.
    alignment: '',
    matchOps: '',
    scope: [],
    maxKeys: '',
    store: 'gate',
    confidence: '',
    source: '',
    asOf: '',
  }
}

export function blankLane(isDefault = false) {
  return {
    _k: nextRowKey(),
    name: '', capKind: 'ceiling', capValue: '', concurrency: 4, floor: 0, default: isDefault,
  }
}

export function blankDraft(application = 'default') {
  return {
    application,
    name: '',
    version: 1,
    egress: '',
    budgets: [blankBudget()],
    lanes: [blankLane(true)],
    cost: { field: 'httpCost', default: 1, max: 1 },
    pacing: { leaseSeconds: 1, batch: 200 },
    admitted: { partitionBy: 'connection', partitions: 64 },
  }
}

/* -------------------------------------------------------- wire -> draft */

export function toDraft(spec) {
  if (!spec) return blankDraft()
  return {
    application: spec.application ?? 'default',
    name: spec.name ?? '',
    version: spec.version ?? 1,
    egress: spec.egress ?? '',
    budgets: (spec.budgets ?? []).map((b) => {
      const p = splitPeriod(b.periodSeconds)
      return {
        _k: nextRowKey(),
        id: b.id ?? '',
        cap: b.cap ?? '',
        periodValue: p.value,
        periodUnit: p.unit,
        alignment: b.alignment ?? '',
        matchOps: (b.match?.op ?? []).join(', '),
        scope: [...(b.scope ?? [])],
        maxKeys: b.maxKeys ?? '',
        store: b.store ?? 'gate',
        confidence: b.confidence ?? '',
        source: b.source ?? '',
        asOf: b.asOf ?? '',
      }
    }),
    lanes: (spec.lanes ?? []).map((l) => {
      const [kind, value] = String(l.cap ?? 'ceiling').split(':')
      return {
        _k: nextRowKey(),
        name: l.name ?? '',
        capKind: CAP_KINDS.includes(kind) ? kind : 'ceiling',
        capValue: value ?? '',
        concurrency: l.concurrency ?? 1,
        floor: l.floor ?? 0,
        default: !!l.default,
      }
    }),
    cost: {
      field: spec.cost?.field ?? 'httpCost',
      default: spec.cost?.default ?? 1,
      max: spec.cost?.max ?? 1,
    },
    pacing: {
      leaseSeconds: spec.pacing?.leaseSeconds ?? 1,
      batch: spec.pacing?.batch ?? 200,
    },
    admitted: {
      partitionBy: spec.admitted?.partitionBy ?? 'connection',
      partitions: spec.admitted?.partitions ?? 64,
    },
  }
}

/* -------------------------------------------------------- draft -> wire */

const n = (v, fallback = 0) => {
  const x = Number(v)
  return Number.isFinite(x) ? x : fallback
}
const trim = (v) => String(v ?? '').trim()

/*
  The document that will actually be PUT.

  Optional fields are OMITTED rather than sent empty. The server declares
  `deny_unknown_fields` and treats `alignment` as required with no default, so
  an empty string is a deserialization error and a `null` source is not the same
  as an absent one. What the JSON disclosure shows is this, byte for byte —
  there is no second serialisation anywhere.
*/
export function toSpec(draft) {
  const spec = {
    application: trim(draft.application) || 'default',
    name: trim(draft.name),
    version: n(draft.version, 1),
    budgets: draft.budgets.map((b) => {
      const out = {
        id: trim(b.id),
        cap: n(b.cap),
        periodSeconds: joinPeriod(b.periodValue, b.periodUnit),
        alignment: b.alignment || undefined,
        scope: [...b.scope],
        store: b.store,
        confidence: b.confidence || undefined,
      }
      const ops = trim(b.matchOps).split(',').map((s) => s.trim()).filter(Boolean)
      if (ops.length) out.match = { op: ops }
      if (trim(b.maxKeys) !== '') out.maxKeys = n(b.maxKeys)
      if (trim(b.source)) out.source = trim(b.source)
      if (trim(b.asOf)) out.asOf = trim(b.asOf)
      return out
    }),
    lanes: draft.lanes.map((l) => ({
      name: trim(l.name),
      cap: laneCap(l),
      concurrency: n(l.concurrency, 1),
      floor: n(l.floor),
      default: !!l.default,
    })),
    cost: {
      field: trim(draft.cost.field),
      default: n(draft.cost.default),
      max: n(draft.cost.max),
    },
    pacing: {
      leaseSeconds: n(draft.pacing.leaseSeconds, 1),
      batch: n(draft.pacing.batch, 200),
    },
    admitted: {
      partitionBy: draft.admitted.partitionBy,
      partitions: n(draft.admitted.partitions, 64),
    },
  }
  if (trim(draft.egress)) spec.egress = trim(draft.egress)
  return spec
}

export function laneCap(l) {
  if (l.capKind === 'absolute' || l.capKind === 'share') return `${l.capKind}:${n(l.capValue)}`
  return l.capKind
}

/* Reservation: what a lane takes off the top before anything is measured. A
   `ceiling` lane reserves nothing and is allocated the residual, which is why
   it does not appear here. Mirrors the `reserved` sum in the validator. */
export function laneReservation(l) {
  if (l.capKind === 'share') return Math.max(0, n(l.capValue))
  if (l.capKind === 'ceiling-minus-measured') return Math.max(0, n(l.floor))
  return 0
}

/* ------------------------------------------------------------ the mirror */

/*
  Keys are the same field paths the editor uses for its inputs, so a problem
  found here and a problem parsed out of a 422 land in exactly the same place.

    name, application, version, egress
    budgets                       — the collection
    budgets.<i>.<field>
    lanes, lanes.<i>.<field>
    cost.field | cost.default | cost.max
    pacing.leaseSeconds | pacing.batch
    admitted.partitionBy | admitted.partitions
*/
export function validateDraft(draft) {
  const out = {}
  const add = (path, msg) => {
    if (!out[path]) out[path] = msg
  }

  const okName = (s) => /^[a-z0-9][a-z0-9-]{0,62}$/.test(s)
  if (!okName(trim(draft.application)))
    add('application', 'Lowercase letters, digits and dashes, starting with a letter or digit.')
  if (!okName(trim(draft.name)))
    add('name', 'Lowercase letters, digits and dashes, starting with a letter or digit.')

  if (!draft.budgets.length) add('budgets', 'A target with no budget does not limit anything.')
  if (!draft.lanes.length) add('lanes', 'A target needs at least one lane.')

  const costMax = n(draft.cost.max)
  if (!trim(draft.cost.field)) add('cost.field', 'Name the field on the work item that carries the cost.')
  if (costMax < n(draft.cost.default)) add('cost.max', 'cost.max is below cost.default.')

  /* ------------------------------------------------------------ budgets */
  const seenIds = new Set()
  draft.budgets.forEach((b, i) => {
    const p = `budgets.${i}`
    const id = trim(b.id)
    if (!id) add(`${p}.id`, 'Every budget needs an id: it is what a denial names.')
    else if (seenIds.has(id)) add(`${p}.id`, `Duplicate budget id \`${id}\` — denials would not be attributable.`)
    seenIds.add(id)

    const cap = n(b.cap)
    if (cap <= 0) add(`${p}.cap`, 'A cap must be above zero.')
    // Rule 3, and the one the spec calls the most likely production failure.
    else if (cap < costMax)
      add(
        `${p}.cap`,
        `This cap is ${cap} but a single item may cost ${costMax}: that item could never be admitted here, ` +
          'and it would sit at the head of its lane forever without ever reaching a DLQ.'
      )

    if (joinPeriod(b.periodValue, b.periodUnit) < 1) add(`${p}.period`, 'The shortest window is one second.')
    if (!b.alignment) add(`${p}.alignment`, 'No default: rolling and calendar differ by a factor of two at the boundary, so this has to be chosen.')
    if (!b.confidence) add(`${p}.confidence`, 'Say where the number came from — it changes how much of it is enforced.')

    // Rule 6.
    if (b.scope.length && trim(b.maxKeys) === '')
      add(`${p}.maxKeys`, 'A scoped budget is one counter per key. Declare how many there are.')
    // Rule 5.
    if (b.store === 'gate' && trim(b.maxKeys) !== '' && n(b.maxKeys) > GATE_MAX_KEYS)
      add(
        `${p}.maxKeys`,
        `${n(b.maxKeys).toLocaleString()} keys in the gate's state document, which is re-read in full every ` +
          `cycle. Above ${GATE_MAX_KEYS.toLocaleString()} it belongs on kv.`
      )

    // Rule 8.
    if (b.confidence === 'documented' && !trim(b.source))
      add(`${p}.source`, 'Documented means the vendor publishes it — cite where.')
    if (b.confidence === 'documented' && !trim(b.asOf))
      add(`${p}.asOf`, 'Documented needs the date it was read: these numbers age.')
  })

  /* -------------------------------------------------------------- lanes */
  const seenLanes = new Set()
  draft.lanes.forEach((l, i) => {
    const p = `lanes.${i}`
    const nm = trim(l.name)
    if (!nm) add(`${p}.name`, 'A lane needs a name: it becomes a partition and a queue.')
    else if (seenLanes.has(nm)) add(`${p}.name`, `Duplicate lane \`${nm}\`.`)
    seenLanes.add(nm)

    if (n(l.concurrency) < 1) add(`${p}.concurrency`, 'A lane with no consumers never runs.')
    if (l.capKind === 'share' && (n(l.capValue) <= 0 || n(l.capValue) > 1))
      add(`${p}.cap`, 'A share is a fraction of the ceiling, between 0 and 1.')
    if (l.capKind === 'absolute' && n(l.capValue) <= 0)
      add(`${p}.cap`, 'An absolute reservation is a rate in cost units per second, above zero.')
    if (l.capKind === 'ceiling-minus-measured' && (n(l.floor) < 0 || n(l.floor) > 1))
      add(`${p}.floor`, 'The floor is a fraction of the ceiling, between 0 and 1.')
    if (l.capKind === 'ceiling-minus-measured' && draft.lanes.length > 1 && n(l.floor) <= 0)
      add(
        `${p}.floor`,
        'Ceiling-minus-measured with no floor: until a meter has run there is nothing to subtract, ' +
          'so this lane would admit nothing at all.'
      )
  })

  // Rule 2.
  const defaults = draft.lanes.filter((l) => l.default).length
  if (defaults !== 1)
    add(
      'lanes',
      defaults === 0
        ? 'No lane is the default, so an item that names no lane would be routed at random.'
        : `${defaults} lanes are marked default; exactly one may be.`
    )

  // The measured one: two lanes each holding "the ceiling" enforce it twice.
  const takers = draft.lanes.filter((l) => l.capKind === 'ceiling' || l.capKind === 'absolute')
  if (takers.length > 1)
    takers.slice(1).forEach((l) => {
      const i = draft.lanes.indexOf(l)
      add(
        `lanes.${i}.cap`,
        `${takers.length} lanes claim the whole ceiling. Each lane is its own partition with its own counters, ` +
          'so the ceiling would be enforced once per lane; at most one may claim it, the rest declare a share or a floor.'
      )
    })

  const reserved = draft.lanes.reduce((a, l) => a + laneReservation(l), 0)
  if (reserved > 1 + 1e-9)
    add('lanes', `Lane reservations sum to ${reserved.toFixed(2)} of the ceiling; they must not exceed 1.00.`)

  /* ------------------------------------------------------------- pacing */
  const lease = n(draft.pacing.leaseSeconds, 1)
  if (lease < 1) add('pacing.leaseSeconds', 'Whole seconds, minimum one — sub-second pacing is not expressible.')

  // Rule 4: the tightest gate-stored budget decides what a lease's worth is.
  const rates = draft.budgets
    .filter((b) => b.store === 'gate')
    .map((b) => {
      const p = joinPeriod(b.periodValue, b.periodUnit)
      return p > 0 ? n(b.cap) / p : 0
    })
    .filter((r) => r > 0)
  if (rates.length) {
    const perLease = Math.min(...rates) * lease
    if (n(draft.pacing.batch, 0) < perLease)
      add(
        'pacing.batch',
        `A lease of ${lease}s allows ${Math.round(perLease).toLocaleString()} items of the tightest budget, ` +
          'so this batch would be the limiter instead of the budget and the ceiling would never be reached.'
      )
  }

  if (n(draft.admitted.partitions) < 1)
    add('admitted.partitions', 'At least one partition, or nothing consumes the admitted work.')

  return out
}

/*
  Warnings the console can compute without a round trip. Not refusals: the PUT
  succeeds and the server says the same thing back, but knowing before you press
  the button is worth more than knowing after.
*/
export function draftWarnings(draft) {
  const out = []
  const lease = n(draft.pacing.leaseSeconds, 1)
  const periods = draft.budgets
    .filter((b) => b.store === 'gate')
    .map((b) => joinPeriod(b.periodValue, b.periodUnit))
    .filter((p) => p > 0)
  if (periods.length) {
    const tightest = Math.min(...periods)
    if (lease * 5 > tightest)
      out.push(
        `A lease of ${lease}s against a tightest window of ${tightest}s: the lane wakes about once per window ` +
          'and cannot recover the budget that decayed while it was parked, so expect roughly three quarters of ' +
          'the declared ceiling' +
          (tightest <= 1 ? ' — and a one-second window cannot do better, because the lease floor is one second.' : '.')
      )
  }
  draft.budgets
    .filter((b) => b.store === 'kv' && b.alignment === 'rolling')
    .forEach((b) =>
      out.push(
        `\`${trim(b.id) || 'this budget'}\` is rolling on kv, which is a fixed window whatever it declares: ` +
          'up to twice the cap at the boundary.'
      )
    )
  return out
}

/* ------------------------------------------------ server refusal -> field */

/* The validator's rule names, and the input each one came from. Rules whose
   subject is one budget or one lane also carry its id in backticks, which is
   how the sentence finds its row. */
const RULE_FIELD = {
  application: () => 'application',
  name: () => 'name',
  budgets: () => 'budgets',
  lanes: () => 'lanes',
  'default-lane': () => 'lanes',
  'lane-shares': (i) => (i === null ? 'lanes' : `lanes.${i}.cap`),
  'lane-unique': (i) => (i === null ? 'lanes' : `lanes.${i}.name`),
  'lane-concurrency': (i) => (i === null ? 'lanes' : `lanes.${i}.concurrency`),
  'lane-floor': (i) => (i === null ? 'lanes' : `lanes.${i}.floor`),
  cost: () => 'cost.max',
  'budget-unique': (i) => (i === null ? 'budgets' : `budgets.${i}.id`),
  'budget-cap': (i) => (i === null ? 'budgets' : `budgets.${i}.cap`),
  'budget-period': (i) => (i === null ? 'budgets' : `budgets.${i}.period`),
  'cost-fits': (i) => (i === null ? 'cost.max' : `budgets.${i}.cap`),
  'max-keys': (i) => (i === null ? 'budgets' : `budgets.${i}.maxKeys`),
  'store-fits': (i) => (i === null ? 'budgets' : `budgets.${i}.maxKeys`),
  provenance: (i) => (i === null ? 'budgets' : `budgets.${i}.source`),
  match: (i) => (i === null ? 'budgets' : `budgets.${i}.matchOps`),
  'batch-fits': () => 'pacing.batch',
  pacing: () => 'pacing.leaseSeconds',
}

const LANE_RULES = new Set(['lane-shares', 'lane-unique', 'lane-concurrency', 'lane-floor'])

/*
  Split `[rule] sentence; [rule] sentence` into per-field sentences.

  The full message is still shown at the top of the editor exactly as it
  arrived — these sentences were written by the thing that knows which rule
  broke, and paraphrasing them would throw away the only part that says what to
  change. This just also puts each one next to its input.
*/
export function mapServerProblems(message, draft, status) {
  const fields = {}
  if (!message) return fields

  // 409 has no rule marker: it is one sentence about one field.
  if (status === 409) {
    fields.version = message
    return fields
  }

  /* Split on the `; ` that separates two problems, and NOT on the ones inside
     a sentence: `[lane-shares] 2 lanes claim the whole ceiling; at most one
     may…` is one problem, and cutting it at the semicolon would put half a
     sentence under an input. The boundary is a semicolon followed by the next
     `[rule]` marker. */
  let matched = false
  for (const chunk of String(message).split(/;\s*(?=\[[a-z-]+\])/)) {
    const m = /^\s*\[([a-z-]+)\]\s*([\s\S]+)$/.exec(chunk)
    if (!m) continue
    matched = true
    const [, name, detail] = m
    const resolve = RULE_FIELD[name]
    if (!resolve) continue
    const subject = detail.match(/`([^`]+)`/)?.[1] ?? null
    let index = null
    if (subject !== null) {
      const list = LANE_RULES.has(name) ? draft.lanes : draft.budgets
      const key = LANE_RULES.has(name) ? 'name' : 'id'
      const at = list.findIndex((x) => trim(x[key]) === subject)
      if (at >= 0) index = at
    }
    const path = resolve(index)
    // Two rules can land on the same input; joined with the separator the
    // server itself used, so the pair still reads as two sentences.
    fields[path] = fields[path] ? `${fields[path]}; ${detail.trim()}` : detail.trim()
  }

  /* A body the server could not deserialize at all never reaches the
     validator, so it carries no rule markers — it names a serde path instead:
     `budgets[0].alignment`, or `budgets[0]: missing field \`alignment\``. That
     is still one field, and still worth landing on it. */
  if (!matched) {
    const at = /(budgets|lanes)\[(\d+)\](?:\.(\w+))?/.exec(message)
    if (at) {
      const [, coll, i, dotted] = at
      const field = dotted ?? /missing field `(\w+)`/.exec(message)?.[1] ?? null
      const path = field
        ? `${coll}.${i}.${field === 'periodSeconds' ? 'period' : field}`
        : coll
      fields[path] = message
    } else {
      const cost = /\bcost\.(\w+)/.exec(message)
      if (cost) fields[`cost.${cost[1]}`] = message
    }
  }

  return fields
}
