/*
  The tooltip copy for the target editor, kept in one file on purpose.

  A form for this document is not hard because the fields are numerous; it is
  hard because half of them are traps. `alignment` has no default and guessing
  it is a factor-of-two overshoot at the window boundary. `cost.max` below a cap
  is a lane that blocks forever without ever reaching a DLQ. Two lanes both
  claiming the ceiling enforce it twice. None of that is visible from the field
  name, and a tooltip that reads "the alignment of the window" is worse than no
  tooltip because it costs a hover to learn nothing.

  So every entry here says WHY the field exists and WHAT GOES WRONG when it is
  set badly, in the register of TARGET_SPEC.md — dry, concrete, with the
  measured numbers where the spec has measured numbers. The § markers point at
  the section of that document the text is drawn from, so the two can be kept
  honest against each other.

  Prose, not markup: the tooltip renders text, and a paragraph an operator can
  read on a phone is the whole requirement.
*/

export const HELP = {
  /* ------------------------------------------------------------- identity */

  // §2, and the doc comment on TargetSpec::application
  application:
    'Who owns this target. Applications do not share ceilings — that is the ' +
    'whole point of the concept. Two teams calling the same vendor with their ' +
    'own credentials have two ceilings, so they get two targets and never ' +
    'coordinate; two callers that share one credential are not two ' +
    'applications, they are two lanes of one target. The name scopes the ' +
    'queues, the stored spec, the observability and — the part that matters ' +
    'most — which targets a sync is allowed to reap.',

  // §2
  name:
    'The identity of the target, and the string every queue and stored ' +
    'document is derived from. Renaming is not a modification: it is a new ' +
    'target, and the old one keeps running until it drains. Lowercase, digits ' +
    'and dashes, up to 63 characters.',

  // §2, §10
  version:
    'Bumped whenever a change re-founds the counters: a budget\'s period, ' +
    'alignment, scope or store; the admitted partitioning; a removed lane. A ' +
    'PUT that changes one of those without a higher version is refused with a ' +
    '409. The reason is mechanical — a new partition id is a counter that ' +
    'starts at zero, so an in-place change would restart the limiter with a ' +
    'full tank at exactly the moment you are changing the limits because ' +
    'something went wrong. Caps, floors, concurrency and provenance are hot: ' +
    'they apply immediately and need no bump.',

  // §2, §3.5
  egress:
    'A label for the network identity this target leaves through — ' +
    'informative only. Two targets behind the same NAT contend for the same ' +
    'per-IP ceiling, and no gate can enforce that, because the gate\'s ' +
    'isolation is exactly the partition and two targets are two partitions. ' +
    'The shared ceiling is declared as its own budget resource and referenced; ' +
    'this field only records which identity we are talking about.',

  /* -------------------------------------------------------------- budgets */

  // §3
  budgets:
    'The unit of limit. A work item spends every budget whose match selects ' +
    'it, and is admitted only if all of them admit; the denial names the ' +
    'budget that refused. A target with no budget limits nothing.',

  // §3
  budgetId:
    'Appears in every denial, every metric and every alarm — this is the ' +
    'string an operator reads when they want to know which window is holding ' +
    'the work. Make it readable: `ip-10s`, not `b2`. Unique within the target.',

  // §3, §5
  cap:
    'The ceiling for one window, in cost units rather than messages. One work ' +
    'item can produce many HTTP calls — a calendar push touches N listings and ' +
    'the adapter emits a call per listing — and the vendor counts calls, so ' +
    'counting messages here would enforce the wrong limit.',

  // §3, §6
  period:
    'The length of the window the cap applies over; the minimum is one second. ' +
    'The shortest window across all budgets is the one that binds in practice, ' +
    'and it is also what the pacing lease has to stay well under. Changing it ' +
    'changes what the accumulated state means, so it re-founds the counters ' +
    'and needs the version bumped.',

  // §3.1 — the field the whole "no default" argument was written for
  alignment:
    'Rolling means never more than the cap in any window of that length. ' +
    'Calendar means the counter resets on the clock boundary. They differ by a ' +
    'factor of two at that boundary: with calendar and a cap of 100 a minute ' +
    'you can legitimately send 100 calls at 12:00:59 and 100 more at 12:01:00. ' +
    'If the vendor means rolling and we implement calendar, that burst is a ' +
    'breach — and it is the first thing that happens under load, not the last. ' +
    'There is no default here on purpose: whoever declares the budget has to ' +
    'go and look at what the vendor says. When the vendor does not say, ' +
    'rolling with confidence "assumed" is the safe answer, because rolling is ' +
    'the tighter of the two.',

  // §3.2
  matchOp:
    'Which operations this budget selects, by declared operation name. It can ' +
    'never be a URL: the gate decides before the HTTP call exists, so there is ' +
    'no path to match against and there never will be. Dot-separated segments ' +
    'with a glob on the suffix — `listing.*` takes `listing.create` and ' +
    '`listing.rooms`. No regex; a pattern language in a config file is a ' +
    'failure surface we do not pay for. Leave it empty to select everything, ' +
    'which is how a global ceiling is written. The consequence lands on the ' +
    'caller: an op nobody declared is a rejected push, not a default, because ' +
    'otherwise one typo runs that traffic under the global ceiling alone and ' +
    'nobody finds out.',

  // §3.3
  scope:
    'The dimensions that make up the counter key. Nothing selected is one ' +
    'counter for the whole target; host is one counter per host; entity is one ' +
    'per listing, apartment or property. Every dimension named here must be ' +
    'present as an attribute on the work item, or the push is refused — better ' +
    'a rejected push than a counter silently keyed on nothing. Per app, per ' +
    'host, per listing and per machine account are all this one field with a ' +
    'different dimension. Changing it re-founds the counters and needs a ' +
    'version bump.',

  // §3.4
  maxKeys:
    'How many counters this budget actually holds. A budget with no scope is ' +
    'one number; a budget scoped to entity on a portal with 200,000 listings ' +
    'is 200,000 numbers, and they have to live somewhere. This is a verifiable ' +
    'declaration, not an estimate: with store "gate" the PUT is refused above ' +
    '5,000 keys. Required as soon as a scope is set, because undeclared ' +
    'cardinality is cardinality nobody can check.',

  // §3.4
  store:
    'Where the counters live. "gate" keeps them in the partition\'s state ' +
    'document — that document has no size ceiling applied to it and is re-read ' +
    'in full on every cycle, so a big document is a big re-read, every cycle. ' +
    'Fine at low cardinality, and nowhere else. "kv" is one row per key with ' +
    'incr and max, which is exactly what kv was written for. The price of kv ' +
    'is declared rather than hidden: the decision leaves the cycle and becomes ' +
    'a synchronous out-of-band call, so the gate is no longer pure for that ' +
    'budget and its spend is not rolled back if the cycle aborts. The rule is ' +
    'one line: low cardinality in the gate, high cardinality in kv.',

  // §3.6
  confidence:
    'Where the number came from, and it is not editorial — it changes the ' +
    'behaviour. "documented" is what the vendor publishes: enforced at 100%, ' +
    'and it must cite a source and a date. "inferred" is our own deduction ' +
    'from real sources: enforced at 100% and marked. "assumed" means we do not ' +
    'know: enforced at 70% of what it says, and drawn hatched everywhere in ' +
    'this console so a guess never looks like a measurement. Recording it is ' +
    'also what stops an assumption surviving two years because nobody ' +
    'remembered it was one.',

  // §3.6, §9 rule 8
  source:
    'The citation behind the number — a URL, a ticket, a run name, a person. ' +
    'Required when the budget claims to be documented, because a number with ' +
    'no source is worse than a declared gap: it cannot be argued with.',

  // §3.6
  asOf:
    'The date this number was read. Rate limits age, and this console flags ' +
    'any source older than a year on every page it appears on. Required when ' +
    'the budget claims to be documented.',

  /* ---------------------------------------------------------------- lanes */

  // §4, §4.1
  lanes:
    'How the ceiling is divided. Lanes are partitions of the same push queue, ' +
    'each with its own pinned gate, so their budgets, their denials and their ' +
    'parking are independent — one lane holding work does not touch the other.',

  // §4
  laneName:
    'Becomes a partition name and a queue name, so it is lowercase, digits and ' +
    'dashes. Removing a lane later re-founds the counters and needs a version ' +
    'bump.',

  // §4.1, and the measured overshoot in TargetSpec::lane_share
  laneCap:
    'How much of the ceiling this lane may claim. Each lane is its own ' +
    'partition holding its own copy of the counters, so the ceiling has to be ' +
    'DIVIDED between lanes and can never be handed to each of them: two lanes ' +
    'both told "use the ceiling" enforce it twice, and a run against a ' +
    'declared 50/s peaked at 93/s before that was refused at declare time. ' +
    '"ceiling" takes whatever the others have not reserved — at most one lane ' +
    'may. "ceiling-minus-measured" takes the residual with a guaranteed floor: ' +
    'the lane that absorbs what the others do not use, at the cost of one ' +
    'measurement window of lag during which the lanes together can overshoot. ' +
    '"absolute" is a static reservation in cost units per second — simple, and ' +
    'it wastes whatever the lane does not use. "share" is the same reservation ' +
    'written as a fraction of the ceiling.',

  // §4, §11.3
  concurrency:
    'How many consumers run this lane. It bounds the work in flight, not the ' +
    'rate — and it is the only place a pure concurrency ceiling towards the ' +
    'vendor can be expressed at all, which means per lane, never per key.',

  // §4, and the lane-floor rule
  floor:
    'Only meaningful for ceiling-minus-measured: the fraction of the ceiling ' +
    'this lane keeps whichever way the measurement goes. With more than one ' +
    'lane it has to be above zero — until a meter has run there is nothing to ' +
    'subtract yet, and a lane with no floor would sit there admitting nothing.',

  // §4, §9 rule 2
  defaultLane:
    'Exactly one lane carries the default, and items that name no lane go to ' +
    'it. Zero defaults means items routed at random, which is why the PUT is ' +
    'refused rather than a lane being picked on your behalf.',

  /* ----------------------------------------------------------------- cost */

  // §5
  costField:
    'The field of the work item that carries its cost. Budgets are denominated ' +
    'in HTTP calls and one item produces N of them, so the producer declares ' +
    'how many. Counting messages instead enforces the wrong limit.',

  // §5
  costDefault:
    'What an item costs when that field is missing. Declared even when it is ' +
    'obviously 1, because a silent default is where the permanent block below ' +
    'comes from.',

  // §5, §9 rule 3 — the field the spec calls the most likely production failure
  costMax:
    'The most a single work item may cost. This is a validation gate, not ' +
    'documentation: the PUT is refused if any budget caps below it. An item ' +
    'that costs more than a cap can never be admitted by that budget, so it ' +
    'sits at the head of its lane forever — and it never reaches a DLQ, ' +
    'because a lease expiring does not charge a retry. A permanent, silent ' +
    'block, and the most likely way this system breaks in production.',

  /* --------------------------------------------------------------- pacing */

  // §6, and the measured figures in validate::pacing_warnings
  leaseSeconds:
    'Two things at once, and both of them push it downwards. It is the quantum ' +
    'at which denied work comes back, and it is the window a lane goes without ' +
    'admitting anything if the replica running it dies. It also beats against ' +
    'the tightest budget window: a lane wakes once per lease, and if it was ' +
    'denied it does not wake again until the next one, so a lease as long as ' +
    'the window means waking at the top of a window that has not decayed yet. ' +
    'Measured against a 200/s ceiling: a 1s window with a 1s lease held 152/s, ' +
    'while the same ceiling written over 10s held 205/s. Keep it under a fifth ' +
    'of the tightest window. Integer seconds with a floor of one — sub-second ' +
    'pacing is not expressible, and a one-second window therefore cannot be ' +
    'paced better than that.',

  // §6, §9 rule 4
  batch:
    'How many items a cycle may take. It has to be at least what a lease\'s ' +
    'worth of the tightest budget allows, or the batch becomes the limiter ' +
    'instead of the budget: the vendor\'s ceiling is never reached, capacity ' +
    'is left on the table, and every gauge in this console shows you ' +
    'comfortably under cap while it happens.',

  /* ------------------------------------------------------------- admitted */

  // §7, §8.1, §10
  partitionBy:
    'How admitted work is repartitioned before consumers pick it up. "entity" ' +
    'serialises per listing or apartment, because the lease on a partition is ' +
    'the mutex — that is how one-mutation-in-flight is obtained with no ' +
    'distributed lock and no fencing token a vendor would refuse anyway. ' +
    '"connection" spreads by connection, "none" does not partition. Changing ' +
    'it changes every partition id, and a new partition id is a counter that ' +
    'restarts at zero, so it re-founds the state and needs a version bump.',

  // §7
  partitions:
    'The real ceiling on execution parallelism: N consumers on a lane with 8 ' +
    'partitions get 8, not N. With entity partitioning a finite number also ' +
    'means two entities can land in the same bucket — that is MORE ' +
    'serialisation than needed, never less, which is the only safe way to be ' +
    'ignorant of a cardinality nobody has measured.',
}

/* What this form cannot express, said out loud rather than left as an absence.
   These sections of TARGET_SPEC.md have no representation in the document the
   server accepts today, so there is nothing to build a control for. */
export const NOT_IN_THIS_FORM = [
  ['exclusive (§8.1)', 'mutual exclusion per entity — declared, but not part of the document the server accepts yet'],
  ['breach (§8.2)', 'the overshoot taxonomy, evaluated caller-side from rules downloaded with the target'],
  ['observability (§8.3)', 'trace sampling and body capture'],
  ['shared budgets (§3.5)', 'a budget that crosses targets is its own resource, declared with PUT /v1/budgets/{id} and referenced'],
]
