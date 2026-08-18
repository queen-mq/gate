import { ref, computed } from 'vue'

const TOKEN_KEY = 'gate-admin-token'

export const adminToken = ref(localStorage.getItem(TOKEN_KEY) || '')
/*
  Auth state machine for the console:
  - 'unknown'  boot, /api/me in flight
  - 'ready'    a session (or admin token / open mode) answers for us
  - 'login'    the API said 401 — show the login screen
*/
export const authState = ref('unknown')
export const me = ref(null) // { actor, email, role }

/*
  Read is anyone who got through the door; write is the admin list. The server
  enforces it on the METHOD, so the console cannot grant itself anything by
  getting this wrong — but a console that shows an enabled Save to somebody who
  will be answered with a 403 has wasted their time and taught them to distrust
  the buttons. Absent is NOT admin: during the /api/me round trip nothing is
  editable, rather than everything being editable for one frame.
*/
export const isAdmin = computed(() => me.value?.role === 'admin')
export const READ_ONLY_NOTE = 'read-only: your account is not in GATE_ADMIN_EMAILS'
/* A GET that clears the cookie and redirects, so it is a link and not a fetch —
   going through the API client would follow the redirect inside XHR and leave
   the console showing a stale identity. */
export const LOGOUT_URL = '/api/auth/logout'

export function setAdminToken(t) {
  adminToken.value = t
  if (t) localStorage.setItem(TOKEN_KEY, t)
  else localStorage.removeItem(TOKEN_KEY)
}

async function request(path, { method = 'GET', body } = {}) {
  const headers = {}
  if (body !== undefined) headers['content-type'] = 'application/json'
  if (adminToken.value) headers.authorization = `Bearer ${adminToken.value}`

  const res = await fetch(path, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
    credentials: 'same-origin',
  })

  if (res.status === 401) {
    authState.value = 'login'
    throw new Error('sign in required')
  }
  if (res.status === 204) return null

  const text = await res.text()
  let payload = null
  try {
    payload = text ? JSON.parse(text) : null
  } catch {
    // A rejected body never reaches our handlers, so the framework answers in
    // plain text. That text is still the most useful sentence the operator can
    // be shown, so it is carried through as the message rather than replaced
    // with a status code.
    payload = { error: text }
  }
  if (!res.ok) {
    const err = new Error(payload?.error || payload?.message || `HTTP ${res.status}`)
    /* The target editor has to tell 422 from 409 — one means "this document is
       wrong", the other "this document is right but re-founds the counters" —
       and the two need different words above the same form, on different
       fields. */
    err.status = res.status
    throw err
  }
  return payload
}

export const api = {
  get: (p) => request(p),
  post: (p, body) => request(p, { method: 'POST', body }),
  put: (p, body) => request(p, { method: 'PUT', body }),
  patch: (p, body) => request(p, { method: 'PATCH', body }),
  del: (p) => request(p, { method: 'DELETE' }),
}

export async function fetchMe() {
  try {
    me.value = await request('/api/me')
    authState.value = 'ready'
  } catch {
    if (authState.value !== 'login') authState.value = 'login'
  }
}

/* ---------------------------------------------------------------- naming */

export const DEFAULT_APP = 'default'

/*
  A target's identity is the PAIR, never the name alone: two teams may both own
  something they call `airbnb` and they are not the same thing. Every link in
  the console is built here so no view can invent a different shape.
*/
export function targetPath(application, name, suffix = '') {
  const app = encodeURIComponent(application || DEFAULT_APP)
  return `/apps/${app}/targets/${encodeURIComponent(name)}${suffix}`
}

export function targetApi(application, name) {
  return `/api/apps/${encodeURIComponent(application || DEFAULT_APP)}/targets/${encodeURIComponent(name)}`
}

/*
  The meter keys its series and its traces on `application/name`, so a trace row
  carries the pair in one string. Not every row does: anything restored from the
  durable roll-ups is written under the bare name, and rows recorded before
  applications existed have no pair to carry. A console that assumed one of the
  two shapes would silently drop half of them.

  So an unscoped key is reported as such — `application: null` — rather than
  being told it belongs to `default`. Guessing there would be a lie with a link
  on it, and the link would 404.
*/
export function splitTargetKey(key) {
  const s = String(key ?? '')
  const at = s.indexOf('/')
  return at === -1
    ? { application: null, name: s, scoped: false }
    : { application: s.slice(0, at), name: s.slice(at + 1), scoped: true }
}

/* A trace, a breach, a roll-up row: the server sends the application as its
   own field and the target bare, because that is how the table keys them. It
   used to send the pair joined into one string, and some rows still arrive
   that way, so both shapes resolve here rather than at six call sites. */
export function traceRef(t) {
  const app = t?.application
  if (app) return { application: app, name: String(t?.target ?? ''), scoped: true }
  return splitTargetKey(t?.target)
}

export function traceRefPath(t) {
  const k = traceRef(t)
  return k.scoped ? targetPath(k.application, k.name) : `/targets/${encodeURIComponent(k.name)}`
}

/* Where a row that names its target as one string should link. An unscoped one
   goes through the flat route, which looks the application up instead of
   asserting one. */
export function targetKeyPath(key) {
  const k = splitTargetKey(key)
  return k.scoped ? targetPath(k.application, k.name) : `/targets/${encodeURIComponent(k.name)}`
}

/* An unscoped key matches on the name alone, because that is all it claims. */
export function sameTarget(key, application, name) {
  const k = splitTargetKey(key)
  return k.name === name && (!k.scoped || k.application === application)
}

/* ------------------------------------------------------------ formatting */

export function num(n) {
  return (n ?? 0).toLocaleString()
}

/* Rates are the console's main unit and they span four orders of magnitude
   across portals — 20 TPS on one operation, 400/s on another. One decimal
   under 10, none above, so a column of them stays readable. */
export function rate(n) {
  const v = n ?? 0
  if (v === 0) return '0'
  if (v < 10) return v.toFixed(1)
  return Math.round(v).toLocaleString()
}

export function pct(x) {
  if (x === null || x === undefined) return '—'
  return `${Math.round(x * 100)}%`
}

/*
  A budget carries both numbers: `utilisation` is the 0..1 fraction of the
  window that has been spent, `used` the absolute figure behind it. They agree,
  but only the fraction survives a cap that is zero or missing, so it is the
  one every gauge in the console is fed.
*/
export function utilisation(b) {
  if (!b) return 0
  if (b.utilisation !== null && b.utilisation !== undefined) return b.utilisation
  return b.cap > 0 ? (b.used ?? 0) / b.cap : 0
}

/* A period comes off the wire as seconds because that is what the target spec
   declares. Rendering it back as `10s` / `5m` / `7d` keeps the console and the
   spec speaking the same words. */
export function period(seconds) {
  const s = seconds ?? 0
  if (s % 86400 === 0) return `${s / 86400}d`
  if (s % 3600 === 0) return `${s / 3600}h`
  if (s % 60 === 0) return `${s / 60}m`
  return `${s}s`
}

/* Instants arrive as epoch milliseconds from the gate's own clock and as ISO
   strings from anything that went through Postgres on the way. Both are the
   same instant to an operator, so both are accepted everywhere. */
export function toDate(t) {
  if (t === null || t === undefined || t === '') return null
  const d = typeof t === 'number' ? new Date(t) : new Date(t)
  return isNaN(d.getTime()) ? null : d
}

export function ago(t) {
  const d = toDate(t)
  if (!d) return '—'
  const s = (Date.now() - d.getTime()) / 1000
  const abs = Math.abs(s)
  const unit =
    abs < 60 ? [Math.round(abs), 's'] :
    abs < 3600 ? [Math.round(abs / 60), 'm'] :
    abs < 86400 ? [Math.round(abs / 3600), 'h'] :
    [Math.round(abs / 86400), 'd']
  return s >= 0 ? `${unit[0]}${unit[1]} ago` : `in ${unit[0]}${unit[1]}`
}

export function clock(t) {
  const d = toDate(t)
  if (!d) return '—'
  return d.toLocaleTimeString([], {
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  })
}

export function datetime(t) {
  const d = toDate(t)
  if (!d) return '—'
  return d.toLocaleString([], {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
  })
}
