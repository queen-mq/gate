import { createApp } from 'vue'
import { createRouter, createWebHashHistory } from 'vue-router'
import App from './App.vue'
import { api, DEFAULT_APP } from './lib/api.js'
import './style.css'

/*
  A graph's identity is the pair `(application, name)`: two teams may both own
  something they call `airbnb`, holding their own credential against their own
  ceiling, and they are not the same thing. Every route below carries the pair.

  **A graph is the only object.** A "target" is a one-node graph, so the target
  routes resolve to the graph ones rather than rendering a second page that
  would have to say the same things in different words. The old links are kept
  because a link in a runbook does not stop existing when a model changes.

  `beforeEnter` and not `redirect`: a `redirect` function has to answer
  synchronously, and answering this one needs the index.
*/
const NeverRendered = { render: () => null }

async function resolveApplication(to) {
  const { name } = to.params
  let application = DEFAULT_APP
  try {
    const list = (await api.get('/api/targets')) ?? []
    const hit =
      list.find((t) => t.name === name && (t.application || DEFAULT_APP) === DEFAULT_APP) ??
      list.find((t) => t.name === name)
    if (hit) application = hit.application || DEFAULT_APP
  } catch {
    // Unreachable API: send it to `default`, which is where the flat control
    // route would have looked anyway, and let the page report the failure.
  }
  return graphLocation(application, name, to)
}

/* The target shapes, kept as addresses and answered by the graph pages. A
   `/lanes/:lane` link becomes the graph with that path selected, because a lane
   is a path now and the page can show it. */
function graphLocation(app, name, to) {
  const { lane, path } = to.params
  const base = `/apps/${encodeURIComponent(app)}/graphs/${encodeURIComponent(name)}`
  const p = lane || path
  if (p) return `${base}?path=${encodeURIComponent(p)}`
  /* The suffix comes from the ROUTE PATTERN, never from `to.path`. `ok_name`
     admits `edit` as a graph name, so `/targets/edit` ends with `/edit` while
     naming no suffix at all — reading the concrete URL sends that graph to the
     editor instead of to its detail page. The pattern is static. */
  if (to.matched.at(-1)?.path.endsWith('/edit')) return `${base}/edit`
  return base
}

function toGraph(to) {
  return graphLocation(to.params.app, to.params.name, to)
}

const router = createRouter({
  history: createWebHashHistory(),
  routes: [
    { path: '/', component: () => import('./views/Overview.vue'), meta: { nav: 'overview' } },

    { path: '/targets', component: () => import('./views/Targets.vue'), meta: { nav: 'targets' } },
    { path: '/targets/new', redirect: '/graphs/new', meta: { nav: 'graphs' } },

    // A graph is declared whole and drawn whole.
    { path: '/graphs', component: () => import('./views/Graphs.vue'), meta: { nav: 'graphs' } },
    { path: '/graphs/new', component: () => import('./views/GraphEdit.vue'), meta: { nav: 'graphs' } },
    { path: '/apps/:app/graphs/:name', component: () => import('./views/GraphDetail.vue'), props: true, meta: { nav: 'graphs' } },
    { path: '/apps/:app/graphs/:name/edit', component: () => import('./views/GraphEdit.vue'), props: true, meta: { nav: 'graphs' } },
    // A budget's history hangs off the node that declared it: the id is only
    // unique within its node, and two portals both calling a window `per-min`
    // is the normal case, not a collision.
    { path: '/apps/:app/graphs/:name/nodes/:node/budgets/:budget', component: () => import('./views/BudgetHistory.vue'), props: true, meta: { nav: 'graphs' } },

    // ---- the target addresses, answered by the graph pages.
    { path: '/apps/:app/targets/:name', component: NeverRendered, beforeEnter: toGraph, meta: { nav: 'targets' } },
    { path: '/apps/:app/targets/:name/edit', component: NeverRendered, beforeEnter: toGraph, meta: { nav: 'targets' } },
    { path: '/apps/:app/targets/:name/lanes/:lane', component: NeverRendered, beforeEnter: toGraph, meta: { nav: 'targets' } },
    { path: '/apps/:app/targets/:name/budgets/:budget', component: NeverRendered, beforeEnter: toGraph, meta: { nav: 'targets' } },

    { path: '/targets/:name', component: NeverRendered, beforeEnter: resolveApplication, meta: { nav: 'targets' } },
    { path: '/targets/:name/edit', component: NeverRendered, beforeEnter: resolveApplication, meta: { nav: 'targets' } },
    { path: '/targets/:name/lanes/:lane', component: NeverRendered, beforeEnter: resolveApplication, meta: { nav: 'targets' } },
    { path: '/targets/:name/budgets/:budget', component: NeverRendered, beforeEnter: resolveApplication, meta: { nav: 'targets' } },
    { path: '/graphs/:name', component: NeverRendered, beforeEnter: resolveApplication, meta: { nav: 'graphs' } },

    { path: '/budgets', component: () => import('./views/SharedBudgets.vue'), meta: { nav: 'budgets' } },
    { path: '/traces', component: () => import('./views/Traces.vue'), meta: { nav: 'traces' } },
    { path: '/:pathMatch(.*)*', redirect: '/' },
  ],
  scrollBehavior: () => ({ top: 0 }),
})

createApp(App).use(router).mount('#app')
