import { createApp } from 'vue'
import { createRouter, createWebHashHistory } from 'vue-router'
import App from './App.vue'
import { api, DEFAULT_APP } from './lib/api.js'
import './style.css'

/*
  A target's identity is the pair `(application, name)`: two teams may both own
  something they call `airbnb`, holding their own credential against their own
  ceiling, and they are not the same thing. Every route below carries the pair.

  The flat routes are older than applications and are kept, because a link in a
  runbook does not stop existing when a URL scheme changes. They resolve rather
  than redirect blindly: the API's own flat route resolves inside `default`, but
  a bookmark almost always meant "the one target with that name", so the name is
  looked up and only falls back to `default` when nothing answers. A guard, not
  a view, so the resolution happens once and the address bar is right before
  anything renders.

  `beforeEnter` and not `redirect`: a `redirect` function has to answer
  synchronously, and answering this one needs the target index.
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
  const tail = to.fullPath.replace(/^#?\/targets\/[^/]+/, '')
  return `/apps/${encodeURIComponent(application)}/targets/${encodeURIComponent(name)}${tail}`
}

const router = createRouter({
  history: createWebHashHistory(),
  routes: [
    { path: '/', component: () => import('./views/Overview.vue'), meta: { nav: 'overview' } },
    { path: '/targets', component: () => import('./views/Targets.vue'), meta: { nav: 'targets' } },
    { path: '/targets/new', component: () => import('./views/TargetEdit.vue'), meta: { nav: 'targets' } },

    { path: '/apps/:app/targets/:name', component: () => import('./views/TargetDetail.vue'), props: true, meta: { nav: 'targets' } },
    { path: '/apps/:app/targets/:name/edit', component: () => import('./views/TargetEdit.vue'), props: true, meta: { nav: 'targets' } },
    { path: '/apps/:app/targets/:name/lanes/:lane', component: () => import('./views/LaneDetail.vue'), props: true, meta: { nav: 'targets' } },
    // A budget's history hangs off its target rather than off /budgets: the id
    // is only unique within the target that declared it, and two portals both
    // calling a window `per-min` is the normal case, not a collision.
    { path: '/apps/:app/targets/:name/budgets/:budget', component: () => import('./views/BudgetHistory.vue'), props: true, meta: { nav: 'targets' } },

    { path: '/targets/:name', component: NeverRendered, beforeEnter: resolveApplication, meta: { nav: 'targets' } },
    { path: '/targets/:name/edit', component: NeverRendered, beforeEnter: resolveApplication, meta: { nav: 'targets' } },
    { path: '/targets/:name/lanes/:lane', component: NeverRendered, beforeEnter: resolveApplication, meta: { nav: 'targets' } },
    { path: '/targets/:name/budgets/:budget', component: NeverRendered, beforeEnter: resolveApplication, meta: { nav: 'targets' } },

    // A graph is declared whole and drawn whole: the pair is in the path for the
    // same reason a target's is, because two teams may both own an `airbnb`.
    { path: '/graphs', component: () => import('./views/Graphs.vue'), meta: { nav: 'graphs' } },
    { path: '/apps/:app/graphs/:name', component: () => import('./views/GraphDetail.vue'), props: true, meta: { nav: 'graphs' } },

    { path: '/budgets', component: () => import('./views/SharedBudgets.vue'), meta: { nav: 'budgets' } },

    { path: '/traces', component: () => import('./views/Traces.vue'), meta: { nav: 'traces' } },
    { path: '/:pathMatch(.*)*', redirect: '/' },
  ],
  scrollBehavior: () => ({ top: 0 }),
})

createApp(App).use(router).mount('#app')
