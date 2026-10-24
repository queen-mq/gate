import { ref } from 'vue'

// What the top bar names: the page's own title and, on a page below a list,
// the one crumb back to it. PageHeader writes it; Header reads it.
export const pageCrumb = ref(null) // { title, parent: { to, label } | null }
