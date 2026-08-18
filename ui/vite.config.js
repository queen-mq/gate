import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import tailwindcss from '@tailwindcss/vite'

// The build output is embedded into the gate binary by rust-embed, so it
// must be fully self-contained and served from the root. `vite dev` proxies the
// API to a locally running gate so the console can be worked on with hot
// reload against real state.
export default defineConfig({
  plugins: [vue(), tailwindcss()],
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    // One CSS file, and one JS chunk per lazily-routed view — rollup numbers
    // them off the same name. Fixed names rather than hashed ones: the assets
    // are embedded in the binary and revalidated with `no-cache`, so a hash
    // would buy nothing and make every build a different set of files.
    rollupOptions: {
      output: {
        entryFileNames: 'assets/console.js',
        chunkFileNames: 'assets/console.js',
        assetFileNames: 'assets/[name][extname]',
      },
    },
  },
  server: {
    port: 5274,
    proxy: {
      '/api': 'http://localhost:8788',
      '/v1': 'http://localhost:8788',
    },
  },
})
