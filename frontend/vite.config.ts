import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import path from 'node:path';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [react(), tailwindcss()],
  build: {
    // MapLibre and Vega remain lazy and load only when a user opens a map or
    // chart. Split always-used UI libraries for better browser caching while
    // allowing those two intentional feature bundles to exceed Vite's generic
    // 500 kB warning threshold.
    chunkSizeWarningLimit: 1_100,
    rollupOptions: {
      output: {
        manualChunks: {
          'vendor-icons': ['lucide-react'],
          'vendor-markdown': ['react-markdown', 'remark-gfm'],
          'vendor-motion': ['motion'],
          'vendor-react': ['react', 'react-dom'],
        },
      },
    },
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, 'src'),
    },
  },
  server: {
    port: 5173,
    host: '127.0.0.1',
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
      },
      '/health': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
      },
      '/geo': {
        target: 'http://127.0.0.1:8000',
        changeOrigin: true,
      },
    },
  },
});
