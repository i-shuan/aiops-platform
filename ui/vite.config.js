import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// 靜態檔由 aiops-lite 自己在 /ui/ 底下提供（src/admin.py）。開發時 `npm run dev`，/api 轉給本機跑的 aiops-lite。
export default defineConfig({
  base: '/ui/',
  plugins: [react()],
  build: { outDir: 'dist', emptyOutDir: true, sourcemap: false },
  server: { proxy: { '/api': 'http://127.0.0.1:8080' } },
});
