import { defineConfig } from 'vite';

const LB = 'http://10.1.75.53:6205';
const LB_WS = 'ws://10.1.75.53:6205';

export default defineConfig({
  server: {
    host: '0.0.0.0',
    port: 3000,

    proxy: {
      '/ws': {
        target: LB_WS,
        ws: true,
        changeOrigin: true,
      },
      '/health': {
        target: LB,
        changeOrigin: true,
      },
      '/message': {
        target: LB,
        changeOrigin: true,
      },
      '/feed': {
        target: LB,
        changeOrigin: true,
      },
      '/lb': {
        target: LB,
        changeOrigin: true,
      },
      '/api': {
        target: LB,
        changeOrigin: true,
      },
    },
  },
});
