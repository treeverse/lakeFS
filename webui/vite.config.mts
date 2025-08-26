import react from '@vitejs/plugin-react-swc';
import eslintPlugin from 'vite-plugin-eslint';
import replace from '@rollup/plugin-replace';
import * as http from "node:http";

// https://vitejs.dev/config/
export default ({ command }) => {
  const baseConfig = {
    test: {
      environment: 'happy-dom',
      exclude: ["./test/e2e/**/*", "./node_modules/**/*"],
    },
    plugins: [
      replace({
          preventAssignment: true,
          include: ['src/**/*.jsx', 'src/**/*.js'],
          values: {
            __buildVersion: process.env.VERSION || 'dev',
          }
      }),
      react(),
      eslintPlugin({
        include: ['src/**/*.jsx', 'src/**/*.js', 'src/**/*.ts', 'src/**/*.tsx'],
        failOnWarning: false,
        failOnError: true
      }),
    ],
    publicDir: './pub',
    build: {
      rollupOptions: {
        output: {
          manualChunks: {
            vendor: ['react', 'react-dom'],
            router: ['react-router-dom'],
            ui: ['@mui/material', '@emotion/react', '@emotion/styled'],
            utils: ['lodash', 'dayjs']
          }
        }
      }
    }
  };

  // in development
  if (command === 'serve' || command === 'test') {
    return {
      ...baseConfig,
      server: {
        port: 3000,
        proxy: {
          '/api': {
            target: 'http://localhost:8000',
            changeOrigin: true,
            secure: false
          },
          '/oidc/login': {
            target: 'http://localhost:8000',
            changeOrigin: false,
            secure: false
          },
          '/logout': {
            target: 'http://localhost:8000',
            changeOrigin: false,
            secure: false
          },
          '/': {
            target: 'http://localhost:8000',
            changeOrigin: false,
            secure: false,
            bypass: (req: http.IncomingMessage) : void | null | undefined | false | string => {
              if ("x-amz-date" in req.headers) {
                return null;
              }
              return req.url;
            }
          }
        }
      },
      build: {
        sourcemap: 'inline',
      },
    };
  } 
  // while building
  return baseConfig;
};
