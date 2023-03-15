import react from '@vitejs/plugin-react-swc';
import eslintPlugin from 'vite-plugin-eslint';
import replace from '@rollup/plugin-replace';

// https://vitejs.dev/config/
export default ({ command }) => {
  const baseConfig = {
    test: {
      environment: 'happy-dom',
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
        include: ['src/**/*.jsx', 'src/**/*.js', 'src/**/*.ts', 'src/**/*.tsx']
      })
    ],
    publicDir: './pub',
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
