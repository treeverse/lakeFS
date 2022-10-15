import reactRefresh from '@vitejs/plugin-react-refresh';
import eslintPlugin from 'vite-plugin-eslint';
import replace from '@rollup/plugin-replace';

// https://vitejs.dev/config/
export default ({ command }) => {
  const baseConfig = {
    plugins: [
      replace({
          preventAssignment: true,
          include: ['src/**/*.jsx', 'src/**/*.js'],
          values: {
            __buildVersion: process.env.VERSION || 'dev',
          }
      }),
      reactRefresh(),
      eslintPlugin({
        include: ['src/**/*.jsx', 'src/**/*.js', 'src/**/*.ts', 'src/**/*.tsx']
      })
    ],
    publicDir: './pub',
    build: {
      sourcemap: true
    }
  };

  // in development
  if (command === 'serve') {
    return {
      ...baseConfig,
      server: {
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
      }
    };
  } 
  // while building
  return baseConfig;
};
