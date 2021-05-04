import reactRefresh from '@vitejs/plugin-react-refresh';
import eslintPlugin from 'vite-plugin-eslint';


// https://vitejs.dev/config/
export default ({ command }) => {
  const baseConfig = {
    plugins: [
      reactRefresh(),
      eslintPlugin({
        include: ['src/**/*.jsx', 'src/**/*.js']
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
          }
        }
      }
    };
  } 
  // while building
  return baseConfig;
};
