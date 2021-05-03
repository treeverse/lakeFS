import reactRefresh from '@vitejs/plugin-react-refresh'

// https://vitejs.dev/config/
export default ({ command }) => {
  const baseConfig = {
    plugins: [reactRefresh()],
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
