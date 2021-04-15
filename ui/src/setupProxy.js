const { createProxyMiddleware } = require('http-proxy-middleware');

const localServer = 'http://localhost:8000';

module.exports = function(app) {
    const handler = createProxyMiddleware({
        target: localServer,
        changeOrigin: true,
    });
    app.use('/api', handler)
    app.use('/auth', handler)
};
