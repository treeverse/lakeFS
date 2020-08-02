const { createProxyMiddleware } = require('http-proxy-middleware');

module.exports = function(app) {
    const handler = createProxyMiddleware({
        target: 'http://localhost:8000',
        changeOrigin: true,
    });
    app.use('/api', handler)
    app.use('/auth', handler)
};
