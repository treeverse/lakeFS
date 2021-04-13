const { PHASE_DEVELOPMENT_SERVER } = require('next/constants')

module.exports = (phase, {defaultConfig}) => {
    if (phase === PHASE_DEVELOPMENT_SERVER) {
        return {
            async rewrites() {
                return [
                    {
                        source: '/api/v1/:path*',
                        destination: 'http://localhost:8000/api/v1/:path*',
                    },
                ];
            },
        }
    }
}