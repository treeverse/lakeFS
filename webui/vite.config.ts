import { defineConfig, splitVendorChunkPlugin } from 'vite';
import react from '@vitejs/plugin-react';
// import replace from "@rollup/plugin-replace"; // TODO: see TODO below
import eslintPlugin from "vite-plugin-eslint";
import * as http from "node:http";

const baseConfig = {
    test: {
        environment: 'happy-dom',
        exclude: ["./test/e2e/**/*", "./node_modules/**/*"],
    },
    plugins: [
        // TODO: revive this? (it's from the deleted "vite.config.mts", and isn't compatible with vite-plugin-eslint)
        // replace({
        //     preventAssignment: true,
        //     include: ['src/**/*.jsx', 'src/**/*.js'],
        //     values: {
        //         __buildVersion: process.env.VERSION || 'dev',
        //     }
        // }),
        react(),
        eslintPlugin({
            include: ['src/**/*.jsx', 'src/**/*.js', 'src/**/*.ts', 'src/**/*.tsx']
        }),
        splitVendorChunkPlugin(),
    ],
    publicDir: './pub',
};

export default defineConfig(({command, mode}) => {
    if (mode === 'npm') {
        return {
            build: {
                outDir: 'dist_npm',
                lib: {
                    entry: 'src/index.ts',
                    name: 'LakeFSApp',
                    fileName: (format) => `index.${format}.js`
                },
                rollupOptions: {
                    external: ['react', 'react-dom'],
                    output: {
                        globals: {
                            react: 'React',
                            'react-dom': 'ReactDOM'
                        }
                    }
                }
            },
            plugins: [react()]
        }
    }

    // in development
    // TODO: do we need to also support 'test'?
    // if (command === 'serve' || command === 'test') {
    if (command === 'serve') {
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

    return baseConfig;
});
