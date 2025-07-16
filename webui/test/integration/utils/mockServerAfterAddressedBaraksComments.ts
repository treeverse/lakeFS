import { createServer } from 'http';
import { readFileSync } from 'fs';
import { join, normalize } from 'path';
import mime from 'mime-types';

export class MockServer {
    private server: any;
    private port: number;
    private hostname: string;
    private apiMocks: Map<string, any> = new Map();
    private webuiDir: string;

    constructor(port: number = 3002) {
        this.port = port;
        // Use the same BASE_URL that Playwright config uses
        const baseUrl = process.env.BASE_URL || "http://localhost:8000";
        this.hostname = new URL(baseUrl).hostname;
        this.webuiDir = normalize(join(__dirname, '..', '..', '..'));
    }

    setApiMock(path: string, response: any) {
        this.apiMocks.set(path, response);
    }

    async start(): Promise<string> {
        return new Promise((resolve, reject) => {
            this.server = createServer((req, res) => {
                this.routeRequest(req, res);
            });

            this.server.listen(this.port, () => {
                resolve(`http://${this.hostname}:${this.port}`);
            });

            this.server.on('error', (error: any) => {
                console.error('Mock server startup error:', {
                    port: this.port,
                    error: error instanceof Error ? error.message : String(error),
                    stack: error instanceof Error ? error.stack : undefined
                });
                reject(error);
            });
        });
    }

    async stop(): Promise<void> {
        return new Promise((resolve, reject) => {
            if (this.server) {
                this.server.close((error: any) => {
                    if (error) {
                        console.error('Mock server shutdown error:', {
                            error: error instanceof Error ? error.message : String(error),
                            stack: error instanceof Error ? error.stack : undefined
                        });
                        reject(error);
                    } else {
                        resolve();
                    }
                });
            } else {
                resolve();
            }
        });
    }

    private routeRequest(req: any, res: any) {
        const requestPath = req.url?.split('?')[0];

        if (req.method === 'OPTIONS') {
            return this.handleCorsPreflightRequest(res);
        }

        if (requestPath && this.apiMocks.has(requestPath)) {
            return this.handleMockRequest(req, res);
        }

        if (req.url?.startsWith('/api/')) {
            return this.handleUnmockedApiRequest(res);
        }

        return this.handleStaticFileRequest(req, res);
    }

    private handleMockRequest(req: any, res: any) {
        try {
            const requestPath = req.url?.split('?')[0];
            console.log("in handleMockRequest", "requestPath:", requestPath)
            const mockResponse = this.apiMocks.get(requestPath);

            this.validateMockResponse(mockResponse, requestPath);

            const headers = this.buildResponseHeaders(mockResponse);

            res.writeHead(mockResponse.status, headers);
            res.end(mockResponse.body);
        } catch (error) {
            console.error('Error handling mock request:', {
                url: req.url,
                method: req.method,
                error: error instanceof Error ? error.message : String(error),
                stack: error instanceof Error ? error.stack : undefined
            });
            this.sendErrorResponse(res);
        }
    }

    private validateMockResponse(mockResponse: any, requestPath: string) {
        if (typeof mockResponse.status !== 'number') {
            throw new Error(`Mock for ${requestPath} missing required 'status' field`);
        }
    }

    private buildResponseHeaders(mockResponse: any) {
        const headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization',
            ...mockResponse.headers
        };

        if (mockResponse.contentType) {
            headers['Content-Type'] = mockResponse.contentType;
        } else if (mockResponse.body) {
            headers['Content-Type'] = 'application/json';
        }

        return headers;
    }

    private sendErrorResponse(res: any) {
        res.writeHead(500, {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        });
        res.end(JSON.stringify({ message: 'Internal server error' }));
    }

    private handleUnmockedApiRequest(res: any) {
        res.writeHead(404, {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        });
        res.end(JSON.stringify({ message: 'API endpoint not mocked' }));
    }

    private handleCorsPreflightRequest(res: any) {
        res.writeHead(200, {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization'
        });
        res.end();
    }

    private handleStaticFileRequest(req: any, res: any) {
        try {
            if (!req.url) {
                this.serveIndexHtml(res);
                return;
            }

            const url = new URL(req.url, `http://${this.hostname}:${this.port}`);
            let filePath = url.pathname;

            if (filePath === '/') {
                filePath = '/index.html';
            }

            const fileContent = this.loadFile(filePath);
            if (fileContent) {
                this.sendFileResponse(res, filePath, fileContent);
            } else {
                this.serveIndexHtml(res);
            }
        } catch (error) {
            console.error('Error serving static file:', {
                url: req.url,
                error: error instanceof Error ? error.message : String(error),
                stack: error instanceof Error ? error.stack : undefined
            });
            this.serveIndexHtml(res);
        }
    }

    private serveIndexHtml(res: any) {
        try {
            const fileContent = this.loadFile('/index.html');
            if (fileContent) {
                this.sendFileResponse(res, '/index.html', fileContent);
            } else {
                res.writeHead(404);
                res.end('Not found');
            }
        } catch (error) {
            console.error('Error serving index.html:', {
                error: error instanceof Error ? error.message : String(error),
                stack: error instanceof Error ? error.stack : undefined
            });
            res.writeHead(404);
            res.end('Not found');
        }
    }

    private sendFileResponse(res: any, filePath: string, content: Buffer) {
        const ext = filePath.split('.').pop() || '';
        const mimeType = mime.lookup(ext) || 'application/octet-stream';
        res.writeHead(200, { 'Content-Type': mimeType });
        res.end(content);
    }

    private loadFile(filePath: string): Buffer | null {
        try {
            const fullPath = normalize(join(this.webuiDir, 'dist', filePath));
            return readFileSync(fullPath);
        } catch (error) {
            console.error('Error loading file:', {
                filePath,
                fullPath: normalize(join(this.webuiDir, 'dist', filePath)),
                error: error instanceof Error ? error.message : String(error)
            });
            return null;
        }
    }
}