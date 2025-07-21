import { resolve, sep } from 'path';
import { createServer } from 'http';
import { readFileSync, stat, Stats } from 'fs';
import { promisify } from 'util';

import mime from 'mime-types';
import { IHttpOperation } from '@stoplight/types';
import { PrismHttp } from '@stoplight/prism-http/dist/client';
import { getHttpOperationsFromSpec } from '@stoplight/prism-cli/dist/operations';
import { createClientFromOperations } from '@stoplight/prism-http/dist/client';

const statAsync = promisify(stat);

interface MockResponse {
  status: number;
  contentType?: string;
  headers?: Record<string, string>;
  body?: string;
}

export class MockServer {
  private server: any;
  private port: number;
  private projectRoot: string;
  private operations: IHttpOperation[];
  private customMocks: Map<string, MockResponse> = new Map();
  private client: PrismHttp;
  private serverUrl: string;

  constructor(port: number = 3002) {
    this.port = port;
    const baseUrl = process.env.BASE_URL || "http://localhost:8000";
    this.projectRoot = resolve(__dirname, '..', '..', '..', '..');
    this.serverUrl = `http://${new URL(baseUrl).hostname}:${this.port}`;
  }

  /**
   * Starts the mock server by creating an HTTP server that handles different types of requests:
   * - /api/v1/* endpoints: Uses Prism client to generate mock responses based on swagger.yml spec
   * - Custom mocked endpoints: Returns predefined responses set via setApiMock()
   * - Static files: Serves frontend assets (HTML, CSS, JS) from webui/dist directory
   *
   * The Prism client parses the OpenAPI specification and generates realistic
   * mock data that matches the schema definitions for API endpoints.
   */
  async start(): Promise<string> {
    return new Promise(async (resolvePromise, reject) => {
      try {
        const specPath = resolve(__dirname, '../../../../lakefs-oss/api/swagger.yml');
        this.operations = await getHttpOperationsFromSpec(specPath);

        this.client = createClientFromOperations(this.operations, {
          mock: { dynamic: false },
          validateRequest: false,
          validateResponse: false,
          checkSecurity: false,
          errors: true,
          upstreamProxy: undefined,
          isProxy: false,
        });

        this.server = createServer((req, res) => this.routeRequest(req, res));
        this.server.listen(this.port, () => {
          console.log(`OpenAPI Mock server started on ${this.serverUrl}`);
          resolvePromise(`${this.serverUrl}`);
        });

        this.server.on('error', reject);
      } catch (e) {
        console.error('Failed to start OpenAPI mock server:', e);
        reject(e);
      }
    });
  }

  async stop(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.server) {
        this.server.close((error: any) => {
          if (error) {
            console.error('OpenAPI Mock server shutdown error:', {
              error: error instanceof Error ? error.message : String(error),
              stack: error instanceof Error ? error.stack : undefined
            });
            reject(error);
          } else {
            console.log('OpenAPI Mock server stopped successfully');
            resolve();
          }
        });
      } else {
        console.log('OpenAPI Mock server was not running');
        resolve();
      }
    });
  }

  setApiMock(path: string, response: MockResponse, method = 'GET') {
    const key = `${method.toUpperCase()} ${path}`;
    this.customMocks.set(key, response);
  }

  clearMocks() {
    this.customMocks.clear();
  }

  removeMock(path: string, method = 'GET') {
    const key = `${method.toUpperCase()} ${path}`;
    return this.customMocks.delete(key);
  }

  getServerUrl(): string {
    return this.serverUrl;
  }

  /**
   * Routes incoming HTTP requests to appropriate handlers based on URL path and method:
   * 1. OPTIONS requests → handleCorsPreflightRequest() for CORS preflight handling
   * 2. Custom mocked endpoints → handleCustomMockRequest() for predefined responses
   * 3. /api/v1/* endpoints → handleApiRequest() for Prism-generated responses from swagger.yml
   * 4. All other requests → handleStaticFileRequest() for serving frontend files
   *
   * Priority order ensures custom mocks override default API behavior, while static files
   * serve as fallback for any unmatched routes.
   */
  private async routeRequest(req: any, res: any) {
    try {
      if (req.method === 'OPTIONS') {
        return this.handleCorsPreflightRequest(res);
      }

      const url = new URL(req.url || '/', this.serverUrl);
      const requestedPath = decodeURIComponent(url.pathname);
      const method = req.method.toUpperCase();

      if (this.customMocks.has(`${method} ${requestedPath}`)) {
        return this.handleCustomMockRequest(res, requestedPath, method);
      }

      if (requestedPath.startsWith('/api/v1')) {
        return this.handleApiRequest(res, requestedPath, method, req.headers);
      }

      return this.handleStaticFileRequest(res, requestedPath);
    } catch (error) {
      console.error('Error routing request:', {
        url: req.url,
        method: req.method,
        error: error instanceof Error ? error.message : String(error),
        stack: error instanceof Error ? error.stack : undefined
      });
      this.sendErrorResponse(res);
    }
  }

  /**
   * Handle a custom request to an endpoint by storing the wanted endpoint in the customMocks map.
   */
  private handleCustomMockRequest(res: any, requestedPath: string, method: string) {
    try {
      const mockResponse = this.customMocks.get(`${method} ${requestedPath}`);

      if (!mockResponse) {
        return this.sendErrorResponse(res);
      }

      const headers: Record<string, string> = {
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

      res.writeHead(mockResponse.status, headers);
      res.end(mockResponse.body);
    } catch (error) {
      console.error('Error handling custom mock request:', {
        requestedPath,
        method,
        error: error instanceof Error ? error.message : String(error),
        stack: error instanceof Error ? error.stack : undefined
      });
      this.sendErrorResponse(res);
    }
  }

  /**
   * Handle an /api/v1/* request using the Prism client, which generates a mock response for the endpoint based on
   * the swagger.yml structure.
   */
  private async handleApiRequest(res: any, requestedPath: string, httpMethod: string, requestHeaders?: any) {
    res.setHeader('Access-Control-Allow-Origin',  '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers','Content-Type, Authorization');

    try {
      // Remove /api/v1 prefix since Prism expects paths as defined in swagger.yml without this prefix)
      const prismPath = requestedPath.startsWith('/api/v1') ? requestedPath.slice('/api/v1'.length) || '/' : requestedPath;
      const method = httpMethod.toLowerCase() as any;

      // Check if request has "prefer" header and use it for Prism config override
      let configOverride = {};
      if (requestHeaders) {
        const preferValue = requestHeaders.prefer || requestHeaders['prefer'];
        if (preferValue && typeof preferValue === 'string') {
          // Parse prefer header for status code (e.g., "code=401")
          const codeMatch = preferValue.match(/code=(\d+)/);
          if (codeMatch) {
            const statusCode = parseInt(codeMatch[1], 10);
            configOverride = { mock: { code: statusCode } };
          }
        }
      }

      const prismRes = await this.client.request(`${prismPath}`, { method }, configOverride);
      const { status, headers, data } = prismRes;
      const body =
          typeof data === 'string' || Buffer.isBuffer(data)
              ? data
              : JSON.stringify(data, null, 2);

      const nodeHeaders: Record<string, string> = {
        'Content-Type': (headers && headers['content-type']) ?? 'application/json',
        ...(headers || {}),
      } as Record<string, string>;

      res.writeHead(status, nodeHeaders);
      res.end(body);

    } catch (err) {
      console.error('[Mock] Unexpected error in handleApiRequest:', err);
      this.send404Response(res);
    }
  }

  /**
   * Serves static files (HTML, CSS, JS) from the webui/dist directory.
   */
  private async handleStaticFileRequest(res: any, requestedPath: string) {
    try {
      if (!requestedPath) {
        return this.serveIndexHtml(res);
      }

      if (requestedPath === '/') {
        requestedPath = '/index.html';
      }

      await this.serveStaticFile(res, requestedPath);
    } catch (error) {
      console.error('Error in handleStaticFileRequest:', {
        requestedPath,
        error: error instanceof Error ? error.message : String(error),
        stack: error instanceof Error ? error.stack : undefined
      });
      return this.serveIndexHtml(res);
    }
  }

  /**
   * Secure static file serving for web pages
   */
  private async serveStaticFile(res: any, requestedPath: string) {
    try {
      const staticRoot = resolve(this.projectRoot, 'webui', 'dist');
      const safePath = this.securePath(staticRoot, requestedPath);

      if (!safePath) {
        return this.serveIndexHtml(res);
      }

      // Check if file exists and get stats
      let stats: Stats;
      try {
        stats = await statAsync(safePath);
      } catch (error) {
        return this.serveIndexHtml(res);
      }

      if (!stats.isFile()) {
        return this.serveIndexHtml(res);
      }

      let fileContent: Buffer;
      try {
        fileContent = readFileSync(safePath);
      } catch (error) {
        console.error('Error reading file:', {
          path: safePath,
          error: error instanceof Error ? error.message : String(error)
        });
        return this.serveIndexHtml(res);
      }

      const ext = requestedPath.split('.').pop() || '';
      const contentType = mime.lookup(ext) || 'application/octet-stream';

      const headers: Record<string, string> = {
        'Content-Type': contentType,
        'Content-Length': fileContent.length.toString(),
        // Security header
        'X-Content-Type-Options': 'nosniff',
      };

      res.writeHead(200, headers);
      res.end(fileContent);

    } catch (error) {
      console.error('Error serving static file:', {
        requestedPath,
        error: error instanceof Error ? error.message : String(error),
        stack: error instanceof Error ? error.stack : undefined
      });
      return this.serveIndexHtml(res);
    }
  }

  private securePath(root: string, requestedPath: string): string | null {
    try {
      // Remove leading slash and normalize
      const cleanPath = requestedPath.replace(/^\/+/, '');
      const fullPath = resolve(root, cleanPath);

      // Ensure the resolved path is within the root directory
      if (!fullPath.startsWith(root + sep) && fullPath !== root) {
        console.warn('Directory traversal attempt detected:', {
          requestedPath,
          resolvedPath: fullPath,
          root
        });
        return null;
      }

      return fullPath;
    } catch (error) {
      console.error('Error in securePath:', error);
      return null;
    }
  }

  private async serveIndexHtml(res: any) {
    try {
      await this.serveStaticFile(res, '/index.html');
    } catch (error) {
      console.error('Error serving index.html:', {
        error: error instanceof Error ? error.message : String(error),
        stack: error instanceof Error ? error.stack : undefined
      });
      res.writeHead(404, {
        'Content-Type': 'text/html',
      });
      res.end('<!DOCTYPE html><html><head><title>404</title></head><body><h1>Page Not Found</h1></body></html>');
    }
  }

  private send404Response(res: any) {
    res.writeHead(404, {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*'
    });
    res.end(JSON.stringify({ message: 'API endpoint not found in OpenAPI spec' }));
  }

  private handleCorsPreflightRequest(res: any) {
    res.writeHead(200, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization'
    });
    res.end();
  }

  private sendErrorResponse(res: any) {
    res.writeHead(500, {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*'
    });
    res.end(JSON.stringify({ message: 'Internal server error' }));
  }
}