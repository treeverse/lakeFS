import { createServer } from 'http';
import { readFileSync } from 'fs';
import { join, normalize } from 'path';
import { createServer as createPrismServer } from '@stoplight/prism-http-server';
import { getHttpOperationsFromSpec } from '@stoplight/prism-cli/dist/operations';
import { createLogger } from '@stoplight/prism-core';
import mime from 'mime-types';

interface MockResponse {
  status: number;
  headers?: Record<string, string>;
  body?: string;
  contentType?: string;
}

interface EndpointConfig {
  statusCode?: number;
  mediaType?: string;
  exampleKey?: string;
  dynamic?: boolean;
}

export class OpenApiMockServer {
  private server: any;
  private prismServer: any;
  private port: number;
  private hostname: string;
  private projectRoot: string;
  private customMocks: Map<string, MockResponse> = new Map();
  private endpointConfigs: Map<string, EndpointConfig> = new Map();
  private prismConfig: any = { dynamic: true };

  constructor(port: number = 3002) {
    this.port = port;
    const baseUrl = process.env.BASE_URL || "http://localhost:8000";
    this.hostname = new URL(baseUrl).hostname;
    this.projectRoot = normalize(join(__dirname, '..', '..', '..'));
  }

  /**
   * Configure Prism mock behavior
   */
  setPrismConfig(config: any) {
    this.prismConfig = { ...this.prismConfig, ...config };
  }

  /**
   * Set a custom mock response for a specific endpoint
   * This overrides the auto-generated response from the OpenAPI spec
   */
  setMockResponse(method: string, path: string, response: MockResponse) {
    const key = `${method.toUpperCase()} ${path}`;
    this.customMocks.set(key, response);
  }

  /**
   * Clear a custom mock response, falling back to auto-generated response
   */
  clearMockResponse(method: string, path: string) {
    const key = `${method.toUpperCase()} ${path}`;
    this.customMocks.delete(key);
  }

  /**
   * Clear all custom mock responses
   */
  clearAllMockResponses() {
    this.customMocks.clear();
  }

  /**
   * Configure response behavior for a specific endpoint
   * Uses automatic OpenAPI response generation with specified configuration
   */
  configureEndpoint(method: string, path: string, config: EndpointConfig) {
    const key = `${method.toUpperCase()} ${path}`;
    this.endpointConfigs.set(key, config);
  }

  /**
   * Clear configuration for a specific endpoint, falling back to global configuration
   */
  clearEndpointConfig(method: string, path: string) {
    const key = `${method.toUpperCase()} ${path}`;
    this.endpointConfigs.delete(key);
  }

  /**
   * Clear all endpoint-specific configurations
   */
  clearAllEndpointConfigs() {
    this.endpointConfigs.clear();
  }

  /**
   * Configure response selection (status code, media type, example key)
   */
  configureResponse(options: {
    statusCode?: number;
    mediaType?: string;
    exampleKey?: string;
    dynamic?: boolean;
  }) {
    if (options.statusCode !== undefined) {
      this.prismConfig = { ...this.prismConfig, code: options.statusCode };
    }
    if (options.dynamic !== undefined) {
      this.prismConfig = { ...this.prismConfig, dynamic: options.dynamic };
    }
  }

  async start(): Promise<string> {
    try {
      return new Promise(async (resolve, reject) => {
        try {
          // Load OpenAPI spec and create operations
          const specPath = normalize(join(this.projectRoot, '..', 'api', 'swagger.yml'));
          const operations = await getHttpOperationsFromSpec(specPath);
          
          // Create Prism server
          this.prismServer = createPrismServer(operations, {
            components: {
              logger: createLogger('MockServer'),
            },
            cors: true,
            config: {
              checkSecurity: false,
              validateRequest: false,
              validateResponse: false,
              mock: this.prismConfig,
              isProxy: false,
              errors: false,
            } as any,
          });

          // Create HTTP server that handles both API and static files
          this.server = createServer(async (req, res) => {
            await this.routeRequest(req, res);
          });

          this.server.listen(this.port, () => {
            console.log(`OpenAPI Mock server (Prism) started on http://${this.hostname}:${this.port}`);
            resolve(`http://${this.hostname}:${this.port}`);
          });

          this.server.on('error', (error: any) => {
            console.error('OpenAPI Mock server startup error:', {
              port: this.port,
              error: error instanceof Error ? error.message : String(error),
              stack: error instanceof Error ? error.stack : undefined
            });
            reject(error);
          });
        } catch (error) {
          console.error('Error creating Prism server:', error);
          reject(error);
        }
      });
    } catch (error) {
      console.error('Failed to start OpenAPI mock server:', error);
      throw error;
    }
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

  private async routeRequest(req: any, res: any) {
    try {
      // Handle CORS preflight requests
      if (req.method === 'OPTIONS') {
        return this.handleCorsPreflightRequest(res);
      }

      const requestPath = req.url?.split('?')[0];
      const method = req.method;
      const mockKey = `${method} ${requestPath}`;

      // Check if we have a custom mock response for this endpoint
      if (this.customMocks.has(mockKey)) {
        return this.handleCustomMockRequest(req, res);
      }

      // Check if this is an API request
      if (req.url?.startsWith('/api/')) {
        return this.handleApiRequest(req, res);
      }

      // Serve static files for non-API requests
      return this.handleStaticFileRequest(req, res);
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

  private handleCustomMockRequest(req: any, res: any) {
    try {
      const requestPath = req.url?.split('?')[0];
      const method = req.method;
      const mockKey = `${method} ${requestPath}`;
      const mockResponse = this.customMocks.get(mockKey);

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
        url: req.url,
        method: req.method,
        error: error instanceof Error ? error.message : String(error),
        stack: error instanceof Error ? error.stack : undefined
      });
      this.sendErrorResponse(res);
    }
  }

  private async handleApiRequest(req: any, res: any) {
    try {
      // Add CORS headers
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

      // Check for endpoint-specific configuration
      const requestPath = req.url?.split('?')[0];
      const method = req.method;
      const endpointKey = `${method} ${requestPath}`;
      const endpointConfig = this.endpointConfigs.get(endpointKey);

      if (endpointConfig) {
        // Configure Prism for this specific request
        const tempConfig = { ...this.prismConfig };
        if (endpointConfig.statusCode !== undefined) {
          tempConfig.code = endpointConfig.statusCode;
        }
        if (endpointConfig.dynamic !== undefined) {
          tempConfig.dynamic = endpointConfig.dynamic;
        }

        // Create a temporary mock HTTP request object for Prism
        const prismRequest = {
          method: method.toLowerCase(),
          url: { path: requestPath },
          headers: req.headers,
          body: req.body
        };

        // Use Prism to generate the response
        const prismResult = await this.prismServer.request(prismRequest);
        
        res.writeHead(prismResult.output.statusCode, prismResult.output.headers);
        res.end(prismResult.output.body);
      } else {
        // Use default Prism behavior
        const prismRequest = {
          method: method.toLowerCase(),
          url: { path: requestPath },
          headers: req.headers,
          body: req.body
        };

        const prismResult = await this.prismServer.request(prismRequest);
        
        res.writeHead(prismResult.output.statusCode, prismResult.output.headers);
        res.end(prismResult.output.body);
      }
    } catch (error) {
      console.error('Error handling API request with Prism:', {
        url: req.url,
        method: req.method,
        error: error instanceof Error ? error.message : String(error),
        stack: error instanceof Error ? error.stack : undefined
      });
      
      // Fallback to 404 for unmocked API endpoints
      this.send404Response(res);
    }
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

  private send404Response(res: any) {
    res.writeHead(404, {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*'
    });
    res.end(JSON.stringify({ message: 'API endpoint not found in OpenAPI spec' }));
  }

  private loadFile(filePath: string): Buffer | null {
    try {
      const fullPath = normalize(join(this.projectRoot, 'webui', 'dist', filePath));
      return readFileSync(fullPath);
    } catch (error) {
      console.error('Error loading file:', {
        filePath,
        fullPath: normalize(join(this.projectRoot, 'webui', 'dist', filePath)),
        error: error instanceof Error ? error.message : String(error)
      });
      return null;
    }
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