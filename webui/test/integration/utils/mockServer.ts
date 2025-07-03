import { createServer } from 'http';
import { readFileSync } from 'fs';
import { join } from 'path';

export class MockServer {
  private server: any;
  private port: number;
  private hostname: string;
  private apiMocks: Map<string, any> = new Map();

  constructor(port: number = 3002) {
    this.port = port;
    // Use the same BASE_URL that Playwright config uses
    const baseUrl = process.env.BASE_URL || "http://localhost:8000";
    this.hostname = new URL(baseUrl).hostname;
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

      this.server.on('error', reject);
    });
  }

  async stop(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.server) {
        this.server.close((error: any) => {
          if (error) {
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
      const mockResponse = this.apiMocks.get(requestPath);

      this.validateMockResponse(mockResponse, requestPath);
      
      const headers = this.buildResponseHeaders(mockResponse);
      
      res.writeHead(mockResponse.status, headers);
      res.end(mockResponse.body);
    } catch (error) {
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

      let filePath = req.url === '/' ? '/index.html' : req.url;
      if (filePath?.includes('?')) {
        filePath = filePath.split('?')[0];
      }

      if (!filePath || filePath === '/') {
        filePath = '/index.html';
      }
      
      const fullPath = join(process.cwd(), 'dist', filePath);
      const content = readFileSync(fullPath);
      
      const contentType = this.getContentType(filePath);
      
      res.writeHead(200, { 'Content-Type': contentType });
      res.end(content);
    } catch (error) {
      this.serveIndexHtml(res);
    }
  }

  private serveIndexHtml(res: any) {
    try {
      const indexPath = join(process.cwd(), 'dist', 'index.html');
      const content = readFileSync(indexPath);
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(content);
    } catch (indexError) {
      res.writeHead(404);
      res.end('Not found');
    }
  }

  private getContentType(filePath: string): string {
    if (filePath?.endsWith('.js')) return 'application/javascript';
    if (filePath?.endsWith('.css')) return 'text/css';
    if (filePath?.endsWith('.json')) return 'application/json';
    return 'text/html';
  }
}