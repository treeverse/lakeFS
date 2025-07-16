import { OpenApiMockServer } from './openApiMockServer';

/**
 * Helper functions for configuring OpenAPI mock responses using Prism
 * 
 * This replaces the old manual mock responses with automatic response generation
 * from the OpenAPI specification using Prism. The mock server will automatically
 * generate responses based on your swagger.yml file.
 */

export interface OpenApiMockConfig {
  statusCode?: number;
  mediaType?: string;
  exampleKey?: string;
  dynamic?: boolean;
}

/**
 * Configure the mock server to return responses with specific status codes
 * 
 * @param server - The OpenAPI mock server instance
 * @param statusCode - The HTTP status code to return (e.g., 200, 401, 500)
 * 
 * @example
 * // Return 401 responses for all API endpoints
 * mockUnauthenticated(server, 401);
 */
export function mockWithStatusCode(server: OpenApiMockServer, statusCode: number) {
  server.configureResponse({ statusCode });
}

/**
 * Configure the mock server to return successful responses (200/201)
 * 
 * @param server - The OpenAPI mock server instance
 * @param dynamic - Whether to generate dynamic fake data (default: true)
 * 
 * @example
 * mockSuccessResponses(server, true); // Returns dynamic data
 * mockSuccessResponses(server, false); // Returns static examples from spec
 */
export function mockSuccessResponses(server: OpenApiMockServer, dynamic: boolean = true) {
  server.configureResponse({ statusCode: 200, dynamic });
}

/**
 * Configure the mock server to return 401 Unauthorized responses
 * 
 * @param server - The OpenAPI mock server instance
 * 
 * @example
 * mockUnauthenticated(server);
 */
export function mockUnauthenticated(server: OpenApiMockServer) {
  server.configureResponse({ statusCode: 401 });
}

/**
 * Configure the mock server to return 403 Forbidden responses
 * 
 * @param server - The OpenAPI mock server instance
 * 
 * @example
 * mockForbidden(server);
 */
export function mockForbidden(server: OpenApiMockServer) {
  server.configureResponse({ statusCode: 403 });
}

/**
 * Configure the mock server to return 404 Not Found responses
 * 
 * @param server - The OpenAPI mock server instance
 * 
 * @example
 * mockNotFound(server);
 */
export function mockNotFound(server: OpenApiMockServer) {
  server.configureResponse({ statusCode: 404 });
}

/**
 * Configure the mock server to return 500 Internal Server Error responses
 * 
 * @param server - The OpenAPI mock server instance
 * 
 * @example
 * mockServerError(server);
 */
export function mockServerError(server: OpenApiMockServer) {
  server.configureResponse({ statusCode: 500 });
}

/**
 * Configure the mock server to return 501 Not Implemented responses
 * 
 * @param server - The OpenAPI mock server instance
 * 
 * @example
 * mockNotImplemented(server);
 */
export function mockNotImplemented(server: OpenApiMockServer) {
  server.configureResponse({ statusCode: 501 });
}

/**
 * Configure the mock server to return responses with specific media type
 * 
 * @param server - The OpenAPI mock server instance
 * @param mediaType - The media type to return (e.g., 'application/json', 'application/xml')
 * 
 * @example
 * mockWithMediaType(server, 'application/xml');
 */
export function mockWithMediaType(server: OpenApiMockServer, mediaType: string) {
  server.configureResponse({ mediaType });
}

/**
 * Configure the mock server to return a specific named example
 * 
 * @param server - The OpenAPI mock server instance
 * @param exampleKey - The key of the example to return (as defined in OpenAPI spec)
 * 
 * @example
 * mockWithExample(server, 'successful-user');
 */
export function mockWithExample(server: OpenApiMockServer, exampleKey: string) {
  server.configureResponse({ exampleKey });
}

/**
 * Configure the mock server to generate dynamic responses
 * 
 * @param server - The OpenAPI mock server instance
 * @param dynamic - Whether to generate dynamic fake data using Faker.js
 * 
 * @example
 * mockDynamic(server, true);  // Generate random realistic data
 * mockDynamic(server, false); // Use static examples from spec
 */
export function mockDynamic(server: OpenApiMockServer, dynamic: boolean = true) {
  server.configureResponse({ dynamic });
}

/**
 * Set a custom mock response for a specific endpoint
 * This overrides the automatic OpenAPI response generation
 * 
 * @param server - The OpenAPI mock server instance
 * @param method - HTTP method (GET, POST, etc.)
 * @param path - API path (e.g., '/api/v1/user')
 * @param response - Custom response object
 * 
 * @example
 * mockCustomResponse(server, 'GET', '/api/v1/user', {
 *   status: 200,
 *   contentType: 'application/json',
 *   body: JSON.stringify({ id: 'test-user', name: 'Test User' })
 * });
 */
export function mockCustomResponse(
  server: OpenApiMockServer,
  method: string,
  path: string,
  response: {
    status: number;
    contentType?: string;
    body?: string;
    headers?: Record<string, string>;
  }
) {
  server.setMockResponse(method, path, response);
}

/**
 * Clear custom mock responses and return to automatic OpenAPI generation
 * 
 * @param server - The OpenAPI mock server instance
 * 
 * @example
 * clearAllMocks(server);
 */
export function clearAllMocks(server: OpenApiMockServer) {
  server.clearAllMockResponses();
}

/**
 * Clear a specific custom mock response
 * 
 * @param server - The OpenAPI mock server instance
 * @param method - HTTP method (GET, POST, etc.)
 * @param path - API path (e.g., '/api/v1/user')
 * 
 * @example
 * clearMock(server, 'GET', '/api/v1/user');
 */
export function clearMock(server: OpenApiMockServer, method: string, path: string) {
  server.clearMockResponse(method, path);
}

/**
 * Reset the mock server to default configuration
 * 
 * @param server - The OpenAPI mock server instance
 * 
 * @example
 * resetToDefaults(server);
 */
export function resetToDefaults(server: OpenApiMockServer) {
  server.clearAllMockResponses();
  server.clearAllEndpointConfigs();
  server.configureResponse({ statusCode: 200, dynamic: true });
}

/**
 * Configure a specific endpoint to return a specific status code
 * Uses automatic OpenAPI response generation with the specified status code
 * 
 * @param server - The OpenAPI mock server instance
 * @param method - HTTP method (GET, POST, etc.)
 * @param path - API path (e.g., '/api/v1/user')
 * @param statusCode - The HTTP status code to return
 * @param dynamic - Whether to generate dynamic fake data (default: true)
 * 
 * @example
 * // Configure /api/v1/user to return 200 with dynamic data
 * mockEndpointWithStatus(server, 'GET', '/api/v1/user', 200, true);
 * 
 * // Configure /api/v1/auth to return 401 with automatic response from spec
 * mockEndpointWithStatus(server, 'POST', '/api/v1/auth', 401);
 */
export function mockEndpointWithStatus(
  server: OpenApiMockServer,
  method: string,
  path: string,
  statusCode: number,
  dynamic: boolean = true
) {
  server.configureEndpoint(method, path, { statusCode, dynamic });
}

/**
 * Configure a specific endpoint to return successful responses (200/201)
 * Uses automatic OpenAPI response generation
 * 
 * @param server - The OpenAPI mock server instance
 * @param method - HTTP method (GET, POST, etc.)
 * @param path - API path (e.g., '/api/v1/user')
 * @param dynamic - Whether to generate dynamic fake data (default: true)
 * 
 * @example
 * mockEndpointSuccess(server, 'GET', '/api/v1/user', true);
 */
export function mockEndpointSuccess(
  server: OpenApiMockServer,
  method: string,
  path: string,
  dynamic: boolean = true
) {
  server.configureEndpoint(method, path, { statusCode: 200, dynamic });
}

/**
 * Configure a specific endpoint to return 401 Unauthorized responses
 * Uses automatic OpenAPI response generation
 * 
 * @param server - The OpenAPI mock server instance
 * @param method - HTTP method (GET, POST, etc.)
 * @param path - API path (e.g., '/api/v1/user')
 * 
 * @example
 * mockEndpointUnauthenticated(server, 'GET', '/api/v1/user');
 */
export function mockEndpointUnauthenticated(
  server: OpenApiMockServer,
  method: string,
  path: string
) {
  server.configureEndpoint(method, path, { statusCode: 401 });
}

/**
 * Configure a specific endpoint to return 403 Forbidden responses
 * Uses automatic OpenAPI response generation
 * 
 * @param server - The OpenAPI mock server instance
 * @param method - HTTP method (GET, POST, etc.)
 * @param path - API path (e.g., '/api/v1/user')
 * 
 * @example
 * mockEndpointForbidden(server, 'GET', '/api/v1/admin');
 */
export function mockEndpointForbidden(
  server: OpenApiMockServer,
  method: string,
  path: string
) {
  server.configureEndpoint(method, path, { statusCode: 403 });
}

/**
 * Configure a specific endpoint to return 404 Not Found responses
 * Uses automatic OpenAPI response generation
 * 
 * @param server - The OpenAPI mock server instance
 * @param method - HTTP method (GET, POST, etc.)
 * @param path - API path (e.g., '/api/v1/user')
 * 
 * @example
 * mockEndpointNotFound(server, 'GET', '/api/v1/nonexistent');
 */
export function mockEndpointNotFound(
  server: OpenApiMockServer,
  method: string,
  path: string
) {
  server.configureEndpoint(method, path, { statusCode: 404 });
}

/**
 * Configure a specific endpoint to return 500 Internal Server Error responses
 * Uses automatic OpenAPI response generation
 * 
 * @param server - The OpenAPI mock server instance
 * @param method - HTTP method (GET, POST, etc.)
 * @param path - API path (e.g., '/api/v1/user')
 * 
 * @example
 * mockEndpointServerError(server, 'GET', '/api/v1/user');
 */
export function mockEndpointServerError(
  server: OpenApiMockServer,
  method: string,
  path: string
) {
  server.configureEndpoint(method, path, { statusCode: 500 });
}

/**
 * Configure a specific endpoint to return 501 Not Implemented responses
 * Uses automatic OpenAPI response generation
 * 
 * @param server - The OpenAPI mock server instance
 * @param method - HTTP method (GET, POST, etc.)
 * @param path - API path (e.g., '/api/v1/user')
 * 
 * @example
 * mockEndpointNotImplemented(server, 'GET', '/api/v1/feature');
 */
export function mockEndpointNotImplemented(
  server: OpenApiMockServer,
  method: string,
  path: string
) {
  server.configureEndpoint(method, path, { statusCode: 501 });
}

/**
 * Configure a specific endpoint with custom configuration
 * Uses automatic OpenAPI response generation with specified configuration
 * 
 * @param server - The OpenAPI mock server instance
 * @param method - HTTP method (GET, POST, etc.)
 * @param path - API path (e.g., '/api/v1/user')
 * @param config - Endpoint configuration
 * 
 * @example
 * mockEndpointWithConfig(server, 'GET', '/api/v1/user', {
 *   statusCode: 200,
 *   mediaType: 'application/xml',
 *   exampleKey: 'user-success',
 *   dynamic: false
 * });
 */
export function mockEndpointWithConfig(
  server: OpenApiMockServer,
  method: string,
  path: string,
  config: {
    statusCode?: number;
    mediaType?: string;
    exampleKey?: string;
    dynamic?: boolean;
  }
) {
  server.configureEndpoint(method, path, config);
}

/**
 * Clear configuration for a specific endpoint
 * The endpoint will fall back to global configuration
 * 
 * @param server - The OpenAPI mock server instance
 * @param method - HTTP method (GET, POST, etc.)
 * @param path - API path (e.g., '/api/v1/user')
 * 
 * @example
 * clearEndpointConfig(server, 'GET', '/api/v1/user');
 */
export function clearEndpointConfig(server: OpenApiMockServer, method: string, path: string) {
  server.clearEndpointConfig(method, path);
}

/**
 * Clear all endpoint-specific configurations
 * All endpoints will use global configuration
 * 
 * @param server - The OpenAPI mock server instance
 * 
 * @example
 * clearAllEndpointConfigs(server);
 */
export function clearAllEndpointConfigs(server: OpenApiMockServer) {
  server.clearAllEndpointConfigs();
} 