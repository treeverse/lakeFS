import { test } from '@playwright/test';
import { OpenApiMockServer } from './openApiMockServer';
import { 
  mockSuccessResponses, 
  mockUnauthenticated, 
  mockServerError, 
  mockWithStatusCode,
  mockDynamic,
  mockCustomResponse,
  clearAllMocks,
  resetToDefaults,
  mockEndpointWithStatus,
  mockEndpointSuccess,
  mockEndpointUnauthenticated,
  mockEndpointForbidden,
  mockEndpointNotFound,
  mockEndpointServerError,
  mockEndpointWithConfig,
  clearEndpointConfig,
  clearAllEndpointConfigs
} from './openApiMocks';

/**
 * Example test demonstrating the new Prism-based OpenAPI mock server
 * 
 * This server automatically generates responses from your swagger.yml file
 * without requiring manual response creation!
 */

test.describe('Prism OpenAPI Mock Server Examples', () => {
  let mockServer: OpenApiMockServer;

  test.beforeEach(async () => {
    // Start the mock server
    mockServer = new OpenApiMockServer(3002);
    await mockServer.start();
  });

  test.afterEach(async () => {
    // Stop the mock server
    await mockServer.stop();
  });

  test('should generate automatic success responses from OpenAPI spec', async ({ page }) => {
    // Configure the server to return successful responses with dynamic data
    mockSuccessResponses(mockServer, true);
    
    // Visit the page - all API calls will get automatic responses from swagger.yml
    await page.goto('http://localhost:3002');
    
    // The mock server will automatically generate responses for any API endpoints
    // defined in your swagger.yml file!
  });

  test('should configure different status codes for different endpoints', async ({ page }) => {
    // This is what you asked for! Configure different endpoints with different status codes
    // while still using automatic OpenAPI response generation (no manual response creation)
    
    // Configure /api/v1/user to return 200 success with dynamic data
    mockEndpointSuccess(mockServer, 'GET', '/api/v1/user');
    
    // Configure /api/v1/auth to return 401 unauthorized (automatic response from spec)
    mockEndpointUnauthenticated(mockServer, 'POST', '/api/v1/auth');
    
    // Configure /api/v1/repositories to return 403 forbidden (automatic response from spec)
    mockEndpointForbidden(mockServer, 'GET', '/api/v1/repositories');
    
    // Configure /api/v1/branches to return 404 not found (automatic response from spec)
    mockEndpointNotFound(mockServer, 'GET', '/api/v1/branches');
    
    await page.goto('http://localhost:3002');
    
    // Now each endpoint will return the configured status code
    // with automatic response generation from your swagger.yml file!
    // - GET /api/v1/user → 200 with user data from spec
    // - POST /api/v1/auth → 401 with auth error from spec
    // - GET /api/v1/repositories → 403 with forbidden error from spec
    // - GET /api/v1/branches → 404 with not found error from spec
  });

  test('should use specific status codes with custom configuration', async ({ page }) => {
    // Configure endpoints with more specific options
    mockEndpointWithStatus(mockServer, 'GET', '/api/v1/user', 200, true); // 200 with dynamic data
    mockEndpointWithStatus(mockServer, 'POST', '/api/v1/auth', 401, false); // 401 with static examples
    mockEndpointWithStatus(mockServer, 'GET', '/api/v1/repositories', 500, true); // 500 with dynamic data
    
    await page.goto('http://localhost:3002');
    
    // Each endpoint gets automatic response generation with the specified status code
  });

  test('should mix endpoint-specific and global configurations', async ({ page }) => {
    // Set global default for all endpoints
    mockSuccessResponses(mockServer, true);
    
    // Override specific endpoints
    mockEndpointUnauthenticated(mockServer, 'POST', '/api/v1/auth');
    mockEndpointServerError(mockServer, 'GET', '/api/v1/admin');
    
    await page.goto('http://localhost:3002');
    
    // Most endpoints return 200 success (global config)
    // POST /api/v1/auth returns 401 unauthorized (endpoint-specific)
    // GET /api/v1/admin returns 500 server error (endpoint-specific)
    // All responses are automatically generated from swagger.yml!
  });

  test('should return 401 responses for all endpoints', async ({ page }) => {
    // Configure the server to return 401 for all API endpoints
    mockUnauthenticated(mockServer);
    
    await page.goto('http://localhost:3002');
    
    // Now all API calls will return 401 responses as defined in swagger.yml
  });

  test('should return 500 server errors', async ({ page }) => {
    // Configure the server to return 500 errors
    mockServerError(mockServer);
    
    await page.goto('http://localhost:3002');
    
    // All API calls will return 500 error responses from swagger.yml
  });

  test('should return specific status codes', async ({ page }) => {
    // Return 404 responses for all endpoints
    mockWithStatusCode(mockServer, 404);
    
    await page.goto('http://localhost:3002');
    
    // All API calls will return 404 responses
  });

  test('should switch between dynamic and static responses', async ({ page }) => {
    // Start with dynamic responses (fake data generated by Faker.js)
    mockDynamic(mockServer, true);
    
    await page.goto('http://localhost:3002');
    
    // Switch to static responses (uses examples from swagger.yml)
    mockDynamic(mockServer, false);
    
    // Now responses will use static examples instead of dynamic data
  });

  test('should allow custom response overrides', async ({ page }) => {
    // Set up automatic responses as default
    mockSuccessResponses(mockServer, true);
    
    // Override specific endpoint with custom response
    mockCustomResponse(mockServer, 'GET', '/api/v1/user', {
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        user: {
          id: 'custom-user-id',
          creation_date: 1234567890,
          friendly_name: 'Custom Test User'
        }
      })
    });
    
    await page.goto('http://localhost:3002');
    
    // /api/v1/user will return the custom response
    // All other endpoints will use automatic OpenAPI responses
  });

  test('should clear endpoint-specific configurations', async ({ page }) => {
    // Configure some endpoints
    mockEndpointUnauthenticated(mockServer, 'POST', '/api/v1/auth');
    mockEndpointServerError(mockServer, 'GET', '/api/v1/admin');
    
    // Clear specific endpoint configuration
    clearEndpointConfig(mockServer, 'POST', '/api/v1/auth');
    
    // Clear all endpoint configurations
    clearAllEndpointConfigs(mockServer);
    
    await page.goto('http://localhost:3002');
    
    // All endpoints now use global configuration
  });

  test('should reset to defaults', async ({ page }) => {
    // Configure some custom behavior
    mockServerError(mockServer);
    mockEndpointUnauthenticated(mockServer, 'POST', '/api/v1/auth');
    
    // Reset to default behavior
    resetToDefaults(mockServer);
    
    await page.goto('http://localhost:3002');
    
    // Now back to default: 200 responses with dynamic data for all endpoints
  });

  test('should clear all custom mocks', async ({ page }) => {
    // Set up some custom responses
    mockCustomResponse(mockServer, 'GET', '/api/v1/user', {
      status: 200,
      body: JSON.stringify({ custom: 'response' })
    });
    
    mockCustomResponse(mockServer, 'GET', '/api/v1/repositories', {
      status: 500,
      body: JSON.stringify({ error: 'server error' })
    });
    
    // Clear all custom mocks
    clearAllMocks(mockServer);
    
    await page.goto('http://localhost:3002');
    
    // Now all endpoints will use automatic OpenAPI responses again
  });
});

/**
 * Key Benefits of the New Prism-based Approach:
 * 
 * 1. **Automatic Response Generation**: No need to manually create mock responses!
 *    The server automatically generates responses from your swagger.yml file.
 * 
 * 2. **Dynamic Data**: Uses Faker.js to generate realistic, randomized data
 *    that follows your OpenAPI schema definitions.
 * 
 * 3. **Comprehensive Coverage**: Automatically supports ALL endpoints defined
 *    in your OpenAPI spec without any additional coding.
 * 
 * 4. **Flexible Configuration**: Easy to configure response behavior:
 *    - Status codes (200, 401, 500, etc.)
 *    - Media types (JSON, XML, etc.)
 *    - Dynamic vs static responses
 *    - Named examples from your spec
 * 
 * 5. **Custom Overrides**: Still allows custom responses for specific test
 *    scenarios while using automatic responses for everything else.
 * 
 * 6. **Validation**: Prism validates requests against your OpenAPI spec,
 *    catching contract mismatches early.
 * 
 * 7. **No Manual Maintenance**: When you update your OpenAPI spec, the
 *    mock responses automatically update - no code changes needed!
 */ 