# OpenAPI Mock Server (Prism-based)

This directory contains a **Prism-based OpenAPI mock server** that automatically generates responses from your `swagger.yml` OpenAPI specification. This approach eliminates the need for manual mock response creation and provides true OpenAPI-driven testing.

## 🎯 Key Benefits

✅ **Automatic Response Generation** - No manual mock creation needed!  
✅ **Dynamic Data** - Uses Faker.js for realistic, randomized responses  
✅ **Comprehensive Coverage** - Supports ALL endpoints in your OpenAPI spec  
✅ **Schema Validation** - Validates requests against your OpenAPI spec  
✅ **Flexible Configuration** - Easy status code, media type, and example selection  
✅ **Custom Overrides** - Override specific endpoints when needed  
✅ **No Maintenance** - Updates automatically when your OpenAPI spec changes  

## 📁 Files

- **`openApiMockServer.ts`** - Main Prism-based mock server implementation
- **`openApiMocks.ts`** - Helper functions for easy configuration
- **`example-prism-test.ts`** - Example test demonstrating usage
- **`README.md`** - This documentation

## 🚀 Quick Start

### Basic Usage

```typescript
import { OpenApiMockServer } from './openApiMockServer';
import { mockSuccessResponses, mockUnauthenticated } from './openApiMocks';

// Start the mock server
const mockServer = new OpenApiMockServer(3002);
await mockServer.start();

// Configure for success responses with dynamic data
mockSuccessResponses(mockServer, true);

// Your tests here...

// Stop the server
await mockServer.stop();
```

### In Playwright Tests

```typescript
import { test } from '@playwright/test';
import { OpenApiMockServer } from './openApiMockServer';
import { mockSuccessResponses, mockUnauthenticated } from './openApiMocks';

test.describe('My Tests', () => {
  let mockServer: OpenApiMockServer;

  test.beforeEach(async () => {
    mockServer = new OpenApiMockServer(3002);
    await mockServer.start();
  });

  test.afterEach(async () => {
    await mockServer.stop();
  });

  test('should handle authenticated users', async ({ page }) => {
    mockSuccessResponses(mockServer, true);
    await page.goto('http://localhost:3002');
    // All API calls will get automatic responses from swagger.yml
  });

  test('should handle unauthenticated users', async ({ page }) => {
    mockUnauthenticated(mockServer);
    await page.goto('http://localhost:3002');
    // All API calls will return 401 responses
  });
});
```

## 🔧 Configuration Options

### Response Status Codes

```typescript
import { mockWithStatusCode, mockUnauthenticated, mockServerError } from './openApiMocks';

// Return specific status codes
mockWithStatusCode(mockServer, 404);    // 404 responses
mockUnauthenticated(mockServer);        // 401 responses
mockServerError(mockServer);            // 500 responses
```

### Dynamic vs Static Responses

```typescript
import { mockDynamic, mockSuccessResponses } from './openApiMocks';

// Dynamic responses (realistic fake data via Faker.js)
mockDynamic(mockServer, true);

// Static responses (examples from swagger.yml)
mockDynamic(mockServer, false);

// Success responses with dynamic data
mockSuccessResponses(mockServer, true);
```

### Media Types

```typescript
import { mockWithMediaType } from './openApiMocks';

// Return XML responses
mockWithMediaType(mockServer, 'application/xml');

// Return JSON responses
mockWithMediaType(mockServer, 'application/json');
```

### Named Examples

```typescript
import { mockWithExample } from './openApiMocks';

// Use a specific example from your OpenAPI spec
mockWithExample(mockServer, 'successful-user');
```

### Per-Endpoint Configuration

Configure different endpoints to return different status codes while still using automatic OpenAPI response generation:

```typescript
import { 
  mockEndpointSuccess, 
  mockEndpointUnauthenticated, 
  mockEndpointForbidden,
  mockEndpointNotFound,
  mockEndpointServerError,
  mockEndpointWithStatus,
  mockEndpointWithConfig
} from './openApiMocks';

// Configure different endpoints with different status codes
test('should handle mixed authentication states', async ({ page }) => {
  // /api/v1/user returns 200 success with dynamic data from spec
  mockEndpointSuccess(mockServer, 'GET', '/api/v1/user');
  
  // /api/v1/auth returns 401 unauthorized with automatic response from spec
  mockEndpointUnauthenticated(mockServer, 'POST', '/api/v1/auth');
  
  // /api/v1/admin returns 403 forbidden with automatic response from spec
  mockEndpointForbidden(mockServer, 'GET', '/api/v1/admin');
  
  // /api/v1/nonexistent returns 404 not found with automatic response from spec
  mockEndpointNotFound(mockServer, 'GET', '/api/v1/nonexistent');
  
  await page.goto('http://localhost:3002');
  
  // Each endpoint returns its configured status code
  // All responses are automatically generated from swagger.yml!
});

// More specific configuration with custom options
mockEndpointWithStatus(mockServer, 'GET', '/api/v1/user', 200, true); // 200 with dynamic data
mockEndpointWithStatus(mockServer, 'POST', '/api/v1/auth', 401, false); // 401 with static examples

// Advanced configuration
mockEndpointWithConfig(mockServer, 'GET', '/api/v1/data', {
  statusCode: 200,
  mediaType: 'application/xml',
  exampleKey: 'success-example',
  dynamic: false
});
```

### Mixing Global and Endpoint-Specific Configuration

```typescript
// Set global default for all endpoints
mockSuccessResponses(mockServer, true);

// Override specific endpoints
mockEndpointUnauthenticated(mockServer, 'POST', '/api/v1/auth');
mockEndpointServerError(mockServer, 'GET', '/api/v1/admin');

// Result:
// - Most endpoints return 200 success (global config)
// - POST /api/v1/auth returns 401 unauthorized (endpoint-specific)
// - GET /api/v1/admin returns 500 server error (endpoint-specific)
// - All responses are automatically generated from swagger.yml!
```

### Custom Response Overrides

```typescript
import { mockCustomResponse } from './openApiMocks';

// Override specific endpoints with custom responses
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
```

## 🛠️ Advanced Usage

### Direct Server Configuration

```typescript
// Configure response behavior directly
mockServer.configureResponse({
  statusCode: 200,
  mediaType: 'application/json',
  exampleKey: 'success-example',
  dynamic: true
});

// Set custom mock for specific endpoint
mockServer.setMockResponse('GET', '/api/v1/special', {
  status: 200,
  contentType: 'application/json',
  body: JSON.stringify({ special: 'response' })
});
```

### Reset and Clear

```typescript
import { resetToDefaults, clearAllMocks, clearMock } from './openApiMocks';

// Reset to default configuration
resetToDefaults(mockServer);

// Clear all custom mocks
clearAllMocks(mockServer);

// Clear specific mock
clearMock(mockServer, 'GET', '/api/v1/user');
```

## 🔄 Migration from Old Approach

### Old Approach (Manual)
```typescript
// ❌ OLD: Manual mock creation
server.setApiMock('/api/v1/user', {
  status: 200,
  contentType: 'application/json',
  body: JSON.stringify({
    user: {
      id: "test@example.com",
      creation_date: Math.floor(Date.now() / 1000)
    }
  })
});
```

### New Approach (Automatic)
```typescript
// ✅ NEW: Automatic from OpenAPI spec
mockSuccessResponses(mockServer, true);
// All endpoints automatically get responses from swagger.yml!
```

## 📚 Available Helper Functions

### Global Configuration
| Function | Description |
|----------|-------------|
| `mockSuccessResponses(server, dynamic?)` | Configure 200 responses with optional dynamic data |
| `mockUnauthenticated(server)` | Configure 401 responses |
| `mockForbidden(server)` | Configure 403 responses |
| `mockNotFound(server)` | Configure 404 responses |
| `mockServerError(server)` | Configure 500 responses |
| `mockNotImplemented(server)` | Configure 501 responses |
| `mockWithStatusCode(server, code)` | Configure specific status code |
| `mockWithMediaType(server, type)` | Configure specific media type |
| `mockWithExample(server, key)` | Use named example from spec |
| `mockDynamic(server, dynamic)` | Toggle dynamic vs static responses |

### Per-Endpoint Configuration
| Function | Description |
|----------|-------------|
| `mockEndpointSuccess(server, method, path, dynamic?)` | Configure specific endpoint for 200 responses |
| `mockEndpointUnauthenticated(server, method, path)` | Configure specific endpoint for 401 responses |
| `mockEndpointForbidden(server, method, path)` | Configure specific endpoint for 403 responses |
| `mockEndpointNotFound(server, method, path)` | Configure specific endpoint for 404 responses |
| `mockEndpointServerError(server, method, path)` | Configure specific endpoint for 500 responses |
| `mockEndpointWithStatus(server, method, path, code, dynamic?)` | Configure specific endpoint with status code |
| `mockEndpointWithConfig(server, method, path, config)` | Configure specific endpoint with full config |

### Custom Overrides
| Function | Description |
|----------|-------------|
| `mockCustomResponse(server, method, path, response)` | Override specific endpoint with custom response |

### Cleanup Functions
| Function | Description |
|----------|-------------|
| `clearAllMocks(server)` | Clear all custom overrides |
| `clearMock(server, method, path)` | Clear specific override |
| `clearEndpointConfig(server, method, path)` | Clear specific endpoint configuration |
| `clearAllEndpointConfigs(server)` | Clear all endpoint configurations |
| `resetToDefaults(server)` | Reset to default configuration |

## 🏗️ How It Works

1. **Prism** loads your `swagger.yml` OpenAPI specification
2. **Automatic Response Generation** - Prism generates responses based on:
   - Response schemas in your OpenAPI spec
   - Example values (if provided)
   - Status codes defined for each endpoint
   - Media types (JSON, XML, etc.)
3. **Dynamic Data** - Uses Faker.js to generate realistic fake data that matches your schemas
4. **Validation** - Validates incoming requests against your OpenAPI spec
5. **Custom Overrides** - Allows you to override specific endpoints for edge case testing

## 🔍 Troubleshooting

### Server Won't Start
- Ensure `swagger.yml` exists at the correct path
- Check that all required dependencies are installed
- Verify OpenAPI spec is valid

### Responses Don't Match Expected Format
- Check your OpenAPI spec response definitions
- Ensure response schemas are properly defined
- Use `mockWithExample()` for specific examples

### Custom Overrides Not Working
- Verify the method and path match exactly
- Check that the override is set before making requests
- Use `clearMock()` to reset specific overrides

## 🎯 Best Practices

1. **Use Dynamic Responses** for most tests to catch client-side robustness issues
2. **Use Static Examples** for tests that need predictable data
3. **Override Specific Endpoints** only when testing edge cases
4. **Reset Between Tests** to ensure clean state
5. **Validate Your OpenAPI Spec** to ensure good automatic responses

## 🚀 Next Steps

1. **Delete Old Mock Files** - Remove `apiMocks.ts` and related manual mock files
2. **Update Existing Tests** - Replace manual mocks with Prism-based helpers
3. **Improve OpenAPI Spec** - Add examples and better schemas for richer responses
4. **Add More Tests** - Take advantage of comprehensive endpoint coverage

---

## 📖 Additional Resources

- [Prism Documentation](https://docs.stoplight.io/docs/prism/)
- [OpenAPI Specification](https://swagger.io/specification/)
- [Faker.js Documentation](https://fakerjs.dev/)

**Happy Testing! 🎉** 