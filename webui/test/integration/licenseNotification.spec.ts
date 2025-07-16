import { test, expect, Page, Locator } from "@playwright/test";
import { OpenApiMockServer } from "./utils/openApiMockServer";
import { 
  mockEndpointSuccess,
  mockEndpointUnauthenticated,
  mockEndpointNotFound,
  mockEndpointServerError,
  mockEndpointNotImplemented,
  mockNotImplemented,
  mockCustomResponse,
  mockSuccessResponses
} from "./utils/openApiMocks";
import { createDate, createLicenseToken } from "./utils/testUtils";

// ==============================================
// Local Helper Functions
// ==============================================

/**
 * Set up basic API mocks needed for most tests
 * Includes authenticated user and valid license by default
 */
function setupBasicAPIMocks(server: OpenApiMockServer) {
  // Configure basic endpoints with success responses
  mockEndpointSuccess(server, 'GET', '/api/v1/config');
  mockEndpointSuccess(server, 'GET', '/api/v1/setup_lakefs');
  mockEndpointSuccess(server, 'GET', '/api/v1/repositories');
  
  // Set up authenticated user by default
  mockEndpointSuccess(server, 'GET', '/api/v1/user');
  
  // Set up valid license by default
  const defaultValidToken = createLicenseToken(createDate(60));
  mockLicense(server, defaultValidToken);
}

/**
 * Mock license endpoint with automatic response generation
 */
function mockLicense(server: OpenApiMockServer, token?: string) {
  if (token) {
    // Use custom response when specific token is needed
    mockCustomResponse(server, 'GET', '/api/v1/license', {
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ token })
    });
  } else {
    // Use automatic response generation from OpenAPI spec when no specific token needed
    mockEndpointSuccess(server, 'GET', '/api/v1/license');
  }
}

let mockServer: OpenApiMockServer;
let serverUrl: string;

test.describe("License Notification Banner Tests", () => {
  test.beforeAll(async () => {
    mockServer = new OpenApiMockServer();
    serverUrl = await mockServer.start();
    setupBasicAPIMocks(mockServer);
  });

  test.afterAll(async () => {
    if (mockServer) {
      await mockServer.stop();
    }
  });

  test.beforeEach(async ({ page }) => {
    await page.goto(`${serverUrl}/repositories`);
  });

  test("should not display banner when license is valid (more than 30 days)", async ({ page }) => {
    const token = createLicenseToken(createDate(60));
    mockLicense(mockServer, token);

    await page.reload();
    await assertBannerNotVisible(page);
  });

  test("should display warning banner when license expires within 30 days", async ({ page }) => {
    const token = createLicenseToken(createDate(30, 2));
    mockLicense(mockServer, token);

    await page.reload();
    await assertBannerWithType(page, "Warning: Your lakeFS license will expire in 30 days. Please contact support to renew.", 'warning');
  });

  test("should display warning banner when license expires within 0 days", async ({ page }) => {
    const token = createLicenseToken(createDate(0, 10));
    mockLicense(mockServer, token);
    
    await page.reload();
    await assertBannerWithType(page, "Warning: Your lakeFS license will expire in 0 days. Please contact support to renew.", 'warning');
  });

  test("should display error banner when license expired 15 days ago", async ({ page }) => {
    const token = createLicenseToken(createDate(-15));
    mockLicense(mockServer, token);
    
    await page.reload();
    await assertBannerWithType(page, "Error: Your lakeFS license has expired. Please contact support immediately.", 'error');
  });

  test("should display error banner when license expired 1 hour ago", async ({ page }) => {
    const token = createLicenseToken(createDate(0, -1));
    mockLicense(mockServer, token);
    
    await page.reload();
    await assertBannerWithType(page, "Error: Your lakeFS license has expired. Please contact support immediately.", 'error');
  });

  test("should not display banner when license endpoint returns 401 (Unauthorized)", async ({ page }) => {
    // Use automatic 401 response generation from OpenAPI spec
    mockEndpointUnauthenticated(mockServer, 'GET', '/api/v1/license');
    
    await page.reload();
    await assertBannerNotVisible(page);
  });

  test("should not display banner when license endpoint returns 501 (Not Implemented)", async ({ page }) => {
    // Use automatic 501 response generation from OpenAPI spec for specific endpoint
    mockEndpointNotImplemented(mockServer, 'GET', '/api/v1/license');
    
    await page.reload();
    await assertBannerNotVisible(page);
  });

  test("should not display banner when license endpoint returns 500 (Internal Server Error)", async ({ page }) => {
    // Use automatic 500 response generation from OpenAPI spec
    mockEndpointServerError(mockServer, 'GET', '/api/v1/license');
    
    await page.reload();
    await assertBannerNotVisible(page);
  });

  test("should not display banner when license endpoint returns invalid license token", async ({ page }) => {
    // Use automatic response generation - the invalid token will be handled by client-side validation
    mockLicense(mockServer, "invalid.jwt.token");
    
    await page.reload();
    await assertBannerNotVisible(page);
  });

  test("should not display banner when license endpoint returns empty license token", async ({ page }) => {
    // Use automatic response generation - the empty token will be handled by client-side validation
    mockLicense(mockServer, "");
    
    await page.reload();
    await assertBannerNotVisible(page);
  });

  test("should display banner when user is logged in and not display when user is logged out", async ({ page }) => {
    // Use automatic response generation for login endpoint
    mockEndpointSuccess(mockServer, 'POST', '/api/v1/auth/login');
    
    // Use custom response for logout endpoint - special case with 307 redirect not in swagger
    mockCustomResponse(mockServer, 'GET', '/logout', {
      status: 307,
      headers: {
        'Location': '/auth/login'
      }
    });
    
    const loginResponse = await page.request.post(`${serverUrl}/api/v1/auth/login`, {
      data: {
        access_key_id: "test-access-key",
        secret_access_key: "test-secret-key"
      }
    });
    
    expect(loginResponse.status()).toBe(200);
    const loginResult = await loginResponse.json();
    expect(loginResult.token).toBeDefined();
    expect(loginResult.token_expiration).toBeDefined();
    
    // Use automatic response generation for authenticated user endpoint
    mockEndpointSuccess(mockServer, 'GET', '/api/v1/user');
    
    const token = createLicenseToken(createDate(1, 2));
    mockLicense(mockServer, token);
    
    await page.reload();
    await assertBannerWithType(page, "Warning: Your lakeFS license will expire in 1 days. Please contact support to renew.", 'warning');

    const logoutResponse = await page.request.get(`${serverUrl}/logout`, {
      maxRedirects: 0
    });
    
    expect(logoutResponse.status()).toBe(307);
    
    // Use automatic response generation for unauthenticated user endpoint
    mockEndpointUnauthenticated(mockServer, 'GET', '/api/v1/user');
    
    await page.reload();
    await assertBannerNotVisible(page);
  });

  test("should position banner correctly - sticky to top", async ({ page }) => {
    const token = createLicenseToken(createDate(15));
    mockLicense(mockServer, token);
    
    await page.reload();
    await assertBannerPositioning(page);
    await assertNotificationOffset(page, 'set');
  });

  test("should update correctly the notification offset CSS property when banner appears and disappears", async ({ page }) => {
    const token = createLicenseToken(createDate(15));
    mockLicense(mockServer, token);
    
    await page.reload();
    await assertNotificationOffset(page, 'set');
    
    const validToken = createLicenseToken(createDate(60));
    mockLicense(mockServer, validToken);
    
    await page.reload();
    await page.waitForTimeout(5000);
    
    await assertNotificationOffset(page, 'cleared');
  });

  test("the banner should remain sticky to the top when scrolling down page", async ({ page }) => {
    const token = createLicenseToken(createDate(1, 2));
    mockLicense(mockServer, token);
    
    await page.reload();
    
    await addScrollableContent(page);
    await scrollPageDown(page);
    
    const banner = await assertBannerWithType(page, "Warning: Your lakeFS license will expire in 1 days. Please contact support to renew.", 'warning');
    
    const bannerBox = await banner.boundingBox();
    expect(bannerBox).not.toBeNull();
    if (bannerBox) {
      expect(bannerBox.y).toBeLessThanOrEqual(5); 
    }
  });

  test("should automatically update banner when license changes via polling", async ({ page }) => {
    // This test needs extra time to wait for the polling interval
    test.setTimeout(120000); 
    
    const expiringToken = createLicenseToken(createDate(1, 2)); 
    mockLicense(mockServer, expiringToken);
    
    await page.reload();
    await assertBannerWithType(page, "Warning: Your lakeFS license will expire in 1 days. Please contact support to renew.", 'warning');
    
    const validToken = createLicenseToken(createDate(60)); 
    mockLicense(mockServer, validToken);
    
    // Wait for the polling to pick up the new license (max 1 minute 20 seconds)
    await page.waitForTimeout(80000); 
    
    await assertBannerNotVisible(page);
  });
});

async function assertBannerVisible(page: Page, expectedText: string): Promise<Locator> {
  await page.waitForSelector('.notification-bar .alert', { timeout: 5000 });

  const banner = page.locator('.notification-bar .alert').first();
  await expect(banner).toBeVisible();
  await expect(banner).toContainText(expectedText);

  return banner;
}

async function assertBannerNotVisible(page: Page): Promise<void> {
  await page.waitForTimeout(5000);

  const banner = page.locator('.notification-bar .alert').first();
  await expect(banner).not.toBeVisible();
}

async function assertBannerWithType(page: Page, expectedText: string, type: 'warning' | 'error'): Promise<Locator> {
  const banner = await assertBannerVisible(page, expectedText);
  const cssClass = type === 'warning' ? /alert-warning/ : /alert-danger/;
  await expect(banner).toHaveClass(cssClass);

  const supportLink = banner.locator('a[href="mailto:support@treeverse.io"]');
  await expect(supportLink).toBeVisible();

  return banner;
}

async function assertBannerPositioning(page: Page): Promise<Locator> {
  const bannerContainer = page.locator('.notification-bar');
  await expect(bannerContainer).toBeVisible();
  await expect(bannerContainer).toHaveClass(/w-100/);

  const bannerBox = await bannerContainer.boundingBox();
  expect(bannerBox).not.toBeNull();
  expect(bannerBox!.y).toBeLessThanOrEqual(5);

  return bannerContainer;
}

async function assertNotificationOffset(page: Page, expectedState: 'set' | 'cleared') {
  const notificationOffset = await page.evaluate(() => {
    return getComputedStyle(document.documentElement).getPropertyValue('--notification-offset');
  });

  if (expectedState === 'set') {
    expect(notificationOffset).not.toBe('0px');
    expect(notificationOffset).not.toBe('');
  } else {
    expect(notificationOffset).toBe('0px');
  }

  return notificationOffset;
}

async function addScrollableContent(page: Page) {
  await page.evaluate(() => {
    // Create a tall div to make the page scrollable
    const tallDiv = document.createElement('div');
    tallDiv.style.height = '2000px';
    tallDiv.style.backgroundColor = '#f0f0f0';
    tallDiv.innerHTML = '<p style="padding: 20px;">Scrollable content area</p>';
    document.body.appendChild(tallDiv);
  });
}

async function scrollPageDown(page: Page) {
  await page.evaluate(() => window.scrollTo(0, window.innerHeight * 2));
  await page.waitForTimeout(500);
}