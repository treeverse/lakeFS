import { test, expect, Page, Locator } from "@playwright/test";
import { MockServer } from "./utils/mockServerAfterAddressedBaraksComments";
import { createDate, createLicenseToken } from "./utils/testUtilsWithNewMockServer";

let mockServer: MockServer;
let serverUrl: string;

// Helper function to set up basic API mocks needed for the tests
function setupDefaultMocks(server: MockServer): void {
    // Set up authenticated user
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

    // Set up config
    server.setApiMock('/api/v1/config', {
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
            storage_config: {
                blockstore_description: "",
                blockstore_id: "",
                blockstore_namespace_ValidityRegex: "^local://",
                blockstore_namespace_example: "local://example-bucket/",
                blockstore_type: "local",
                default_namespace_prefix: "local://",
                import_support: false,
                import_validity_regex: "^local://",
                pre_sign_multipart_upload: false,
                pre_sign_support: false,
                pre_sign_support_ui: false
            },
            storage_config_list: [],
            version_config: {
                latest_version: "1.60.0",
                upgrade_recommended: false,
                version: "1.60.0",
                version_context: "lakeFS-Enterprise"
            }
        })
    });

    // Set up lakefs setup
    server.setApiMock('/api/v1/setup_lakefs', {
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
            comm_prefs_missing: false,
            login_config: {
                RBAC: "internal",
                login_cookie_names: [
                    "internal_auth_session"
                ],
                login_failed_message: "The credentials don't match.",
                login_url: "",
                logout_url: ""
            },
            state: "initialized"
        })
    });

    // Set up repositories
    server.setApiMock('/api/v1/repositories', {
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
            pagination: {
                has_more: false,
                max_per_page: 1000,
                next_offset: "",
                results: 0
            },
            results: []
        })
    });
    
    // Set up default valid license
    const defaultValidToken = createLicenseToken(createDate(60));
    server.setApiMock('/api/v1/license', {
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ token: defaultValidToken })
    });
}

// Helper function to mock license with specific token
function mockLicense(server: MockServer, token: string): void {
    server.setApiMock('/api/v1/license', {
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ token })
    });
}

// Helper function to mock license unauthorized
function mockLicenseUnauthorized(server: MockServer): void {
    server.setApiMock('/api/v1/license', {
        status: 401,
        contentType: 'application/json',
        body: JSON.stringify({
            message: "Unauthorized"
        })
    });
}

// Helper function to mock license not implemented
function mockLicenseNotImplemented(server: MockServer): void {
    server.setApiMock('/api/v1/license', {
        status: 501,
        contentType: 'application/json',
        body: JSON.stringify({
            message: "Not Implemented"
        })
    });
}

// Helper function to mock license server error
function mockLicenseServerError(server: MockServer): void {
    server.setApiMock('/api/v1/license', {
        status: 500,
        contentType: 'application/json',
        body: JSON.stringify({
            message: 'Internal Server Error'
        })
    });
}

// Helper function to mock login
function mockLogin(server: MockServer): void {
    const now = Math.floor(Date.now() / 1000);
    const expirationTime = now + 3600;

    server.setApiMock('/api/v1/auth/login', {
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
            token: "mock-auth-token",
            token_expiration: expirationTime
        })
    });
}

// Helper function to mock logout (custom endpoint not in API)
function mockLogout(server: MockServer, logoutRedirectURL: string = '/auth/login'): void {
    server.setApiMock('/logout', {
        status: 307,
        headers: {
            'Location': logoutRedirectURL
        }
    });
}

// Helper function to mock authenticated user
function mockAuthenticated(server: MockServer): void {
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
}

// Helper function to mock unauthenticated user
function mockUnauthenticated(server: MockServer): void {
    server.setApiMock('/api/v1/user', {
        status: 401,
        contentType: 'application/json',
        body: JSON.stringify({
            message: "Unauthorized"
        })
    });
}

test.describe("License Notification Banner Tests", () => {
    test.beforeAll(async () => {
        mockServer = new MockServer();
        serverUrl = await mockServer.start();
        setupDefaultMocks(mockServer);
    });

    test.afterAll(async () => {
        if (mockServer) {
            await mockServer.stop();
        }
    });

    test.beforeEach(async ({ page }) => {
        await page.goto(`${serverUrl}/`);
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
        mockLicenseUnauthorized(mockServer);

        await page.reload();
        await assertBannerNotVisible(page);
    });

    test("should not display banner when license endpoint returns 501 (Not Implemented)", async ({ page }) => {
        mockLicenseNotImplemented(mockServer);

        await page.reload();
        await assertBannerNotVisible(page);
    });

    test("should not display banner when license endpoint returns 500 (Internal Server Error)", async ({ page }) => {
        mockLicenseServerError(mockServer);

        await page.reload();
        await assertBannerNotVisible(page);
    });

    test("should not display banner when license endpoint returns invalid license token", async ({ page }) => {
        mockLicense(mockServer, "invalid.jwt.token");

        await page.reload();
        await assertBannerNotVisible(page);
    });

    test("should not display banner when license endpoint returns empty license token", async ({ page }) => {
        mockLicense(mockServer, "");

        await page.reload();
        await assertBannerNotVisible(page);
    });

    test("should display banner when user is logged in and not display when user is logged out", async ({ page }) => {
        mockLogin(mockServer);
        mockLogout(mockServer, '/auth/login');

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

        mockAuthenticated(mockServer);
        const token = createLicenseToken(createDate(1, 2));
        mockLicense(mockServer, token);

        await page.reload();
        await assertBannerWithType(page, "Warning: Your lakeFS license will expire in 1 days. Please contact support to renew.", 'warning');

        const logoutResponse = await page.request.get(`${serverUrl}/logout`, {
            maxRedirects: 0
        });

        expect(logoutResponse.status()).toBe(307);

        mockUnauthenticated(mockServer);
        mockLicenseUnauthorized(mockServer);

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