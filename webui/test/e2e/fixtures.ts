import { test as base, expect } from "@playwright/test";
import { RepositoriesPage } from "./poms/repositoriesPage";
import { RepositoryPage } from "./poms/repositoryPage";
import { ObjectViewerPage } from "./poms/objectViewerPage";
import { PullsPage } from "./poms/pullsPage";
import { LoginPage } from "./poms/loginPage";
import { SetupPage } from "./poms/setupPage";
import { LakeFSApi } from "./lakeFSApi";
import { getCredentials } from "./credentialsFile";

type E2EFixtures = {
    repositoriesPage: RepositoriesPage;
    repositoryPage: RepositoryPage;
    objectViewerPage: ObjectViewerPage;
    pullsPage: PullsPage;
    loginPage: LoginPage;
    setupPage: SetupPage;
};

type E2EWorkerFixtures = {
    autoSetup: void;
    lakeFSApi: LakeFSApi;
};

export const test = base.extend<E2EFixtures, E2EWorkerFixtures>({
    // When SKIP_SETUP is set, the setup projects are skipped. But if the server
    // is in the "comm prefs missing" state (admin exists, email not filled),
    // every page navigation redirects to /setup and tests fail. This fixture
    // detects that state via API and completes the comm prefs form submission.
    autoSetup: [async ({ playwright }, use) => {
        if (!process.env.SKIP_SETUP) {
            await use();
            return;
        }
        const baseUrl = process.env.BASE_URL || "http://localhost:8000";
        const request = await playwright.request.newContext();
        const resp = await request.get(`${baseUrl}/api/v1/setup_lakefs`);
        const state = await resp.json();
        if (state.state === "initialized" && state.comm_prefs_missing) {
            await request.post(`${baseUrl}/api/v1/setup_comm_prefs`, {
                data: { email: "test@example.com", featureUpdates: false, securityUpdates: false },
            });
        }
        await request.dispose();
        await use();
    }, { auto: true, scope: "worker" }],
    repositoriesPage: async ({ page }, use) => { await use(new RepositoriesPage(page)); },
    repositoryPage: async ({ page }, use) => { await use(new RepositoryPage(page)); },
    objectViewerPage: async ({ page }, use) => { await use(new ObjectViewerPage(page)); },
    pullsPage: async ({ page }, use) => { await use(new PullsPage(page)); },
    loginPage: async ({ page }, use) => { await use(new LoginPage(page)); },
    setupPage: async ({ page }, use) => { await use(new SetupPage(page)); },
    lakeFSApi: [async ({ playwright }, use) => {
        const baseUrl = process.env.BASE_URL || "http://localhost:8000";
        const credentials = await getCredentials();
        if (!credentials) {
            throw new Error("No credentials found. Run setup first.");
        }
        const request = await playwright.request.newContext();
        await use(new LakeFSApi(request, baseUrl, credentials));
        await request.dispose();
    }, { scope: "worker" }],
});

export { expect };
