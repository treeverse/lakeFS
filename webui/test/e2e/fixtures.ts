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
    lakeFSApi: LakeFSApi;
};

type E2EWorkerFixtures = {
    lakeFSApiWorker: LakeFSApi;
};

export const test = base.extend<E2EFixtures, E2EWorkerFixtures>({
    repositoriesPage: async ({ page }, use) => {
        await use(new RepositoriesPage(page));
    },
    repositoryPage: async ({ page }, use) => {
        await use(new RepositoryPage(page));
    },
    objectViewerPage: async ({ page }, use) => {
        await use(new ObjectViewerPage(page));
    },
    pullsPage: async ({ page }, use) => {
        await use(new PullsPage(page));
    },
    loginPage: async ({ page }, use) => {
        await use(new LoginPage(page));
    },
    setupPage: async ({ page }, use) => {
        await use(new SetupPage(page));
    },
    lakeFSApi: async ({ playwright }, use) => {
        const baseUrl = process.env.BASE_URL || "http://localhost:8000";
        const credentials = await getCredentials();
        if (!credentials) {
            throw new Error("No credentials found. Run setup first.");
        }
        const request = await playwright.request.newContext();
        const api = new LakeFSApi(request, baseUrl, credentials);
        await use(api);
        await request.dispose();
    },
    lakeFSApiWorker: [async ({ playwright }, use) => {
        const baseUrl = process.env.BASE_URL || "http://localhost:8000";
        const credentials = await getCredentials();
        if (!credentials) {
            throw new Error("No credentials found. Run setup first.");
        }
        const request = await playwright.request.newContext();
        const api = new LakeFSApi(request, baseUrl, credentials);
        await use(api);
        await request.dispose();
    }, { scope: "worker" }],
});

export { expect };
