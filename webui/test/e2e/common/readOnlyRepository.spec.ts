import { test, expect } from "@playwright/test";
import { RepositoriesPage } from "../poms/repositoriesPage";

const READ_ONLY_REPO_NAME = 'ro-test-repo';

test.describe("Read Only Repository", () => {
    test.beforeAll(async ({ browser }) => {
        const context = await browser.newContext();
        await context.request.post('/api/v1/repositories', {
            data: {
                name: READ_ONLY_REPO_NAME,
                storage_namespace: 'local://ro_test_repo',
                read_only: true,
            },
        });
    });

    test("Read only indicator shown on repositories page", async ({ page }) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await expect(repositoriesPage.readOnlyIndicatorLocator).toBeVisible();
    });

    test("Read only indicator shown on repository page and upload button is disabled", async ({ page }) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(READ_ONLY_REPO_NAME);
        const repositoryPage = new RepositoriesPage(page);
        await expect(repositoryPage.readOnlyIndicatorLocator).toBeVisible();
        await expect(repositoryPage.uploadButtonLocator).toBeDisabled();
    });
})