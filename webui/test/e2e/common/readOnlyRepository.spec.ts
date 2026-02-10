import { test, expect } from "../fixtures";

const READ_ONLY_REPO_NAME = 'ro-test-repo';

test.describe("Read Only Repository", () => {
    test.describe.configure({ mode: "serial" });
    test("Read only indicator shown on repositories page", async ({ repositoriesPage, lakeFSApi }) => {
        const storageNamespace = (process.env.REPO_STORAGE_NAMESPACE_PREFIX || 'local://') + 'ro_test_repo';
        await lakeFSApi.createRepository(READ_ONLY_REPO_NAME, storageNamespace, { readOnly: true, ifNotExists: true });
        await repositoriesPage.goto();
        await expect(repositoriesPage.readOnlyIndicatorLocator).toBeVisible();
    });

    test("Read only indicator shown on repository page and upload button is disabled", async ({ repositoriesPage, repositoryPage }) => {
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(READ_ONLY_REPO_NAME);
        await expect(repositoryPage.readOnlyIndicatorLocator).toBeVisible();
        await expect(repositoryPage.objects.uploadButtonLocator).toBeDisabled();
    });
});
