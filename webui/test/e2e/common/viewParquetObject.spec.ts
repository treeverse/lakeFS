import { test, expect } from "../fixtures";

const TEST_REPO_NAME = "test-repo";
const PARQUET_OBJECT_NAME = "lakes.parquet";

test.describe("Object Viewer - Parquet File", () => {
    test.describe.configure({ mode: "serial" });

    test("create repo w/ sample data", async ({ repositoriesPage, repositoryPage }) => {
        await repositoriesPage.goto();
        await repositoriesPage.createRepository(TEST_REPO_NAME, true);
    });

    test("view parquet object", async ({ page, repositoriesPage, repositoryPage }) => {
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(TEST_REPO_NAME);
        await repositoryPage.objects.clickObject(PARQUET_OBJECT_NAME);
        await expect(page.getByRole("button", { name: "Execute" })).toBeVisible();
    });
});
