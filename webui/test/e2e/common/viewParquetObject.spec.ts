import { test, expect } from "@playwright/test";
import { RepositoriesPage } from "../poms/repositoriesPage";
import { RepositoryPage } from "../poms/repositoryPage";

const TEST_REPO_NAME = "test-repo";
const PARQUET_OBJECT_NAME = "lakes.parquet";

test.describe("Object Viewer - Parquet File", () => {
    test.describe.configure({ mode: "serial" });
    test("create repo w/ sample data", async ({page}) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await repositoriesPage.createRepository(TEST_REPO_NAME, true);
    });

    test("view parquet object", async ({page}) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(TEST_REPO_NAME);

        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.clickObject(PARQUET_OBJECT_NAME);
        await expect(page.getByText("Loading...")).not.toBeVisible();
    })
})

