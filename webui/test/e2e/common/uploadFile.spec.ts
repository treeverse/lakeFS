import { test, expect } from '@playwright/test';
import { RepositoriesPage } from "../poms/repositoriesPage";
import { RepositoryPage } from "../poms/repositoryPage";
import { ObjectViewerPage } from "../poms/objectViewerPage";

const TEST_REPO_NAME = "test";


test.describe("Upload File", () => {
    test.describe.configure({mode: "serial"});
		test("create repo", async ({page}) => {
			const repositoriesPage = new RepositoriesPage(page);
			await repositoriesPage.goto();
			await repositoriesPage.createRepository(TEST_REPO_NAME, true);
		});

		test("view and query parquet object", async ({page}) => {
			const repositoriesPage = new RepositoriesPage(page);
			await repositoriesPage.goto();
			await repositoriesPage.goToRepository(TEST_REPO_NAME);

			const repositoryPage = new RepositoryPage(page);
			await repositoryPage.clickObject(TEST_REPO_NAME);
			await expect(page.getByText("Loading...")).not.toBeVisible();

			const objectViewerPage = new ObjectViewerPage(page);
			await objectViewerPage.clickExecuteButton();
			expect(await objectViewerPage.getResultRowCount()).toEqual(5);
		});
})
