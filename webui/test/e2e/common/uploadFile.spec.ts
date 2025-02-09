import { test, expect } from '@playwright/test';
import { RepositoriesPage } from "../poms/repositoriesPage";
import { RepositoryPage } from "../poms/repositoryPage";
import fs from "fs";
import path from "path";

const TEST_REPO_NAME = "test-upload";
const FILE_NAME = "test-upload.txt";


test.describe("Upload File", () => {
    test.describe.configure({mode: "serial"});
		test("Create repo", async ({page}) => {
			const repositoriesPage = new RepositoriesPage(page);
			await repositoriesPage.goto();
			await repositoriesPage.createRepository(TEST_REPO_NAME, true);
		});

		test("Upload file and verify path", async ({page}) => {
			const repositoriesPage = new RepositoriesPage(page);
			await repositoriesPage.goto();
			await repositoriesPage.goToRepository(TEST_REPO_NAME);

			const filePath = path.join(__dirname, FILE_NAME);
			fs.writeFileSync(filePath, "This is a test file for Playwright upload.");

			const repositoryPage = new RepositoryPage(page);
			await repositoryPage.uploadObject(filePath);

  			await expect(page.getByRole('complementary')).toContainText(`lakefs://${TEST_REPO_NAME}/main/${FILE_NAME}`);
  			await page.getByRole('button', { name: 'Upload', exact: true }).click();
  			await expect(page.getByRole('rowgroup')).toContainText(FILE_NAME);
		});
})
