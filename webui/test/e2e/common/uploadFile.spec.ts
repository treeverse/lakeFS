import { test, expect } from '@playwright/test';
import { RepositoriesPage } from "../poms/repositoriesPage";
import { RepositoryPage } from "../poms/repositoryPage";
import fs from "fs";
import path from "path";

const TEST_REPO_NAME = "test-upload";
const FILE_NAME = "test-upload.txt";
const TEST_BRANCH = "test-branch";

test.describe("Upload File", () => {
    test.describe.configure({mode: "serial"});
		test("Create repo, Upload file check path", async ({page}) => {
			const repositoriesPage = new RepositoriesPage(page);
			await repositoriesPage.goto();
			await repositoriesPage.createRepository(TEST_REPO_NAME, true);

			const filePath = path.join(__dirname, FILE_NAME);
			fs.writeFileSync(filePath, "This is a test file for Playwright upload.");

			const repositoryPage = new RepositoryPage(page);

			await repositoryPage.createBranch(TEST_BRANCH)
			await repositoryPage.switchBranch(TEST_BRANCH)
			await repositoryPage.uploadObject(filePath);
  			await expect(page.getByRole('complementary')).toContainText(`lakefs://${TEST_REPO_NAME}/${TEST_BRANCH}/${FILE_NAME}`);
	        
			await page.waitForTimeout(3000);
            await page.reload();
            await expect(page.getByRole('rowgroup')).toContainText(FILE_NAME);
		});
})
