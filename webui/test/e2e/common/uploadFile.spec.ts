import { test, expect } from '@playwright/test';
import { RepositoriesPage } from "../poms/repositoriesPage";
import { RepositoryPage } from "../poms/repositoryPage";
import fs from "fs";
import path from "path";

const TEST_REPO_NAME = "test-upload";
const FILE_NAME = "test-upload.txt";
const TEST_BRANCH = "test-branch";
const PREFIX = "prefix/";

test.describe("Upload File", () => {
    test.describe.configure({mode: "serial"});
		test("Create repo, Upload file check path", async ({page}) => {
			// create new repo
			const repositoriesPage = new RepositoriesPage(page);
			await repositoriesPage.goto();
			await repositoriesPage.createRepository(TEST_REPO_NAME, true);

			// create file to upload
			const filePath = path.join(__dirname, FILE_NAME);
			fs.writeFileSync(filePath, "This is a test file for Playwright upload.");
			const repositoryPage = new RepositoryPage(page);

			// create new branch to upload file to
			await repositoryPage.createBranch(TEST_BRANCH)
			await repositoryPage.gotoObjectsTab();
			await repositoryPage.switchBranch(TEST_BRANCH)
			await repositoryPage.uploadObject(filePath);
			
			// upload file, check path 
			await expect(page.getByText(FILE_NAME)).toBeVisible();
			await page.getByRole('button', { name: 'Upload 1 File' }).click();
            await expect(page.getByRole('cell', {name: FILE_NAME})).toBeVisible();

			// upload again with prefix, check path
			await repositoryPage.uploadObject(filePath);
			await page.getByRole('textbox', { name: 'Common Destination Directory' }).fill(PREFIX);
			await expect(page.getByText(PREFIX+FILE_NAME)).toBeVisible();
			await page.getByRole('button', { name: 'Upload 1 File' }).click();
			await page.getByRole('cell', { name: PREFIX }).click();
 			await expect(page.getByRole('cell', { name: FILE_NAME })).toBeVisible();

	});
})
