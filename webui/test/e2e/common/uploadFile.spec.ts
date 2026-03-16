import { test, expect } from "../fixtures";

const TEST_REPO_NAME = "test-upload";
const FILE_NAME = "test-upload.txt";
const TEST_BRANCH = "test-branch";
const PREFIX = "prefix/";

const fileBuffer = {
    name: FILE_NAME,
    mimeType: "text/plain",
    buffer: Buffer.from("This is a test file for Playwright upload."),
};

test.describe("Upload File", () => {
    test.describe.configure({ mode: "serial" });

    test("Create repo, Upload file check path", async ({ page, repositoriesPage, repositoryPage }) => {
        await test.step("create repository with sample data", async () => {
            await repositoriesPage.goto();
            await repositoriesPage.createRepository(TEST_REPO_NAME, true);
        });

        await test.step("create branch and upload file", async () => {
            await repositoryPage.branches.createBranch(TEST_BRANCH);
            await repositoryPage.gotoObjectsTab();
            await repositoryPage.branches.switchBranch(TEST_BRANCH);
            await repositoryPage.objects.uploadFiles(fileBuffer);
        });

        await test.step("verify upload path", async () => {
            await expect(page.getByText(FILE_NAME)).toBeVisible();
            await page.getByRole('button', { name: 'Upload 1 File' }).click();
            await expect(page.getByRole('cell', { name: FILE_NAME })).toBeVisible();
        });

        await test.step("upload with prefix and verify path", async () => {
            await repositoryPage.objects.uploadFiles(fileBuffer);
            await page.getByRole('textbox', { name: 'Common Destination Directory' }).fill(PREFIX);
            await expect(page.getByText(PREFIX + FILE_NAME)).toBeVisible();
            await page.getByRole('button', { name: 'Upload 1 File' }).click();
            await page.getByRole('cell', { name: PREFIX }).click();
            await expect(page.getByRole('cell', { name: FILE_NAME })).toBeVisible();
        });
    });
});
