import { test, expect } from '@playwright/test';
import { RepositoriesPage } from '../poms/repositoriesPage';
import { RepositoryPage } from '../poms/repositoryPage';
import fs from 'fs';
import path from 'path';

const TEST_REPO_NAME = 'test-commit-merge';
const SOURCE_BRANCH = 'feature-branch';
const DEST_BRANCH = 'main';
const COMMIT_MESSAGE = 'Test commit';
const MERGE_MESSAGE = 'Test merge';

test.describe('Commit and Merge Operations', () => {
    test.describe.configure({ mode: 'serial' });

    let setupComplete = false;
    const createdFiles: string[] = [];

    // Clean up any leftover files from previous crashed test runs
    test.beforeAll(() => {
        const testDir = __dirname;
        const pattern = /^sync-(commit-merge-test|conflict-test)-in-syncCommitMerge-playwright-file(-\d+)?\.txt$/;

        try {
            const files = fs.readdirSync(testDir);
            files.forEach((file) => {
                if (pattern.test(file)) {
                    const filePath = path.join(testDir, file);
                    try {
                        fs.unlinkSync(filePath);
                    } catch (error) {
                        console.error(`Failed to delete ${file}:`, error);
                    }
                }
            });
        } catch (error) {
            console.error('Failed to clean up leftover files:', error);
        }
    });

    // Clean up files created during this test run
    test.afterAll(() => {
        createdFiles.forEach((filePath) => {
            try {
                if (fs.existsSync(filePath)) {
                    fs.unlinkSync(filePath);
                }
            } catch (error) {
                console.error(`Failed to delete file ${filePath}:`, error);
            }
        });
    });

    test('Setup: Create repository', async ({ page }) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await repositoriesPage.createRepository(TEST_REPO_NAME, true);
        setupComplete = true;
    });

    test('Commit: Upload file and commit changes', async ({ page }) => {
        expect(setupComplete).toBeTruthy();
        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.goto(TEST_REPO_NAME);
        await repositoryPage.createBranch(SOURCE_BRANCH);
        await repositoryPage.gotoObjectsTab();
        await repositoryPage.switchBranch(SOURCE_BRANCH);
        await expect(page.getByRole('button', { name: 'Upload' })).toBeVisible({ timeout: 10000 });

        const fileCount = 2;
        const filePaths: string[] = [];

        for (let i = 0; i < fileCount; i++) {
            const fileName = `sync-commit-merge-test-in-syncCommitMerge-playwright-file-${i}.txt`;
            const filePath = path.join(__dirname, fileName);
            // Make each file larger to increase commit time
            fs.writeFileSync(filePath, `Testing commit functionality - File ${i}\n`.repeat(1000));
            filePaths.push(filePath);
            createdFiles.push(filePath);
        }

        await repositoryPage.uploadMultipleObjects(filePaths);

        await expect(
            page.getByText(/sync-commit-merge-test-in-syncCommitMerge-playwright-file(-\d+)?\.txt/).first(),
        ).toBeVisible();
        await page.getByRole('button', { name: `Upload ${fileCount} Files` }).click();

        await expect(
            page
                .getByRole('cell', { name: /sync-commit-merge-test-in-syncCommitMerge-playwright-file(-\d+)?\.txt/ })
                .first(),
        ).toBeVisible({ timeout: 10000 });

        await repositoryPage.showOnlyChanges();
        const count = await repositoryPage.getUncommittedCount();
        expect(count).toBeGreaterThan(0);

        // Set up network request listener to verify sync commit endpoint is called
        const commitRequestPromise = page.waitForRequest((request) => {
            return request.url().includes('/commits') && request.method() === 'POST';
        });

        await repositoryPage.commitChanges(COMMIT_MESSAGE);

        // Verify the commit used the sync endpoint: /branches/{branch}/commits
        const commitRequest = await commitRequestPromise;
        expect(commitRequest.url()).toMatch(/\/branches\/[^/]+\/commits$/);

        await expect(page.getByRole('dialog')).not.toBeVisible({ timeout: 120000 }); // 2 min timeout

        await page.getByRole('link', { name: 'Commits' }).click();
        await expect(page.getByText(COMMIT_MESSAGE)).toBeVisible({ timeout: 10000 });

        // Verify we're on the correct branch's commits
        await expect(page.getByRole('button', { name: `branch: ${SOURCE_BRANCH}` })).toBeVisible();

        await repositoryPage.gotoObjectsTab();
        await expect(page.getByRole('button', { name: 'Uncommitted Changes' })).not.toBeVisible();

        await expect(
            page
                .getByRole('cell', { name: /sync-commit-merge-test-in-syncCommitMerge-playwright-file(-\d+)?\.txt/ })
                .first(),
        ).toBeVisible();
    });

    test('Merge: Merge feature branch into main', async ({ page }) => {
        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.goto(TEST_REPO_NAME);
        await repositoryPage.gotoCompareTab();

        // Set up comparison: feature-branch -> main
        await repositoryPage.selectComparedToBranch(SOURCE_BRANCH);

        // Wait for diff to load
        await expect(
            page.getByText(/sync-commit-merge-test-in-syncCommitMerge-playwright-file(-\d+)?\.txt/).first(),
        ).toBeVisible({ timeout: 10000 });

        // Set up network request listener to verify sync merge endpoint is called
        const mergeRequestPromise = page.waitForRequest((request) => {
            return request.url().includes('/merge/') && request.method() === 'POST';
        });

        await repositoryPage.merge(MERGE_MESSAGE);

        // Verify the merge used the sync endpoint: /refs/{sourceRef}/merge/{destinationBranch}
        const mergeRequest = await mergeRequestPromise;
        expect(mergeRequest.url()).toMatch(/\/refs\/[^/]+\/merge\/[^/]+$/);

        await expect(page.getByRole('dialog')).not.toBeVisible({ timeout: 120000 }); // 2 min timeout

        await page.getByRole('link', { name: 'Commits' }).click();
        await expect(page.getByText(MERGE_MESSAGE)).toBeVisible({ timeout: 10000 });

        // Verify we're viewing the destination branch (main) commits
        await expect(page.getByRole('button', { name: `branch: ${DEST_BRANCH}` })).toBeVisible();

        // Go to objects and verify the merged files are there
        await page.goto(`/repositories/${TEST_REPO_NAME}/objects?ref=${DEST_BRANCH}`);
        await expect(
            page
                .getByRole('cell', { name: /sync-commit-merge-test-in-syncCommitMerge-playwright-file(-\d+)?\.txt/ })
                .first(),
        ).toBeVisible();
    });

    test('Commit: Handle empty commit attempt', async ({ page }) => {
        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.goto(TEST_REPO_NAME);
        await repositoryPage.gotoObjectsTab();
        await repositoryPage.switchBranch(SOURCE_BRANCH);

        await expect(page.getByRole('button', { name: 'Uncommitted Changes' })).not.toBeVisible();
    });

    test('Merge: Handle no-diff merge attempt', async ({ page }) => {
        await page.goto(`/repositories/${TEST_REPO_NAME}/compare?ref=${DEST_BRANCH}&compare=${DEST_BRANCH}`);

        await expect(page.getByRole('button', { name: 'Merge' })).toBeDisabled();
    });

    test('Merge: With merge strategy on conflict', async ({ page }) => {
        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.goto(TEST_REPO_NAME);

        // Create a conflict scenario: modify same file on both branches
        // First, create a file on a temp branch and merge it into main
        const tempBranch = 'temp-conflict-branch';
        await repositoryPage.createBranch(tempBranch);
        await repositoryPage.gotoObjectsTab();
        await repositoryPage.switchBranch(tempBranch);

        const fileName = 'sync-conflict-test-in-syncCommitMerge-playwright-file.txt';
        const filePath = path.join(__dirname, fileName);
        fs.writeFileSync(filePath, 'Version on main branch');
        createdFiles.push(filePath);

        await repositoryPage.uploadObject(filePath);
        await expect(page.getByText(fileName)).toBeVisible();
        await page.getByRole('button', { name: 'Upload 1 File' }).click();
        await expect(page.getByRole('cell', { name: fileName })).toBeVisible({ timeout: 10000 });

        // Commit on temp branch
        await repositoryPage.showOnlyChanges();
        await repositoryPage.commitChanges('Add conflict file on temp');
        await expect(page.getByRole('dialog')).not.toBeVisible({ timeout: 120000 });

        // Merge temp branch into main
        await page.goto(`/repositories/${TEST_REPO_NAME}/compare?ref=${DEST_BRANCH}&compare=${tempBranch}`);
        await repositoryPage.merge('Merge conflict file to main');
        await expect(page.getByRole('dialog')).not.toBeVisible({ timeout: 120000 });

        // Now modify same file on feature branch
        await repositoryPage.gotoObjectsTab();
        await repositoryPage.switchBranch(SOURCE_BRANCH);

        fs.writeFileSync(filePath, 'Version on feature branch');
        await repositoryPage.uploadObject(filePath);
        await expect(page.getByText(fileName)).toBeVisible();
        await page.getByRole('button', { name: 'Upload 1 File' }).click();
        await expect(page.getByRole('cell', { name: fileName })).toBeVisible({ timeout: 10000 });

        // Commit on feature branch
        await repositoryPage.showOnlyChanges();
        await repositoryPage.commitChanges('Add conflict file on feature');
        await expect(page.getByRole('dialog')).not.toBeVisible({ timeout: 120000 });

        // Navigate to Compare page with feature-branch -> main
        await page.goto(`/repositories/${TEST_REPO_NAME}/compare?ref=${DEST_BRANCH}&compare=${SOURCE_BRANCH}`);

        await page.getByRole('button', { name: 'Merge' }).click();

        await page.getByLabel('Strategy').click();
        await page.getByRole('option', { name: 'source-wins' }).click();

        await page.getByPlaceholder('Commit Message (Optional)').fill('Merge with source-wins strategy');

        await page.getByRole('dialog').getByRole('button', { name: 'Merge' }).click();

        await expect(page.getByRole('dialog')).not.toBeVisible({ timeout: 120000 });

        // Verify the merge commit was created
        await page.getByRole('link', { name: 'Commits' }).click();
        await expect(page.getByText('Merge with source-wins strategy')).toBeVisible({ timeout: 10000 });
        await expect(page.getByRole('button', { name: `branch: ${DEST_BRANCH}` })).toBeVisible();

        await page.goto(`/repositories/${TEST_REPO_NAME}/objects?ref=${DEST_BRANCH}`);
        await expect(page.getByRole('cell', { name: fileName })).toBeVisible();

        await page.getByRole('link', { name: fileName }).click();

        // Verify the content is from the source branch (feature-branch) due to source-wins strategy
        await expect(page.getByText('Version on feature branch')).toBeVisible({ timeout: 10000 });
    });
});
