import { test, expect } from '../fixtures';
import { TIMEOUT_LONG_OPERATION, TIMEOUT_ELEMENT_VISIBLE } from '../timeouts';

const TEST_REPO_NAME = 'test-commit-merge';
const SOURCE_BRANCH = 'feature-branch';
const DEST_BRANCH = 'main';
const COMMIT_MESSAGE = 'Test commit';
const MERGE_MESSAGE = 'Test merge';
const FILE_1_NAME = 'sync-commit-merge-test-in-syncCommitMerge-playwright-file-0.txt';
const FILE_2_NAME = 'sync-commit-merge-test-in-syncCommitMerge-playwright-file-1.txt';
const CONFLICT_FILE_NAME = 'sync-conflict-test-in-syncCommitMerge-playwright-file.txt';

test.describe('Commit and Merge Operations', () => {
    test.describe.configure({ mode: 'serial' });

    test('Setup: Create repository', async ({ repositoriesPage }) => {
        await repositoriesPage.goto();
        await repositoriesPage.createRepository(TEST_REPO_NAME, true);
    });

    test('Commit: Upload file and commit changes', async ({ page, repositoryPage }) => {
        await repositoryPage.goto(TEST_REPO_NAME);
        await repositoryPage.branches.createBranch(SOURCE_BRANCH);
        await repositoryPage.gotoObjectsTab();
        await repositoryPage.branches.switchBranch(SOURCE_BRANCH);
        await expect(page.getByRole('button', { name: 'Upload', exact: true })).toBeVisible({ timeout: TIMEOUT_ELEMENT_VISIBLE });

        const fileBuffers = [
            {
                name: FILE_1_NAME,
                mimeType: 'text/plain',
                buffer: Buffer.from('Testing commit functionality - File 0\n'.repeat(1000))
            },
            {
                name: FILE_2_NAME,
                mimeType: 'text/plain',
                buffer: Buffer.from('Testing commit functionality - File 1\n'.repeat(1000))
            }
        ];

        await test.step("upload files", async () => {
            await repositoryPage.objects.uploadFiles(fileBuffers);
            await expect(page.getByText(FILE_1_NAME).first()).toBeVisible({ timeout: TIMEOUT_ELEMENT_VISIBLE });
            await page.getByRole('button', { name: 'Upload 2 Files' }).click();
            await expect(page.getByRole('cell', { name: FILE_1_NAME }).first()).toBeVisible({ timeout: TIMEOUT_ELEMENT_VISIBLE });
        });

        await test.step("commit changes and verify sync endpoint", async () => {
            await repositoryPage.changes.showOnlyChanges();
            await expect.poll(() => repositoryPage.changes.getUncommittedCount()).toBe(2);

            const commitRequestPromise = page.waitForRequest((request) => {
                return request.url().includes('/commits') && request.method() === 'POST';
            });

            await repositoryPage.changes.commitChanges(COMMIT_MESSAGE);

            const commitRequest = await commitRequestPromise;
            expect(commitRequest.url()).toContain(`/branches/${SOURCE_BRANCH}/commits`);

            await expect(page.getByRole('dialog')).not.toBeVisible({ timeout: TIMEOUT_LONG_OPERATION });
        });

        await test.step("verify commit in commits tab", async () => {
            await page.getByRole('link', { name: 'Commits' }).click();
            await expect(page.getByText(COMMIT_MESSAGE)).toBeVisible({ timeout: TIMEOUT_ELEMENT_VISIBLE });
            await expect(page.getByRole('button', { name: `branch: ${SOURCE_BRANCH}` })).toBeVisible();

            await repositoryPage.gotoObjectsTab();
            await expect(page.getByRole('button', { name: 'Uncommitted Changes' })).not.toBeVisible();
            await expect(page.getByRole('cell', { name: FILE_1_NAME }).first()).toBeVisible();
        });
    });

    test('Merge: Merge feature branch into main', async ({ page, repositoryPage }) => {
        await repositoryPage.goto(TEST_REPO_NAME);
        await repositoryPage.gotoCompareTab();
        await repositoryPage.branches.selectComparedToBranch(SOURCE_BRANCH);

        await expect(page.getByText(FILE_1_NAME).first()).toBeVisible({ timeout: TIMEOUT_ELEMENT_VISIBLE });

        await test.step("merge and verify sync endpoint", async () => {
            const mergeRequestPromise = page.waitForRequest((request) => {
                return request.url().includes('/merge/') && request.method() === 'POST';
            });

            await repositoryPage.changes.merge(MERGE_MESSAGE);

            const mergeRequest = await mergeRequestPromise;
            expect(mergeRequest.url()).toContain(`/refs/${SOURCE_BRANCH}/merge/${DEST_BRANCH}`);

            await expect(page.getByRole('dialog')).not.toBeVisible({ timeout: TIMEOUT_LONG_OPERATION });
        });

        await test.step("verify merge in commits tab", async () => {
            await page.getByRole('link', { name: 'Commits' }).click();
            await expect(page.getByText(MERGE_MESSAGE)).toBeVisible({ timeout: TIMEOUT_ELEMENT_VISIBLE });
            await expect(page.getByRole('button', { name: `branch: ${DEST_BRANCH}` })).toBeVisible();

            await page.goto(`/repositories/${TEST_REPO_NAME}/objects?ref=${DEST_BRANCH}`);
            await expect(page.getByRole('cell', { name: FILE_1_NAME }).first()).toBeVisible();
        });
    });

    test('Commit: Handle empty commit attempt', async ({ page, repositoryPage }) => {
        await repositoryPage.goto(TEST_REPO_NAME);
        await repositoryPage.gotoObjectsTab();
        await repositoryPage.branches.switchBranch(SOURCE_BRANCH);
        await expect(page.getByRole('button', { name: 'Uncommitted Changes' })).not.toBeVisible();
    });

    test('Merge: Handle no-diff merge attempt', async ({ page }) => {
        await page.goto(`/repositories/${TEST_REPO_NAME}/compare?ref=${DEST_BRANCH}&compare=${DEST_BRANCH}`);
        await expect(page.getByRole('button', { name: 'Merge' })).toBeDisabled();
    });

    test('Merge: With merge strategy on conflict', async ({ page, repositoryPage }) => {
        await repositoryPage.goto(TEST_REPO_NAME);

        await test.step("create conflict file on temp branch and merge to main", async () => {
            const tempBranch = 'temp-conflict-branch';
            await repositoryPage.branches.createBranch(tempBranch);
            await repositoryPage.gotoObjectsTab();
            await repositoryPage.branches.switchBranch(tempBranch);

            await repositoryPage.objects.uploadFiles({
                name: CONFLICT_FILE_NAME,
                mimeType: 'text/plain',
                buffer: Buffer.from('Version on main branch')
            });
            await expect(page.getByText(CONFLICT_FILE_NAME)).toBeVisible({ timeout: TIMEOUT_ELEMENT_VISIBLE });
            await page.getByRole('button', { name: 'Upload 1 File' }).click();
            await expect(page.getByRole('cell', { name: CONFLICT_FILE_NAME })).toBeVisible({ timeout: TIMEOUT_ELEMENT_VISIBLE });

            await repositoryPage.changes.showOnlyChanges();
            await repositoryPage.changes.commitChanges('Add conflict file on temp');
            await expect(page.getByRole('dialog')).not.toBeVisible({ timeout: TIMEOUT_LONG_OPERATION });

            await page.goto(`/repositories/${TEST_REPO_NAME}/compare?ref=${DEST_BRANCH}&compare=${tempBranch}`);
            await repositoryPage.changes.merge('Merge conflict file to main');
            await expect(page.getByRole('dialog')).not.toBeVisible({ timeout: TIMEOUT_LONG_OPERATION });
        });

        await test.step("create same file on feature branch", async () => {
            await repositoryPage.gotoObjectsTab();
            await repositoryPage.branches.switchBranch(SOURCE_BRANCH);

            await repositoryPage.objects.uploadFiles({
                name: CONFLICT_FILE_NAME,
                mimeType: 'text/plain',
                buffer: Buffer.from('Version on feature branch')
            });
            await expect(page.getByText(CONFLICT_FILE_NAME)).toBeVisible({ timeout: TIMEOUT_ELEMENT_VISIBLE });
            await page.getByRole('button', { name: 'Upload 1 File' }).click();
            await expect(page.getByRole('cell', { name: CONFLICT_FILE_NAME })).toBeVisible({ timeout: TIMEOUT_ELEMENT_VISIBLE });

            await repositoryPage.changes.showOnlyChanges();
            await repositoryPage.changes.commitChanges('Add conflict file on feature');
            await expect(page.getByRole('dialog')).not.toBeVisible({ timeout: TIMEOUT_LONG_OPERATION });
        });

        await test.step("merge with source-wins strategy", async () => {
            await page.goto(`/repositories/${TEST_REPO_NAME}/compare?ref=${DEST_BRANCH}&compare=${SOURCE_BRANCH}`);

            await page.getByRole('button', { name: 'Merge' }).click();
            await page.getByLabel('Strategy').click();
            await page.getByRole('option', { name: 'source-wins' }).click();
            await page.getByPlaceholder('Commit Message (Optional)').fill('Merge with source-wins strategy');
            await page.getByRole('dialog').getByRole('button', { name: 'Merge' }).click();

            await expect(page.getByRole('dialog')).not.toBeVisible({ timeout: TIMEOUT_LONG_OPERATION });
        });

        await test.step("verify merge result", async () => {
            await page.getByRole('link', { name: 'Commits' }).click();
            await expect(page.getByText('Merge with source-wins strategy')).toBeVisible({ timeout: TIMEOUT_ELEMENT_VISIBLE });
            await expect(page.getByRole('button', { name: `branch: ${DEST_BRANCH}` })).toBeVisible();

            await page.goto(`/repositories/${TEST_REPO_NAME}/objects?ref=${DEST_BRANCH}`);
            await expect(page.getByRole('cell', { name: CONFLICT_FILE_NAME })).toBeVisible();
            await page.getByRole('link', { name: CONFLICT_FILE_NAME }).click();
            await expect(page.getByText('Version on feature branch')).toBeVisible({ timeout: TIMEOUT_ELEMENT_VISIBLE });
        });
    });
});
