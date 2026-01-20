import { test, expect } from '@playwright/test';
import { RepositoriesPage } from '../poms/repositoriesPage';
import { RepositoryPage } from '../poms/repositoryPage';

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

    let setupComplete = false;

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
        await expect(page.getByRole('button', { name: 'Upload', exact: true })).toBeVisible({ timeout: 10000 });

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

        await repositoryPage.uploadMultipleObjects(fileBuffers);

        await expect(page.getByText(FILE_1_NAME).first()).toBeVisible({ timeout: 10000 });
        await page.getByRole('button', { name: 'Upload 2 Files' }).click();

        await expect(page.getByRole('cell', { name: FILE_1_NAME }).first()).toBeVisible({ timeout: 10000 });

        await repositoryPage.showOnlyChanges();
        const count = await repositoryPage.getUncommittedCount();
        expect(count).toBe(2);

        // Set up network request listener to verify sync commit endpoint is called
        const commitRequestPromise = page.waitForRequest((request) => {
            return request.url().includes('/commits') && request.method() === 'POST';
        });

        await repositoryPage.commitChanges(COMMIT_MESSAGE);

        // Verify the commit used the sync endpoint: /branches/{branch}/commits
        const commitRequest = await commitRequestPromise;
        expect(commitRequest.url()).toContain(`/branches/${SOURCE_BRANCH}/commits`);

        await expect(page.getByRole('dialog')).not.toBeVisible({ timeout: 120000 }); // 2 min timeout

        await page.getByRole('link', { name: 'Commits' }).click();
        await expect(page.getByText(COMMIT_MESSAGE)).toBeVisible({ timeout: 10000 });

        // Verify we're on the correct branch's commits
        await expect(page.getByRole('button', { name: `branch: ${SOURCE_BRANCH}` })).toBeVisible();

        await repositoryPage.gotoObjectsTab();
        await expect(page.getByRole('button', { name: 'Uncommitted Changes' })).not.toBeVisible();

        await expect(page.getByRole('cell', { name: FILE_1_NAME }).first()).toBeVisible();
    });

    test('Merge: Merge feature branch into main', async ({ page }) => {
        expect(setupComplete).toBeTruthy();
        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.goto(TEST_REPO_NAME);
        await repositoryPage.gotoCompareTab();

        // Set up comparison: feature-branch -> main
        await repositoryPage.selectComparedToBranch(SOURCE_BRANCH);

        // Wait for diff to load
        await expect(page.getByText(FILE_1_NAME).first()).toBeVisible({ timeout: 10000 });

        // Set up network request listener to verify sync merge endpoint is called
        const mergeRequestPromise = page.waitForRequest((request) => {
            return request.url().includes('/merge/') && request.method() === 'POST';
        });

        await repositoryPage.merge(MERGE_MESSAGE);

        // Verify the merge used the sync endpoint: /refs/{sourceRef}/merge/{destinationBranch}
        const mergeRequest = await mergeRequestPromise;
        expect(mergeRequest.url()).toContain(`/refs/${SOURCE_BRANCH}/merge/${DEST_BRANCH}`);

        await expect(page.getByRole('dialog')).not.toBeVisible({ timeout: 120000 }); // 2 min timeout

        await page.getByRole('link', { name: 'Commits' }).click();
        await expect(page.getByText(MERGE_MESSAGE)).toBeVisible({ timeout: 10000 });

        // Verify we're viewing the destination branch (main) commits
        await expect(page.getByRole('button', { name: `branch: ${DEST_BRANCH}` })).toBeVisible();

        // Go to objects and verify the merged files are there
        await page.goto(`/repositories/${TEST_REPO_NAME}/objects?ref=${DEST_BRANCH}`);
        await expect(page.getByRole('cell', { name: FILE_1_NAME }).first()).toBeVisible();
    });

    test('Commit: Handle empty commit attempt', async ({ page }) => {
        expect(setupComplete).toBeTruthy();
        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.goto(TEST_REPO_NAME);
        await repositoryPage.gotoObjectsTab();
        await repositoryPage.switchBranch(SOURCE_BRANCH);

        await expect(page.getByRole('button', { name: 'Uncommitted Changes' })).not.toBeVisible();
    });

    test('Merge: Handle no-diff merge attempt', async ({ page }) => {
        expect(setupComplete).toBeTruthy();
        await page.goto(`/repositories/${TEST_REPO_NAME}/compare?ref=${DEST_BRANCH}&compare=${DEST_BRANCH}`);

        await expect(page.getByRole('button', { name: 'Merge' })).toBeDisabled();
    });

    test('Merge: With merge strategy on conflict', async ({ page }) => {
        expect(setupComplete).toBeTruthy();
        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.goto(TEST_REPO_NAME);

        // Create a conflict scenario: modify same file on both branches
        // First, create a file on a temp branch and merge it into main
        const tempBranch = 'temp-conflict-branch';
        await repositoryPage.createBranch(tempBranch);
        await repositoryPage.gotoObjectsTab();
        await repositoryPage.switchBranch(tempBranch);

        const fileBuffer = {
            name: CONFLICT_FILE_NAME,
            mimeType: 'text/plain',
            buffer: Buffer.from('Version on main branch')
        };

        await repositoryPage.uploadObject(fileBuffer);
        await expect(page.getByText(CONFLICT_FILE_NAME)).toBeVisible({ timeout: 10000 });
        await page.getByRole('button', { name: 'Upload 1 File' }).click();
        await expect(page.getByRole('cell', { name: CONFLICT_FILE_NAME })).toBeVisible({ timeout: 10000 });

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

        const featureFileBuffer = {
            name: CONFLICT_FILE_NAME,
            mimeType: 'text/plain',
            buffer: Buffer.from('Version on feature branch')
        };
        await repositoryPage.uploadObject(featureFileBuffer);
        await expect(page.getByText(CONFLICT_FILE_NAME)).toBeVisible({ timeout: 10000 });
        await page.getByRole('button', { name: 'Upload 1 File' }).click();
        await expect(page.getByRole('cell', { name: CONFLICT_FILE_NAME })).toBeVisible({ timeout: 10000 });

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
        await expect(page.getByRole('cell', { name: CONFLICT_FILE_NAME })).toBeVisible();

        await page.getByRole('link', { name: CONFLICT_FILE_NAME }).click();

        // Verify the content is from the source branch (feature-branch) due to source-wins strategy
        await expect(page.getByText('Version on feature branch')).toBeVisible({ timeout: 10000 });
    });
});
