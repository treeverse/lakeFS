import { test, expect } from "@playwright/test";
import { RepositoriesPage } from "../poms/repositoriesPage";
import { RepositoryPage } from "../poms/repositoryPage";
import { ObjectViewerPage } from "../poms/objectViewerPage";

const TEST_REPO_NAME = "revert-test-repo";
const TEST_BRANCH_NAME = "feature-branch";

test.describe("Revert Commit", () => {
    test.describe.configure({ mode: "serial" });

    test("setup: create repository with sample data", async ({ page }) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await repositoriesPage.createRepository(TEST_REPO_NAME, true);

        const repositoryPage = new RepositoryPage(page);
        const repoHeaderLink = repositoryPage.breadcrumbsLocator.getByRole("link", {
            name: TEST_REPO_NAME,
            exact: true,
        });
        await expect(repoHeaderLink).toBeVisible();
    });

    test("setup: create branch and make changes", async ({ page }) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(TEST_REPO_NAME);

        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.createBranch(TEST_BRANCH_NAME);

        // Make a change - delete a file
        await repositoryPage.gotoObjectsTab();
        await repositoryPage.switchBranch(TEST_BRANCH_NAME);
        await repositoryPage.deleteFirstObjectInDirectory("images/");

        // Commit the change
        await repositoryPage.showOnlyChanges();
        expect(await repositoryPage.getUncommittedCount()).toEqual(1);
        await repositoryPage.commitChanges("Delete image file");
        // Toggle should not be visible when there are no uncommitted changes
        await expect(page.locator('#show-uncommitted-toggle')).not.toBeVisible();
    });

    test("revert single commit", async ({ page }) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(TEST_REPO_NAME);

        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.gotoCommitsTab();
        await repositoryPage.switchBranch(TEST_BRANCH_NAME);

        // Wait for commits to load
        await page.waitForSelector(".list-group-item", { timeout: 10000 });

        // Get the initial commit count
        const initialCommitCount = await repositoryPage.getCommitsCount();
        expect(initialCommitCount).toBeGreaterThan(0);

        // Enter revert mode
        await repositoryPage.clickRevertButton();
        await expect(page.getByRole("button", { name: "Cancel" })).toBeVisible();

        // Select the first commit (most recent)
        await repositoryPage.selectCommitsForRevert(1);

        // Continue to preview page
        await expect(page.getByRole("button", { name: /Continue/ })).toBeVisible();
        await repositoryPage.clickContinueRevert();

        // Verify we're on the revert preview page
        await expect(page.getByRole("heading", { name: "Revert Commits" })).toBeVisible();
        await expect(page.getByText("Commits to Revert (in order)")).toBeVisible();

        // Fill in commit message
        await repositoryPage.fillRevertMessage("Revert: Delete image file");

        // Apply the revert
        await repositoryPage.clickApplyRevert();

        // Confirm in the modal
        await expect(page.getByText(/Are you sure you want to revert/)).toBeVisible();
        await repositoryPage.confirmRevert();

        // Wait for redirect back to commits page
        await page.waitForURL(/.*\/commits.*/);

        // Verify the new commit was created
        const newCommitCount = await repositoryPage.getCommitsCount();
        expect(newCommitCount).toBe(initialCommitCount + 1);

        // Verify the first commit message contains "Revert"
        const firstCommitMessage = await repositoryPage.getFirstCommitMessage();
        expect(firstCommitMessage).toContain("Revert");
    });

    test("revert multiple commits", async ({ page }) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(TEST_REPO_NAME);

        const repositoryPage = new RepositoryPage(page);
        // Navigate to objects tab first to establish a known starting point
        await repositoryPage.gotoObjectsTab();
        await repositoryPage.switchBranch(TEST_BRANCH_NAME);

        // Make two more changes and commit them
        await repositoryPage.deleteFirstObjectInDirectory("images/");
        await repositoryPage.showOnlyChanges();
        await repositoryPage.commitChanges("Second deletion");

        await repositoryPage.gotoObjectsTab();
        await repositoryPage.deleteFirstObjectInDirectory("images/");
        await repositoryPage.showOnlyChanges();
        await repositoryPage.commitChanges("Third deletion");

        // Go to commits tab
        await repositoryPage.gotoCommitsTab();

        // Wait for commits to load
        await page.waitForSelector(".list-group-item", { timeout: 10000 });

        const initialCommitCount = await repositoryPage.getCommitsCount();

        // Enter revert mode and select 2 commits
        await repositoryPage.clickRevertButton();
        await repositoryPage.selectCommitsForRevert(2);

        // Continue to preview page
        await repositoryPage.clickContinueRevert();

        // Verify we're on the revert preview page
        await expect(page.getByRole("heading", { name: "Revert Commits" })).toBeVisible();

        // Verify we're reverting 2 commits - check in the commits list header
        await expect(page.getByText("Commits to Revert (in order)")).toBeVisible();

        // For multiple commits, commit message field is hidden - each commit gets default message
        // Verify the commit message field is NOT visible
        await expect(page.getByPlaceholder(/Describe the revert|Revert commit/)).not.toBeVisible();

        // Apply the revert
        await repositoryPage.clickApplyRevert();

        // Confirm in the modal - check for specific text in modal
        await expect(page.getByText(/Are you sure you want to revert/)).toBeVisible();
        await repositoryPage.confirmRevert();

        // Wait for redirect
        await page.waitForURL(/.*\/commits.*/);

        // Verify 2 new revert commits were created
        const newCommitCount = await repositoryPage.getCommitsCount();
        expect(newCommitCount).toBe(initialCommitCount + 2);
    });

    test("cancel revert operation", async ({ page }) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(TEST_REPO_NAME);

        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.gotoCommitsTab();
        await repositoryPage.switchBranch(TEST_BRANCH_NAME);

        // Wait for commits to load
        await page.waitForSelector(".list-group-item", { timeout: 10000 });

        const initialCommitCount = await repositoryPage.getCommitsCount();

        // Enter revert mode
        await repositoryPage.clickRevertButton();
        await repositoryPage.selectCommitsForRevert(1);
        await repositoryPage.clickContinueRevert();

        // Cancel on the preview page
        await expect(page.getByRole("heading", { name: "Revert Commits" })).toBeVisible();
        await repositoryPage.cancelRevert();

        // Verify we're back on commits page
        await page.waitForURL(/.*\/commits.*/);

        // Verify no new commits were created
        const finalCommitCount = await repositoryPage.getCommitsCount();
        expect(finalCommitCount).toBe(initialCommitCount);
    });

    test("test allow empty commit option", async ({ page }) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(TEST_REPO_NAME);

        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.gotoCommitsTab();
        await repositoryPage.switchBranch(TEST_BRANCH_NAME);

        // Enter revert mode and select a commit
        await repositoryPage.clickRevertButton();
        await repositoryPage.selectCommitsForRevert(1);
        await repositoryPage.clickContinueRevert();

        // Verify the allow empty commit checkbox is present
        await expect(page.getByLabel(/Allow empty commit/)).toBeVisible();

        // Check the allow empty commit checkbox
        await repositoryPage.setAllowEmptyCommit(true);

        // Verify it's checked
        await expect(page.getByLabel(/Allow empty commit/)).toBeChecked();

        // Cancel without applying
        await repositoryPage.cancelRevert();
    });

    test("revert mode toggle", async ({ page }) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(TEST_REPO_NAME);

        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.gotoCommitsTab();
        await repositoryPage.switchBranch(TEST_BRANCH_NAME);

        // Enter revert mode
        await repositoryPage.clickRevertButton();

        // Verify Cancel button appears
        await expect(page.getByRole("button", { name: "Cancel" })).toBeVisible();

        // Verify checkboxes are visible
        const checkboxes = page.locator('input[type="checkbox"]');
        expect(await checkboxes.count()).toBeGreaterThan(0);

        // Click Cancel to exit revert mode
        await repositoryPage.clickRevertButton(); // Now it's "Cancel"

        // Verify Revert button is back
        await expect(page.getByRole("button", { name: "Revert" })).toBeVisible();
        await expect(page.getByRole("button", { name: "Cancel" })).not.toBeVisible();
    });

    test("revert with metadata fields", async ({ page }) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(TEST_REPO_NAME);

        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.gotoCommitsTab();
        await repositoryPage.switchBranch(TEST_BRANCH_NAME);

        // Enter revert mode and select a commit
        await repositoryPage.clickRevertButton();
        await repositoryPage.selectCommitsForRevert(1);
        await repositoryPage.clickContinueRevert();

        // Verify we're on the revert preview page
        await expect(page.getByRole("heading", { name: "Revert Commits" })).toBeVisible();

        // Fill in commit message
        await repositoryPage.fillRevertMessage("Revert with metadata");

        // Add metadata fields
        await repositoryPage.addRevertMetadata("environment", "production");
        await repositoryPage.addRevertMetadata("ticket", "ISSUE-123");

        // Verify metadata fields are present
        await expect(page.getByPlaceholder("Key").nth(0)).toHaveValue("environment");
        await expect(page.getByPlaceholder("Value").nth(0)).toHaveValue("production");
        await expect(page.getByPlaceholder("Key").nth(1)).toHaveValue("ticket");
        await expect(page.getByPlaceholder("Value").nth(1)).toHaveValue("ISSUE-123");

        // Apply the revert
        await repositoryPage.clickApplyRevert();
        await repositoryPage.confirmRevert();

        // Wait for redirect
        await page.waitForURL(/.*\/commits.*/);

        // Verify successful completion
        await expect(page.getByRole("button", { name: "Revert" })).toBeVisible();
    });

    test("metadata validation - empty key error", async ({ page }) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(TEST_REPO_NAME);

        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.gotoCommitsTab();
        await repositoryPage.switchBranch(TEST_BRANCH_NAME);

        // Enter revert mode and select a commit
        await repositoryPage.clickRevertButton();
        await repositoryPage.selectCommitsForRevert(1);
        await repositoryPage.clickContinueRevert();

        // Fill in commit message
        await repositoryPage.fillRevertMessage("Test validation");

        // Add metadata field with empty key
        await page.getByRole("button", { name: /Add Metadata field/ }).click();
        await page.getByPlaceholder("Value").last().fill("some value");

        // Try to apply without filling key
        await repositoryPage.clickApplyRevert();

        // The modal should not open because validation failed
        // Verify we're still on the revert page (not showing confirmation modal)
        await expect(page.getByRole("heading", { name: "Revert Commits" })).toBeVisible();

        // The error should be shown after blur
        await page.getByPlaceholder("Key").last().click();
        await page.getByPlaceholder("Value").last().click(); // Blur the key field
        await expect(page.getByText("Key is required")).toBeVisible();
    });
});
