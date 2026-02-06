import { test, expect } from "../fixtures";
import { TIMEOUT_ELEMENT_VISIBLE } from "../timeouts";

const TEST_REPO_NAME = "revert-test-repo";
const TEST_BRANCH_NAME = "feature-branch";

test.describe("Revert Commit", () => {
    test.describe.configure({ mode: "serial" });

    test("setup: create repository and branch with changes", async ({ page, repositoriesPage, repositoryPage }) => {
        await test.step("create repository with sample data", async () => {
            await repositoriesPage.goto();
            await repositoriesPage.createRepository(TEST_REPO_NAME, true);
            const repoHeaderLink = repositoryPage.breadcrumbsLocator.getByRole("link", {
                name: TEST_REPO_NAME,
                exact: true,
            });
            await expect(repoHeaderLink).toBeVisible();
        });

        await test.step("create branch and delete a file", async () => {
            await repositoryPage.branches.createBranch(TEST_BRANCH_NAME);
            await repositoryPage.gotoObjectsTab();
            await repositoryPage.branches.switchBranch(TEST_BRANCH_NAME);
            await repositoryPage.objects.deleteFirstObjectInDirectory("images/");
        });

        await test.step("commit the change", async () => {
            await repositoryPage.changes.showOnlyChanges();
            expect(await repositoryPage.changes.getUncommittedCount()).toEqual(1);
            await repositoryPage.changes.commitChanges("Delete image file");
            await expect(page.getByRole("button", { name: "Uncommitted Changes" })).toHaveCount(0);
        });
    });

    test("revert single commit", async ({ page, repositoriesPage, repositoryPage }) => {
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(TEST_REPO_NAME);
        await repositoryPage.gotoCommitsTab();
        await repositoryPage.branches.switchBranch(TEST_BRANCH_NAME);

        await page.waitForSelector(".list-group-item", { timeout: TIMEOUT_ELEMENT_VISIBLE });

        const initialCommitCount = await test.step("get initial commit count", async () => {
            const count = await repositoryPage.commits.getCommitsCount();
            expect(count).toBeGreaterThan(0);
            return count;
        });

        await test.step("enter revert mode and select commit", async () => {
            await repositoryPage.revert.clickRevertButton();
            await expect(page.getByRole("button", { name: "Cancel" })).toBeVisible();
            await repositoryPage.revert.selectCommitsForRevert(1);
            await expect(page.getByRole("button", { name: /Continue/ })).toBeVisible();
            await repositoryPage.revert.clickContinueRevert();
        });

        await test.step("fill revert message and apply", async () => {
            await expect(page.getByRole("heading", { name: "Revert Commits" })).toBeVisible();
            await expect(page.getByText("Commits to Revert (in order)")).toBeVisible();
            await repositoryPage.revert.fillRevertMessage("Revert: Delete image file");
            await repositoryPage.revert.clickApplyRevert();
            await expect(page.getByText(/Are you sure you want to revert/)).toBeVisible();
            await repositoryPage.revert.confirmRevert();
        });

        await test.step("verify revert commit was created", async () => {
            await page.waitForURL(/.*\/commits.*/);
            const newCommitCount = await repositoryPage.commits.getCommitsCount();
            expect(newCommitCount).toBe(initialCommitCount + 1);
            const firstCommitMessage = await repositoryPage.commits.getFirstCommitMessage();
            expect(firstCommitMessage).toContain("Revert");
        });
    });

    test("revert multiple commits", async ({ page, repositoriesPage, repositoryPage }) => {
        await test.step("make two more changes", async () => {
            await repositoriesPage.goto();
            await repositoriesPage.goToRepository(TEST_REPO_NAME);
            await repositoryPage.gotoObjectsTab();
            await repositoryPage.branches.switchBranch(TEST_BRANCH_NAME);

            await repositoryPage.objects.deleteFirstObjectInDirectory("images/");
            await repositoryPage.changes.showOnlyChanges();
            await repositoryPage.changes.commitChanges("Second deletion");

            await repositoryPage.gotoObjectsTab();
            await repositoryPage.objects.deleteFirstObjectInDirectory("images/");
            await repositoryPage.changes.showOnlyChanges();
            await repositoryPage.changes.commitChanges("Third deletion");
        });

        await repositoryPage.gotoCommitsTab();
        await page.waitForSelector(".list-group-item", { timeout: TIMEOUT_ELEMENT_VISIBLE });
        const initialCommitCount = await repositoryPage.commits.getCommitsCount();

        await test.step("select and revert 2 commits", async () => {
            await repositoryPage.revert.clickRevertButton();
            await repositoryPage.revert.selectCommitsForRevert(2);
            await repositoryPage.revert.clickContinueRevert();

            await expect(page.getByRole("heading", { name: "Revert Commits" })).toBeVisible();
            await expect(page.getByText("Commits to Revert (in order)")).toBeVisible();
            await expect(page.getByPlaceholder(/Describe the revert|Revert commit/)).not.toBeVisible();

            await repositoryPage.revert.clickApplyRevert();
            await expect(page.getByText(/Are you sure you want to revert/)).toBeVisible();
            await repositoryPage.revert.confirmRevert();
        });

        await test.step("verify 2 revert commits were created", async () => {
            await page.waitForURL(/.*\/commits.*/);
            const newCommitCount = await repositoryPage.commits.getCommitsCount();
            expect(newCommitCount).toBe(initialCommitCount + 2);
        });
    });

    test("cancel revert operation", async ({ page, repositoriesPage, repositoryPage }) => {
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(TEST_REPO_NAME);
        await repositoryPage.gotoCommitsTab();
        await repositoryPage.branches.switchBranch(TEST_BRANCH_NAME);

        await page.waitForSelector(".list-group-item", { timeout: TIMEOUT_ELEMENT_VISIBLE });
        const initialCommitCount = await repositoryPage.commits.getCommitsCount();

        await test.step("enter revert mode and cancel", async () => {
            await repositoryPage.revert.clickRevertButton();
            await repositoryPage.revert.selectCommitsForRevert(1);
            await repositoryPage.revert.clickContinueRevert();
            await expect(page.getByRole("heading", { name: "Revert Commits" })).toBeVisible();
            await repositoryPage.revert.cancelRevert();
        });

        await test.step("verify no new commits", async () => {
            await page.waitForURL(/.*\/commits.*/);
            const finalCommitCount = await repositoryPage.commits.getCommitsCount();
            expect(finalCommitCount).toBe(initialCommitCount);
        });
    });

    test("test allow empty commit option", async ({ page, repositoriesPage, repositoryPage }) => {
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(TEST_REPO_NAME);
        await repositoryPage.gotoCommitsTab();
        await repositoryPage.branches.switchBranch(TEST_BRANCH_NAME);

        await repositoryPage.revert.clickRevertButton();
        await repositoryPage.revert.selectCommitsForRevert(1);
        await repositoryPage.revert.clickContinueRevert();

        await expect(page.getByLabel(/Allow empty commit/)).toBeVisible();
        await repositoryPage.revert.setAllowEmptyCommit(true);
        await expect(page.getByLabel(/Allow empty commit/)).toBeChecked();

        await repositoryPage.revert.cancelRevert();
    });

    test("revert mode toggle", async ({ page, repositoriesPage, repositoryPage }) => {
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(TEST_REPO_NAME);
        await repositoryPage.gotoCommitsTab();
        await repositoryPage.branches.switchBranch(TEST_BRANCH_NAME);

        await test.step("enter revert mode", async () => {
            await repositoryPage.revert.clickRevertButton();
            await expect(page.getByRole("button", { name: "Cancel" })).toBeVisible();
            const checkboxes = page.locator('input[type="checkbox"]');
            expect(await checkboxes.count()).toBeGreaterThan(0);
        });

        await test.step("exit revert mode", async () => {
            await repositoryPage.revert.clickRevertButton();
            await expect(page.getByRole("button", { name: "Revert" })).toBeVisible();
            await expect(page.getByRole("button", { name: "Cancel" })).not.toBeVisible();
        });
    });

    test("revert with metadata fields", async ({ page, repositoriesPage, repositoryPage }) => {
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(TEST_REPO_NAME);
        await repositoryPage.gotoCommitsTab();
        await repositoryPage.branches.switchBranch(TEST_BRANCH_NAME);

        await repositoryPage.revert.clickRevertButton();
        await repositoryPage.revert.selectCommitsForRevert(1);
        await repositoryPage.revert.clickContinueRevert();

        await test.step("fill message and add metadata", async () => {
            await expect(page.getByRole("heading", { name: "Revert Commits" })).toBeVisible();
            await repositoryPage.revert.fillRevertMessage("Revert with metadata");
            await repositoryPage.revert.addRevertMetadata("environment", "production");
            await repositoryPage.revert.addRevertMetadata("ticket", "ISSUE-123");

            await expect(page.getByPlaceholder("Key").nth(0)).toHaveValue("environment");
            await expect(page.getByPlaceholder("Value").nth(0)).toHaveValue("production");
            await expect(page.getByPlaceholder("Key").nth(1)).toHaveValue("ticket");
            await expect(page.getByPlaceholder("Value").nth(1)).toHaveValue("ISSUE-123");
        });

        await test.step("apply and verify", async () => {
            await repositoryPage.revert.clickApplyRevert();
            await repositoryPage.revert.confirmRevert();
            await page.waitForURL(/.*\/commits.*/);
            await expect(page.getByRole("button", { name: "Revert" })).toBeVisible();
        });
    });

    test("metadata validation - empty key error", async ({ page, repositoriesPage, repositoryPage }) => {
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(TEST_REPO_NAME);
        await repositoryPage.gotoCommitsTab();
        await repositoryPage.branches.switchBranch(TEST_BRANCH_NAME);

        await repositoryPage.revert.clickRevertButton();
        await repositoryPage.revert.selectCommitsForRevert(1);
        await repositoryPage.revert.clickContinueRevert();

        await repositoryPage.revert.fillRevertMessage("Test validation");

        await test.step("add empty key and verify validation error", async () => {
            await page.getByRole("button", { name: /Add Metadata field/ }).click();
            await page.getByPlaceholder("Value").last().fill("some value");
            await repositoryPage.revert.clickApplyRevert();

            await expect(page.getByRole("heading", { name: "Revert Commits" })).toBeVisible();

            await page.getByPlaceholder("Key").last().click();
            await page.getByPlaceholder("Value").last().click();
            await expect(page.getByText("Key is required")).toBeVisible();
        });
    });
});
