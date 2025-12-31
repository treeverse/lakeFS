import { test, expect } from '@playwright/test';
import { RepositoriesPage } from "../poms/repositoriesPage";
import { RepositoryPage } from "../poms/repositoryPage";
import fs from "fs";
import path from "path";

const TEST_REPO_NAME = "test-commit-merge";
const SOURCE_BRANCH = "feature-branch";
const DEST_BRANCH = "main";
const COMMIT_MESSAGE = "Test commit";
const MERGE_MESSAGE = "Test merge";

test.describe("Commit and Merge Operations", () => {
    test.describe.configure({ mode: "serial" });

    let setupComplete = false;

    test("Setup: Create repository and branches", async ({ page }) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await repositoriesPage.createRepository(TEST_REPO_NAME, true);

    });

    test("Commit: Upload file and commit changes", async ({ page }) => {
        expect(setupComplete).toBeTruthy();
        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.goto(TEST_REPO_NAME);
        await repositoryPage.createBranch(SOURCE_BRANCH);
        await repositoryPage.gotoObjectsTab();
        await repositoryPage.switchBranch(SOURCE_BRANCH);
        await expect(page.getByRole('button', { name: 'Upload' })).toBeVisible({ timeout: 10000 });

        const fileName = "commit-test.txt";
        const filePath = path.join(__dirname, fileName);
        fs.writeFileSync(filePath, "Testing commit functionality");

        await repositoryPage.uploadObject(filePath);
        await expect(page.getByText(fileName)).toBeVisible();
        await page.getByRole('button', { name: 'Upload 1 File' }).click();

        await expect(page.getByRole('cell', { name: fileName })).toBeVisible({ timeout: 10000 });

        await repositoryPage.showOnlyChanges();
        const count = await repositoryPage.getUncommittedCount();
        expect(count).toBeGreaterThan(0);

        await repositoryPage.commitChanges(COMMIT_MESSAGE);

        await expect(page.getByRole("dialog")).not.toBeVisible({ timeout: 120000 }); // 2 min timeout

        await repositoryPage.gotoObjectsTab();
        await expect(page.getByRole("button", { name: "Uncommitted Changes" })).not.toBeVisible();

        await expect(page.getByRole('cell', { name: fileName })).toBeVisible();

        // Cleanup
        fs.unlinkSync(filePath);
    });

    test("Merge: Merge feature branch into main", async ({ page }) => {
        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.goto(TEST_REPO_NAME);
        await repositoryPage.gotoCompareTab();

        // Set up comparison: feature-branch -> main
        await repositoryPage.selectComparedToBranch(SOURCE_BRANCH);

        // Wait for diff to load
        await expect(page.getByText("commit-test.txt")).toBeVisible({ timeout: 10000 });

        await repositoryPage.merge(MERGE_MESSAGE);

        await expect(page.getByRole("dialog")).not.toBeVisible({ timeout: 120000 }); // 2 min timeout

        await page.goto(`/repositories/${TEST_REPO_NAME}/objects?ref=${DEST_BRANCH}`);
        await expect(page.getByRole('cell', { name: "commit-test.txt" })).toBeVisible();
    });

    test("Verify commit appears in history", async ({ page }) => {
        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.goto(TEST_REPO_NAME);

        await page.getByRole("link", { name: "Commits" }).click();

        await expect(page.getByText(COMMIT_MESSAGE)).toBeVisible();
        await expect(page.getByText(MERGE_MESSAGE)).toBeVisible();
    });

    test("Commit: Handle empty commit attempt", async ({ page }) => {
        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.goto(TEST_REPO_NAME);
        await repositoryPage.gotoObjectsTab();
        await repositoryPage.switchBranch(SOURCE_BRANCH);

        await expect(page.getByRole("button", { name: "Uncommitted Changes" })).not.toBeVisible();
    });

    test("Merge: Handle no-diff merge attempt", async ({ page }) => {
        await page.goto(`/repositories/${TEST_REPO_NAME}/compare?ref=${DEST_BRANCH}&compare=${DEST_BRANCH}`);

        await expect(page.getByRole("button", { name: "Merge" })).toBeDisabled();
    });

    test("Merge: With merge strategy on conflict", async ({ page }) => {
        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.goto(TEST_REPO_NAME);

        // Create a conflict scenario: modify same file on both branches
        // First, create a file on a temp branch and merge it into main
        const tempBranch = "temp-conflict-branch";
        await repositoryPage.createBranch(tempBranch);
        await repositoryPage.gotoObjectsTab();
        await repositoryPage.switchBranch(tempBranch);

        const fileName = "conflict-test.txt";
        const filePath = path.join(__dirname, fileName);
        fs.writeFileSync(filePath, "Version on main branch");

        await repositoryPage.uploadObject(filePath);
        await expect(page.getByText(fileName)).toBeVisible();
        await page.getByRole('button', { name: 'Upload 1 File' }).click();
        await expect(page.getByRole('cell', { name: fileName })).toBeVisible({ timeout: 10000 });

        // Commit on temp branch
        await repositoryPage.showOnlyChanges();
        await repositoryPage.commitChanges("Add conflict file on temp");
        await expect(page.getByRole("dialog")).not.toBeVisible({ timeout: 120000 });

        // Merge temp branch into main
        await page.goto(`/repositories/${TEST_REPO_NAME}/compare?ref=${DEST_BRANCH}&compare=${tempBranch}`);
        await repositoryPage.merge("Merge conflict file to main");
        await expect(page.getByRole("dialog")).not.toBeVisible({ timeout: 120000 });

        // Now modify same file on feature branch
        await repositoryPage.gotoObjectsTab();
        await repositoryPage.switchBranch(SOURCE_BRANCH);

        fs.writeFileSync(filePath, "Version on feature branch");
        await repositoryPage.uploadObject(filePath);
        await expect(page.getByText(fileName)).toBeVisible();
        await page.getByRole('button', { name: 'Upload 1 File' }).click();
        await expect(page.getByRole('cell', { name: fileName })).toBeVisible({ timeout: 10000 });

        // Commit on feature branch
        await repositoryPage.showOnlyChanges();
        await repositoryPage.commitChanges("Add conflict file on feature");
        await expect(page.getByRole("dialog")).not.toBeVisible({ timeout: 120000 });

        // Navigate to Compare page with feature-branch -> main
        await page.goto(`/repositories/${TEST_REPO_NAME}/compare?ref=${DEST_BRANCH}&compare=${SOURCE_BRANCH}`);

        await page.getByRole("button", { name: "Merge" }).click();

        await page.getByLabel("Strategy").click();
        await page.getByRole("option", { name: "source-wins" }).click();

        await page.getByPlaceholder("Commit Message (Optional)").fill("Merge with source-wins strategy");

        await page
            .getByRole("dialog")
            .getByRole("button", { name: "Merge" })
            .click();

        await expect(page.getByRole("dialog")).not.toBeVisible({ timeout: 120000 });

        // Cleanup
        fs.unlinkSync(filePath);
    });
});
