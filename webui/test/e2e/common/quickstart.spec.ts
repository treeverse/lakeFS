import { test, expect } from "../fixtures";
import { TIMEOUT_ELEMENT_VISIBLE } from "../timeouts";

const QUICKSTART_REPO_NAME = "quickstart";
const PARQUET_OBJECT_NAME = "lakes.parquet";
const NEW_BRANCH_NAME = "denmark-lakes";

const SELECT_QUERY =
    "SELECT country, COUNT(*) FROM READ_PARQUET('lakefs://quickstart/main/lakes.parquet') GROUP BY country ORDER BY COUNT(*) DESC LIMIT 5;";
const CREATE_TABLE_QUERY =
    "CREATE OR REPLACE TABLE lakes AS SELECT * FROM READ_PARQUET('lakefs://quickstart/denmark-lakes/lakes.parquet');";
const DELETE_QUERY = "DELETE FROM lakes WHERE Country != 'Denmark';";
const COPY_QUERY =
    "COPY lakes TO 'lakefs://quickstart/denmark-lakes/lakes.parquet';";
const SELECT_NEW_BRANCH =
    "DROP TABLE lakes; SELECT country, COUNT(*) FROM READ_PARQUET('lakefs://quickstart/denmark-lakes/lakes.parquet') GROUP BY country ORDER BY COUNT(*) DESC LIMIT 5;";

test.describe("Quickstart", () => {
    test.describe.configure({ mode: "serial" });

    test("create repo w/ sample data", async ({ repositoriesPage, repositoryPage }) => {
        await repositoriesPage.goto();
        await repositoriesPage.createRepository(QUICKSTART_REPO_NAME, true);
        const repoHeaderLink = repositoryPage.breadcrumbsLocator.getByRole("link", { name: QUICKSTART_REPO_NAME, exact: true });
        await expect(repoHeaderLink).toBeVisible();
    });

    test("view and query parquet object", async ({ page, repositoriesPage, repositoryPage, objectViewerPage }) => {
        await test.step("navigate to parquet object", async () => {
            await repositoriesPage.goto();
            await repositoriesPage.goToRepository(QUICKSTART_REPO_NAME);
            await repositoryPage.objects.clickObject(PARQUET_OBJECT_NAME);
            await expect(page.getByRole("button", { name: "Execute" })).toBeVisible();
        });

        await test.step("run select query", async () => {
            await objectViewerPage.enterQuery(SELECT_QUERY);
            await objectViewerPage.clickExecuteButton();
            await expect.poll(() => objectViewerPage.getResultRowCount()).toEqual(5);
        });
    });

    test("transforming data", async ({ page, repositoriesPage, repositoryPage, objectViewerPage }) => {
        await test.step("create branch and navigate to parquet", async () => {
            await repositoriesPage.goto();
            await repositoriesPage.goToRepository(QUICKSTART_REPO_NAME);
            await repositoryPage.branches.createBranch(NEW_BRANCH_NAME);
            await repositoryPage.gotoObjectsTab();
            await repositoryPage.objects.clickObject(PARQUET_OBJECT_NAME);
            await expect(page.getByRole("button", { name: "Execute" })).toBeVisible();
        });

        await test.step("run DuckDB transformation queries", async () => {
            await objectViewerPage.enterQuery(CREATE_TABLE_QUERY);
            await objectViewerPage.clickExecuteButton();

            await objectViewerPage.enterQuery(DELETE_QUERY);
            await objectViewerPage.clickExecuteButton();

            await objectViewerPage.enterQuery(COPY_QUERY);
            await objectViewerPage.clickExecuteButton();
        });

        await test.step("verify transformation result", async () => {
            await objectViewerPage.enterQuery(SELECT_NEW_BRANCH);
            await objectViewerPage.clickExecuteButton();
            await expect.poll(() => objectViewerPage.getResultRowCount()).toEqual(1);
        });
    });

    // DuckDB COPY writes to lakeFS via the S3 gateway, which may not be available in all setups.
    // If the S3 gateway is not configured, the COPY silently fails and no uncommitted changes are created.
    test("commit and merge", async ({ page, repositoriesPage, repositoryPage, objectViewerPage }) => {
        await test.step("commit changes on branch", async () => {
            await repositoriesPage.goto();
            await repositoriesPage.goToRepository(QUICKSTART_REPO_NAME);
            await repositoryPage.gotoObjectsTab();
            await repositoryPage.branches.switchBranch(NEW_BRANCH_NAME);
            // DuckDB COPY in the previous test writes to lakeFS asynchronously.
            // Reload to ensure the page fetches the latest uncommitted changes state.
            await page.reload();
            await expect(page.getByRole("button", { name: "Uncommitted Changes" })).toBeVisible({ timeout: TIMEOUT_ELEMENT_VISIBLE });
            await repositoryPage.changes.showOnlyChanges();
            await expect(page.getByText("Showing 1 change for branch")).toBeVisible();
            expect(await repositoryPage.changes.getUncommittedCount()).toEqual(1);
            await repositoryPage.changes.commitChanges("denmark");
            await expect(page.getByRole("button", { name: "Uncommitted Changes" })).toHaveCount(0);
        });

        await test.step("merge branch into main", async () => {
            await repositoryPage.gotoCompareTab();
            await repositoryPage.branches.switchBaseBranch("main");
            await expect(page.getByText("Showing changes between")).toBeVisible();
            expect(await repositoryPage.changes.getUncommittedCount()).toEqual(1);
            await repositoryPage.changes.merge("merge commit");
            await expect(page.getByText("No changes")).toBeVisible();
        });

        await test.step("verify merged data on main", async () => {
            await repositoriesPage.goto();
            await repositoriesPage.goToRepository(QUICKSTART_REPO_NAME);
            await repositoryPage.objects.clickObject(PARQUET_OBJECT_NAME);
            await expect(page.getByRole("button", { name: "Execute" })).toBeVisible();
            await objectViewerPage.enterQuery(SELECT_QUERY);
            await objectViewerPage.clickExecuteButton();
            await expect.poll(() => objectViewerPage.getResultRowCount()).toEqual(1);
        });
    });

    test("pull requests", async ({ page, repositoriesPage, repositoryPage, pullsPage }) => {
        const branchNameForPull = "branch-for-pull-1";

        await test.step("create branch and make changes", async () => {
            await repositoriesPage.goto();
            await repositoriesPage.goToRepository(QUICKSTART_REPO_NAME);
            await repositoryPage.branches.createBranch(branchNameForPull);
            await repositoryPage.gotoObjectsTab();
            await repositoryPage.branches.switchBranch(branchNameForPull);
            await repositoryPage.objects.deleteFirstObjectInDirectory("images/");
        });

        await test.step("commit changes", async () => {
            await repositoryPage.gotoObjectsTab();
            await repositoryPage.changes.showOnlyChanges();
            expect(await repositoryPage.changes.getUncommittedCount()).toEqual(1);
            await repositoryPage.changes.commitChanges("Commit for pull-1");
            await expect(page.getByRole("button", { name: "Uncommitted Changes" })).toHaveCount(0);
        });

        await test.step("verify empty pull requests list", async () => {
            await repositoryPage.gotoPullRequestsTab();
            await expect(page.getByText("Create Pull Request")).toBeVisible();
            expect(await pullsPage.getPullsListCount()).toEqual(0);
        });

        const pullDetails = { title: "PR for branch 1", description: "A description for PR 1" };

        await test.step("create pull request", async () => {
            await pullsPage.clickCreatePullButton();
            await expect(page.getByRole("heading", { name: "Create Pull Request" })).toBeVisible();
            await pullsPage.switchCompareBranch(branchNameForPull);
            await pullsPage.fillPullTitle(pullDetails.title);
            await pullsPage.fillPullDescription(pullDetails.description);
            await pullsPage.clickCreatePullButton();
            expect(await pullsPage.getBranchesCompareURI()).toEqual(`main...${branchNameForPull}/`);
        });

        await test.step("merge pull request and verify", async () => {
            await pullsPage.clickMergePullButton();
            await repositoryPage.gotoPullRequestsTab();
            await pullsPage.gotoPullsTab("closed");
            const firstPullRowDetails = await pullsPage.getFirstPullsRowDetails();
            expect(firstPullRowDetails.title).toEqual(pullDetails.title);
            expect(firstPullRowDetails.description).toMatch(/^Merged/);
        });
    });

    test("repository settings", async ({ page, repositoryPage }) => {
        await repositoryPage.goto(QUICKSTART_REPO_NAME);
        await repositoryPage.gotoSettingsTab();

        await expect(page.getByRole("heading", { name: "General" })).toBeVisible();
        const container = page.locator('.container');

        async function validateRow(elementText: string, inputValue: string | undefined = undefined) {
            const rowText = container.getByText(elementText, { exact: true });
            await expect(rowText).toBeVisible();
            const valueInput = rowText.locator('..').getByRole("textbox");
            await expect(valueInput).toBeVisible();
            if (inputValue) {
                await expect(valueInput).toHaveValue(inputValue);
            }
        }

        await validateRow("Repository name", QUICKSTART_REPO_NAME);
        await validateRow("Storage namespace", "local://" + QUICKSTART_REPO_NAME);
        await validateRow("Default branch", "main");
    });
});
