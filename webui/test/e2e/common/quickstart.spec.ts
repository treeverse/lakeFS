import { test, expect } from "@playwright/test";
import { RepositoriesPage } from "../poms/repositoriesPage";
import { RepositoryPage } from "../poms/repositoryPage";
import { ObjectViewerPage } from "../poms/objectViewerPage";
import { PullsPage } from "../poms/pullsPage";

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
    test.describe.configure({mode: "serial"});
    test("create repo w/ sample data", async ({page}) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await repositoriesPage.createRepository(QUICKSTART_REPO_NAME, true);
    });

    test("view and query parquet object", async ({page}) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(QUICKSTART_REPO_NAME);

        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.clickObject(PARQUET_OBJECT_NAME);
        await expect(page.getByText("Loading...")).not.toBeVisible();

        const objectViewerPage = new ObjectViewerPage(page);
        await objectViewerPage.enterQuery(SELECT_QUERY);
        await objectViewerPage.clickExecuteButton();
        expect(await objectViewerPage.getResultRowCount()).toEqual(5);
    });

    test("transforming data", async ({page}) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(QUICKSTART_REPO_NAME);

        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.createBranch(NEW_BRANCH_NAME);

        await repositoryPage.gotoObjectsTab();
        await repositoryPage.clickObject(PARQUET_OBJECT_NAME);
        await expect(page.getByText("Loading...")).not.toBeVisible();

        const objectViewerPage = new ObjectViewerPage(page);
        await objectViewerPage.enterQuery(CREATE_TABLE_QUERY);
        await objectViewerPage.clickExecuteButton();

        await objectViewerPage.enterQuery(DELETE_QUERY);
        await objectViewerPage.clickExecuteButton();

        await objectViewerPage.enterQuery(COPY_QUERY);
        await objectViewerPage.clickExecuteButton();

        await objectViewerPage.enterQuery(SELECT_NEW_BRANCH);
        await objectViewerPage.clickExecuteButton();
        expect(await objectViewerPage.getResultRowCount()).toEqual(1);
    });

    test("commit and merge", async ({page}) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(QUICKSTART_REPO_NAME);

        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.gotoUncommittedChangeTab();
        await repositoryPage.switchBranch(NEW_BRANCH_NAME);
        await expect(page.getByText("Showing changes for branch")).toBeVisible();
        expect(await repositoryPage.getUncommittedCount()).toEqual(1);

        await repositoryPage.commitChanges("denmark");
        await expect(page.getByText("No changes")).toBeVisible();

        await repositoryPage.gotoCompareTab();
        await repositoryPage.switchBaseBranch("main");
        await expect(page.getByText("Showing changes between")).toBeVisible();
        expect(await repositoryPage.getUncommittedCount()).toEqual(1);
        await repositoryPage.merge("merge commit");
        await expect(page.getByText("No changes")).toBeVisible();

        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(QUICKSTART_REPO_NAME);
        await repositoryPage.clickObject(PARQUET_OBJECT_NAME);
        await expect(page.getByText("Loading...")).not.toBeVisible();
        const objectViewerPage = new ObjectViewerPage(page);
        await objectViewerPage.enterQuery(SELECT_QUERY);
        await objectViewerPage.clickExecuteButton();
        expect(await objectViewerPage.getResultRowCount()).toEqual(1);
    });

    test("pull requests", async ({page}) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(QUICKSTART_REPO_NAME);

        const branchNameForPull = "branch-for-pull-1";

        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.createBranch(branchNameForPull);

        // delete a file in the branch
        await repositoryPage.gotoObjectsTab();
        await repositoryPage.switchBranch(branchNameForPull);
        await repositoryPage.deleteFirstObjectInDirectory("images/");

        // commit the change
        await repositoryPage.gotoUncommittedChangeTab();
        expect(await repositoryPage.getUncommittedCount()).toEqual(1);
        await repositoryPage.commitChanges("Commit for pull-1");
        await expect(page.getByText("No changes")).toBeVisible();

        // pulls list sanity
        await repositoryPage.gotoPullRequestsTab();
        await expect(page.getByText("Create Pull Request")).toBeVisible();
        const pullsPage = new PullsPage(page);
        expect(await pullsPage.getPullsListCount()).toEqual(0);

        // create a pull request
        await pullsPage.clickCreatePullButton();
        await expect(page.getByRole("heading", {name: "Create Pull Request"})).toBeVisible();
        await pullsPage.switchCompareBranch(branchNameForPull);
        const pullDetails = {title: "PR for branch 1", description: "A description for PR 1"};
        await pullsPage.fillPullTitle(pullDetails.title);
        await pullsPage.fillPullDescription(pullDetails.description);
        await pullsPage.clickCreatePullButton();
        expect(await pullsPage.getBranchesCompareURI()).toEqual(`main...${branchNameForPull}/`);

        // merge the pull request
        await pullsPage.clickMergePullButton();
        await repositoryPage.gotoPullRequestsTab();
        await pullsPage.gotoPullsTab("closed");
        const firstPullRowDetails = await pullsPage.getFirstPullsRowDetails();
        expect(firstPullRowDetails.title).toEqual(pullDetails.title);
        expect(firstPullRowDetails.description).toMatch(/^Merged/)
    });
});
