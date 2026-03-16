import { Locator, Page } from "@playwright/test";
import { BranchOperations } from "../operations/branchOperations";
import { ObjectOperations } from "../operations/objectOperations";
import { ChangesOperations } from "../operations/changesOperations";
import { RevertOperations } from "../operations/revertOperations";
import { CommitsOperations } from "../operations/commitsOperations";

export class RepositoryPage {
    public readOnlyIndicatorLocator: Locator;
    public breadcrumbsLocator: Locator;

    public branches: BranchOperations;
    public objects: ObjectOperations;
    public changes: ChangesOperations;
    public revert: RevertOperations;
    public commits: CommitsOperations;

    constructor(private page: Page) {
        this.readOnlyIndicatorLocator = this.page.locator("text=Read-only");
        this.breadcrumbsLocator = this.page.locator("ol.breadcrumb");

        this.branches = new BranchOperations(page);
        this.objects = new ObjectOperations(page);
        this.changes = new ChangesOperations(page);
        this.revert = new RevertOperations(page);
        this.commits = new CommitsOperations(page);
    }

    async goto(repoName: string): Promise<void> {
        await this.page.goto(`/repositories/${repoName}`);
    }

    async gotoObjectsTab(): Promise<void> {
        await this.page.getByRole("link", { name: "Objects", exact: true }).first().click();
    }

    async gotoCompareTab(): Promise<void> {
        await this.page.getByRole("link", { name: "Compare" }).click();
    }

    async gotoPullRequestsTab(): Promise<void> {
        await this.page.getByRole("link", { name: "Pull Requests" }).click();
    }

    async gotoSettingsTab(): Promise<void> {
        await this.page.getByRole("link", { name: "Settings" }).click();
    }

    async gotoCommitsTab(): Promise<void> {
        await this.page.getByRole("link", { name: "Commits" }).click();
    }
}
