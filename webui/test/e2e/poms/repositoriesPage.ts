import { Locator, Page, expect } from "@playwright/test";

const SAMPLE_REPO_README_TITLE = "Welcome to the Lake!";
const REGULAR_REPO_README_TITLE = "To get started with this repository:";

export class RepositoriesPage {
    private page: Page;

    public noRepositoriesTitleLocator: Locator;
    public readOnlyIndicatorLocator: Locator;
    public uploadButtonLocator: Locator;
    public createRepositoryButtonLocator: Locator;
    public searchInputLocator: Locator;

    constructor(page: Page) {
        this.page = page;
        this.noRepositoriesTitleLocator = this.page.getByText("Welcome to LakeFS!");
        this.readOnlyIndicatorLocator = this.page.locator("text=Read-only");
        this.uploadButtonLocator = this.page.locator("text=Upload Object");
        this.createRepositoryButtonLocator = this.page.getByRole("button", { name: "Create Repository" });
        this.searchInputLocator = this.page.getByPlaceholder("Find a repository...");
    }

    async goto(): Promise<void> {
        await this.page.goto("/repositories");
    }

    async goToRepository(repoName: string): Promise<void> {
        await this.page.getByRole("link", { name: repoName, exact: true }).click();
    }

    async createSampleRepository(): Promise<void> {
        await this.page.getByRole("button", { name: "Create Sample Repository" }).click();
        expect(this.page.getByRole("heading", { name: SAMPLE_REPO_README_TITLE })).toBeVisible();
    }

    async createRepository(repoName: string, includeSampleData: boolean): Promise<void> {
        await this.createRepositoryButtonLocator.click();
        await this.page.getByLabel("Repository ID").fill(repoName);
        if (includeSampleData) {
            await this.page.getByLabel("Add sample data, hooks, and configuration").check();
        }
        await this.page.getByRole("dialog").getByRole("button", { name: "Create Repository", exact: true }).click();
        if (includeSampleData) {
            await expect(this.page.getByRole("heading", { name: SAMPLE_REPO_README_TITLE })).toBeVisible();
            return;
        }
        expect(this.page.getByRole("heading", { name: REGULAR_REPO_README_TITLE })).toBeVisible();
    }
}
