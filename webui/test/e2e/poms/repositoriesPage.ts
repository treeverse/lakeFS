import { Locator, Page } from "@playwright/test";

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
        await this.page.click(`text=${repoName}`);
    }
}
