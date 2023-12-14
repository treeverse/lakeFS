import { Locator, Page } from "@playwright/test";

export class RepositoriesPage {
    private page: Page;

    public noRepositoriesTitleLocator: Locator;

    constructor(page: Page) {
        this.page = page;
        this.noRepositoriesTitleLocator = this.page.getByText("Welcome to LakeFS!");
    }

    async goto(): Promise<void> {
        await this.page.goto("/repositories");
    }
}
