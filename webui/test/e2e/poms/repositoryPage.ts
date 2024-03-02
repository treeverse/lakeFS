import {Locator, Page } from "@playwright/test";

export class RepositoryPage {
    private page: Page;

    public readOnlyIndicatorLocator: Locator;

    constructor(page: Page) {
        this.page = page;
        this.readOnlyIndicatorLocator = this.page.locator("text=Read-only");
    }

    async goto(repoName: string): Promise<void> {
        await this.page.goto(`/repositories/${repoName}`);
    }
}