import { Locator, Page } from "@playwright/test";

export class PullsPage {
    private page: Page;

    constructor(page: Page) {
        this.page = page;
    }

    async getPullsListCount(): Promise<number> {
        await this.page.locator("div.pulls-list").isVisible();
        return this.page
            .locator("div.pulls-list")
            .locator("pull-row")
            .count();
    }

    async switchCompareBranch(name: string): Promise<void> {
        await this.page.getByRole("button", {name: "to branch: "}).click();
        await this.page.getByRole("button", {name}).click();
    }

    async clickCreatePullButton(): Promise<void> {
        await this.page.getByRole("button", {name: "Create Pull Request"}).click();
    }

    async getBranchesCompareURI(): Promise<string> {
        // The new UI shows branch info as "admin wants to merge [source] into [dest]"
        // Expected format is "dest...source/"
        // Find the text that contains "wants to merge X into Y"
        const mergeInfoText = this.page.locator('text=wants to merge');
        await mergeInfoText.waitFor({ state: 'visible', timeout: 5000 });

        // Get the parent element and extract branch names from links
        const parent = mergeInfoText.locator('..');
        const links = parent.locator('a');

        // First link is source branch, second is destination
        const source = await links.nth(0).textContent() || '';
        const dest = await links.nth(1).textContent() || '';

        return `${dest}...${source}/`;
    }

    async clickMergePullButton(): Promise<void> {
        await this.page.getByRole("button", {name: "Merge pull request"}).click();
    }

    async fillPullTitle(title: string): Promise<void> {
        await this.page.getByPlaceholder("Add a title...").fill(title);
    }

    async fillPullDescription(description: string): Promise<void> {
        await this.page.getByPlaceholder("Describe your changes...").fill(description);
    }

    async gotoPullsTab(id: string): Promise<void> {
        await this.page.locator(`#pulls-tabs-tab-${id}`).click();
    }

    async getFirstPullsRowDetails(): Promise<{title: string, description: string}> {
        const firstPullRow = this.page.locator("div.pull-row").first();
        const title = await firstPullRow.locator(".pull-title").innerText();
        const description = await firstPullRow.locator(".pull-description").innerText();
        return {title, description};
    }
}
