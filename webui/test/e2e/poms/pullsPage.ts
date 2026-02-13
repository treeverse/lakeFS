import { Page } from "@playwright/test";
import { TIMEOUT_NAVIGATION } from "../timeouts";

export class PullsPage {
    constructor(private page: Page) {}

    async getPullsListCount(): Promise<number> {
        const pullsList = this.page.locator("div.pulls-list");
        try {
            await pullsList.waitFor({ state: "visible", timeout: TIMEOUT_NAVIGATION });
        } catch {
            return 0;
        }
        return pullsList.locator("div.pull-row").count();
    }

    async switchCompareBranch(name: string): Promise<void> {
        await this.page.getByRole("button", {name: "to branch: "}).click();
        await this.page.getByRole("button", {name}).click();
    }

    async clickCreatePullButton(): Promise<void> {
        await this.page.getByRole("button", {name: "Create Pull Request"}).click();
    }

    async getBranchesCompareURI(): Promise<string> {
        return this.page.locator("div.lakefs-uri").innerText();
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

    async gotoPullsTab(name: string): Promise<void> {
        await this.page.getByRole("tab", { name, exact: false }).click();
    }

    async getFirstPullsRowDetails(): Promise<{title: string, description: string}> {
        const firstPullRow = this.page.locator("div.pull-row").first();
        const title = await firstPullRow.locator(".pull-title").innerText();
        const description = await firstPullRow.locator(".pull-description").innerText();
        return {title, description};
    }
}
