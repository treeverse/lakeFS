import { Page } from "@playwright/test";

export class CommitsOperations {
    constructor(private page: Page) {}

    async getCommitsCount(): Promise<number> {
        await this.page.locator(".card .list-group").waitFor({ state: "visible" });
        return this.page.locator(".list-group-item .clearfix").count();
    }

    async getFirstCommitMessage(): Promise<string> {
        await this.page.locator(".list-group-item").first().waitFor({ state: "visible" });
        const firstCommit = this.page.locator(".list-group-item").first();
        const message = await firstCommit.locator("h6 a").textContent();
        return message?.trim() || "";
    }
}
