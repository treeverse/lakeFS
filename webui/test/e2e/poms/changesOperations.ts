import { expect, Page } from "@playwright/test";

export class ChangesOperations {
    constructor(private page: Page) {}

    async showOnlyChanges(): Promise<void> {
        await this.page.getByRole("button", { name: "Uncommitted Changes" }).click();
    }

    async getUncommittedCount(): Promise<number> {
        await expect(this.page.locator(".tree-container div.card")).toBeVisible();
        return this.page
            .locator("table.table")
            .locator("tbody")
            .locator("tr")
            .count();
    }

    async commitChanges(commitMsg: string): Promise<void> {
        await this.page.locator('button[id="changes-dropdown"]').click();
        await this.page.getByRole("button", { name: "Commit Changes" }).click();
        if (commitMsg?.length) {
            await this.page.getByPlaceholder("Commit Message").fill(commitMsg);
        }
        await this.page
            .getByRole("dialog")
            .getByRole("button", { name: "Commit Changes" })
            .click();
    }

    async merge(commitMsg: string): Promise<void> {
        await this.page.getByRole("button", { name: "Merge" }).click();
        if (commitMsg?.length) {
            await this.page
                .getByPlaceholder("Commit Message (Optional)")
                .fill(commitMsg);
        }
        await this.page
            .getByRole("dialog")
            .getByRole("button", { name: "Merge" })
            .click();
    }
}
