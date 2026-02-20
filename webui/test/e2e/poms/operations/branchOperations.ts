import { Page } from "@playwright/test";
import { TIMEOUT_NAVIGATION } from "../../timeouts";

export class BranchOperations {
    constructor(private page: Page) {}

    async createBranch(name: string): Promise<void> {
        await this.page
            .getByRole("link", { name: "Branches", exact: false })
            .click();
        await this.page.getByRole("button", { name: "Create Branch" }).click();
        await this.page.getByPlaceholder("Branch Name").fill(name);
        await this.page
            .getByRole("button", { name: "Create", exact: true })
            .click();
    }

    async switchBranch(name: string): Promise<void> {
        await this.page.getByRole("button", { name: "branch: " }).click();
        await this.page.getByRole("button", { name }).click();
        await this.page.waitForURL(/.*ref=.*/, { timeout: TIMEOUT_NAVIGATION });
    }

    async selectComparedToBranch(name: string): Promise<void> {
        await this.page.getByRole("button", { name: "Compared to branch: " }).click();
        await this.page.getByRole("button", { name, exact: true }).first().click();
    }

    async switchBaseBranch(name: string): Promise<void> {
        await this.page.getByRole("button", { name: "Base branch: " }).click();
        await this.page.getByRole("button", { name, exact: true }).first().click();
    }
}
