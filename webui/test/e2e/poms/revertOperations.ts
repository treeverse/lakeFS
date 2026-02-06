import { Page } from "@playwright/test";
import { TIMEOUT_NAVIGATION } from "../timeouts";

export class RevertOperations {
    constructor(private page: Page) {}

    async clickRevertButton(): Promise<void> {
        const revertButton = this.page.getByRole("button", { name: "Revert" });
        const cancelButton = this.page.getByRole("button", { name: "Cancel" });

        try {
            await revertButton.waitFor({ state: 'visible', timeout: TIMEOUT_NAVIGATION });
            await revertButton.click();
        } catch {
            await cancelButton.waitFor({ state: 'visible', timeout: TIMEOUT_NAVIGATION });
            await cancelButton.click();
        }
    }

    async selectCommitsForRevert(commitCount: number): Promise<void> {
        const checkboxes = this.page.locator('input[type="checkbox"]').filter({ hasNot: this.page.locator('input[type="checkbox"][disabled]') });
        for (let i = 0; i < commitCount; i++) {
            await checkboxes.nth(i).check();
        }
    }

    async clickContinueRevert(): Promise<void> {
        await this.page.getByRole("button", { name: /Continue/ }).click();
    }

    async fillRevertMessage(message: string): Promise<void> {
        const textarea = this.page.getByPlaceholder(/Describe the revert|Revert commit/);
        await textarea.clear();
        await textarea.fill(message);
    }

    async addRevertMetadata(key: string, value: string): Promise<void> {
        await this.page.getByRole("button", { name: /Add Metadata field/ }).click();

        const keyInputs = this.page.getByPlaceholder("Key");
        const valueInputs = this.page.getByPlaceholder("Value");
        const count = await keyInputs.count();

        await keyInputs.nth(count - 1).fill(key);
        await valueInputs.nth(count - 1).fill(value);
    }

    async setAllowEmptyCommit(allow: boolean): Promise<void> {
        const checkbox = this.page.getByLabel(/Allow empty commit/);
        if (allow) {
            await checkbox.check();
        } else {
            await checkbox.uncheck();
        }
    }

    async clickApplyRevert(): Promise<void> {
        await this.page.getByRole("button", { name: "Apply" }).click();
    }

    async confirmRevert(): Promise<void> {
        await this.page.getByRole("button", { name: "Yes" }).click();
    }

    async cancelRevert(): Promise<void> {
        await this.page.getByRole("button", { name: "Cancel" }).click();
    }
}
