import { Page } from "@playwright/test";
import { TIMEOUT_ELEMENT_VISIBLE, TIMEOUT_NAVIGATION } from "../../timeouts";

export class ObjectOperations {
    public uploadButtonLocator;

    constructor(private page: Page) {
        this.uploadButtonLocator = this.page.getByRole("button", { name: "Upload", exact: true });
    }

    async clickObject(objectName: string): Promise<void> {
        await this.page
            .getByRole("cell", { name: objectName })
            .getByRole("link")
            .click();
    }

    async uploadFiles(filePathsOrBuffers: (string | { name: string; mimeType: string; buffer: Buffer }) | (string | { name: string; mimeType: string; buffer: Buffer })[]): Promise<void> {
        await this.page.getByRole("button", { name: "Upload", exact: true }).click();
        await this.page.getByText("Drag & drop files or folders here").click();
        const fileInput = this.page.locator('input[type="file"]');
        await fileInput.setInputFiles(filePathsOrBuffers as any);
    }

    async deleteFirstObjectInDirectory(dirName: string): Promise<void> {
        await this.page.getByRole("link", { name: dirName }).click();

        await this.page.locator('table tbody tr').first().waitFor({ state: 'visible', timeout: TIMEOUT_ELEMENT_VISIBLE });

        const firstRow = this.page.locator('table tbody tr').first();
        const actionButton = firstRow.locator('button').last();

        await firstRow.scrollIntoViewIfNeeded();
        await firstRow.hover();
        await actionButton.waitFor({ state: 'visible', timeout: TIMEOUT_NAVIGATION });

        await actionButton.click({ force: true });
        await this.page.getByRole('button', { name: 'Delete' }).click();
        await this.page.getByRole("button", { name: "Yes" }).click();
    }
}
