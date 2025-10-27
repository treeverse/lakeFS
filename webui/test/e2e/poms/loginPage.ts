import { Page } from "@playwright/test";

export class LoginPage {
    private page: Page;

    constructor(page: Page) {
        this.page = page;
    }

    async goto(): Promise<void> {
        await this.page.goto("/login");
    }

    async doLogin(accessKeyId: string, secretAccessKey: string): Promise<void> {
        await this.page.getByPlaceholder("Access Key ID").fill(accessKeyId);
        await this.page.getByPlaceholder("Secret Access Key").fill(secretAccessKey);
        await this.page.getByRole("button", { name: "Login" }).click();
        await this.page.waitForURL(/.*\/repositories/); // wait for redirect
    }
}
