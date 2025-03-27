import { Download, Locator, Page } from "@playwright/test";
import { LakeFSCredentials } from "../types";

export class SetupPage {
    private page: Page;

    public emailErrorSelectorText = "Please enter your email address.";
    public usernameErrorSelectorText = "Please enter your admin username.";
    public usernameInputLocator: Locator;
    public downloadCredentialsButtonLocator: Locator;
    public goToLoginButtonLocator: Locator;
    public setupFinishedTitleLocator: Locator;

    constructor(page: Page) {
        this.page = page;
        this.usernameInputLocator = this.page.getByPlaceholder("Admin Username");
        this.downloadCredentialsButtonLocator = this.page.getByRole("link", { name: "Download credentials" });
        this.goToLoginButtonLocator = this.page.getByRole("button", { name: "Go To Login" });
        this.setupFinishedTitleLocator = this.page.getByText("You're all set!");
    }

    async goto(): Promise<void> {
        await this.page.goto("/setup");
    }

    async fillForm(email: string, username = "admin", receiveUpdatesChecked = true): Promise<void> {
        await this.usernameInputLocator.fill(username);
        await this.page.getByLabel("Email").fill(email);
        if (receiveUpdatesChecked) {
            await this.page.getByLabel("I'd like to receive security, product and feature updates").check();
        }
        await this.page.getByRole("button", { name: "Setup" }).click();
    }

    async downloadCredentialsButton(): Promise<Download> {
        const downloadPromise = this.page.waitForEvent("download");
        await this.downloadCredentialsButtonLocator.click();
        return downloadPromise;
    }

    async goToLoginButton(): Promise<Page> {
        const pagePromise = this.page.context().waitForEvent("page");
        await this.goToLoginButtonLocator.click();
        return pagePromise;
    }

    async getCredentials(): Promise<LakeFSCredentials> {
        const accessKeyId = await this.page.getByRole("code").nth(0).innerText();
        const secretAccessKey = await this.page.getByRole("code").nth(1).innerText();
        return { accessKeyId, secretAccessKey };
    }
}
