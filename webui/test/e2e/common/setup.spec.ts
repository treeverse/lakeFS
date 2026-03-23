import { test, expect } from "@playwright/test";
import { SetupPage } from "../poms/pages/setupPage";

test.describe("Setup Page Validation", () => {
    test("initial navigation to the base URL should redirect to /setup", async ({ page }) => {
        await page.goto("/");
        await page.waitForURL(/.*\/setup/);
    });

    test("username has a default value of 'admin'", async ({ page }) => {
        const setupPage = new SetupPage(page);
        await setupPage.goto();
        const usernameInput = setupPage.usernameInputLocator;
        await expect(usernameInput).toHaveValue("admin");
    });

    test("username is required", async ({ page }) => {
        const setupPage = new SetupPage(page);
        await setupPage.goto();
        await setupPage.fillForm("test@treeverse.io", "");
        const error = page.getByText(setupPage.usernameErrorSelectorText);
        await expect(error).toBeVisible();
    });

    test("email is required", async ({ page }) => {
        const setupPage = new SetupPage(page);
        await setupPage.goto();
        await setupPage.fillForm("");
        const error = page.getByText(setupPage.emailErrorSelectorText);
        await expect(error).toBeVisible();
    });

    test("shows error for free email", async ({ page }) => {
        const setupPage = new SetupPage(page);
        await setupPage.goto();
        await setupPage.fillForm("user@gmail.com", "admin", true, "Jane", "Doe", "Acme Inc.");
        const error = page.getByText("Please use a business email address");
        await expect(error).toBeVisible();
    });

    test("shows error for invalid email", async ({ page }) => {
        const setupPage = new SetupPage(page);
        await setupPage.goto();
        await setupPage.fillForm("nope@nope", "admin", true, "Jane", "Doe", "Acme Inc.");
        const error = page.getByText("Invalid email address");
        await expect(error).toBeVisible();
    });
});
