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
        await setupPage.fillForm("test@example.com", "");
        const error = page.getByText(setupPage.usernameErrorSelectorText);
        await expect(error).toBeVisible();
    });

    test("first name is required", async ({ page }) => {
        const setupPage = new SetupPage(page);
        await setupPage.goto();
        await setupPage.fillForm("test@example.com", "admin", true, "");
        const error = page.getByText(setupPage.firstNameErrorSelectorText);
        await expect(error).toBeVisible();
    });

    test("last name is required", async ({ page }) => {
        const setupPage = new SetupPage(page);
        await setupPage.goto();
        await setupPage.fillForm("test@example.com", "admin", true, "Test", "");
        const error = page.getByText(setupPage.lastNameErrorSelectorText);
        await expect(error).toBeVisible();
    });

    test("email is required", async ({ page }) => {
        const setupPage = new SetupPage(page);
        await setupPage.goto();
        await setupPage.fillForm("");
        const error = page.getByText(setupPage.emailErrorSelectorText);
        await expect(error).toBeVisible();
    });

    test("company name is required", async ({ page }) => {
        const setupPage = new SetupPage(page);
        await setupPage.goto();
        await setupPage.fillForm("test@example.com", "admin", true, "Test", "User", "");
        const error = page.getByText(setupPage.companyNameErrorSelectorText);
        await expect(error).toBeVisible();
    });
});
