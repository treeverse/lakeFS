import { test, expect } from "@playwright/test";
import { SetupPage } from "../poms/setupPage";
import { LoginPage } from "../poms/loginPage";
import { RepositoriesPage } from "../poms/repositoriesPage";
import { COMMON_STORAGE_STATE_PATH } from "../consts";
import {writeCredentials} from "../credentialsFile";

const LAKECTL_CONFIGURATION_FILE_NAME = "lakectl.yaml";

test.describe("Setup Page", () => {
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

    test("email is required", async ({ page }) => {
        const setupPage = new SetupPage(page);
        await setupPage.goto();
        await setupPage.fillForm("");
        const error = await page.getByText(setupPage.emailErrorSelectorText);
        await expect(error).toBeVisible();
    });

    test("successfully submitting the form", async ({ page }) => {
        const setupPage = new SetupPage(page);
        await setupPage.goto();
        await setupPage.fillForm("test@example.com");

        await expect(setupPage.setupFinishedTitleLocator).toBeVisible();
        await expect(setupPage.downloadCredentialsButtonLocator).toBeVisible();
        await expect(setupPage.goToLoginButtonLocator).toBeVisible();

        // download credentials
        const download = await setupPage.downloadCredentialsButton();
        expect(download.suggestedFilename()).toBe(LAKECTL_CONFIGURATION_FILE_NAME);

        // open login page in a new tab
        // and do login
        const credentials = await setupPage.getCredentials();
        const loginTab = await setupPage.goToLoginButton();
        await expect(loginTab).toHaveURL(/.*\/login/);
        const loginPage = new LoginPage(loginTab);
        await loginPage.doLogin(credentials.accessKeyId, credentials.secretAccessKey);
        const repositoriesPage = new RepositoriesPage(loginTab);
        await expect(repositoriesPage.noRepositoriesTitleLocator).toBeVisible();

        // save local storage state
        await loginTab.context().storageState({ path: COMMON_STORAGE_STATE_PATH });
        // dump raw credentials to a file
        await writeCredentials(credentials);
    });

    test("after successful setup, navigating to the base URL should redirect to /login", async ({ page }) => {
        await page.goto("/");
        await page.waitForURL(/.*\/login/);
    });
});
