import { test, expect } from "@playwright/test";
import { SetupPage } from "../poms/setupPage";
import { LoginPage } from "../poms/loginPage";
import { RepositoriesPage } from "../poms/repositoriesPage";
import { STORAGE_STATE_PATH, writeCredentials } from "../credentialsFile";

test("setup lakeFS and save auth state", async ({ page }) => {
    await test.step("submit setup form", async () => {
        const setupPage = new SetupPage(page);
        await setupPage.goto();
        await setupPage.fillForm("test@example.com");
        await expect(setupPage.setupFinishedTitleLocator).toBeVisible();
        await expect(setupPage.downloadCredentialsButtonLocator).toBeVisible();
        await expect(setupPage.goToLoginButtonLocator).toBeVisible();
    });

    const credentials = await test.step("extract credentials", async () => {
        const setupPage = new SetupPage(page);
        return setupPage.getCredentials();
    });

    await test.step("login and save storage state", async () => {
        const setupPage = new SetupPage(page);
        const loginTab = await setupPage.goToLoginButton();
        await expect(loginTab).toHaveURL(/.*\/login/);
        const loginPage = new LoginPage(loginTab);
        await loginPage.doLogin(credentials.accessKeyId, credentials.secretAccessKey);
        const repositoriesPage = new RepositoriesPage(loginTab);
        await expect(repositoriesPage.noRepositoriesTitleLocator).toBeVisible();

        await loginTab.context().storageState({ path: STORAGE_STATE_PATH });
        await writeCredentials(credentials);
    });
});
