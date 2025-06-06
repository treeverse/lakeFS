import { test, expect } from "@playwright/test";
import { RepositoriesPage } from "../poms/repositoriesPage";
import { RepositoryPage } from "../poms/repositoryPage";
import { LoginPage } from "../poms/loginPage";
import { getCredentials } from "../credentialsFile";

const TEST_REPO_NAME = "test-repo";
const PARQUET_OBJECT_NAME = "lakes.parquet";

test.describe("Object Viewer - Parquet File", () => {
    test.describe.configure({ mode: "serial" });
    test("create repo w/ sample data", async ({page}) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await repositoriesPage.createRepository(TEST_REPO_NAME, true);
    });

    test("view parquet object", async ({page}) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await repositoriesPage.goToRepository(TEST_REPO_NAME);

        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.clickObject(PARQUET_OBJECT_NAME);
        await expect(page.getByRole("button", { name: "Execute" })).toBeVisible();
    });

    test("view parquet object w/ logout and login", async ({page}) => {
        test.skip(); // this currently fails for some race condition. skipping until fixed

        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await page.getByRole('button', { name: "admin" }).click();
        await page.getByRole("button", { name: "Logout" }).click();
        await page.waitForURL(/.*\/auth\/login/); // wait for redirect after logout from / to /auth/login to avoid race conditions
        const loginPage = new LoginPage(page);
        const credentials = await getCredentials();
        if (!credentials) {
            test.fail();
            return;
        }
        await loginPage.doLogin(credentials.accessKeyId, credentials.secretAccessKey);
        const repositoryPage = new RepositoryPage(page);
        await repositoryPage.goto(TEST_REPO_NAME);
        await repositoryPage.clickObject(PARQUET_OBJECT_NAME);
        await expect(page.getByText("Loading...")).not.toBeVisible();
    });
})

