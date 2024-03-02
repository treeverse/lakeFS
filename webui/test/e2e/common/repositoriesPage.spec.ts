import { test, expect } from "@playwright/test";
import { RepositoriesPage } from "../poms/repositoriesPage";

test.describe("Repositories Page", () => {
    test("create repository button", async ({ page }) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await expect(repositoriesPage.createRepositoryButtonLocator).toBeVisible();
    });

    test("search input", async ({ page }) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await expect(repositoriesPage.searchInputLocator).toBeVisible();
    });
});
