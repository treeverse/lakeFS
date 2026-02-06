import { test, expect } from "../fixtures";

test.describe("Repositories Page", () => {
    test("create repository button", async ({ repositoriesPage }) => {
        await repositoriesPage.goto();
        await expect(repositoriesPage.createRepositoryButtonLocator).toBeVisible();
    });

    test("search input", async ({ repositoriesPage }) => {
        await repositoriesPage.goto();
        await expect(repositoriesPage.searchInputLocator).toBeVisible();
    });
});
