import { test, expect } from "@playwright/test";
import { RepositoriesPage } from "../poms/repositoriesPage";

test.describe("Repositories Page", () => {
    test("welcome card is shown when no repositories exist", async ({ page }) => {
        const repositoriesPage = new RepositoriesPage(page);
        await repositoriesPage.goto();
        await expect(repositoriesPage.noRepositoriesTitleLocator).toBeVisible();
    });
});