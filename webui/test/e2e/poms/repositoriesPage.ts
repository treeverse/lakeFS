import { Locator, Page, expect } from "@playwright/test";
import { TIMEOUT_LONG_OPERATION } from "../timeouts";

export const SAMPLE_REPO_README_TITLE = "Welcome to the Lake!";
export const REGULAR_REPO_README_TITLE = "Your repository is ready!";

export class RepositoriesPage {
    public noRepositoriesTitleLocator: Locator;
    public readOnlyIndicatorLocator: Locator;
    public createRepositoryButtonLocator: Locator;
    public searchInputLocator: Locator;

    constructor(protected page: Page) {
        this.noRepositoriesTitleLocator = this.page.getByText("Welcome to LakeFS!");
        this.readOnlyIndicatorLocator = this.page.locator("text=Read-only");
        this.createRepositoryButtonLocator = this.page.getByRole("button", { name: "Create Repository" });
        this.searchInputLocator = this.page.getByPlaceholder("Search repositories...");
    }

    async goto(): Promise<void> {
        await this.page.goto("/repositories");
    }

    async goToRepository(repoName: string): Promise<void> {
        await this.page.getByRole("link", { name: repoName, exact: true }).click();
    }

    async createSampleRepository(): Promise<void> {
        await this.page.getByRole("button", { name: "Create Sample Repository" }).click();
        await expect(this.page.getByRole("heading", { name: SAMPLE_REPO_README_TITLE })).toBeVisible({ timeout: TIMEOUT_LONG_OPERATION });
    }

    async createRepository(repoName: string, includeSampleData: boolean): Promise<void> {
        await this.createRepositoryButtonLocator.click();
        await this.page.getByRole('textbox', { name: 'Repository ID' }).fill(repoName);

        // When the server has no default_namespace_prefix (e.g. S3 without a configured prefix),
        // the Storage Namespace field is shown and must be filled manually.
        const storageNamespaceField = this.page.getByRole('textbox', { name: 'Storage Namespace' });
        if (await storageNamespaceField.isVisible() && !(await storageNamespaceField.inputValue())) {
            const prefix = process.env.REPO_STORAGE_NAMESPACE_PREFIX || 'local://';
            await storageNamespaceField.fill(prefix + repoName);
        }

        if (includeSampleData) {
            await this.page.getByRole('checkbox', { name: 'Add sample data, hooks' }).check();
        }
        await this.page.getByRole("dialog").getByRole("button", { name: "Create Repository", exact: true }).click();

        const expectedTitle = includeSampleData ? SAMPLE_REPO_README_TITLE : REGULAR_REPO_README_TITLE;
        await expect(this.page.getByRole("heading", { name: expectedTitle })).toBeVisible({ timeout: TIMEOUT_LONG_OPERATION });
    }
}
