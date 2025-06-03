import { Locator, Page } from "@playwright/test";

export class RepositoryPage {
  private page: Page;

  public readOnlyIndicatorLocator: Locator;
  public breadcrumbsLocator: Locator;

  constructor(page: Page) {
    this.page = page;
    this.readOnlyIndicatorLocator = this.page.locator("text=Read-only");
    this.breadcrumbsLocator = this.page.locator("ol.breadcrumb");
  }

  async goto(repoName: string): Promise<void> {
    await this.page.goto(`/repositories/${repoName}`);
  }

  async clickObject(objectName: string): Promise<void> {
    await this.page
      .getByRole("cell", { name: objectName })
      .getByRole("link")
      .click();
  }

  // branch operations

  async createBranch(name: string): Promise<void> {
    await this.page
      .getByRole("link", { name: "Branches", exact: false })
      .click();
    await this.page.getByRole("button", { name: "Create Branch" }).click();
    await this.page.getByPlaceholder("Branch Name").fill(name);
    await this.page
      .getByRole("button", { name: "Create", exact: true })
      .click();
  }

  async switchBranch(name: string): Promise<void> {
    await this.page.getByRole("button", { name: "branch: " }).click();
    await this.page.getByRole("button", { name }).click();
  }

  // file manipulation operations

  async deleteFirstObjectInDirectory(dirName: string): Promise<void> {
    await this.page.getByRole("link", { name: dirName }).click();
    const firstRow = this.page.locator('table tbody tr').first();
    await firstRow.hover();
    await firstRow.locator('button').last().click();
    await this.page.getByRole('button', { name: 'Delete' }).click();
    await this.page.getByRole("button", { name: "Yes" }).click();
  }

  // uncommitted changes operations

  async getUncommittedCount(): Promise<number> {
    await this.page.locator("div.card").isVisible();
    return this.page
      .locator("table.table")
      .locator("tbody")
      .locator("tr")
      .count();
  }

  async commitChanges(commitMsg: string): Promise<void> {
    await this.page.getByRole("button", { name: "Commit Changes" }).click();
    if (commitMsg?.length) {
      await this.page.getByPlaceholder("Commit Message").fill(commitMsg);
    }
    await this.page
      .getByRole("dialog")
      .getByRole("button", { name: "Commit Changes" })
      .click();
  }

  // merge operations

  async merge(commitMsg: string): Promise<void> {
    await this.page.getByRole("button", { name: "Merge" }).click();
    if (commitMsg?.length) {
      await this.page
        .getByPlaceholder("Commit Message (Optional)")
        .fill(commitMsg);
    }
    await this.page
      .getByRole("dialog")
      .getByRole("button", { name: "Merge" })
      .click();
  }

  async switchBaseBranch(name: string): Promise<void> {
    await this.page.getByRole("button", { name: "Base branch: " }).click();
    await this.page.getByRole("button", { name }).click();
  }

  // navigation

  async gotoObjectsTab(): Promise<void> {
    await this.page.getByRole("link", { name: "Objects" }).click();
  }

  async gotoUncommittedChangeTab(): Promise<void> {
    await this.page.getByRole("link", { name: "Uncommitted Changes" }).click();
  }

  async gotoCompareTab(): Promise<void> {
    await this.page.getByRole("link", { name: "Compare" }).click();
  }

  async gotoPullRequestsTab(): Promise<void> {
    await this.page.getByRole("link", { name: "Pull Requests" }).click();
  }

  async gotoSettingsTab(): Promise<void> {
    await this.page.getByRole("link", { name: "Settings" }).click();
  }

  async uploadObject(filePath: string): Promise<void> {
    await this.page.getByRole("button", { name: "Upload Object" }).click();
    await this.page.getByText("Drag & drop files or folders here").click();
    const fileInput = await this.page.locator('input[type="file"]');
    await fileInput.setInputFiles(filePath);
  }
}
