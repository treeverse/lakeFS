import { Locator, Page } from "@playwright/test";

export class RepositoryPage {
  private page: Page;

  public readOnlyIndicatorLocator: Locator;

  constructor(page: Page) {
    this.page = page;
    this.readOnlyIndicatorLocator = this.page.locator("text=Read-only");
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

  async gotoObjectsTab(): Promise<void> {
    await this.page.getByRole("link", { name: "Objects" }).click();
  }

  async gotoUncommittedChangeTab(): Promise<void> {
    await this.page.getByRole("link", { name: "Uncommitted Changes" }).click();
  }

  async gotoCompareTab(): Promise<void> {
    await this.page.getByRole("link", { name: "Compare" }).click();
  }
}
