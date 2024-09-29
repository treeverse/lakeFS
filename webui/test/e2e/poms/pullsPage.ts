import { Page } from "@playwright/test";

export class PullsPage {
  private page: Page;

  constructor(page: Page) {
    this.page = page;
  }

  async getPullsListCount(): Promise<number> {
    await this.page.locator("div.pulls-list").isVisible();
    return this.page
        .locator("div.pulls-list")
        .locator("pull-row")
        .count();
  }

  async getResultRowCount(): Promise<number> {
    return this.page
        .locator("table.table")
        .locator("tbody")
        .locator("tr")
        .count();
  }

  async switchCompareBranch(name: string): Promise<void> {
    await this.page.getByRole("button", { name: "to branch: " }).click();
    await this.page.getByRole("button", { name }).click();
  }

  async clickCreatePullButton(): Promise<void> {
    await this.page.getByRole("button", { name: "Create Pull Request" }).click();
  }

  async fillPullTitle(title: string): Promise<void> {
    await this.page.getByPlaceholder("Add a title...").fill(title);
  }
}
