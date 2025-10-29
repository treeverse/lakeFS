import { Page } from "@playwright/test";

export class ObjectViewerPage {
  private page: Page;

  constructor(page: Page) {
    this.page = page;
  }

  async enterQuery(query: string): Promise<void> {
    await this.page
      .locator("div.syntax-editor")
      .locator("textarea")
      .fill(query);
  }

  async clickExecuteButton(): Promise<void> {
    await this.page.getByRole("button", { name: "Execute" }).click();
    await this.page.getByRole("button", { name: "Execute" }).isDisabled();
    await this.page.getByRole("button", { name: "Execute" }).isEnabled();
  }

  async getResultRowCount(): Promise<number> {
    return this.page
      .locator("table.table")
      .locator("tbody")
      .locator("tr")
      .count();
  }
}
