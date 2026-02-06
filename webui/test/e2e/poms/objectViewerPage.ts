import { expect, Page } from "@playwright/test";

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
    const executeButton = this.page.getByRole("button", { name: "Execute" });
    await executeButton.click();
    // the button is disabled while the query is running and enabled after it finishes, so waiting for the end state
    await expect(executeButton).toBeEnabled();
  }

  async getResultRowCount(): Promise<number> {
    return this.page
      .locator("table.table")
      .locator("tbody")
      .locator("tr")
      .count();
  }
}
