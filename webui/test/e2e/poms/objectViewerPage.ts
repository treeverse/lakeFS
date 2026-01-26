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
    // Wait for query to complete - the button will be disabled during execution
    await this.page.getByRole("button", { name: "Execute" }).isDisabled();
    await this.page.getByRole("button", { name: "Execute" }).isEnabled({ timeout: 30000 });
    // Wait a bit more for results to render
    await this.page.waitForTimeout(500);
  }

  async getResultRowCount(): Promise<number> {
    // Wait for the query results table to be visible (first table in the Preview tab)
    // The object viewer panel has multiple tables (data, info, blame) - we want the first one
    await this.page.locator(".object-viewer-panel table.table").first().waitFor({ state: 'visible', timeout: 10000 });
    return this.page
      .locator(".object-viewer-panel table.table")
      .first()
      .locator("tbody")
      .locator("tr")
      .count();
  }
}
