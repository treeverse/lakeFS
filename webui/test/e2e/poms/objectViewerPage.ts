import { expect, Page } from "@playwright/test";
import { TIMEOUT_NAVIGATION, TIMEOUT_LONG_OPERATION } from "../timeouts";

export class ObjectViewerPage {
  constructor(private page: Page) {}

  async enterQuery(query: string): Promise<void> {
    await this.page.locator("div.syntax-editor textarea").fill(query);
  }

  async clickExecuteButton(): Promise<void> {
    const submitBtn = this.page.locator('button[type="submit"]');
    await submitBtn.click();
    // Wait for the query to complete: button shows "Executing..." (disabled) while loading,
    // then reverts to "Execute" (enabled) when done. Try to observe the loading state first;
    // if the query completes before we can check, that's fine.
    await expect(submitBtn).toBeDisabled({ timeout: TIMEOUT_NAVIGATION }).catch(() => {});
    await expect(submitBtn).toBeEnabled({ timeout: TIMEOUT_LONG_OPERATION });
  }

  async getResultRowCount(): Promise<number> {
    return this.page.locator("table.table tbody tr").count();
  }
}
