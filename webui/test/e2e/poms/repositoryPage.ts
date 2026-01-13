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
    await this.page.getByPlaceholder("Branch Nam").fill(name);
    await this.page
        .getByRole("button", { name: "Create", exact: true })
        .click();
  }

  async switchBranch(name: string): Promise<void> {
    await this.page.getByRole("button", { name: "branch: " }).click();
    await this.page.getByRole("button", { name }).click();
    // Wait for URL to update after branch switch
    await this.page.waitForURL(/.*ref=.*/, { timeout: 5000 });
  }

  async selectComparedToBranch(name: string): Promise<void> {
    await this.page.getByRole("button", { name: "Compared to branch: " }).click();
    await this.page.getByRole("button", { name, exact: true }).first().click();
  }

  async switchBaseBranch(name: string): Promise<void> {
    await this.page.getByRole("button", { name: "Base branch: " }).click();
    await this.page.getByRole("button", { name, exact: true }).first().click();
  }

  // file manipulation operations

  async deleteFirstObjectInDirectory(dirName: string): Promise<void> {
    await this.page.getByRole("link", { name: dirName }).click();

    // Wait for the table to be visible and stable after navigation
    await this.page.locator('table tbody tr').first().waitFor({ state: 'visible', timeout: 10000 });

    const firstRow = this.page.locator('table tbody tr').first();
    const actionButton = firstRow.locator('button').last();

    // Scroll the row into the viewport center to avoid navbar overlap
    await firstRow.scrollIntoViewIfNeeded();

    // Hover and wait for the action button to actually become visible
    await firstRow.hover();
    await actionButton.waitFor({ state: 'visible', timeout: 5000 });

    // Click with force since navbar sometimes intercepts even though button is visible
    await actionButton.click({ force: true });
    await this.page.getByRole('button', { name: 'Delete' }).click();
    await this.page.getByRole("button", { name: "Yes" }).click();
  }

  // uncommitted changes operations

  async showOnlyChanges(): Promise<void> {
    await this.page.getByRole("button", { name: "Uncommitted Changes" }).click();
  }

  async getUncommittedCount(): Promise<number> {
    await this.page.locator(".tree-container div.card").isVisible();
    return this.page
        .locator("table.table")
        .locator("tbody")
        .locator("tr")
        .count();
  }

  async commitChanges(commitMsg: string): Promise<void> {
    // Click the Actions dropdown (empty button next to Uncommitted Changes)
    await this.page.locator('button[id="changes-dropdown"]').click();
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

  // navigation

  async gotoObjectsTab(): Promise<void> {
    await this.page.getByRole("link", { name: "Objects", exact: true }).first().click();
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

  async gotoCommitsTab(): Promise<void> {
    await this.page.getByRole("link", { name: "Commits" }).click();
  }

  async uploadObject(filePathOrBuffer: string | { name: string; mimeType: string; buffer: Buffer }): Promise<void> {
    await this.page.getByRole("button", { name: "Upload", exact: true }).click();
    await this.page.getByText("Drag & drop files or folders here").click();
    const fileInput = await this.page.locator('input[type="file"]');
    await fileInput.setInputFiles(filePathOrBuffer as any);
  }

  async uploadMultipleObjects(filePathsOrBuffers: (string | { name: string; mimeType: string; buffer: Buffer })[]): Promise<void> {
    await this.page.getByRole("button", { name: "Upload", exact: true }).click();
    await this.page.getByText("Drag & drop files or folders here").click();
    const fileInput = await this.page.locator('input[type="file"]');
    await fileInput.setInputFiles(filePathsOrBuffers as any);
  }

  // revert operations

  async clickRevertButton(): Promise<void> {
    // The button text changes between "Revert" and "Cancel" depending on mode
    // Wait for at least one button to be visible, then click it
    const revertButton = this.page.getByRole("button", { name: "Revert" });
    const cancelButton = this.page.getByRole("button", { name: "Cancel" });

    // Wait for either button to exist
    try {
      await revertButton.waitFor({ state: 'visible', timeout: 5000 });
      await revertButton.click();
    } catch {
      // If Revert button not found, try Cancel button
      await cancelButton.waitFor({ state: 'visible', timeout: 5000 });
      await cancelButton.click();
    }
  }

  async selectCommitsForRevert(commitCount: number): Promise<void> {
    // Select the first N commits using checkboxes
    const checkboxes = this.page.locator('input[type="checkbox"]').filter({ hasNot: this.page.locator('input[type="checkbox"][disabled]') });
    for (let i = 0; i < commitCount; i++) {
      await checkboxes.nth(i).check();
    }
  }

  async clickContinueRevert(): Promise<void> {
    await this.page.getByRole("button", { name: /Continue/ }).click();
  }

  async fillRevertMessage(message: string): Promise<void> {
    const textarea = this.page.getByPlaceholder(/Describe the revert|Revert commit/);
    await textarea.clear();
    await textarea.fill(message);
  }

  async addRevertMetadata(key: string, value: string): Promise<void> {
    // Click "Add Metadata field" button
    await this.page.getByRole("button", { name: /Add Metadata field/ }).click();

    // Fill in the last (most recent) key-value pair
    const keyInputs = this.page.getByPlaceholder("Key");
    const valueInputs = this.page.getByPlaceholder("Value");
    const count = await keyInputs.count();

    await keyInputs.nth(count - 1).fill(key);
    await valueInputs.nth(count - 1).fill(value);
  }

  async setAllowEmptyCommit(allow: boolean): Promise<void> {
    const checkbox = this.page.getByLabel(/Allow empty commit/);
    if (allow) {
      await checkbox.check();
    } else {
      await checkbox.uncheck();
    }
  }

  async clickApplyRevert(): Promise<void> {
    await this.page.getByRole("button", { name: "Apply" }).click();
  }

  async confirmRevert(): Promise<void> {
    await this.page.getByRole("button", { name: "Yes" }).click();
  }

  async cancelRevert(): Promise<void> {
    await this.page.getByRole("button", { name: "Cancel" }).click();
  }

  async getCommitsCount(): Promise<number> {
    // Wait for the commits card to be visible
    await this.page.locator(".card .list-group").waitFor({ state: "visible" });

    // Count commit items
    return this.page.locator(".list-group-item .clearfix").count();
  }

  async getFirstCommitMessage(): Promise<string> {
    // Wait for commits to load
    await this.page.locator(".list-group-item").first().waitFor({ state: "visible" });

    const firstCommit = this.page.locator(".list-group-item").first();
    const message = await firstCommit.locator("h6 a").textContent();
    return message?.trim() || "";
  }
}