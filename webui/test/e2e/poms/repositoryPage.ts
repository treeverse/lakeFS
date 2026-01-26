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
    // New DataBrowser UI uses a tree structure
    await this.page
        .locator('.tree-node-name')
        .filter({ hasText: objectName })
        .first()
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
    // Click on the directory in the tree to expand it
    const dirNode = this.page
        .locator('.tree-node-name')
        .filter({ hasText: dirName })
        .first();
    await dirNode.click();

    // Wait for tree to update and show children
    await this.page.waitForTimeout(1000);

    // Find the first file inside the directory (files don't end with '/')
    // Get all tree-node-name elements that don't end with '/' and pick one that's visible
    const fileNodes = this.page.locator('.tree-node-name').filter({ hasNotText: '/' });
    const fileCount = await fileNodes.count();

    // Find the first actual file (skip the directory itself)
    for (let i = 0; i < fileCount; i++) {
      const node = fileNodes.nth(i);
      const text = await node.textContent();
      // Skip if it's a directory (contains /)
      if (text && !text.includes('/')) {
        await node.click();
        break;
      }
    }

    // Wait for the right panel to show the object and click Actions dropdown
    await this.page.locator('.object-actions-btn').first().waitFor({ state: 'visible', timeout: 5000 });
    await this.page.locator('.object-actions-btn').first().click();

    // Click Delete in the dropdown menu
    await this.page.getByRole('button', { name: 'Delete' }).click();
    await this.page.getByRole("button", { name: "Yes" }).click();
  }

  // uncommitted changes operations

  async showOnlyChanges(): Promise<void> {
    // Toggle the "Uncommitted only" switch
    const toggle = this.page.locator('#show-uncommitted-toggle');
    await toggle.waitFor({ state: 'visible', timeout: 5000 });
    await toggle.click();
  }

  async getUncommittedCount(): Promise<number> {
    // Handle both new DataBrowser (tree-based) and Compare tab (table-based)
    // First try to find the data browser layout (Objects tab)
    const dataBrowserLayout = this.page.locator(".data-browser-layout");
    const dataBrowserExists = await dataBrowserLayout.count() > 0;

    if (dataBrowserExists) {
      await dataBrowserLayout.waitFor({ state: 'visible', timeout: 5000 });
      // Wait a bit for the tree to fully render
      await this.page.waitForTimeout(500);
      // Count tree nodes that have a diff indicator (added, removed, or changed)
      // The diff icons are SVGs with class tree-node-diff-icon
      // Try finding by class first, fall back to counting visible file nodes
      const diffIconCount = await this.page.locator('svg.tree-node-diff-icon').count();
      if (diffIconCount > 0) {
        return diffIconCount;
      }
      // Fall back: in showOnlyChanges mode, count all visible file tree nodes
      // Files don't end with '/' in their name
      return this.page.locator('.tree-node-name').filter({ hasNotText: '/' }).count();
    }

    // Fall back to table-based counting (Compare tab)
    await this.page.locator("table.table").waitFor({ state: 'visible', timeout: 5000 });
    return this.page
        .locator("table.table")
        .locator("tbody")
        .locator("tr.leaf-entry-row")
        .count();
  }

  async commitChanges(commitMsg: string): Promise<void> {
    // Click the Commit button in the action bar (use exact match to avoid matching tree nodes)
    await this.page.getByRole("button", { name: "Commit", exact: true }).click();
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
    await this.page.getByRole("button", { name: "Merge", exact: true }).click();
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
    // Tab was renamed from "Objects" to "Data"
    // Wait for the link to be stable before clicking
    const dataLink = this.page.getByRole("link", { name: "Data", exact: true }).first();
    await dataLink.waitFor({ state: 'visible', timeout: 5000 });
    await this.page.waitForTimeout(200); // Brief pause for stability
    await dataLink.click();
    // Wait for the data browser to load
    await this.page.locator('.data-browser-layout').waitFor({ state: 'visible', timeout: 10000 });
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
    // Upload is now in the Actions dropdown in the right panel
    await this.page.locator('.object-actions-btn').first().click();
    // Click on "Upload Objects" in the dropdown
    await this.page.getByRole('button', { name: 'Upload Objects' }).click();
    await this.page.getByText("Drag & drop files or folders here").click();
    const fileInput = await this.page.locator('input[type="file"]');
    await fileInput.setInputFiles(filePathOrBuffer as any);
  }

  async uploadMultipleObjects(filePathsOrBuffers: (string | { name: string; mimeType: string; buffer: Buffer })[]): Promise<void> {
    // Upload is now in the Actions dropdown in the right panel
    await this.page.locator('.object-actions-btn').first().click();
    // Click on "Upload Objects" in the dropdown
    await this.page.getByRole('button', { name: 'Upload Objects' }).click();
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