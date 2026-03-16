# WebUI End-to-End Tests

Playwright-based E2E tests for the lakeFS web interface.

## Prerequisites

- Node.js >= 18
- A running lakeFS instance (default: `http://localhost:8000`)
- Chromium browser for Playwright

Install dependencies and browser:

```bash
cd webui
npm install
npx playwright install chromium
```

## Running Tests

### Full suite (including setup)

Runs the setup flow (creates admin user, saves credentials), then all tests:

```bash
npx playwright test
```

### Skip setup (already-configured lakeFS)

If lakeFS is already set up and credentials are saved at `test/playwright/`:

```bash
SKIP_SETUP=true npx playwright test
```

### Run specific project

```bash
SKIP_SETUP=true npx playwright test --project=common
SKIP_SETUP=true npx playwright test --project=quickstart
```

### Run a single test file

```bash
SKIP_SETUP=true npx playwright test test/e2e/common/quickstart.spec.ts
```

## Showing the Browser (Headed Mode)

Run tests with the browser visible:

```bash
npx playwright test --headed
```

## Debugging

### Playwright Inspector (step-through debugger)

Opens an interactive inspector window where you can step through each action, inspect selectors, and see the page state:

```bash
npx playwright test --debug
```

Debug a single test:

```bash
npx playwright test test/e2e/common/quickstart.spec.ts --debug
```

### Trace Viewer

Traces are saved on failure by default (`trace: "retain-on-failure"` in config). View a trace:

```bash
npx playwright show-trace test-results/<test-folder>/trace.zip
```

### UI Mode

Interactive mode with live reloading, test explorer, and built-in trace viewer:

```bash
npx playwright test --ui
```

### VS Code Integration

Install the [Playwright Test for VS Code](https://marketplace.visualstudio.com/items?itemName=ms-playwright.playwright) extension. It provides:

- Click-to-run individual tests
- Breakpoint debugging
- Live browser preview with "Show Browser" checkbox
- Selector picker via "Pick locator"

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `BASE_URL` | `http://localhost:8000` | lakeFS server URL |
| `SKIP_SETUP` | (unset) | Skip setup projects when set to any value |
| `REPO_STORAGE_NAMESPACE_PREFIX` | `local://` | Storage namespace prefix for test repositories |

## Design Principles

### Fixtures over manual construction

Tests receive POMs via [Playwright fixtures](https://playwright.dev/docs/test-fixtures) instead of constructing them manually. Import `test` and `expect` from `../fixtures`, not from `@playwright/test`:

```ts
// Good
import { test, expect } from "../fixtures";
test("my test", async ({ repositoriesPage, repositoryPage }) => { ... });

// Bad — don't construct POMs manually in tests
import { test } from "@playwright/test";
import { RepositoriesPage } from "../poms/repositoriesPage";
test("my test", async ({ page }) => {
    const repositoriesPage = new RepositoriesPage(page);
});
```

### Facade pattern for repository operations

`RepositoryPage` is a thin facade that composes sub-POMs by domain. Access operations through the appropriate sub-POM:

```ts
repositoryPage.branches.createBranch("feature");
repositoryPage.branches.switchBranch("feature");
repositoryPage.objects.uploadFiles("file.txt");
repositoryPage.changes.commitChanges("my commit");
repositoryPage.changes.merge("merge msg");
repositoryPage.revert.clickRevertButton();
repositoryPage.commits.getCommitsCount();
```

When adding new repository operations, add them to the relevant sub-POM (or create a new one) — don't add methods directly to `RepositoryPage`.

### Named timeouts

Use the constants from `timeouts.ts` instead of magic numbers:

```ts
import { TIMEOUT_LONG_OPERATION, TIMEOUT_ELEMENT_VISIBLE, TIMEOUT_NAVIGATION } from "../timeouts";

await expect(element).toBeVisible({ timeout: TIMEOUT_ELEMENT_VISIBLE });  // 10s
await page.waitForURL(/.*ref=.*/, { timeout: TIMEOUT_NAVIGATION });       // 5s
await expect(result).toBeVisible({ timeout: TIMEOUT_LONG_OPERATION });    // 120s
```

### Assertions, not boolean checks

Use Playwright's auto-retrying assertions. Never use `.isVisible()`, `.isEnabled()`, or `.isDisabled()` as assertions — they return a boolean instantly without waiting:

```ts
// Good — auto-retries until timeout
await expect(element).toBeVisible();
await expect(button).toBeDisabled();

// Bad — returns true/false immediately, no retries
await element.isVisible();
```

Similarly, for counting elements, use `expect.poll()`:

```ts
// Good — retries until count matches
await expect.poll(() => locator.count()).toEqual(3);

// Bad — checks once, instantly
expect(await locator.count()).toEqual(3);
```

### REST API for test setup, UI for test assertions

Use `lakeFSApi` (via the fixture) to set up preconditions like creating repositories. Use the UI to verify behavior. This keeps tests fast and focused on what they're actually testing.

## Adding a New Test

1. Create a spec file in `common/` (e.g., `common/myFeature.spec.ts`).

2. Import from fixtures, not from `@playwright/test`:
   ```ts
   import { test, expect } from "../fixtures";
   ```

3. Destructure the fixtures you need:
   ```ts
   test("should do something", async ({ repositoryPage, lakeFSApi }) => {
       // lakeFSApi for setup, repositoryPage for UI interaction
   });
   ```

4. If you need new UI operations, add methods to the appropriate sub-POM under `poms/`. If the operations don't fit an existing sub-POM, create a new `*Operations.ts` file, add it as a property on `RepositoryPage`, and register it as a fixture in `fixtures.ts` if it needs to be used directly.

5. If you need new API operations, add methods to `LakeFSApi` in `lakeFSApi.ts`.

6. For features unsupported by the local block adapter, tag the test with `@exclude-local`:
   ```ts
   test("import @exclude-local", async ({ repositoryPage }) => { ... });
   ```
   These can be skipped with `npx playwright test --grep-invert @exclude-local`.

## Test Projects (playwright.config.ts)

| Project | Description | Dependencies |
|---|---|---|
| `setup-validation` | Setup form validation | None |
| `common-setup` | Saves credentials and auth state | `setup-validation` |
| `common` | All common tests (excludes quickstart) | `common-setup` |
| `quickstart` | Quickstart tutorial tests (serial) | `common-setup` |
