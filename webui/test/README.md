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

If lakeFS is already set up and credentials are saved at `test/playwright/.auth/`:

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

Slow down execution to watch interactions:

```bash
npx playwright test --headed --timeout=0
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

## Project Structure

```
test/e2e/
├── fixtures.ts              # Custom Playwright fixtures (POMs, API helper)
├── lakeFSApi.ts             # lakeFS REST API wrapper for test setup
├── timeouts.ts              # Named timeout constants
├── credentialsFile.ts       # Read/write saved credentials
├── types.ts                 # Shared TypeScript types
├── consts.ts                # File path constants
├── poms/                    # Page Object Models
│   ├── repositoriesPage.ts  # Repositories list page
│   ├── repositoryPage.ts    # Single repository (facade)
│   ├── branchOperations.ts  # Branch create/switch actions
│   ├── objectOperations.ts  # File upload/delete/navigate
│   ├── changesOperations.ts # Uncommitted changes, commit, merge
│   ├── commitsOperations.ts # Commits tab
│   ├── revertOperations.ts  # Revert commit flow
│   ├── objectViewerPage.ts  # DuckDB query editor
│   ├── pullsPage.ts         # Pull requests
│   ├── loginPage.ts         # Login page
│   └── setupPage.ts         # Initial setup page
└── common/                  # Test specs
    ├── setup.spec.ts            # Setup form validation tests
    ├── setupInfra.spec.ts       # Setup infrastructure (saves auth state)
    ├── quickstart.spec.ts       # Quickstart tutorial flow
    ├── repositoriesPage.spec.ts # Repository list tests
    ├── readOnlyRepository.spec.ts
    ├── syncCommitMerge.spec.ts  # Commit and merge workflows
    ├── uploadFile.spec.ts       # File upload tests
    ├── viewParquetObject.spec.ts
    └── revertCommit.spec.ts     # Revert commit tests
```

## Test Projects (playwright.config.ts)

| Project | Description | Dependencies |
|---|---|---|
| `setup-validation` | Setup form validation | None |
| `common-setup` | Saves credentials and auth state | `setup-validation` |
| `common` | All common tests (excludes quickstart) | `common-setup` |
| `quickstart` | Quickstart tutorial tests (serial) | `common-setup` |
| `integration` | Integration tests | None |
