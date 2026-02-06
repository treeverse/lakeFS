import { defineConfig, devices } from "@playwright/test";
/**
 * Read environment variables from file.
 * https://github.com/motdotla/dotenv
 */
import "dotenv/config";
import { COMMON_STORAGE_STATE_PATH } from "./test/e2e/consts";

const BASE_URL = process.env.BASE_URL || "http://localhost:8000";
const SKIP_SETUP = !!process.env.SKIP_SETUP;

export default defineConfig({
  testDir: "test",
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: 0,
  workers: 1,
  reporter: process.env.CI
    ? [["github"], ["junit", { outputFile: "test-results.xml" }]]
    : [["list", { printSteps: true }]],
  use: {
    baseURL: BASE_URL,
    trace: "retain-on-failure",
  },
  projects: [
    ...(!SKIP_SETUP ? [
      {
        name: "setup-validation",
        use: {
          ...devices["Desktop Chrome"],
        },
        testMatch: "test/e2e/common/setup.spec.ts",
      },
      {
        name: "common-setup",
        dependencies: ["setup-validation"],
        use: {
          ...devices["Desktop Chrome"],
        },
        testMatch: "test/e2e/common/setupInfra.spec.ts",
      },
    ] : []),
    {
      // common tests are applicable to any combination of database and block adapter
      name: "common",
      dependencies: SKIP_SETUP ? [] : ["common-setup"],
      use: {
        ...devices["Desktop Chrome"],
        storageState: COMMON_STORAGE_STATE_PATH,
      },
      testMatch: "test/e2e/common/**/*.spec.ts",
      testIgnore: [
        "test/e2e/common/setup.spec.ts",
        "test/e2e/common/setupInfra.spec.ts",
        "test/e2e/common/quickstart.spec.ts",
      ],
    },
    {
      name: "quickstart",
      dependencies: SKIP_SETUP ? [] : ["common-setup"],
      use: {
        ...devices["Desktop Chrome"],
        storageState: COMMON_STORAGE_STATE_PATH,
      },
      testMatch: "test/e2e/common/**/quickstart.spec.ts",
      testIgnore: [
        "test/e2e/common/setup.spec.ts",
        "test/e2e/common/setupInfra.spec.ts",
      ],
    },
    {
      name: "integration",
      use: {
          ...devices["Desktop Chrome"],
      },
      testMatch: "test/integration/**/*.spec.ts",
    },
  ],
});
