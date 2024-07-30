import { defineConfig, devices } from "@playwright/test";
/**
 * Read environment variables from file.
 * https://github.com/motdotla/dotenv
 */
import "dotenv/config";
import { COMMON_STORAGE_STATE_PATH } from "./test/e2e/consts";

const BASE_URL = process.env.BASE_URL || "http://localhost:8000";

export default defineConfig({
  testDir: "test/e2e",
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
    {
      name: "common-setup",
      use: {
        ...devices["Desktop Chrome"],
      },
      testMatch: "test/e2e/common/setup.spec.ts",
    },
    {
      // common tests are applicable to any combination of database and block adapter
      name: "common",
      dependencies: ["common-setup"],
      use: {
        ...devices["Desktop Chrome"],
        storageState: COMMON_STORAGE_STATE_PATH,
      },
      testMatch: "test/e2e/common/**/*.spec.ts",
      testIgnore: [
        "test/e2e/common/setup.spec.ts",
        "test/e2e/common/quickstart.spec.ts",
      ],
    },
    {
      name: "quickstart",
      dependencies: ["common-setup"],
      use: {
        ...devices["Desktop Chrome"],
        storageState: COMMON_STORAGE_STATE_PATH,
      },
      testMatch: "test/e2e/common/**/quickstart.spec.ts",
      testIgnore: "test/e2e/common/setup.spec.ts",
    },
  ],
});
