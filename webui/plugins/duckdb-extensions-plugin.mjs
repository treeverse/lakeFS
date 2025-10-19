import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import https from "https";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// List of core extensions to bundle
const EXTENSIONS = ["parquet", "json", "httpfs", "icu", "avro"];

// Platforms to download for
const PLATFORMS = ["wasm_eh", "wasm_mvp"];

// We verify that in case the package version changes we update the duckdb version.
// Update the following when you update the WASM package version.
// See: https://github.com/duckdb/duckdb-wasm/releases
const WASM_PACKAGE_VERSION = "1.30.0";
const DUCKDB_VERSION = "1.3.2";

// Get DuckDB WASM version from installed package
function getWasmPackageVersion() {
  const packageJsonPath = path.join(
    __dirname,
    "../node_modules/@duckdb/duckdb-wasm/package.json",
  );
  const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));
  return packageJson.version;
}

// Download file from URL
async function downloadFile(url, destPath) {
  return new Promise((resolve, reject) => {
    https
      .get(url, (response) => {
        if (response.statusCode === 404) {
          reject(new Error(`File not found: ${url}`));
        } else if (response.statusCode !== 200) {
          reject(
            new Error(`Failed to download ${url}: ${response.statusCode}`),
          );
        }
        const file = fs.createWriteStream(destPath);
        response.pipe(file);
        file.on("finish", () => {
          file.close();
          resolve();
        });
        file.on("error", reject);
      })
      .on("error", reject);
  });
}

// Main plugin
export default function duckdbExtensionsPlugin() {
  let isBuilding = false;

  return {
    name: "duckdb-extensions-plugin",
    async buildStart() {
      // Only run during build, not during serve
      if (this.meta.watchMode) {
        return;
      }

      isBuilding = true;
    },
    async generateBundle() {
      if (!isBuilding) {
        return;
      }

      const wasmPackageVersion = getWasmPackageVersion();
      if (wasmPackageVersion !== WASM_PACKAGE_VERSION) {
        throw new Error(
          `DuckDB WASM package version mismatch. Please update the WASM_PACKAGE_VERSION and the relevant DUCKDB_VERSION.`,
        );
      }
      const baseUrl = "https://extensions.duckdb.org";

      console.log(`\nBundling DuckDB WASM extensions...`);
      console.log(`  WASM version: @duckdb/duckdb-wasm@${wasmPackageVersion}`);
      console.log(`  DuckDB version: ${DUCKDB_VERSION}`);

      // Create output directory structure
      const outDir = path.join(
        __dirname,
        `../dist/duckdb-wasm/v${DUCKDB_VERSION}`,
      );
      if (!fs.existsSync(outDir)) {
        fs.mkdirSync(outDir, { recursive: true });
      }

      let failCount = 0;

      for (const platform of PLATFORMS) {
        const platformDir = path.join(outDir, platform);
        if (!fs.existsSync(platformDir)) {
          fs.mkdirSync(platformDir, { recursive: true });
        }

        for (const ext of EXTENSIONS) {
          const fileName = `${ext}.duckdb_extension.wasm`;
          const url = `${baseUrl}/v${DUCKDB_VERSION}/${platform}/${fileName}`;
          const filePath = path.join(platformDir, fileName);
          // console.log(`  Downloading from: ${url}`);

          try {
            await downloadFile(url, filePath);
          } catch (error) {
            console.warn(`      ❌ ${error.message}`);
            failCount++;
          }
        }
      }

      if (failCount > 0) {
        throw new Error(
          `\nFailed to download ${failCount} extensions for DuckDB v${DUCKDB_VERSION}. Make sure the version is available on extensions.duckdb.org.`,
        );
      }

      console.log(`\nAll extensions bundled successfully at /duckdb-wasm\n`);
    },
  };
}
