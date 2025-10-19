import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import https from "https";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// List of core extensions to bundle
const EXTENSIONS = [
    "parquet",
    "json",
    "httpfs",
    "icu",
    "fts",
    "spatial",
    "excel",
];

// Platforms to download for
const PLATFORMS = ["wasm_eh", "wasm_mvp"];

// Mapping of @duckdb/duckdb-wasm versions to supported DuckDB versions
// See: https://github.com/duckdb/duckdb-wasm/releases
const WASM_TO_DUCKDB_VERSION = {
    "1.30.0": "1.3.2", // DuckDB WASM 1.30.0 is based on DuckDB 1.3.2
    "1.29.0": "1.3.2",
    "1.28.0": "1.3.1",
};

// Get DuckDB WASM version from installed package
function getWasmVersion() {
    const packageJsonPath = path.join(
        __dirname,
        "../node_modules/@duckdb/duckdb-wasm/package.json"
    );
    const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));
    return packageJson.version;
}

// Get the supported DuckDB version for the installed WASM version
function getSupportedDuckDBVersion() {
    const wasmVersion = getWasmVersion();
    const duckdbVersion = WASM_TO_DUCKDB_VERSION[wasmVersion];

    if (!duckdbVersion) {
        throw new Error(
            `Unknown DuckDB WASM version: ${wasmVersion}. Please add it to WASM_TO_DUCKDB_VERSION mapping.`
        );
    }

    return duckdbVersion;
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

            const wasmVersion = getWasmVersion();
            const duckdbVersion = getSupportedDuckDBVersion();
            const baseUrl = "https://extensions.duckdb.org";

            console.log(`\nBundling DuckDB WASM extensions...`);
            console.log(`  WASM version: @duckdb/duckdb-wasm@${wasmVersion}`);
            console.log(`  DuckDB version: ${duckdbVersion}`);
            console.log(`  Downloading from: ${baseUrl}/v${duckdbVersion}/{platform}/{extension}`);

            // Create output directory structure
            const outDir = path.join(__dirname, "../dist/duckdb-wasm");
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
                    const url = `${baseUrl}/v${duckdbVersion}/${platform}/${fileName}`;
                    const filePath = path.join(platformDir, fileName);

                    try {
                        console.log(`    ${ext} (${platform})`);
                        await downloadFile(url, filePath);
                        console.log(`      Downloaded`);
                    } catch (error) {
                        console.warn(`      ❌ ${error.message}`);
                        failCount++;
                    }
                }
            }

            if (failCount > 0) {
                throw new Error(
                    `\nFailed to download ${failCount} extensions for DuckDB v${duckdbVersion}. Make sure the version is available on extensions.duckdb.org.`
                );
            }

            console.log(`\nAll extensions bundled successfully at /duckdb-wasm\n`);
        },
    };
}
