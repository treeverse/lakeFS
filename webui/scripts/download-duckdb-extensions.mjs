#!/usr/bin/env node

// Downloads DuckDB WASM extensions so they can be served from the same origin
// as the lakeFS UI, enabling air-gapped / on-prem deployments without access
// to extensions.duckdb.org or community-extensions.duckdb.org.
//
// Extensions are placed under pub/duckdb-extensions/ which Vite copies into
// dist/ at build time.

import fs from 'node:fs';
import path from 'node:path';
import { pipeline } from 'node:stream/promises';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

// @duckdb/duckdb-wasm npm version -> DuckDB core version tag.
// The core version determines the extension repository path.
// Update both values when upgrading @duckdb/duckdb-wasm.
const DUCKDB_WASM_VERSION = '1.32.0';
const DUCKDB_CORE_VERSION = 'v1.4.3';

const EXTENSIONS = ['parquet', 'json', 'icu', 'arrow'];
const PLATFORMS = ['wasm_eh', 'wasm_mvp'];
const EXTENSION_REPOS = [
    'https://extensions.duckdb.org',
    'https://community-extensions.duckdb.org',
];

function getInstalledVersion() {
    const pkgPath = path.join(ROOT, 'node_modules', '@duckdb', 'duckdb-wasm', 'package.json');
    const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf-8'));
    return pkg.version;
}

// Returns true if downloaded, false if the extension is not available (404).
async function download(url, dest) {
    const res = await fetch(url);
    if (res.status === 404) {
        return false;
    }
    if (!res.ok) {
        throw new Error(`Failed to download ${url}: ${res.status} ${res.statusText}`);
    }
    fs.mkdirSync(path.dirname(dest), { recursive: true });
    const fileStream = fs.createWriteStream(dest);
    await pipeline(res.body, fileStream);
    return true;
}

async function main() {
    const wasmVersion = getInstalledVersion();
    if (wasmVersion !== DUCKDB_WASM_VERSION) {
        console.error(
            `@duckdb/duckdb-wasm version ${wasmVersion} does not match expected ${DUCKDB_WASM_VERSION}. ` +
            `Update DUCKDB_WASM_VERSION and DUCKDB_CORE_VERSION in ${path.basename(fileURLToPath(import.meta.url))}.`
        );
        process.exit(1);
    }

    const coreVersion = DUCKDB_CORE_VERSION;
    console.log(`DuckDB WASM ${wasmVersion} -> core ${coreVersion}`);

    const outDir = path.join(ROOT, 'pub', 'duckdb-extensions');

    let allPresent = true;
    const toDownload = [];
    for (const platform of PLATFORMS) {
        for (const ext of EXTENSIONS) {
            const relPath = path.join(coreVersion, platform, `${ext}.duckdb_extension.wasm`);
            const dest = path.join(outDir, relPath);
            if (fs.existsSync(dest) || fs.existsSync(dest + '.unavailable')) {
                continue;
            }
            allPresent = false;
            toDownload.push({ relPath, dest });
        }
    }

    if (allPresent) {
        console.log('All DuckDB extensions already present, skipping download.');
        return;
    }

    console.log(`Downloading ${toDownload.length} extension file(s)...`);
    for (const { relPath, dest } of toDownload) {
        process.stdout.write(`  ${relPath} ... `);
        let ok = false;
        for (const repo of EXTENSION_REPOS) {
            ok = await download(`${repo}/${relPath}`, dest);
            if (ok) break;
        }
        if (ok) {
            console.log('ok');
        } else {
            // Mark as unavailable so we don't retry on every build.
            fs.mkdirSync(path.dirname(dest), { recursive: true });
            fs.writeFileSync(dest + '.unavailable', '');
            console.log('not available (skipped)');
        }
    }

    console.log('Done.');
}

main();
