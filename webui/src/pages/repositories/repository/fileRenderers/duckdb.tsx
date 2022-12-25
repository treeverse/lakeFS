import * as duckdb from '@duckdb/duckdb-wasm';
import duckdb_wasm from '@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url';
import mvp_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url';
import duckdb_wasm_eh from '@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url';
import eh_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url';



const MANUAL_BUNDLES: duckdb.DuckDBBundles = {
    mvp: {
        mainModule: duckdb_wasm,
        mainWorker: mvp_worker,
    },
    eh: {
        mainModule: duckdb_wasm_eh,
        mainWorker: eh_worker,
    },
};
// Select a bundle based on browser checks
let _bundle: duckdb.DuckDBBundle;
let bundlePromise: Promise<duckdb.DuckDBBundle>;

function getBundle(): Promise<duckdb.DuckDBBundle> {
    if (_bundle) return new Promise(() => _bundle)
    if (!bundlePromise) {
        bundlePromise = duckdb.selectBundle(MANUAL_BUNDLES)
    }
    return bundlePromise
}

export async function withConnection(cb: (conn: duckdb.AsyncDuckDBConnection) => void) {
    // Instantiate the async version of DuckDB-wasm
    const bundle = await getBundle();
    if (!bundle.mainWorker) {
        throw Error("could not initialize DuckDB")
    }
    const worker = new Worker(bundle.mainWorker);
    const logger = new duckdb.VoidLogger();
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    const conn = await db.connect()
    await conn.query(`CREATE MACRO lakefs_object(repoId, refId, path) AS '${document.location.protocol}//${document.location.host}/api/v1/repositories/' || repoId || '/refs/' || refId || '/objects?path=' || replace(path, '/', '%2F');`)
    const results = await cb(conn)
    await conn.close()
    await db.terminate()
    await worker.terminate()
    return results
}
