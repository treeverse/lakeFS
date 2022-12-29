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


let _db: duckdb.AsyncDuckDB | null
let _worker: Worker | null

async function getDB(): Promise<duckdb.AsyncDuckDB> {
    if (!_db) {
        const bundle = await duckdb.selectBundle(MANUAL_BUNDLES)
        if (!bundle.mainWorker) {
            throw Error("could not initialize DuckDB")
        }
        _worker = new Worker(bundle.mainWorker);
        const logger = new duckdb.VoidLogger();
        const db = new duckdb.AsyncDuckDB(logger, _worker);
        await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
        const conn = await db.connect()
        await conn.query(`CREATE MACRO lakefs_object(repoId, refId, path) AS '${document.location.protocol}//${document.location.host}/api/v1/repositories/' || repoId || '/refs/' || refId || '/objects?path=' || replace(path, '/', '%2F');`)
        await conn.close()
        _db = db
    }
    return _db
}

async function teardownDB() {
    if (_db) {
        await _db.terminate()
        _db = null
    }
    if (_worker) {
        await _worker.terminate()
        _worker = null
    }
}

export async function withConnection(cb: (conn: duckdb.AsyncDuckDBConnection) => void) {
    // Instantiate the async version of DuckDB-wasm
    const db = await getDB()
    const conn = await db.connect()
    const results = await cb(conn)
    await conn.close()
    return results
}
