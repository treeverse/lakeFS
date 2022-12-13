import * as duckdb from '@duckdb/duckdb-wasm';
import duckdb_wasm from '@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url';
import mvp_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url';
import duckdb_wasm_eh from '@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url';
import eh_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url';
import {VoidLogger} from "@duckdb/duckdb-wasm/dist/types/src/log";

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
const bundle = await duckdb.selectBundle(MANUAL_BUNDLES);


export async function sampleParquet(filename: string, content: Blob) {
    // Instantiate the async version of DuckDB-wasm
    const worker = new Worker(bundle.mainWorker!);
    const logger = new duckdb.VoidLogger();
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    const buff = new Uint8Array(await content.arrayBuffer())
    await db.registerFileBuffer(filename, buff)
    const c = await db.connect()
    const results =  {
        sample: (await c.query(`SELECT * FROM "${filename}" LIMIT 100`)).toArray(),
        totalRows: (await c.query(`SELECT COUNT(*) total FROM "${filename}"`)).toArray(),
        schema: (await c.query(`DESCRIBE SELECT * FROM "${filename}"`)).toArray(),
    }
    await c.close()
    await db.terminate()
    return results;
}

