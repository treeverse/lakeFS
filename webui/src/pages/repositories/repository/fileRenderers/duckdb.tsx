import * as duckdb from '@duckdb/duckdb-wasm';
import duckdb_wasm from '@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url';
import mvp_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url';
import duckdb_wasm_eh from '@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url';
import eh_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url';
import {AsyncDuckDB, AsyncDuckDBConnection} from "@duckdb/duckdb-wasm";


// based on the replacement rules on the percent-encoding MDN page:
// https://developer.mozilla.org/en-US/docs/Glossary/Percent-encoding
// also, I tried doing something nicer with list comprehensions and printf('%x') to convert
//  from unicode code point to hex - DuckDB didn't seem to evaluate lambdas and list comprehensions
// Issue: https://github.com/duckdb/duckdb/issues/5821
// when padding a macro to a table function such as read_parquet() or read_csv().
// so - string replacements it is.
const DUCKDB_SEED_SQL = `
CREATE MACRO p_encode(s) AS 
    list_aggregate([
        case when x in (':', '/', '?', '#', '[', ']', '@', '!', '$', '&', '''', '(', ')', '*', '+', ',', ';', '=', '%', ' ') 
            then printf('%%%X', unicode(x))  else x end
        for x 
        in string_split(s, '')
    ], 'string_agg', '');
    
CREATE MACRO lakefs_object(repoId, refId, path) AS
    '${document.location.protocol}//${document.location.host}/api/v1/repositories/' ||
    p_encode(repoId) || '/refs/' || p_encode(refId) || '/objects?path=' || p_encode(path);
`

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

let _db: AsyncDuckDB | null = null;

async function getDuckDB(): Promise<duckdb.AsyncDuckDB> {
    if (_db !== null) {
        return _db
    }
    const bundle = await duckdb.selectBundle(MANUAL_BUNDLES)
    if (!bundle.mainWorker) {
        throw Error("could not initialize DuckDB")
    }
    const worker = new Worker(bundle.mainWorker)
    const logger = new duckdb.VoidLogger()
    const db = new duckdb.AsyncDuckDB(logger, worker)
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker)
    const conn = await db.connect()
    await conn.query(DUCKDB_SEED_SQL)
    await conn.close()
    _db = db
    return _db
}

export async function getDuckDBConnection(): Promise<duckdb.AsyncDuckDBConnection> {
    const db = await getDuckDB()
    return db.connect()
}

export async function closeDuckDBConnection(conn: AsyncDuckDBConnection | null) {
    if (conn !== null) {
        await conn.close()
    }
    const db = await getDuckDB()
    await db.flushFiles()
    await db.dropFiles()
}
