import * as duckdb from '@duckdb/duckdb-wasm';
import duckdb_wasm from '@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url';
import mvp_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url';
import duckdb_wasm_eh from '@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url';
import eh_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url';


// based on the replacement rules on the percent-encoding MDN page:
// https://developer.mozilla.org/en-US/docs/Glossary/Percent-encoding
// also, I tried doing something nicer with list comprehensions and printf('%x') to convert
//  from unicode code point to hex - DuckDB didn't seem to evaluate lambdas and list comprehensions
// Issue: https://github.com/duckdb/duckdb/issues/5821
// when padding a macro to a table function such as read_parquet() or read_csv().
// so - string replacements it is.
const URL_ENCODE_MACRO_SQL = `
CREATE MACRO p_encode(s) AS
    replace(
        replace(
            replace(
                replace(
                    replace(
                        replace(
                            replace(
                                replace(
                                    replace(
                                        replace(
                                            replace(
                                                replace(
                                                    replace(
                                                        replace(
                                                            replace(
                                                                replace(
                                                                    replace(
                                                                        replace(
                                                                            replace(s, '%', '%25'),
                                                                            '/', '%2F'),
                                                                        '?', '%3F'),
                                                                    '#', '%23'),
                                                                '[', '%5B'),
                                                            ']', '%5D'),
                                                        '@', '%40'),
                                                    '!', '%21'),
                                                '$', '%24'),
                                            '&', '%26'),
                                        '''', '%27'),
                                    '(', '%28'),
                                ')', '%29'),
                            '+', '%2B'),
                        ',', '%2C'),
                    ';', '%3B'),
                '=','%3D'),
            ' ', '%20'),
        ':', '%3A');
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


let _db: duckdb.AsyncDuckDB | null
let _worker: Worker | null

async function getDB(): Promise<duckdb.AsyncDuckDB> {
    if (!_db) {
        const bundle = await duckdb.selectBundle(MANUAL_BUNDLES)
        if (!bundle.mainWorker) {
            throw Error("could not initialize DuckDB")
        }
        _worker = new Worker(bundle.mainWorker);
        const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), _worker);
        await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
        const conn = await db.connect()
        // await conn.query(`SET access_mode = READ_ONLY`)
        await conn.query(URL_ENCODE_MACRO_SQL)
        await conn.query(`
            CREATE MACRO lakefs_object(repoId, refId, path) AS
                '${document.location.protocol}//${document.location.host}/api/v1/repositories/' ||
                p_encode(repoId) || '/refs/' || p_encode(refId) || '/objects?path=' || p_encode(path);
        `)
        await conn.close()
        _db = db
    }
    return _db
}

export async function getConnection(): Promise<duckdb.AsyncDuckDBConnection> {
    // Instantiate the async version of DuckDB-wasm
    const db = await getDB()
    return await db.connect()
}
