import * as duckdb from '@duckdb/duckdb-wasm';
import * as arrow from 'apache-arrow';
import {AsyncDuckDB, AsyncDuckDBConnection, DuckDBDataProtocol} from '@duckdb/duckdb-wasm';
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
    await conn.close()
    _db = db
    return _db
}


// taken from @duckdb/duckdb-wasm/dist/types/src/bindings/tokens.d.ts
// which, unfortunately, we cannot import.
const DUCKDB_STRING_CONSTANT = 2;
const LAKEFS_URI_PATTERN = /^(['"]?)(lakefs:\/\/(.*))(['"])$/;

// returns a mapping of `lakefs://..` URIs to their `s3://...` equivalent
async function extractFiles(conn: AsyncDuckDBConnection, sql: string): Promise<{ [name: string]: string }> {
    const tokenized = await conn.bindings.tokenize(sql)
    let prev = 0;
    const fileMap: { [name: string]: string } = {};
    tokenized.offsets.forEach((offset, i) => {
        let currentToken = sql.length;
        if (i < tokenized.offsets.length - 1) {
            currentToken = tokenized.offsets[i+1];
        }
        const part = sql.substring(prev, currentToken);
        prev = currentToken;
        if (tokenized.types[i] === DUCKDB_STRING_CONSTANT) {
            const matches = part.match(LAKEFS_URI_PATTERN)
            if (matches !== null) {
                fileMap[matches[2]] = `s3://${matches[3]}`;
            }
        }
    })
    return fileMap
}

/* eslint-disable  @typescript-eslint/no-explicit-any */
export async function runDuckDBQuery(sql: string):  Promise<arrow.Table<any>> {
    const db = await getDuckDB()
    /* eslint-disable  @typescript-eslint/no-explicit-any */
    let result: arrow.Table<any>
    const conn  = await db.connect()
    try {
        // TODO (ozk): read this from the server's configuration?
        await conn.query(`SET s3_region='us-east-1';`)
        // set the example values (used to make sure the S3 gateway picks up the request)
        // real authentication is done using the existing swagger cookie or token
        await conn.query(`SET s3_access_key_id='use_swagger_credentials';`)
        await conn.query(`SET s3_secret_access_key='these_are_meaningless_but_must_be_set';`)
        await conn.query(`SET s3_endpoint='${document.location.protocol}//${document.location.host}'`)

        // register lakefs uri-ed files as s3 files
        const fileMap = await extractFiles(conn, sql)
        const fileNames = Object.getOwnPropertyNames(fileMap)
        await Promise.all(fileNames.map(
            fileName => db.registerFileURL(fileName, fileMap[fileName], DuckDBDataProtocol.S3, true)
        ))
        // execute the query
        result = await conn.query(sql)

        // remove registrations
        await Promise.all(fileNames.map(fileName => db.dropFile(fileName)))
    } finally {
        await closeDuckDBConnection(conn)
    }
    return result
}

async function closeDuckDBConnection(conn: AsyncDuckDBConnection | null) {
    if (conn !== null) {
        await conn.close()
    }
    const db = await getDuckDB()
    await db.flushFiles()
    await db.dropFiles()
}
