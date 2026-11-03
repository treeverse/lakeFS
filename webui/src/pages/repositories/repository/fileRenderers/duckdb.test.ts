import { describe, it, expect } from 'vitest';
import { extractFiles } from './duckdb';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';

describe('extractFiles', () => {
    it('handles CJK non-ASCII characters without offset misalignment', async () => {
        const sql = "SELECT * FROM 'lakefs://my-repo/main/名前=1/data.parquet'";
        const encoded = new TextEncoder().encode(sql);
        // String literal starts at index 14 ('...') and ends at encoded.length
        const mockConn = {
            bindings: {
                tokenize: async () => ({
                    offsets: [0, 14],
                    types: [0, 2], // 2 = DUCKDB_STRING_CONSTANT
                }),
            },
        } as unknown as AsyncDuckDBConnection;

        const fileMap = await extractFiles(mockConn, sql);
        expect(fileMap).toHaveProperty('lakefs://my-repo/main/名前=1/data.parquet');
        const s3Uri = fileMap['lakefs://my-repo/main/名前=1/data.parquet'];
        expect(s3Uri).toMatch(/^s3:\/\/my-repo\/main\/%E5%90%8D%E5%89%8D%3D1\/data\.parquet\?r=/);
    });

    it('correctly preserves and encodes spark %05d patterns', async () => {
        const sql = "SELECT * FROM 'lakefs://my-repo/main/part-0%05d-data.parquet'";
        const mockConn = {
            bindings: {
                tokenize: async () => ({
                    offsets: [0, 14],
                    types: [0, 2], // 2 = DUCKDB_STRING_CONSTANT
                }),
            },
        } as unknown as AsyncDuckDBConnection;

        const fileMap = await extractFiles(mockConn, sql);
        expect(fileMap).toHaveProperty('lakefs://my-repo/main/part-0%05d-data.parquet');
        const s3Uri = fileMap['lakefs://my-repo/main/part-0%05d-data.parquet'];
        expect(s3Uri).toMatch(/^s3:\/\/my-repo\/main\/part-0%2505d-data\.parquet\?r=/);
    });
});
