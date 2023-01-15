import React, {FC, FormEvent, useCallback, useEffect, useRef, useState} from "react";
import {Error, Loading} from "../../../../lib/components/controls";
import {getConnection} from "./duckdb";
import * as duckdb from '@duckdb/duckdb-wasm';
import * as arrow from 'apache-arrow';
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import {DatabaseIcon} from "@primer/octicons-react";
import dayjs from "dayjs";
import {RendererComponent} from "./types";
import Table from "react-bootstrap/Table";


const MAX_RESULTS_RETURNED = 1000;

export const DataLoader: FC = () => {
    return <Loading/>
}

export const DuckDBRenderer: FC<RendererComponent> = ({repoId, refId, path, fileExtension }) => {
    let initialQuery = `SELECT * 
FROM read_parquet(lakefs_object('${repoId}', '${refId}', '${path}')) 
LIMIT 20`;
    if (fileExtension === 'csv') {
        initialQuery = `SELECT * 
FROM read_csv(lakefs_object('${repoId}', '${refId}', '${path}'), AUTO_DETECT = TRUE) 
LIMIT 20`
    } else if (fileExtension === 'tsv') {
        initialQuery = `SELECT * 
FROM read_csv(lakefs_object('${repoId}', '${refId}', '${path}'), DELIM='\t', AUTO_DETECT=TRUE) 
LIMIT 20`
    }
    const [shouldSubmit, setShouldSubmit] = useState<boolean>(true)
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const [data, setData] = useState<arrow.Table<any> | null>(null);
    const [error, setError] = useState<string | null>(null)
    const [loading, setLoading] = useState<boolean>(false)

    const sql = useRef<HTMLTextAreaElement>(null);

    const handleSubmit = useCallback((event: FormEvent<HTMLFormElement>) => {
        event.preventDefault()
        setShouldSubmit(prev => !prev)
    }, [setShouldSubmit])

    useEffect(() => {
        if (!sql || !sql.current)
            return
        const runQuery = async (sql: string) => {
            setLoading(true)
            setError(null)
            let conn: duckdb.AsyncDuckDBConnection | null
            try {
                conn =  await getConnection()
            } catch (e) {
                setData(null)
                setError(e.toString())
                setLoading(false)
                return
            }

            try {
                const results = await conn.query(sql)
                setData(results)
                setError(null)
            } catch (e) {
                setError(e.toString())
                setData(null)
            } finally {
                setLoading(false)
                if (conn !== null)
                    await conn.close()
            }
        }
        runQuery(sql.current.value).catch(console.error);
    }, [repoId, refId, path, shouldSubmit])

    let content;
    const button = (
        <Button type="submit" variant="success" disabled={loading}>
            <DatabaseIcon /> {" "}
            { loading ? "Executing..." : "Execute" }
        </Button>
    );

    if (error) {
        content = <Error error={error}/>
    } else if (data === null) {
        content = <DataLoader/>
    } else {

        if (!data || data.numRows === 0) {
            content = (
                <p className="text-md-center mt-5 mb-5">
                    No rows returned.
                </p>
            )
        } else {
            const fields = data.schema.fields
            const totalRows = data.numRows
            let res = data;
            if (totalRows > MAX_RESULTS_RETURNED) {
                res = data.slice(0, MAX_RESULTS_RETURNED)
            }
            content = (
                <>
                    {(res.numRows < data.numRows) &&
                        <small>{`Showing only the first ${res.numRows.toLocaleString()} rows (out of ${data.numRows.toLocaleString()})`}</small>
                    }
                    <div className="object-viewer-sql-results">
                        <Table striped bordered hover size={"sm"} responsive={true}>
                            <thead className="table-dark">
                            <tr>
                                {fields.map((field, i) =>
                                    <th key={i}>
                                        {field.name}
                                        <br/>
                                        <small>{field.type.toString()}</small>
                                    </th>
                                )}
                            </tr>
                            </thead>
                            <tbody>
                            {[...res].map((row, i) => (
                                <tr key={`row-${i}`}>
                                    {[...row].map((v, j: number) => {
                                        return (
                                            <DataRow key={`col-${i}-${j}`} value={v[1]}/>
                                        )

                                    })}
                                </tr>
                            ))}
                            </tbody>
                        </Table>
                    </div>
                </>
            )
        }
    }

    return (
        <div>
            <Form onSubmit={handleSubmit}>
                <Form.Group className="mb-2 mt-2" controlId="objectQuery">
                    <Form.Control as="textarea" className="object-viewer-sql-input" rows={5} defaultValue={initialQuery} spellCheck={false} ref={sql} autoComplete="off"/>

                    <Form.Text className="text-muted align-right">
                        Powered by <a href="https://duckdb.org/2021/10/29/duckdb-wasm.html" target="_blank" rel="noreferrer">DuckDB-WASM</a>.
                        For a full SQL reference, see the <a href="https://duckdb.org/docs/sql/statements/select" target="_blank" rel="noreferrer">DuckDB Documentation</a>
                    </Form.Text>

                </Form.Group>
                {button}
            </Form>
            <div className="mt-3">
                {content}
            </div>
        </div>
    )
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const DataRow: FC<{ value: any }> = ({ value }) => {
    let dataType = 'regular';
    if (typeof value === 'string') {
        dataType = 'string';
    } else if (value instanceof Date) {
        dataType = 'date'
    } else if (typeof value === 'number') {
        dataType = 'number'
    }

    if (dataType === 'string') {
        return <td>{value}</td>
    }

    if (dataType === 'date') {
        return <td>{dayjs(value).format()}</td>
    }

    if (dataType === 'number') {
        return <td>{value.toLocaleString("en-US")}</td>
    }

    return <td>{""  + value}</td>;
}