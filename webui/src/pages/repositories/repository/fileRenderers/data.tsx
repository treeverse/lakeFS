import React, {FC, FormEvent, useCallback, useEffect, useRef, useState} from "react";
import {Error, Loading} from "../../../../lib/components/controls";
import {withConnection} from "./duckdb";
import {Table} from "react-bootstrap";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import {DatabaseIcon} from "@primer/octicons-react";
import dayjs from "dayjs";
import {RendererComponent} from "./types";


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
    const [data, setData] = useState<any[] | null>(null);
    const [error, setError] = useState<string | null>(null)
    const [loading, setLoading] = useState<boolean>(false)

    const sql = useRef<HTMLTextAreaElement>(null);

    const handleSubmit = useCallback((event: FormEvent<HTMLFormElement>) => {
        event.preventDefault()
        setShouldSubmit(!shouldSubmit)
    }, [setShouldSubmit])

    useEffect(() => {
        setLoading(true)
        withConnection(async conn => {
            if (!sql || !sql.current) return
            const results = await conn.query(sql.current.value)
            const data = results.toArray()
            setData(data)
            setError(null)

        }).catch(e => {
            setError(e.toString())
        }).finally(() => {
            setLoading(false)
        })
    }, [repoId, refId, path, shouldSubmit])


    let content;
    let button = (
        <Button type="submit" variant={"success"} >
            <DatabaseIcon/>{' '}
            Execute
        </Button>
    )
    if (loading) {
        button = (
            <Button type="submit" variant={"success"} disabled>
                <DatabaseIcon/>{' '}
                Executing...
            </Button>
        )
    }

    if (error) {
        content = <Error error={error}/>
    } else if (data === null) {
        content = <DataLoader/>
    } else {
        if (!data || data.length === 0) {
            content = (
                <p className={"text-md-center mt-5 mb-5"}>
                    No rows returned.
                </p>
            )
        } else {
            const totalRows = data.length
            let res = data;
            if (totalRows > MAX_RESULTS_RETURNED) {
                res = data.slice(0, MAX_RESULTS_RETURNED)
            }
            content = (
                <>
                    {(res.length < data.length) &&
                        <small>{`Showing only the first ${res.length.toLocaleString()} rows (out of ${data.length.toLocaleString()})`}</small>
                    }
                    <div className="object-viewer-sql-results">
                        <Table striped bordered hover size={"sm"} responsive={"sm"} className="sticky-table">
                            <thead className="thead-dark">
                            <tr>
                                {Object.getOwnPropertyNames(res[0]).map(name =>
                                    <th key={name}>{name}</th>
                                )}
                            </tr>
                            </thead>
                            <tbody>
                            {res.map((row, i) => (
                                <tr key={`row-${i}`}>
                                    {Object.getOwnPropertyNames(res[0]).map(name => (
                                        <DataRow key={`row-${i}-${name}`} value={row[name]}/>
                                    ))}
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
                    <Form.Control as="textarea" className="object-viewer-sql-input" rows={5} defaultValue={initialQuery} spellCheck={false} ref={sql} />

                    <Form.Text className="text-muted align-right">
                        Powered by <a href="https://duckdb.org/2021/10/29/duckdb-wasm.html" target="_blank" rel="noreferrer">DuckDB-WASM</a>.
                        For a full SQL reference, see the <a href="https://duckdb.org/docs/sql/statements/select" target="_blank" rel="noreferrer">DuckDB Documentation</a>
                    </Form.Text>

                </Form.Group>
                {button}
            </Form>
            <div className={"mt-3"}>
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