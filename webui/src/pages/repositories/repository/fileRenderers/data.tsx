import React, {FC, useEffect, useState} from "react";
import {Error, Loading} from "../../../../lib/components/controls";
import {withConnection} from "./duckdb";
import {Table} from "react-bootstrap";
import Alert from "react-bootstrap/Alert";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import {DatabaseIcon} from "@primer/octicons-react";
import dayjs from "dayjs";
import {RendererComponent} from "./types";

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
    const [query, setQuery] = useState<string>(initialQuery)
    const [shouldSubmit, setShouldSubmit] = useState<boolean>(true)
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const [data, setData] = useState<any[] | null>(null);
    const [error, setError] = useState<string | null>(null)

    useEffect(() => {
        withConnection(async conn => {
            const results = await conn.query(query)
            const data = results.toArray()
            setData(data)
            setError(null)
        }).catch(e => {
            setError(e.toString())
        })
    }, [repoId, refId, path, shouldSubmit])

    let content;
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
            if (totalRows > 100) {
                res = data.slice(0, 100)
            }
            content = (
                <>
                    <Table striped bordered hover size={"sm"} responsive={"sm"}>
                        <thead>
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
                    {(res.length < data.length) &&
                        <Alert>{`Showing only the first ${res.length} rows (out of ${data.length})`}</Alert>
                    }
                </>
            )
        }
    }


    return (
        <div>
            <Form onSubmit={(e) => {
                e.preventDefault()
                setShouldSubmit(!shouldSubmit)
            }}>
                <Form.Group className="mb-2 mt-2" controlId="objectQuery">
                    <Form.Control as="textarea" className="object-viewer-sql-input" rows={5} value={query} spellCheck={false} onChange={e => {
                        setQuery(e.target.value)
                    }} />
                    <Form.Text className="text-muted align-right">
                        Powered by <a href="https://duckdb.org/2021/10/29/duckdb-wasm.html" target="_blank" rel="noreferrer">DuckDB-WASM</a>.
                        For a full SQL reference, see the <a href="https://duckdb.org/docs/sql/statements/select" target="_blank" rel="noreferrer">DuckDB Documentation</a>
                    </Form.Text>
                </Form.Group>
                <Button type="submit" variant={"success"} >
                    <DatabaseIcon/>{' '}
                    Execute
                </Button>
            </Form>
            <div className={"mt-3 object-viewer-sql-results"}>
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