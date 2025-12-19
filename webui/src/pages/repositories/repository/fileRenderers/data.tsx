import React, { FC, FormEvent, useCallback, useEffect, useState } from "react";
import { runDuckDBQuery } from "./duckdb";
import * as arrow from "apache-arrow";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import { ChevronRightIcon, HistoryIcon } from "@primer/octicons-react";
import dayjs from "dayjs";
import Table from "react-bootstrap/Table";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";

import { SQLEditor } from "./editor";
import { RendererComponent } from "./types";
import { AlertError, Loading } from "../../../../lib/components/controls";

// File size threshold for using LIMIT queries (1MB)
const LARGE_FILE_THRESHOLD = 1024 * 1024;

// Default row limit for large files
const DEFAULT_ROW_LIMIT = 1000;

export const DataLoader: FC = () => {
  return <Loading />;
};

// Query templates - each type has a limited and full version
const QUERY_TEMPLATES: { [key: string]: string } = {
  parquet: `SELECT * FROM READ_PARQUET('lakefs://{{repo}}/{{ref}}/{{path}}', hive_partitioning=false)`,
  csv: `SELECT * FROM READ_CSV('lakefs://{{repo}}/{{ref}}/{{path}}', AUTO_DETECT=TRUE)`,
  tsv: `SELECT * FROM READ_CSV('lakefs://{{repo}}/{{ref}}/{{path}}', DELIM='\\t', AUTO_DETECT=TRUE)`,
};

export const DuckDBRenderer: FC<RendererComponent> = ({
  repoId,
  refId,
  path,
  fileExtension,
  sizeBytes,
}) => {
  const isLargeFile = sizeBytes > LARGE_FILE_THRESHOLD;
  const [useFullQuery, setUseFullQuery] = useState(!isLargeFile);

  // Apply template variables to generate the actual query
  const applyTemplate = (template: string): string => {
    return template
      .replace(/\{\{repo\}\}/g, repoId)
      .replace(/\{\{ref\}\}/g, refId)
      .replace(/\{\{path\}\}/g, path);
  };

  // Get the appropriate query based on file size and user preference
  const getQuery = (forceFullQuery: boolean = false): string => {
    let template = QUERY_TEMPLATES[fileExtension] || QUERY_TEMPLATES.parquet;
    if (!forceFullQuery) {
      template += " LIMIT " + DEFAULT_ROW_LIMIT;
    }
    return applyTemplate(template);
  };

  const initialQuery = getQuery();
  const [sql, setSql] = useState(initialQuery);
  const [shouldSubmit, setShouldSubmit] = useState<boolean>(true);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const [data, setData] = useState<arrow.Table<any> | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(false);

  const handleSubmit = useCallback(
    (event: FormEvent<HTMLFormElement>) => {
      event.preventDefault();
      setShouldSubmit((prev) => !prev);
    },
    [setShouldSubmit],
  );

  const handleRun = useCallback(() => {
    setShouldSubmit((prev) => !prev);
  }, [setShouldSubmit]);

  const sqlChangeHandler = useCallback(
    (data: React.SetStateAction<string>) => {
      setSql(data);
    },
    [setSql],
  );

  // Reset query to the default (based on current useFullQuery state)
  const handleResetQuery = useCallback(() => {
    const query = getQuery();
    setSql(query);
  }, [useFullQuery, repoId, refId, path, fileExtension]);

  // Handle loading full data for large files
  const handleLoadFullData = useCallback(() => {
    setUseFullQuery(true);
    const fullQuery = getQuery(true);
    setSql(fullQuery);
    setShouldSubmit((prev) => !prev);
  }, [repoId, refId, path, fileExtension]);

  useEffect(() => {
    if (!sql) {
      return;
    }
    const runQuery = async (sql: string) => {
      setLoading(true);
      setError(null);
      try {
        const results = await runDuckDBQuery(sql);
        setData(results);
      } catch (e) {
        setError(e.toString());
        setData(null);
      } finally {
        setLoading(false);
      }
    };
    runQuery(sql).catch(console.error);
  }, [repoId, refId, path, shouldSubmit]);

  let content;
  const button = (
    <Button type="submit" variant="success" disabled={loading}>
      <ChevronRightIcon /> {loading ? "Executing..." : "Execute"}
    </Button>
  );

  if (error) {
    content = <AlertError error={error} />;
  } else if (data === null) {
    content = <DataLoader />;
  } else {
    if (!data || data.numRows === 0) {
      content = <p className="text-md-center mt-5 mb-5">No rows returned.</p>;
    } else {
      const fields = data.schema.fields;
      content = (
        <div className="object-viewer-sql-results">
          <Table bordered hover responsive={true}>
            <thead className="table-dark">
              <tr>
                {fields.map((field, i) => (
                  <th key={i}>
                    <div className="d-flex flex-column">
                      <span>{field.name}</span>
                      <small>{field.type.toString()}</small>
                    </div>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {(() => {
                const columns = fields.map((_, index) => data.getChildAt(index));
                return Array.from({ length: data.numRows }).map((_, i) => (
                  <tr key={`row-${i}`}>
                    {columns.map((col, j) => (
                      <DataRow key={`col-${i}-${j}`} value={col?.get(i)} />
                    ))}
                  </tr>
                ));
              })()}
            </tbody>
          </Table>
        </div>
      );
    }
  }

  return (
    <div>
      <Form onSubmit={handleSubmit}>
        <Form.Group className="mt-2 mb-1" controlId="objectQuery">
          <SQLEditor
            value={sql}
            onChange={sqlChangeHandler}
            onRun={handleRun}
          />
        </Form.Group>

        <div className="d-flex mb-4">
          <div className="d-flex flex-fill justify-content-start align-items-center gap-2">
            {button}
            <OverlayTrigger
              placement="top"
              overlay={<Tooltip>Reset to default query</Tooltip>}
            >
              <span>
                <Button
                  variant="outline-secondary"
                  onClick={handleResetQuery}
                  disabled={loading}
                >
                  <HistoryIcon />
                </Button>
              </span>
            </OverlayTrigger>
            {isLargeFile && !useFullQuery && (
              <OverlayTrigger
                placement="top"
                overlay={
                  <Tooltip>
                    Large file detected. Click to load all rows (may be slow)
                  </Tooltip>
                }
              >
                <Button
                  variant="outline-primary"
                  onClick={handleLoadFullData}
                  disabled={loading}
                >
                  Load All Rows
                </Button>
              </OverlayTrigger>
            )}
          </div>

          <div className="d-flex justify-content-end">
            <p className="text-muted text-end powered-by">
              <small>
                Powered by{" "}
                <a
                  href="https://duckdb.org/2021/10/29/duckdb-wasm.html"
                  target="_blank"
                  rel="noreferrer"
                >
                  DuckDB-WASM
                </a>
                . For a full SQL reference, see the{" "}
                <a
                  href="https://duckdb.org/docs/sql/statements/select"
                  target="_blank"
                  rel="noreferrer"
                >
                  DuckDB Documentation
                </a>
              </small>
            </p>
          </div>
        </div>
      </Form>
      <div className="mt-3">{content}</div>
    </div>
  );
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const DataRow: FC<{ value: any }> = ({ value }) => {
  let dataType = "regular";
  if (typeof value === "string") {
    dataType = "string";
  } else if (value instanceof Date) {
    dataType = "date";
  } else if (typeof value === "number") {
    dataType = "number";
  }

  if (dataType === "string") {
    return <td className="string-cell">{value}</td>;
  }

  if (dataType === "date") {
    return <td className="date-cell">{dayjs(value).format()}</td>;
  }

  if (dataType === "number") {
    return <td className="number-cell">{value.toLocaleString("en-US")}</td>;
  }

  return <td>{"" + value}</td>;
};
