import React, {
  FC,
  FormEvent,
  useCallback,
  useContext,
  useEffect,
  useState,
} from "react";
import { runDuckDBQuery } from "./duckdb";
import * as arrow from "apache-arrow";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import { ChevronRightIcon, SyncIcon, GearIcon } from "@primer/octicons-react";
import dayjs from "dayjs";
import Table from "react-bootstrap/Table";
import Modal from "react-bootstrap/Modal";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";

import { SQLEditor } from "./editor";
import { RendererComponent } from "./types";
import { AlertError, Loading } from "../../../../lib/components/controls";
import { AppContext, AppActionType } from "../../../../lib/hooks/appContext";

const MAX_RESULTS_RETURNED = 1000;

export const DataLoader: FC = () => {
  return <Loading />;
};

// Default query templates with placeholders
const DEFAULT_QUERY_TEMPLATES: { [key: string]: string } = {
  parquet:
    "SELECT * FROM READ_PARQUET('lakefs://{{repo}}/{{ref}}/{{path}}', hive_partitioning=false) LIMIT 20",
  csv: "SELECT * FROM READ_CSV('lakefs://{{repo}}/{{ref}}/{{path}}', AUTO_DETECT = TRUE) LIMIT 20",
  tsv: "SELECT * FROM READ_CSV('lakefs://{{repo}}/{{ref}}/{{path}}', DELIM='\\t', AUTO_DETECT=TRUE) LIMIT 20",
};

export const DuckDBRenderer: FC<RendererComponent> = ({
  repoId,
  refId,
  path,
  fileExtension,
}) => {
  const { state, dispatch } = useContext(AppContext);
  const [showTemplateModal, setShowTemplateModal] = useState(false);
  const [editingTemplate, setEditingTemplate] = useState("");

  // Get the query template for the current file type
  const getQueryTemplate = (fileType: string): string => {
    const customTemplate = state.settings.sqlQueryTemplates[fileType];
    if (customTemplate) {
      return customTemplate;
    }
    return DEFAULT_QUERY_TEMPLATES[fileType] || "";
  };

  // Apply template variables to generate the actual query
  const applyTemplate = (template: string): string => {
    return template
      .replace(/\{\{repo\}\}/g, repoId)
      .replace(/\{\{ref\}\}/g, refId)
      .replace(/\{\{path\}\}/g, path);
  };

  const currentFileType = fileExtension;
  const currentTemplate = getQueryTemplate(currentFileType);
  const initialQuery = applyTemplate(currentTemplate);

  const hasCustomTemplate = !!state.settings.sqlQueryTemplates[currentFileType];
  const hasDefaultTemplate = !!DEFAULT_QUERY_TEMPLATES[currentFileType];
  const isTemplateSupported = hasDefaultTemplate || hasCustomTemplate;

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

  const [sql, setSql] = useState(initialQuery);
  const sqlChangeHandler = useCallback(
    (data: React.SetStateAction<string>) => {
      setSql(data);
    },
    [setSql],
  );

  // Reset query to the default template (without re-executing)
  // Reset query to the current template (custom or default)
  const handleResetQuery = useCallback(() => {
    const query = applyTemplate(currentTemplate);
    setSql(query);
  }, [currentTemplate, repoId, refId, path]);

  // Open template customization modal
  const handleOpenTemplateModal = useCallback(() => {
    setEditingTemplate(currentTemplate);
    setShowTemplateModal(true);
  }, [currentTemplate]);

  // Save custom template (without re-executing)
  const handleSaveTemplate = useCallback(() => {
    const defaultTemplate = DEFAULT_QUERY_TEMPLATES[currentFileType] || "";
    const newTemplates = { ...state.settings.sqlQueryTemplates };

    // If the template matches the default, remove the custom template
    if (editingTemplate === defaultTemplate) {
      delete newTemplates[currentFileType];
    } else {
      newTemplates[currentFileType] = editingTemplate;
    }

    dispatch({ type: AppActionType.setSqlQueryTemplates, value: newTemplates });
    setShowTemplateModal(false);
    // Apply the new template without executing
    const newQuery = applyTemplate(editingTemplate);
    setSql(newQuery);
  }, [
    editingTemplate,
    currentFileType,
    dispatch,
    state.settings.sqlQueryTemplates,
    repoId,
    refId,
    path,
  ]);

  // Reset template to default in the modal (updates the editing template)
  const handleResetTemplateInModal = useCallback(() => {
    const defaultTemplate = DEFAULT_QUERY_TEMPLATES[currentFileType] || "";
    setEditingTemplate(defaultTemplate);
  }, [currentFileType]);

  // Check if editing template matches the default
  const isEditingTemplateDefault =
    editingTemplate === (DEFAULT_QUERY_TEMPLATES[currentFileType] || "");

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
      const totalRows = data.numRows;
      let res = data;
      if (totalRows > MAX_RESULTS_RETURNED) {
        res = data.slice(0, MAX_RESULTS_RETURNED);
      }
      content = (
        <>
          {res.numRows < data.numRows && (
            <small>{`Showing only the first ${res.numRows.toLocaleString()} rows (out of ${data.numRows.toLocaleString()})`}</small>
          )}
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
                {[...res].map((row, i) => (
                  <tr key={`row-${i}`}>
                    {[...row].map((v, j: number) => {
                      return <DataRow key={`col-${i}-${j}`} value={v[1]} />;
                    })}
                  </tr>
                ))}
              </tbody>
            </Table>
          </div>
        </>
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
                  disabled={loading || !isTemplateSupported}
                >
                  <SyncIcon />
                </Button>
              </span>
            </OverlayTrigger>
            <OverlayTrigger
              placement="top"
              overlay={
                <Tooltip>
                  {isTemplateSupported
                    ? `Customize default query template for ${currentFileType.toUpperCase()} files`
                    : `No template available for ${currentFileType.toUpperCase()} files`}
                </Tooltip>
              }
            >
              <span>
                <Button
                  variant={"outline-secondary"}
                  onClick={handleOpenTemplateModal}
                  disabled={!isTemplateSupported}
                >
                  <GearIcon />
                  {hasCustomTemplate && " *"}
                </Button>
              </span>
            </OverlayTrigger>
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

      <Modal
        show={showTemplateModal}
        onHide={() => setShowTemplateModal(false)}
        size="lg"
      >
        <Modal.Header closeButton>
          <Modal.Title>
            Customize Query Template for {currentFileType.toUpperCase()} Files
          </Modal.Title>
        </Modal.Header>
        <Modal.Body>
          <p className="text-muted mb-3">
            Customize the default SQL query template. Use placeholders that will
            be replaced with actual values:
          </p>
          <ul className="text-muted small mb-3">
            <li>
              <code>{"{{repo}}"}</code> - Repository name
            </li>
            <li>
              <code>{"{{ref}}"}</code> - Branch or commit reference
            </li>
            <li>
              <code>{"{{path}}"}</code> - File path
            </li>
          </ul>
          <Form.Group>
            <Form.Label>Query Template</Form.Label>
            <Form.Control
              as="textarea"
              rows={4}
              value={editingTemplate}
              onChange={(e) => setEditingTemplate(e.target.value)}
              className="font-monospace"
            />
          </Form.Group>
          <div className="mt-3">
            <small className="text-muted">
              <strong>Preview:</strong> {applyTemplate(editingTemplate)}
            </small>
          </div>
        </Modal.Body>
        <Modal.Footer>
          <Button
            variant="outline-secondary"
            onClick={handleResetTemplateInModal}
            disabled={isEditingTemplateDefault}
          >
            Reset to Default
          </Button>
          <Button
            variant="secondary"
            onClick={() => setShowTemplateModal(false)}
          >
            Cancel
          </Button>
          <Button variant="primary" onClick={handleSaveTemplate}>
            Save Template
          </Button>
        </Modal.Footer>
      </Modal>
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
