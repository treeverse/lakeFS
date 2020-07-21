import React, {useRef, useState} from 'react';

import Form from "react-bootstrap/Form";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import {connect} from "react-redux";
import {resetImportObjects, resetImportObjectsDryRun} from "../actions/objects";
import Table from "react-bootstrap/Table";
import moment from "moment";
import {CheckIcon, CpuIcon, GitBranchIcon, LinkExternalIcon, UploadIcon} from "@primer/octicons-react";
import {useHistory} from "react-router-dom";
import Collapse from "react-bootstrap/Collapse";

export const IMPORT_FROM_S3_BRANCH_NAME="import-from-inventory";

export const DataImportForm = connect(
    ({objects}) => ({
        importState: objects.import,
        importDryRunState: objects.importDryRun,
    }),
    {resetImportObjects, resetImportObjectsDryRun}
)(({repoId, onSubmit, onTest, onCancel, importState, importDryRunState, resetImportObjects, resetImportObjectsDryRun}) => {
    const history = useHistory();
    const fieldNameOffset = 3;
    const [formValid, setFormValid] = useState(false);
    const manifestURLField = useRef(null)
    const checkValidity = () => {
        if (!manifestURLField.current.value.match("s3://.*/manifest.json")) {
            setFormValid(false);
            return;
        }
        setFormValid(true);
    };

    const isDisabled = () => {
        return !formValid || importState.inProgress || importDryRunState.inProgress || importState.done;
    }

    return (

        <Form className={"mt-1"} onSubmit={(e) => {
            e.preventDefault();
            if (isDisabled()) {
                return;
            }
            resetImportObjectsDryRun();
            onSubmit(manifestURLField.current.value);
        }}>
            <Collapse in={!importState.done}><div id="import-form-controls">
                <div className="d-flex align-items-center">
                    <p><UploadIcon size="48" /></p>
                    <p className="mx-2">Use <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/storage-inventory.html" target="_blank" rel="noopener noreferrer">S3 Inventory<LinkExternalIcon/></a> to bring data into lakeFS. After importing, you will be able to access your data through the lakeFS S3-compatible endpoint.</p>
                </div>
                <div className="d-flex align-items-center">
                    <p><CpuIcon size="48" /></p>
                    <p className="mx-2">To save time and costs, <b>your original data will not be copied</b> to the repository storage. Instead, metadata is created for your objects, and only future changes are saved in the repository storage.</p>
                </div>
                <div className="d-flex align-items-center">
                    <p><GitBranchIcon size="48" /></p>
                    <p className="mx-2">You will find your data in a dedicated lakeFS branch. Repeating the import process with more recent inventories will add the diff as new commits to this branch. You can later merge this branch into your main branch.</p>
                </div>
                <Form.Group as={Row} controlId="repoId">
                    <Form.Label column sm={fieldNameOffset}>Repository ID</Form.Label>
                    <Col>
                        <Form.Control type="text" readOnly value={repoId} onChange={checkValidity}/>
                    </Col>
                </Form.Group>

                <Form.Group as={Row}>
                    <Form.Label column sm={fieldNameOffset}>Destination Branch</Form.Label>
                    <Col>
                        <Form.Control type="text" readOnly value={IMPORT_FROM_S3_BRANCH_NAME}/>
                    </Col>
                </Form.Group>
                <Form.Group as={Row} controlId="manifestUrl">
                    <Form.Label column sm={fieldNameOffset}>S3 Inventory Manifest Url</Form.Label>
                    <Col>
                        <Form.Control type="text" ref={manifestURLField}
                                      placeholder="s3://example-bucket/inventory/YYYY-MM-DDT00-00Z/manifest.json"
                                      onChange={checkValidity}/>
                    </Col>
                </Form.Group></div></Collapse>
            {importState.error &&
            <Row>
                <Col md={{offset: fieldNameOffset}}>
                    <Alert variant={"danger"}>{importState.error}</Alert>
                </Col>
            </Row>}
            {importState.done && ((p) => (
                <Row><Col md={{offset: 1, span: 10}} className="mb-2">
                    <Table size="sm">
                        <thead><tr><th colSpan={2}><CheckIcon/> Import successful!</th></tr></thead>
                        <tbody>
                            <tr>
                                <td>Objects added / changed</td>
                                <td><code>{p["added_or_changed"] || 0}</code>{p["previous_manifest"] && " (diff only)"}</td>
                            </tr>
                            <tr>
                                <td>Objects deleted</td>
                                <td><code>{p["deleted"] || 0}</code>{p["previous_manifest"] && " (diff only)"}</td>
                            </tr>
                        </tbody>
                    </Table>
                </Col></Row>
            ))(importState.payload)}

            {importDryRunState.error &&
            <Row>
                <Col md={{offset: fieldNameOffset}}>
                    <Alert variant={"danger"}>{importDryRunState.error}</Alert>
                </Col>
            </Row>}
            {importDryRunState.done && ((p) => (
                <Row><Col md={{offset: 1, span: 10}} className="mb-2">
                        <Table size="sm">
                            <thead><tr><th colSpan={2}><CheckIcon/> Test successful!</th></tr></thead>
                            <tbody>
                            {p["previous_manifest"] && <><tr>
                                    <td>Previous import</td>
                                    <td><code>{moment.unix(p["previous_import_date"]).format("MM/DD/YYYY HH:mm:ss")}</code></td>
                                </tr>
                                <tr>
                                    <td>Previous manifest imported</td>
                                    <td><code>{p["previous_manifest"]}</code></td>
                                </tr></>}
                                <tr>
                                    <td>Objects to add / change</td>
                                    <td><code>{p["added_or_changed"] || 0}</code>{p["previous_manifest"] && " (diff only)"}</td>
                                </tr>
                                <tr>
                                    <td>Objects to delete</td>
                                    <td><code>{p["deleted"] || 0}</code>{p["previous_manifest"] && " (diff only)"}</td>
                                </tr>
                            </tbody></Table>
                    </Col></Row>
            ))(importDryRunState.payload)}

            {importState.done ? <Row className="justify-content-md-center"><Col md="auto">
                <Button variant="info" className="mr-2" onClick={(e) => {
                    e.preventDefault();
                    history.push(`/repositories/${repoId}/tree?branch=${IMPORT_FROM_S3_BRANCH_NAME}`);
                    onCancel();
                }}>
                    Show files
                </Button>
                <Button variant="info" className="mr-2" onClick={(e) => {
                    e.preventDefault();
                    history.push(`/repositories/${repoId}/commits?branch=${IMPORT_FROM_S3_BRANCH_NAME}`);
                    onCancel();
                }}>
                    Show commit
                </Button>
                <Button variant="secondary" onClick={(e) => {
                    e.preventDefault();
                    onCancel();
                }}>
                    Close
                </Button>
            </Col></Row> : <Row className="pt-3 justify-content-md-center">
                <Col md={"auto"} >
                    <Button variant="success" type="submit" className="mr-2" disabled={isDisabled()} aria-controls="import-help-text" aria-expanded={!importState.done}>
                        {(importState.inProgress) ? 'Importing...' : 'Import'}
                    </Button>
                    <Button variant="secondary" className="mr-2" disabled={isDisabled()} onClick={(e) => {
                        e.preventDefault();
                        if (isDisabled()) {
                            return;
                        }
                        resetImportObjects();
                        onTest(manifestURLField.current.value);
                    }}>
                        {(importDryRunState.inProgress) ? 'Testing...' : 'Test'}
                    </Button>
                    <Button variant="secondary" onClick={(e) => {
                        e.preventDefault();
                        onCancel();
                    }}>Cancel</Button>
                </Col>
            </Row>
            }
        </Form>
    );
});