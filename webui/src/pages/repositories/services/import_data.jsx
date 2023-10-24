import {imports} from "../../../lib/api";
import Row from "react-bootstrap/Row";
import InputGroup from 'react-bootstrap/InputGroup';
import Col from "react-bootstrap/Col";
import {LinearProgress} from "@mui/material";
import React, {useState} from "react";
import Button from "react-bootstrap/Button";
import Alert from "react-bootstrap/Alert";
import Form from "react-bootstrap/Form";
import {MetadataFields} from "../../../lib/components/repository/changes";

const ImportPhase = {
    NotStarted: 0,
    InProgress: 1,
    Completed: 2,
    Failed: 3,
}

const startImport = async (setImportID, prependPath, commitMsg, sourceRef, repoId, refId, metadata = {}) => {
    const response = await imports.create(repoId, refId, sourceRef, prependPath, commitMsg, metadata);
    setImportID(response.id);
}

const ImportProgress = ({numObjects}) => {
    return (<Row>
        <Col>
            <div className='import-text'>
                Imported
                    <span className='import-num-objects'> {numObjects} </span>
                objects so far...
            </div>
            <div>
                <LinearProgress color="success"/>
            </div>
            <div>
                <small><abbr><br/>Please leave this tab open while import is in progress...</abbr></small>
            </div>
        </Col>
    </Row>);
}

const ImportDone = ({numObjects, branch = ''}) => {
    return (<Row>
        <Col>
            <div className={"mt-10 mb-2 me-2 row mt-4 import-success"}>
                <p><strong>Success!</strong></p>
            </div>
            <div className='import-text'>
                <strong>
                    <span className='import-num-objects'> {numObjects} </span>
                </strong> objects imported and committed into branch
                <strong>
                    <span className='import-num-objects'> {branch} </span>
                </strong>.
            </div>
        </Col>
    </Row>);
}
const ExecuteImportButton = ({isEnabled, importPhase, importFunc, doneFunc}) => {
    switch (importPhase) {
        case ImportPhase.Completed:
            return <Button
                variant="success"
                onClick={doneFunc}
                disabled={!isEnabled}>
                    Close
            </Button>
        case ImportPhase.Failed:
        case ImportPhase.NotStarted:
            return <Button
                variant="success"
                disabled={!isEnabled}
                onClick={importFunc}>
                Import
            </Button>
        case ImportPhase.InProgress:
            return <Button
                variant="success"
                disabled={true}>
                Importing...
            </Button>
    }
}

const ImportForm = ({
                        config,
                        pathStyle,
                        sourceRef,
                        destRef,
                        commitMsgRef,
                        repo,
                        branch,
                        updateSrcValidity,
                        metadataFields,
                        setMetadataFields,
                        err = null,
                        ...rest
                    }) => {
    const [isSourceValid, setIsSourceValid] = useState(true);
    const importValidityRegexStr = config.import_validity_regex;
    const storageNamespaceValidityRegex = RegExp(importValidityRegexStr);
    const updateSourceURLValidity = () => {
        if (!sourceRef.current.value) {
            updateSrcValidity(true);
            setIsSourceValid(true);
            return
        }
        const isValid = storageNamespaceValidityRegex.test(sourceRef.current.value);
        updateSrcValidity(isValid);
        setIsSourceValid(isValid);
    };
    const sourceURIExample = config ? config.blockstore_namespace_example : "s3://my-bucket/path/";
    return (<div {...rest}>
        <Alert variant="info">
            This feature doesn&apos;t copy data. It only creates pointers in the lakeFS metadata.<br/>
            lakeFS will never change objects in the import source.
            &#160;<a href="https://docs.lakefs.io/howto/import.html" target="_blank" rel="noreferrer">Learn more</a>
        </Alert>
        <Form>
            <Form.Group className='mt-4 form-group'>
                <Form.Label>Import from</Form.Label>
                <Form.Control type="text" name="import-from" style={pathStyle} ref={sourceRef} autoFocus
                              placeholder={sourceURIExample}
                              onChange={updateSourceURLValidity}/>
                {isSourceValid === false &&
                    <Form.Text className="text-danger">
                        {`Import source should match the following pattern: "${importValidityRegexStr}"`}
                    </Form.Text>
                }
                {isSourceValid &&
                    <Form.Text style={{color: 'grey', justifyContent: "space-between"}}>
                        A URI on the object store to import from.<br/>
                    </Form.Text>
                }
            </Form.Group>
            <Form.Group className='mt-4 form-group'>
                <Form.Label>Destination</Form.Label>
                <InputGroup>
                    <div className={"input-group-prepend"}>
                        <InputGroup.Text className={"text-muted"}>lakefs://{repo}/{branch}/</InputGroup.Text>
                    </div>
                    <Form.Control type="text" name="destination" ref={destRef}/>
                </InputGroup>
                <Form.Text style={{color: 'grey'}} md={{offset: 2, span: 10000}}>
                    Leave empty to import to the repository&apos;s root.
                </Form.Text>
            </Form.Group>

            <Form.Group className='mt-4 form-group'>
                <Form.Label>Commit message</Form.Label>
                <Form.Control type="text" ref={commitMsgRef} name="commit-message"/>
            </Form.Group>
            <MetadataFields className={"mt-4"} metadataFields={metadataFields} setMetadataFields={setMetadataFields}/>
            {err &&
                <Alert className="mt-4 small" variant={"danger"}>{err.message}</Alert>}
        </Form>
    </div>)
}

export {
    startImport, ImportProgress, ImportDone, ExecuteImportButton, ImportForm, ImportPhase,
}
