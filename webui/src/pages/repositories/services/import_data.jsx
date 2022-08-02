import {branches, commits, metaRanges, NotFoundError, ranges} from "../../../lib/api";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import {LinearProgress} from "@mui/material";
import React, {useState} from "react";
import {Link} from "react-router-dom";
import Button from "react-bootstrap/Button";
import Alert from "react-bootstrap/Alert";
import Form from "react-bootstrap/Form";

const ImportPhase = {
    NotStarted: 0,
    InProgress: 1,
    Completed: 2,
    Failed: 3,
}

const runImport = async (updateImportState, prependPath, commitMsg, sourceRef, branch, repoId, refId) => {
    let paginationResp = {};
    let after = "";
    let importBranchResp;
    let sum = 0;
    const rangeArr = [];
    const importStatusUpdate = {
        importPhase: ImportPhase.InProgress,
        numObj: sum,
    }
    updateImportState(importStatusUpdate);
    do {
        const response = await ranges.createRange(repoId, sourceRef, after, prependPath, paginationResp.continuation_token);
        rangeArr.push(response.range);
        paginationResp = response.pagination;
        after = paginationResp.last_key;
        sum += response.range.count;
        importStatusUpdate.numObj = sum
        updateImportState(importStatusUpdate);
    } while (paginationResp.has_more);
    const metarange = await metaRanges.createMetaRange(repoId, rangeArr);

    try {
        importBranchResp = await branches.get(repoId, branch);
    } catch (error) {
        if (error instanceof NotFoundError) {
            importBranchResp = await createBranch(repoId, refId, branch);
        } else {
            throw error;
        }
    }
    await commits.commit(repoId, importBranchResp.id, commitMsg, {}, metarange.id);
    importStatusUpdate.importPhase = ImportPhase.Completed;
    updateImportState(importStatusUpdate);
}

const createBranch = async (repoId, refId, branch) => {
    // Find root commit for repository
    let hasMore = true;
    let nextOffset = "";
    let baseCommit = refId;
    do {
        let response = await commits.log(repoId, refId, nextOffset, 1000);
        hasMore = response.pagination.has_more;
        nextOffset = response.pagination.next_offset;
        baseCommit = response.results.at(-1);
    } while (hasMore)
    await branches.create(repoId, branch, baseCommit.id);
    return await branches.get(repoId, branch);
}

const ImportProgress = ({numObjects}) => {
    return (<Row>
        <Col>
            <div className='import-text'>
                <p>Imported <strong>
                    <div className='import-num-objects'> {numObjects} </div>
                </strong> objects so far...
                </p>
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

const ImportDone = ({numObjects, importBranch, currBranch = ''}) => {
    return (<Row>
        <Col>
            <div className={"mt-10 mb-2 mr-2 row mt-4 import-success"}>
                <p><strong>Success!</strong></p>
            </div>
            <div className='import-text'>
                <p><strong>
                    <div className='import-num-objects'> {numObjects} </div>
                </strong> objects imported and committed into branch {importBranch}.
                </p>
            </div>
            {(currBranch && importBranch !== currBranch) &&
                <div className='import-text'>
                    <p> Use the&nbsp;<Link to={`compare?ref=${currBranch}&compare=${importBranch}`}
                                           variant="success">Compare tab</Link>&nbsp;to view the changes and merge
                        them to {currBranch}.
                    </p>
                </div>
            }
        </Col>
    </Row>);
}
const ExecuteImportButton = ({isEnabled, importPhase, importFunc}) => {
    const enableImport = importPhase === ImportPhase.NotStarted || importPhase === ImportPhase.Failed;
    return (
        <Button variant="success"
                disabled={!isEnabled || !enableImport}
                onClick={() => {
                        if (importPhase !== ImportPhase.InProgress) {
                            importFunc();
                        }
                    }}>
        {importPhase === ImportPhase.InProgress && 'Importing...'}
        {(importPhase === ImportPhase.NotStarted || importPhase === ImportPhase.Failed) && 'Import'}
        {importPhase === ImportPhase.Completed && 'Import completed'}
    </Button>);
}

const ImportForm = ({
                        config,
                        pathStyle,
                        sourceRef,
                        destRef,
                        repoId,
                        importBranch,
                        path,
                        commitMsgRef,
                        updateSrcValidity,
                        shouldAddPath = false,
                        err = null,
                    }) => {
    const [isSourceValid, setIsSourceValid] = useState(true);
    const storageNamespaceValidityRegexStr = config.blockstore_namespace_ValidityRegex;
    const storageNamespaceValidityRegex = RegExp(storageNamespaceValidityRegexStr);
    const updateSourceURLValidity = () => {
        const isValid = storageNamespaceValidityRegex.test(sourceRef.current.value);
        updateSrcValidity(isValid);
        setIsSourceValid(isValid);
    };
    const basePath = `lakefs://${repoId}/${importBranch}/\u00A0`;
    const sourceURIExample = config ? config.blockstore_namespace_example : "s3://my-bucket/path/";
    return (<>
        <Alert variant="info">
            Import doesn&apos;t copy objects. It only creates links to the objects in the lakeFS metadata layer.
            Don&apos;t worry, we will never change objects in the import source.
            <a href="https://docs.lakefs.io/setup/import.html" target="_blank" rel="noreferrer"> Learn more.</a>
        </Alert>
        <form>
            <Form.Group className='form-group'>
                <Form.Label><strong>Import from:</strong></Form.Label>
                <Form.Control type="text" name="text" style={pathStyle} sm={8} ref={sourceRef} autoFocus
                              placeholder={sourceURIExample}
                              onChange={updateSourceURLValidity}/>
                {isSourceValid === false &&
                    <Form.Text className="text-danger">
                        {`Import source should match the following pattern: "${storageNamespaceValidityRegexStr}"`}
                    </Form.Text>
                }
                <Form.Text style={{color: 'grey', justifyContent: "space-between"}}>
                    A URI on the object store to import from.<br/>
                </Form.Text>
            </Form.Group>
            {shouldAddPath &&
                <Form.Group className='form-group'>
                    <Form.Label><strong>Destination:</strong></Form.Label>
                    <Row noGutters={true}>
                        <Col className="col-auto d-flex align-items-center justify-content-start">
                            {basePath}
                        </Col>
                        <Col style={pathStyle}>
                            <Form.Control type="text" autoFocus name="text" ref={destRef} defaultValue={path}/>
                        </Col>
                    </Row>
                    <Form.Text style={{color: 'grey'}} md={{offset: 2, span: 10000}}>
                        Leave empty to import to the repository&apos;s root.
                    </Form.Text>
                </Form.Group>
            }
            <Form.Group className='form-group'>
                <Form.Label><strong>Commit Message:</strong></Form.Label>
                <Form.Control sm={8} type="text" ref={commitMsgRef} name="text" autoFocus defaultValue={`Imported data from ${config.blockstore_type}`}/>
            </Form.Group>
            {err &&
                <Alert variant={"danger"}>{err.message}</Alert>}
        </form>
    </>)
}

export {
    runImport, ImportProgress, ImportDone, ExecuteImportButton, ImportForm, ImportPhase,
}
