import React, {useRef, useState} from "react";

import {UploadIcon} from "@primer/octicons-react";
import {RepositoryPageLayout} from "../../../lib/components/repository/layout";
import RefDropdown from "../../../lib/components/repository/refDropdown";
import {ActionGroup, ActionsBar, Error, Loading, RefreshButton, Warnings} from "../../../lib/components/controls";
import Button from "react-bootstrap/Button";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import {BsCloudArrowUp} from "react-icons/bs";

import {Tree} from "../../../lib/components/repository/tree";
import {config, objects} from "../../../lib/api";
import {useAPI, useAPIWithPagination} from "../../../lib/hooks/api";
import {RefContextProvider, useRefs} from "../../../lib/hooks/repo";
import {useRouter} from "../../../lib/hooks/router";
import {RefTypeBranch} from "../../../constants";
import Alert from "react-bootstrap/Alert";
import {Link} from "react-router-dom";
import {LinearProgress} from '@mui/material';
import runImport from "../services/importUtils";

const ImportProgress = ({ numObjects}) => {
    return (
        <Row>
            <Col>
                <div className='import-text'>
                    <p>Imported <strong><div className='import-num-objects'> {numObjects} </div></strong> objects so far...</p>
                </div>
                <div>
                    <LinearProgress color="success" />
                </div>
                <div>
                    <small><abbr><br/>Please leave this tab open while import is in progress...</abbr></small>
                </div>
            </Col>
        </Row>
    );
}

const ImportDone = ({ numObjects, importBranch, currBranch}) => {
    return (
        <Row>
            <Col>
                <div className={"mt-10 mb-2 mr-2 row mt-4 import-success"}>
                    <p><strong>Success!</strong></p>
                </div>
                <div className='import-text'>
                    <p><strong><div className='import-num-objects'> {numObjects} </div></strong> objects imported and committed into branch {importBranch}.</p>
                </div>
                <div className='import-text'>
                    <p> Use the&nbsp;<Link to={`compare?ref=${ currBranch }&compare=${ importBranch }`} variant = "success" >Compare tab</Link>&nbsp;to view the changes and merge them to {currBranch}.</p>
                </div>
            </Col>
        </Row>
    );
}

const ImportButton = ({ config, repo, reference, path, onDone, onClick, variant = "success", onHide, show = false, enabled = false}) => {
    const initialState = {
        inProgress: false,
        error: null,
        done: false,
        numObj: 0,
        isSourceValid: null
    }
    const [importState, setImportState] = useState(initialState)

    const sourceRef = useRef(null);
    const destRef = useRef(null);
    const commitMsgRef = useRef(null);
    const storageNamespaceValidityRegexStr = config.blockstore_namespace_ValidityRegex;
    const storageNamespaceValidityRegex = RegExp(storageNamespaceValidityRegexStr);
    const sourceURIExample = config ? config.blockstore_namespace_example : "s3://my-bucket/path/";
    let currBranch = reference.id;
    currBranch = currBranch.match(/^_(.*)_imported$/)?.[1] || currBranch; // trim "_imported" suffix if used as import source
    let importBranch = `_${currBranch}_imported`;

    if (!reference || reference.type !== RefTypeBranch) return <></>

    const checkSourceURLValidity = () => {
        const isSourceValid = storageNamespaceValidityRegex.test(sourceRef.current.value)
        setImportState({inProgress: importState.inProgress, error: importState.error, done: importState.done, numObj: importState.numObj, isSourceValid: isSourceValid});
    };

    const hide = () => {
        if (importState.inProgress) return;
        setImportState(initialState)
        onHide()
    };

    const doImport = async () => {
        setImportState({
            ...initialState,
            inProgress: true
        })
        const updateStateFromImport = ({inProgress, done, numObj}) => {
            setImportState({inProgress, error: null, done, isSourceValid: importState.isSourceValid, numObj})
        }
        try {
            await runImport(updateStateFromImport,
                destRef.current.value,
                commitMsgRef.current.value,
                sourceRef.current.value,
                importBranch,
                repo.id,
                reference.id
            );
            onDone();
        } catch (error) {
            setImportState({...initialState, error})
            throw error
        }
    }
    const basePath = `lakefs://${repo.id}/${importBranch}/\u00A0`;
    const pathStyle = {'minWidth' : '25%'};

    return (
        <>
            <Modal show={show} onHide={hide} size="lg">
                <Modal.Header closeButton>
                    <Modal.Title>Import data from {config.blockstore_type} </Modal.Title>
                </Modal.Header>
                {(importState.done) ? (<ImportDone currBranch={currBranch} importBranch={importBranch} numObjects={importState.numObj} />)
                    : ( <>
                <Modal.Body>
                {(importState.inProgress) ? (<ImportProgress numObjects={importState.numObj} />)
                    : ( <>
                            <Alert variant="info">
                                Import doesn&apos;t copy the object. it only creates links to the objects in the lakeFS metadata layer. Don&apos;t worry, we will never change objects in the import source.
                                <a href="https://docs.lakefs.io/setup/import.html" target="_blank" rel="noreferrer"> Learn more.</a>
                            </Alert>
                            <form>
                            <Form.Group class='form-group'>
                                <Form.Label><strong>Import from:</strong></Form.Label>
                                    <Form.Control type="text" name="text" style={pathStyle} sm={8} ref={sourceRef} autoFocus
                                                  placeholder={sourceURIExample}
                                                  onChange={checkSourceURLValidity}/>
                                    {importState.isSourceValid === false &&
                                    <Form.Text className="text-danger">
                                        {"Import source must start with " + config.blockstore_namespace_ValidityRegex}
                                    </Form.Text>
                                    }
                                <Form.Text style={{ color: 'grey', justifyContent: "space-between"}}>
                                    A URI on the object store to import from.<br/>
                                </Form.Text>
                            </Form.Group>
                            <Form.Group class='form-group'>
                                <Form.Label><strong>Destination:</strong></Form.Label>
                                <Row noGutters={true}>
                                    <Col className="col-auto d-flex align-items-center justify-content-start">
                                        {basePath}
                                    </Col>
                                    <Col style={pathStyle}>
                                        <Form.Control type="text" autoFocus name="text" ref={destRef} defaultValue={path}/>
                                    </Col>
                                </Row>
                                <Form.Text style={{ color: 'grey'}} md={{offset: 2, span: 10000}}>
                                    Leave empty to import to the repository&apos;s root.
                                </Form.Text>
                            </Form.Group>
                            <Form.Group class='form-group'>
                                <Form.Label><strong>Commit Message:</strong></Form.Label>
                                    <Form.Control sm={8} type="text" ref={commitMsgRef} name="text" autoFocus/>
                            </Form.Group>
                            </form>
                            {(importState.error) ? (<Error error={importState.error}/>) : (<></>)}
                        </>
                        )}
                </Modal.Body>

                <Modal.Footer>
                    <Button variant="secondary"  disabled={importState.inProgress} onClick={hide}>
                        Cancel
                    </Button>
                    <Button variant="success" disabled={importState.inProgress || !importState.isSourceValid} onClick={() => {
                        if (importState.inProgress) return;
                        doImport()
                    }}>
                        {(importState.inProgress)? 'Importing...' : 'Import'}
                    </Button>
                </Modal.Footer>
                        </>
                    )}
            </Modal>
            <Button variant={variant} disabled={!enabled} onClick={onClick}>
                <BsCloudArrowUp/> Import
            </Button>
        </>
    );
}

const UploadButton = ({ config, repo, reference, path, onDone, onClick, onHide, show = false}) => {
    const initialState = {
        inProgress: false,
        error: null,
        done: false
    }
    const [uploadState, setUploadState] = useState(initialState)

    const textRef = useRef(null);
    const fileRef = useRef(null);

    if (!reference || reference.type !== RefTypeBranch) return <></>

    const hide = () => {
        if (uploadState.inProgress) return;
        setUploadState(initialState)
        onHide()
    };

    const upload = async () => {
        setUploadState({
            ...initialState,
            inProgress: true
        })
        try {
            await objects.upload(repo.id, reference.id, textRef.current.value, fileRef.current.files[0])
            setUploadState({...initialState})
            onDone()
        } catch (error) {
            setUploadState({...initialState, error})
            throw error
        }
        onHide();
    }

    const basePath = `${repo.id}/${reference.id}/\u00A0`;

    const pathStyle = {'minWidth' : '25%'};

    return (
        <>
            <Modal show={show} onHide={hide}>
                <Modal.Header closeButton>
                    <Modal.Title>Upload Object</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    <Form onSubmit={(e) => {
                        if (uploadState.inProgress) return;
                        upload();
                        e.preventDefault();
                    }}>
            {config?.warnings &&
             <Form.Group controlId="warnings">
                 <Warnings warnings={config.warnings}/>
             </Form.Group>}
                        <Form.Group controlId="path">
                            <Row noGutters={true}>
                                <Col className="col-auto d-flex align-items-center justify-content-start">
                                    {basePath}
                                </Col>
                                <Col style={pathStyle}>
                                    <Form.Control type="text" placeholder="Object name" autoFocus name="text" ref={textRef} defaultValue={path}/>
                                </Col>
                            </Row>
                        </Form.Group>

                        <Form.Group controlId="content">
                            <Form.Control
                                type="file"
                                name="content"
                                ref={fileRef}
                                onChange={(e) => {
                                    const currPath = textRef.current.value.substr(0, textRef.current.value.lastIndexOf('/')+1);
                                    const currName = (e.currentTarget.files.length > 0) ? e.currentTarget.files[0].name : ""
                                    textRef.current.value = currPath + currName;
                                }}
                            />
                        </Form.Group>
                    </Form>
                    {(uploadState.error) ? (<Error error={uploadState.error}/>) : (<></>)}

                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary"  disabled={uploadState.inProgress} onClick={hide}>
                        Cancel
                    </Button>
                    <Button variant="success" disabled={uploadState.inProgress} onClick={() => {
                        if (uploadState.inProgress) return;
                        upload()
                    }}>
                        {(uploadState.inProgress)? 'Uploading...' : 'Upload'}
                    </Button>
                </Modal.Footer>
            </Modal>

            <Button variant={(config.blockstore_type === 'local' || config.blockstore_type === 'mem') ? "success" :"light"} onClick={onClick}>
                <UploadIcon/> Upload Object
            </Button>
        </>
    );
}

const TreeContainer = ({ config, repo, reference, path, after, onPaginate, onRefresh, onUpload, onImport, refreshToken }) => {
    const { results, error, loading, nextPage } = useAPIWithPagination( () => {
        return objects.list(repo.id, reference.id, path, after)
    },[repo.id, reference.id, path, after, refreshToken]);
    const initialState = {
        inProgress: false,
        error: null,
        done: false
    }
    const [deleteState, setDeleteState] = useState(initialState)

    if (loading) return <Loading/>;
    if (error) return <Error error={error}/>;

    return (
        <>
            {deleteState.error && <Error error={deleteState.error} onDismiss={() => setDeleteState(initialState)}/>}
            <Tree
                config={{config}}
                repo={repo}
                reference={reference}
                path={(path) ? path : ""}
                showActions={true}
                results={results}
                after={after}
                nextPage={nextPage}
                onPaginate={onPaginate}
                onUpload={onUpload}
                onImport={onImport}
                onDelete={entry => {
                    objects
                        .delete(repo.id, reference.id, entry.path)
                        .catch(error => {
                            setDeleteState({...initialState, error: error})
                            throw error
                        })
                        .then(onRefresh)
                }}
            /></>
    );
}

const ObjectsBrowser = ({ config, configError }) => {
    const router = useRouter();
    const { path, after } = router.query;
    const { repo, reference, loading, error } = useRefs();
    const [showUpload, setShowUpload] = useState(false);
    const [showImport, setShowImport] = useState(false);
    const [refreshToken, setRefreshToken] = useState(false);
    const refresh = () => setRefreshToken(!refreshToken);

    if (loading) return <Loading/>;
    if (!!error || configError) return <Error error={error || configError}/>;

    return (
        <>
            <ActionsBar>
                <ActionGroup orientation="left">
                    <RefDropdown
                        emptyText={'Select Branch'}
                        repo={repo}
                        selected={reference}
                        withCommits={true}
                        withWorkspace={true}
                        selectRef={ref => router.push({
                            pathname: `/repositories/:repoId/objects`,
                            params: {repoId: repo.id, path: path === undefined ? '' : path},
                            query: {ref: ref.id, path:  path === undefined ? '' : path}
                        })}
                    />
                </ActionGroup>

                <ActionGroup orientation="right">
                    <RefreshButton onClick={refresh} />
                    <UploadButton
                        config={config}
                        path={path}
                        repo={repo}
                        reference={reference}
                        onDone={refresh}
                        onClick={() => { setShowUpload(true); }}
                        onHide={ () => { setShowUpload(false); }}
                        show={showUpload}
                    />
                    <ImportButton
                        config={config}
                        path={path}
                        repo={repo}
                        reference={reference}
                        onDone={refresh}
                        onClick={() => { setShowImport(true); }}
                        onHide={ () => { setShowImport(false); }}
                        show={showImport}
                        enabled={!(config.blockstore_type === 'local' || config.blockstore_type === 'mem')}
                    />
                </ActionGroup>
            </ActionsBar>

            <TreeContainer
                config={config}
                reference={reference}
                repo={repo}
                path={(path) ? path : ""}
                after={(after) ? after : ""}
                onPaginate={after => {
                    const query = {after}
                    if (path) query.path = path
                    if (reference) query.ref = reference.id
                    const url = {pathname: `/repositories/:repoId/objects`, query, params: {repoId: repo.id}}
                    router.push(url)
                }}
                refreshToken={refreshToken}
                onUpload={() => { setShowUpload(true); }}
                onImport={() => { setShowImport(true); }}
                onRefresh={refresh}/>
        </>
    );
};

const RepositoryObjectsPage = () => {
    const { response, error: err, loading } = useAPI(() => {
        return config.getStorageConfig();
    });
    return (
          <RefContextProvider>
              <RepositoryPageLayout activePage={'objects'}>
          {loading && <Loading/>}
                  <ObjectsBrowser config={response} configError={err}/>
              </RepositoryPageLayout>
          </RefContextProvider>
    );
};

export default RepositoryObjectsPage;
