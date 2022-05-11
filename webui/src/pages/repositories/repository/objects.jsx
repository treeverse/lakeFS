import React, {useRef, useState} from "react";

import {UploadIcon} from "@primer/octicons-react";
import {RepositoryPageLayout} from "../../../lib/components/repository/layout";
import RefDropdown from "../../../lib/components/repository/refDropdown";
import {
    ActionGroup,
    ActionsBar,
    Error,
    Loading,
    RefreshButton,
    Warnings
} from "../../../lib/components/controls";
import Button from "react-bootstrap/Button";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { BiImport } from "react-icons/bi";

import {Tree} from "../../../lib/components/repository/tree";
import {branches, commits, config, metaRanges, objects, ranges} from "../../../lib/api";
import {useAPI, useAPIWithPagination} from "../../../lib/hooks/api";
import {RefContextProvider, useRefs} from "../../../lib/hooks/repo";
import {useRouter} from "../../../lib/hooks/router";
import {RefTypeBranch} from "../../../constants";
import Alert from "react-bootstrap/Alert";
import {Link} from "react-router-dom";
import {LinearProgress} from '@mui/material';

const ImportProgress = ({ numObjects}) => {
    return (
        <Row>
            <Col>
                <div style={{display: 'flex', justifyContent: 'center'}}>
                    <p>Imported <strong><div style={{ color: 'green', display: 'inline'}}> {numObjects} </div></strong> objects so far...</p>
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
                <div className={"mt-10 mb-2 mr-2 row mt-4"} style={{color: 'green', display: 'flex', justifyContent: 'center', fontSize: 'large'}}>
                    <p>Import succeeded!</p>
                </div>
                <div style={{display: 'flex', justifyContent: 'center'}}>
                    <p><strong><div style={{ color: 'green', display: 'inline'}}> {numObjects} </div></strong> objects imported and committed into branch <strong>{importBranch}</strong></p>
                </div>
                <div style={{display: 'flex', justifyContent: 'center'}}>
                    <p> &nbsp;<Link to={`compare?ref=${ currBranch }&compare=${ importBranch }`} variant = "success" className="btn btn-success">Compare and Merge</Link>&nbsp;it into the current branch - <strong>{currBranch}</strong></p>
                </div>
                <Alert variant="warning" className={"ml-2 mr-2 row mt-4"}>
                    Merging {importBranch} will delete all files on branch {currBranch}
                </Alert>

            </Col>
        </Row>
    );
}


const ImportButton = ({ config, repo, reference, path, onDone, onClick, variant = "success", onHide, show = false, enabled= false}) => {
    const initialState = {
        inProgress: false,
        error: null,
        done: false,
        numObj: 0,
        isStorageNamespaceValid: null
    }
    const [importState, setImportState] = useState(initialState)

    const sourceRef = useRef(null);
    const destRef = useRef(null);
    const commitMsgRef = useRef(null);
    const storageType = config.blockstore_type
    const DEFAULT_BLOCKSTORE_VALIDITY_REGEX = new RegExp(`^s3://`);
    const storageNamespaceValidityRegexStr = config ? config.blockstore_namespace_ValidityRegex : DEFAULT_BLOCKSTORE_VALIDITY_REGEX;
    const storageNamespaceValidityRegex = RegExp(storageNamespaceValidityRegexStr);
    const sourceURIExample = config ? config.blockstore_namespace_example : "s3://my-bucket/path/";


    if (!reference || reference.type !== RefTypeBranch) return <></>

    const checkStorageNamespaceValidity = () => {
        const isStorageNamespaceValid = storageNamespaceValidityRegex.test(sourceRef.current.value)
        setImportState({inProgress: importState.inProgress, error: importState.error, done: importState.done, numObj: importState.numObj, isStorageNamespaceValid: isStorageNamespaceValid});
    };

    const hide = () => {
        if (importState.inProgress) return;
        setImportState(initialState)
        onHide()
    };

    const ingest = async () => {
        setImportState({
            ...initialState,
            inProgress: true
        })

        try {
            let hasMore = true
            let continuationToken = ""
            let after = ""
            let prepend = `${destRef.current.value}`;
            let importBranchResp
            let sum = importState.numObj
            const importBranch = `${reference.id}-imported`;
            const commitMsg = commitMsgRef.current.value;
            const sourceRefVal = sourceRef.current.value;
            const range_arr = []

            while (hasMore) {
                const response = await ranges.createRange(repo.id, sourceRefVal, after, prepend, continuationToken)
                range_arr.push(response.range)

                let pagination = response.pagination
                hasMore = pagination.has_more
                continuationToken = pagination.continuation_token
                after = pagination.last_key
                sum += response.range.count
                setImportState({inProgress: true, error: null, done: false, isStorageNamespaceValid: importState.isStorageNamespaceValid, numObj: sum})
            }
            const metarange = await metaRanges.createMetaRange(repo.id, range_arr)

            try {
                importBranchResp = await branches.get(repo.id, importBranch)
            } catch (error){
                await branches.create(repo.id, importBranch, reference.id)
                importBranchResp = await branches.get(repo.id, importBranch)
            }

            await commits.commit(repo.id, importBranchResp.id, commitMsg, {}, metarange.id)
            setImportState({inProgress: false, error: null, done: true, numObj: sum, isStorageNamespaceValid: importState.isStorageNamespaceValid})
            onDone()
        } catch (error) {
            setImportState({...initialState, error})
            throw error
        }
    }
    const basePath = `lakefs://${repo.id}/${reference.id}-imported/\u00A0`;
    const pathStyle = {'minWidth' : '25%'};

    return (
        <>
            <Modal show={show} onHide={hide} size="lg">
                <Modal.Header closeButton>
                    <Modal.Title>Import</Modal.Title>
                </Modal.Header>
                {(importState.done) ? (<ImportDone currBranch={reference.id} importBranch={`${reference.id}-imported`} numObjects={importState.numObj} />)
                    : ( <>
                <Modal.Body>
                {(importState.inProgress) ? (<ImportProgress numObjects={importState.numObj} />)
                    : ( <>
                            <Alert variant="warning">
                                <strong>Note:</strong> Importing from your object storage doesn&apos;t copy any data, it creates read-only <q>soft links</q> to existing objects. lakeFS will never modify or delete anything outside the repository&apos;s storage namespace.
                                <a href="https://docs.lakefs.io/setup/import.html" target="_blank" rel="noreferrer"> Learn more.</a>
                            </Alert>
                            <form>
                            <Form.Group class='form-group'>
                                <Form.Label><strong>Source URI:</strong></Form.Label>
                                    <Form.Control type="text" name="text" style={pathStyle} sm={8} ref={sourceRef} autoFocus
                                                  placeholder={sourceURIExample}
                                                  onChange={checkStorageNamespaceValidity}/>
                                    {importState.isStorageNamespaceValid === false &&
                                    <Form.Text className="text-danger">
                                        {"Can only create repository with storage type: " + storageType}
                                    </Form.Text>
                                    }
                                <Form.Text style={{ color: 'grey', justifyContent: "space-between"}}>
                                    Source bucket and prefix path on the object storage to import from.<br/>
                                    <strong>&bull;</strong> Import is feasible only from source object storage that matches the storage namespace of the repository.<br/>
                                    <strong>&bull;</strong> lakeFS needs access to the source.<br/>
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
                                    Path to import the objects.
                                </Form.Text>
                            </Form.Group>
                            <Form.Group class='form-group'>
                                <Form.Label><strong>Commit Message:</strong></Form.Label>
                                    <Form.Control sm={8} type="text" ref={commitMsgRef} name="text" autoFocus/>
                            </Form.Group>
                            </form>
                            {(!!importState.error) ? (<Error error={importState.error}/>) : (<></>)}
                        </>
                        )}
                </Modal.Body>

                <Modal.Footer>
                    <Button variant="secondary"  disabled={importState.inProgress} onClick={hide}>
                        Cancel
                    </Button>
                    <Button variant="success" disabled={importState.inProgress || !importState.isStorageNamespaceValid} onClick={() => {
                        if (importState.inProgress) return;
                        ingest()
                    }}>
                        {(importState.inProgress)? 'Importing...' : 'Import'}
                    </Button>
                </Modal.Footer>
                        </>
                    )}
            </Modal>
            <Button variant={variant} disabled={!enabled} onClick={onClick}>
                <BiImport/> Import
            </Button>
        </>
    );
}

const UploadButton = ({ Config, repo, reference, path, onDone, onClick, onHide, show = false}) => {
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
			{Config?.warnings &&
			 <Form.Group controlId="warnings">
			     <Warnings warnings={Config.warnings}/>
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
                    {(!!uploadState.error) ? (<Error error={uploadState.error}/>) : (<></>)}

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

            <Button variant="light" onClick={onClick}>
                <UploadIcon/> Upload Object
            </Button>
        </>
    );
}

const TreeContainer = ({ repo, reference, path, after, onPaginate, onRefresh, onUpload, onImport, refreshToken }) => {
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
    if (!!error) return <Error error={error}/>;

    return (
        <>
            {deleteState.error && <Error error={deleteState.error} onDismiss={() => setDeleteState(initialState)}/>}
            <Tree
                repo={repo}
                reference={reference}
                path={(!!path) ? path : ""}
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
                reference={reference}
                repo={repo}
                path={(!!path) ? path : ""}
                after={(!!after) ? after : ""}
                onPaginate={after => {
                    const query = {after}
                    if (!!path) query.path = path
                    if (!!reference) query.ref = reference.id
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
