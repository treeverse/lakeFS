import React, {useCallback, useEffect, useRef, useState} from "react";
import * as dayjs from 'dayjs'
import {UploadIcon} from "@primer/octicons-react";
import {RepositoryPageLayout} from "../../../lib/components/repository/layout";
import RefDropdown from "../../../lib/components/repository/refDropdown";
import {
    ActionGroup,
    ActionsBar,
    Error,
    Loading,
    PrefixSearchWidget,
    RefreshButton,
    Warnings
} from "../../../lib/components/controls";
import Button from "react-bootstrap/Button";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Alert from "react-bootstrap/Alert";
import {BsCloudArrowUp} from "react-icons/bs";

import {Tree} from "../../../lib/components/repository/tree";
import {config, objects, refs, staging, retention, repositories, NotFoundError} from "../../../lib/api";
import {useAPI, useAPIWithPagination} from "../../../lib/hooks/api";
import {RefContextProvider, useRefs} from "../../../lib/hooks/repo";
import {useRouter} from "../../../lib/hooks/router";
import {RefTypeBranch} from "../../../constants";
import {
    ExecuteImportButton,
    ImportDone,
    ImportForm,
    ImportPhase,
    ImportProgress,
    runImport
} from "../services/import_data";
import {Box} from "@mui/material";
import {RepoError} from "./error";
import { getContentType, getFileExtension, FileContents } from "./objectViewer";


const README_FILE_NAME = 'README.md';
const REPOSITORY_AGE_BEFORE_GC = 14;

const ImportButton = ({variant = "success", enabled = false, onClick}) => {
    return (
        <Button variant={variant} disabled={!enabled} onClick={onClick}>
            <BsCloudArrowUp/> Import
        </Button>
    )
}

const ImportModal = ({config, repoId, referenceId, referenceType, path = '', onDone, onHide, show = false}) => {
    const [importPhase, setImportPhase] = useState(ImportPhase.NotStarted);
    const [numberOfImportedObjects, setNumberOfImportedObjects] = useState(0);
    const [isImportEnabled, setIsImportEnabled] = useState(false);
    const [importError, setImportError] = useState(null);
    const [metadataFields, setMetadataFields] = useState([])

    const sourceRef = useRef(null);
    const destRef = useRef(null);
    const commitMsgRef = useRef(null);
    let currBranch = referenceId;
    currBranch = currBranch.match(/^_(.*)_imported$/)?.[1] || currBranch; // trim "_imported" suffix if used as import source
    let importBranch = `_${currBranch}_imported`;

    if (!referenceId || referenceType !== RefTypeBranch) return <></>

    const resetState = () => {
        setImportError(null);
        setImportPhase(ImportPhase.NotStarted);
        setIsImportEnabled(false);
        setNumberOfImportedObjects(0);
        setMetadataFields([]);
    }

    const hide = () => {
        if (ImportPhase.InProgress === importPhase || ImportPhase.Merging === importPhase) return;
        resetState()
        onHide()
    };

    const doMerge = async () => {
        setImportPhase(ImportPhase.Merging);
        try {
            await refs.merge(repoId, importBranch, currBranch);
            onDone();
            hide();
        } catch (error) {
            setImportPhase(ImportPhase.MergeFailed);
            setImportError(error);
        }
    };

    const doImport = async () => {
        setImportPhase(ImportPhase.InProgress);
        const updateStateFromImport = ({importPhase, numObj}) => {
            setImportPhase(importPhase);
            setNumberOfImportedObjects(numObj);
        }
        try {
            const metadata = {};
            metadataFields.forEach(pair => metadata[pair.key] = pair.value)
            await runImport(
                updateStateFromImport,
                destRef.current.value,
                commitMsgRef.current.value,
                sourceRef.current.value,
                importBranch,
                repoId,
                referenceId,
                metadata
            );
            onDone();
        } catch (error) {
            setImportPhase(ImportPhase.Failed);
            setImportError(error);
            setIsImportEnabled(false);
        }
    }
    const pathStyle = {'minWidth': '25%'};

    return (
        <>
            <Modal show={show} onHide={hide} size="lg">
                <Modal.Header closeButton>
                    <Modal.Title>Import data from {config.blockstore_type}</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    {
                        (importPhase === ImportPhase.NotStarted ||
                            importPhase === ImportPhase.Failed  ||
                            importPhase === ImportPhase.MergeFailed) &&
                        <ImportForm
                            config={config}
                            pathStyle={pathStyle}
                            sourceRef={sourceRef}
                            destRef={destRef}
                            updateSrcValidity={(isValid) => setIsImportEnabled(isValid)}
                            repoId={repoId}
                            importBranch={importBranch}
                            path={path}
                            commitMsgRef={commitMsgRef}
                            shouldAddPath={true}
                            metadataFields={metadataFields}
                            setMetadataFields={setMetadataFields}
                            err={importError}
                        />
                    }
                    {
                        importPhase === ImportPhase.InProgress &&
                        <ImportProgress numObjects={numberOfImportedObjects}/>
                    }
                    {
                        importPhase === ImportPhase.Completed &&
                        <ImportDone currBranch={currBranch} importBranch={importBranch}
                                    numObjects={numberOfImportedObjects}/>
                    }
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" disabled={importPhase === ImportPhase.InProgress} onClick={hide}>
                        Cancel
                    </Button>

                    <ExecuteImportButton
                        importPhase={importPhase}
                        importFunc={doImport}
                        mergeFunc={doMerge}
                        doneFunc={onDone}
                        isEnabled={isImportEnabled}/>

                </Modal.Footer>
            </Modal>
        </>
    );
}

const UploadButton = ({config, repo, reference, path, onDone, onClick, onHide, show = false}) => {
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
            if (config.pre_sign_support && config.pre_sign_support_ui) {
                let getResp = await staging.get(repo.id, reference.id, textRef.current.value, config.pre_sign_support);

                const putResp = await fetch(getResp.presigned_url, {
                    method: 'PUT',
                    mode: 'cors',
                    body: fileRef.current.files[0]
                });

                let etag = putResp.headers.get('ETag').replace(/["]/g, '')
                await staging.link(repo.id, reference.id, textRef.current.value, getResp, etag, fileRef.current.files[0].size);
            } else {
                await objects.upload(repo.id, reference.id, textRef.current.value, fileRef.current.files[0])
            }
            setUploadState({...initialState})
            onDone()
        } catch (error) {
            setUploadState({...initialState, error})
            throw error
        }
        onHide();
    }

    const basePath = `${repo.id}/${reference.id}/\u00A0`;

    const pathStyle = {'minWidth': '25%'};

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
                            <Form.Group controlId="warnings" className="mb-3">
                                <Warnings warnings={config.warnings}/>
                            </Form.Group>}
                        <Form.Group controlId="path" className="mb-3">
                            <Row className="g-0">
                                <Col className="col-auto d-flex align-items-center justify-content-start">
                                    {basePath}
                                </Col>
                                <Col style={pathStyle}>
                                    <Form.Control type="text" placeholder="Object name" autoFocus name="text"
                                                  ref={textRef} defaultValue={path}/>
                                </Col>
                            </Row>
                        </Form.Group>

                        <Form.Group controlId="content" className="mb-3">
                            <Form.Control
                                type="file"
                                name="content"
                                ref={fileRef}
                                onChange={(e) => {
                                    const currPath = textRef.current.value.substr(0, textRef.current.value.lastIndexOf('/') + 1);
                                    const currName = (e.currentTarget.files.length > 0) ? e.currentTarget.files[0].name : ""
                                    textRef.current.value = currPath + currName;
                                }}
                            />
                        </Form.Group>
                    </Form>
                    {(uploadState.error) ? (<Error error={uploadState.error}/>) : (<></>)}

                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" disabled={uploadState.inProgress} onClick={hide}>
                        Cancel
                    </Button>
                    <Button variant="success" disabled={uploadState.inProgress} onClick={() => {
                        if (uploadState.inProgress) return;
                        upload()
                    }}>
                        {(uploadState.inProgress) ? 'Uploading...' : 'Upload'}
                    </Button>
                </Modal.Footer>
            </Modal>

            <Button
                variant={(!config.import_support) ? "success" : "light"}
                onClick={onClick}>
                <UploadIcon/> Upload Object
            </Button>
        </>
    );
}

const TreeContainer = ({
                           config,
                           repo,
                           reference,
                           path,
                           after,
                           onPaginate,
                           onRefresh,
                           onUpload,
                           onImport,
                           refreshToken
                       }) => {
    const {results, error, loading, nextPage} = useAPIWithPagination(() => {
        return objects.list(repo.id, reference.id, path, after, config.pre_sign_support)
    }, [repo.id, reference.id, path, after, refreshToken]);
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

const ReadmeContainer = ({config, repo, reference, path='', refreshDep=''}) => {
    let readmePath = '';

    if (path) {
        readmePath = path.endsWith('/') ? `${path}${README_FILE_NAME}` : `${path}/${README_FILE_NAME}`;
    } else {
        readmePath = README_FILE_NAME;
    }
    const {response, error, loading} = useAPI(
        () => objects.head(repo.id, reference.id, readmePath),
        [path, refreshDep]);

    if (loading || error) {
        return <></>; // no file found.
    }

    const fileExtension = getFileExtension(readmePath);
    const contentType = getContentType(response?.headers);

    return (
        <FileContents 
            repoId={repo.id} 
            refId={reference.id}
            path={readmePath}
            fileExtension={fileExtension}
            contentType={contentType}
            error={error}
            loading={loading}
            showFullNavigator={false}
            presign={config.pre_sign_support}
        />
    );
}

const NoGCRulesWarning = ({ repoId }) => {
    const storageKey = `show_gc_warning_${repoId}`;
    const [show, setShow] = useState(window.localStorage.getItem(storageKey) !== "false")
    const closeAndRemember = useCallback(() => {
        window.localStorage.setItem(storageKey, "false")
        setShow(false)
    }, [repoId])

    const {response} = useAPI(async() => {
        const repo = await repositories.get(repoId)
        if (!repo.storage_namespace.startsWith('s3:') &&
            !repo.storage_namespace.startsWith('http')) {
            return false;
        }
        const createdAgo = dayjs().diff(dayjs.unix(repo.creation_date), 'days');
        if (createdAgo > REPOSITORY_AGE_BEFORE_GC) {
            try {
                await retention.getGCPolicy(repoId);
            } catch (e) {
                if (e instanceof NotFoundError) {
                    return true
                }
            }
        }
        return false;
    }, [repoId]);

    if (show && response) {
        return (
            <Alert
                variant="warning"
                onClose={closeAndRemember}
                dismissible>
                <strong>Warning</strong>: No garbage collection rules configured for this repository.
                {' '}
                <a href="https://docs.lakefs.io/howto/garbage-collection.html" target="_blank" rel="noreferrer">Learn More</a>.

            </Alert>
        )
    }
    return <></>;
}

const ObjectsBrowser = ({config, configError}) => {
    const router = useRouter();
    const {path, after} = router.query;
    const {repo, reference, loading, error} = useRefs();
    const [showUpload, setShowUpload] = useState(false);
    const [showImport, setShowImport] = useState(false);
    const [refreshToken, setRefreshToken] = useState(false);
    const refresh = () => setRefreshToken(!refreshToken);
    const parts = path && path.split("/") || [];
    const searchSuffix = parts.pop();
    let searchPrefix = parts.join("/");
    searchPrefix = searchPrefix && searchPrefix + "/"

    if (loading || !config) return <Loading/>;
    if (error || configError) return <RepoError error={error || configError}/>;

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
                            query: {ref: ref.id, path: path === undefined ? '' : path}
                        })}
                    />
                </ActionGroup>

                <ActionGroup orientation="right">
                    <PrefixSearchWidget
                        text="Search by Prefix"
                        key={path}
                        defaultValue={searchSuffix}
                        onFilter={prefix => {
                            const query = {path: ""};
                            if (searchPrefix !== undefined) query.path = searchPrefix;
                            if (prefix) query.path += prefix;
                            if (reference) query.ref = reference.id;
                            const url = {pathname: `/repositories/:repoId/objects`, query, params: {repoId: repo.id}};
                            router.push(url);
                        }}/>
                    <RefreshButton onClick={refresh}/>
                    <UploadButton
                        config={config}
                        path={path}
                        repo={repo}
                        reference={reference}
                        onDone={refresh}
                        onClick={() => {
                            setShowUpload(true);
                        }}
                        onHide={() => {
                            setShowUpload(false);
                        }}
                        show={showUpload}
                    />
                    <ImportButton
                        onClick={() => setShowImport(true)}
                        enabled={config.import_support}
                    />
                    <ImportModal
                        config={config}
                        path={path}
                        repoId={repo.id}
                        referenceId={reference.id}
                        referenceType={reference.type}
                        onDone={refresh}
                        onHide={() => {
                            setShowImport(false);
                        }}
                        show={showImport}
                    />
                </ActionGroup>
            </ActionsBar>

            <NoGCRulesWarning repoId={repo.id}/>

            <Box sx={{display: 'flex', flexDirection: 'column', gap: '10px', mb: '30px'}}>
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
                    onUpload={() => {
                        setShowUpload(true);
                    }}
                    onImport={() => {
                        setShowImport(true);
                    }}
                    onRefresh={refresh}/>

                <ReadmeContainer config={config} reference={reference} repo={repo} path={path} refreshDep={refreshToken}/>
            </Box>
        </>
    );
};

const RepositoryObjectsPage = () => {
    const [configRes, setConfigRes] = useState(null);
    const {response, error: err, loading} = useAPI(() => {
        return config.getStorageConfig();
    });

    useEffect(() => {
        if (response) {
            setConfigRes(response);
        }
    }, [response, setConfigRes]);


    return (
        <RefContextProvider>
            <RepositoryPageLayout activePage={'objects'}>
                {loading && <Loading/>}
                <ObjectsBrowser config={configRes} configError={err}/>
            </RepositoryPageLayout>
        </RefContextProvider>
    );
};

export default RepositoryObjectsPage;
