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

import {Tree} from "../../../lib/components/repository/tree";
import {config, objects} from "../../../lib/api";
import {useAPI, useAPIWithPagination} from "../../../lib/hooks/api";
import {RefContextProvider, useRefs} from "../../../lib/hooks/repo";
import {useRouter} from "../../../lib/hooks/router";


const UploadButton = ({ config, repo, reference, path, onDone, variant = "success", onClick, onHide, show = false}) => {
    const initialState = {
        inProgress: false,
        error: null,
        done: false
    }
    const [uploadState, setUploadState] = useState(initialState)

    const textRef = useRef(null);
    const fileRef = useRef(null);

    if (!reference || reference.type !== 'branch') return <></>

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

            <Button variant={variant} onClick={onClick}>
                <UploadIcon/> Upload Object
            </Button>
        </>
    );
}

const TreeContainer = ({ repo, reference, path, after, onPaginate, onRefresh, onUpload, refreshToken }) => {
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
