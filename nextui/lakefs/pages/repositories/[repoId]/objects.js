import {useRouter} from "next/router";
import {SyncIcon, UploadIcon} from "@primer/octicons-react";

import React, {useRef, useState} from "react";
import {RepositoryPageLayout} from "../../../lib/components/repository/layout";
import RefDropdown from "../../../lib/components/repository/refDropdown";
import {ActionGroup, ActionsBar, Loading} from "../../../lib/components/controls";
import Button from "react-bootstrap/Button";
import {OverlayTrigger} from "react-bootstrap";
import Tooltip from "react-bootstrap/Tooltip";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";

import {Tree} from "../../../lib/components/repository/tree";
import {Error} from "../../../lib/components/controls";
import {objects} from "../../../rest/api";
import {useAPIWithPagination} from "../../../rest/hooks";
import {RefContextProvider, useRefs, useRepoAndRef} from "../../../lib/hooks/repo";


const UploadButton = ({ repo, reference, path, onDone, variant = "success"}) => {
    const initialState = {
        inProgress: false,
        error: null,
        done: false
    }

    const [show, setShow] = useState(false)
    const [uploadState, setUploadState] = useState(initialState)

    const textRef = useRef(null);
    const fileRef = useRef(null);

    if (!reference || reference.type !== 'branch') return <></>

    const onHide = () => {
        if (uploadState.inProgress) return;
        setUploadState(initialState)
        setShow(false)
    };

    const upload = async () => {
        setUploadState({
            ...initialState,
            inProgress: true
        })
        try {
            await objects.upload(repo.id, reference.id, textRef.current.value, fileRef.current.files[0])
            setUploadState({...initialState})
            setShow(false)
            onDone()
        } catch (error) {
            setUploadState({...initialState, error})
        }
    }

    const basePath = `${repo.id}/${reference.id}/\u00A0`;

    const pathStyle = {'min-width' : '25%'};

    return (
        <>
            <Modal show={show} onHide={onHide}>
                <Modal.Header closeButton>
                    <Modal.Title>Upload Object</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    <Form onSubmit={(e) => {
                        if (uploadState.inProgress) return;
                        upload()
                        e.preventDefault()
                    }}>
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
                    <Button variant="secondary"  disabled={uploadState.inProgress} onClick={onHide}>
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

            <Button variant={variant} onClick={() => { setShow(true) }}>
                <UploadIcon/> Upload Object
            </Button>
        </>
    );
}

const TreeContainer = ({ repo, reference, path, after, onPaginate, onRefresh, refreshToken }) => {
    const { results, error, loading, nextPage } = useAPIWithPagination( () => {
        return objects.list(repo.id, reference.id, path, after)
    },[repo.id, reference.id, path, after, refreshToken])

    if (loading) return <Loading/>
    if (!!error) return <Error error={error}/>

    return (
        <Tree
            repo={repo}
            reference={reference}
            path={(!!path) ? path : ""}
            showActions={true}
            results={results}
            after={after}
            nextPage={nextPage}
            onPaginate={onPaginate}
            onDelete={entry => {
                objects
                    .delete(repo.id, reference.id, entry.path)
                    .catch(error => console.log(error))
                    .then(onRefresh)
            }}
        />
    )
}


const ObjectsBrowser = () => {
    const router = useRouter()
    const { path, after } = router.query;

    const { repo, reference, loading, error } = useRefs()

    console.log({repo, reference})

    const [refreshToken, setRefreshToken] = useState(false)
    const refresh = () => setRefreshToken(!refreshToken)

    if (loading) return <Loading/>
    if (!!error) return <Error error={error}/>

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
                            pathname: `/repositories/[repoId]/objects`,
                            query: {repoId: repo.id, ref: ref.id}
                        })}
                    />
                </ActionGroup>

                <ActionGroup orientation="right">

                    <OverlayTrigger placement="bottom"
                                    overlay={<Tooltip id="refreshTooltipId">Refresh</Tooltip>}>
                        <Button variant="light" onClick={refresh}>
                            <SyncIcon/>
                        </Button>
                    </OverlayTrigger>

                    <UploadButton path={path} repo={repo} reference={reference} onDone={refresh}/>

                </ActionGroup>
            </ActionsBar>

            <TreeContainer
                reference={reference}
                repo={repo}
                path={(!!path) ? path : ""}
                after={(!!after) ? after : ""}
                onPaginate={after => {
                    const query = {repoId: repo.id, after}
                    if (!!path) query.path = path
                    if (!!reference) query.ref = reference.id
                    const url = {pathname: `/repositories/[repoId]/objects`, query}
                    router.push(url)
                }}
                refreshToken={refreshToken}
                onRefresh={refresh}/>
        </>
    )
}

const RepositoryObjectsPage = () => {
    return (
          <RefContextProvider>
              <RepositoryPageLayout activePage={'objects'}>
                <ObjectsBrowser/>
              </RepositoryPageLayout>
          </RefContextProvider>
    )
}

export default RepositoryObjectsPage;