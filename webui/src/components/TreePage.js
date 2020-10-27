import React, {useCallback, useEffect, useRef, useState} from "react";
import {useHistory, useLocation} from "react-router-dom";
import {connect} from "react-redux";
import {Button, ButtonToolbar, Form, Modal, OverlayTrigger, Tooltip, Row, Col} from "react-bootstrap";
import {GitCommitIcon, SyncIcon} from "@primer/octicons-react";
import {deleteObject, listTree, listTreePaginate, upload, uploadDone} from "../actions/objects";
import RefDropdown from "./RefDropdown";
import Tree from "./Tree";
import {listBranches} from "../actions/branches";
import Alert from "react-bootstrap/Alert";

const UploadButton = connect(
    ({ objects }) => ({ uploadState: objects.upload }),
    ({ upload, uploadDone })
)(({ repo, refId, path, uploadState, upload, uploadDone, show, setShow }) => {
    const textRef = useRef(null);
    const fileRef = useRef(null);

    useEffect(() => {
        if (uploadState.done) {
            setShow(false);
            uploadDone()
        }
    }, [setShow, uploadDone, uploadState.done]);

    if (!refId || refId.type !== 'branch') {
        return <span/>;
    }

    const disabled = uploadState.inProgress;

    const onHide = () => {
        if (disabled) return; setShow(false);
    };

    const basePath = `${repo.id}/${refId.id}/\u00A0`;

    const pathStyle = {
        'min-width' : '25%',
    };

    return (
        <>
            <Modal show={show} onHide={onHide}>
                <Modal.Header closeButton>
                    <Modal.Title>Upload Object</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    <Form onSubmit={(e) => {
                        if (disabled) return;
                        upload(repo.id, refId.id, textRef.current.value, fileRef.current.files[0]);
                        e.preventDefault();
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
                            <Form.Control type="file" name="content" ref={fileRef} onChange={(e) => {
                                const currPath = textRef.current.value.substr(0, textRef.current.value.lastIndexOf('/')+1);
                                const currName = e.currentTarget.files[0].name;
                                textRef.current.value = currPath + currName;
                            }}/>
                        </Form.Group>
                    </Form>
                    {(!!uploadState.error) ? (<Alert variant="danger">{uploadState.error}</Alert>) : (<span/>)}

                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary"  disabled={disabled} onClick={onHide}>
                        Cancel
                    </Button>
                    <Button variant="success" disabled={disabled} onClick={() => {
                        if (disabled) return;
                        upload(repo.id, refId.id, textRef.current.value, fileRef.current.files[0]);
                    }}>
                        {(uploadState.inProgress)? 'Uploading...' : 'Upload'}
                    </Button>
                </Modal.Footer>
            </Modal>
            <Button variant="light" onClick={() => { setShow(true) }}>
                <GitCommitIcon/> Upload Object
            </Button>
        </>
    );
});

const TreePage = ({repo, refId, path, list, listTree, listTreePaginate, uploadState, deleteObject, deleteState, importState}) => {
    const history = useHistory();
    const location = useLocation();
    const[showUploadModal, setShowUploadModal] = useState(false)
    const refreshData = useCallback(() => {
        listTree(repo.id, refId.id, path);
    }, [repo.id, refId, path, listTree]);

    useEffect(() => {
        refreshData();
    }, [refreshData, repo.id, refId, path, listTree, uploadState.done, deleteState.done, importState.done]);

    const paginator = (!list.loading && !!list.payload && list.payload.pagination && list.payload.pagination.has_more);

    return (
        <div className="mt-3">
            <div className="action-bar">
                <ButtonToolbar className="float-left mb-2">

                    <RefDropdown
                        repo={repo}
                        selected={refId}
                        selectRef={(ref) => {
                        const params = new URLSearchParams(location.search);
                        if (ref.type === 'branch') {
                            params.set('branch', ref.id);
                            params.delete('commit'); // if we explicitly selected a branch, remove an existing commit if any
                        } else {
                            params.set('commit', ref.id);
                            params.delete('branch'); // if we explicitly selected a commit, remove an existing branch if any
                        }

                        history.push({...location, search: params.toString()})
                    }}/>

                </ButtonToolbar>

                <ButtonToolbar className="float-right mb-2">
                    <OverlayTrigger placement="bottom" overlay={<Tooltip id="refreshTooltipId">Refresh</Tooltip>}>
                        <Button variant="light" disabled={list.loading} onClick={refreshData}><SyncIcon/></Button>
                    </OverlayTrigger>

                    <UploadButton show={showUploadModal} setShow={setShowUploadModal} refId={refId} repo={repo} path={path}/>
                </ButtonToolbar>
            </div>

            <Tree
                repo={repo}
                refId={refId}
                showActions={true}
                onNavigate={(path) => {
                    const params = new URLSearchParams(location.search);
                    params.set('path', path);
                    history.push({...location, search: params.toString()});
                }}
                onDelete={(entry) => {
                    deleteObject(repo.id, refId.id, entry.path);
                }}
                list={list}
                path={path}
                setShowUploadModal={setShowUploadModal}/>
            {paginator &&
            <p className="tree-paginator">
                <Button variant="outline-primary" onClick={() => {
                    listTreePaginate(repo.id, refId.id, path, list.payload.pagination.next_offset);
                }}>
                    Load More
                </Button>
            </p>
            }
        </div>
    );
};

export default connect(
    ({objects}) => ({
        list: objects.list,
        uploadState: objects.upload,
        deleteState: objects.delete,
        importState: objects.import,
        importDryRunState: objects.importDryRun,
    }),
    ({listTree, listTreePaginate, deleteObject, listBranches})
)(TreePage);
