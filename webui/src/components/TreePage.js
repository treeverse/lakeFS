import React, {useEffect, useState, useRef} from "react";
import {useHistory, useLocation} from "react-router-dom";
import {connect} from "react-redux";

import ButtonToolbar from "react-bootstrap/ButtonToolbar";
import Button from "react-bootstrap/Button";
import Badge from "react-bootstrap/Badge";

import Octicon, {GitCommit, Plus, X} from "@primer/octicons-react";

import {deleteObject, listTree, listTreePaginate, upload, uploadDone} from "../actions/objects";
import {diff, resetDiff} from "../actions/refs";
import RefDropdown from "./RefDropdown";
import Tree from "./Tree";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import {doCommit, resetCommit} from "../actions/commits";
import Alert from "react-bootstrap/Alert";

const UploadButton = connect(
    ({ objects }) => ({ uploadState: objects.upload }),
    ({ upload, uploadDone })
)(({ repo, refId, path, uploadState, upload, uploadDone }) => {
    const [show, setShow] = useState(false);
    const textRef = useRef(null);
    const fileRef = useRef(null);

    useEffect(() => {
        if (uploadState.done) {
            setShow(false);
            uploadDone()
        }
    }, [uploadDone, uploadState.done]);

    if (!refId || refId.type !== 'branch') {
        return <span/>;
    }

    const disabled = uploadState.inProgress;

    const onHide = () => {
        if (disabled) return; setShow(false);
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
                            <Form.Control type="text" placeholder="Object path" autoFocus name="text" ref={textRef} defaultValue={path}/>
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
                <Octicon icon={GitCommit}/> Upload Object
            </Button>
        </>
    );
});

const CommitButton = connect(
    ({ commits }) => ({ commitState: commits.commit }),
    ({ doCommit, resetCommit })
)(({ repo, refId, commitState, doCommit, resetCommit, changes }) => {

    const textRef = useRef(null);

    const [show, setShow] = useState(false);
    const [metadataFields, setMetadataFields] = useState([]);

    const disabled = commitState.inProgress;

    const onHide = () => {
        if (disabled) return;
        setShow(false);
        setMetadataFields([]);
    };

    useEffect(() => {
        if (commitState.done) {
            setShow(false);
            setMetadataFields([]);
            resetCommit();
        }
    }, [resetCommit, commitState.done]);

    const onSubmit = () => {
        if (disabled) return;
        const message = textRef.current.value;
        const metadata = {};
        metadataFields.forEach(pair => {
            if (pair.key.length > 0)
                metadata[pair.key] = pair.value;
        });
        doCommit(repo.id, refId.id, message, metadata);
    };

    if (!refId || refId.type !== 'branch') {
        return <span/>;
    }

    let commitDisabled = true;
    let commitVariant = 'secondary';
    if (changes > 0) {
        commitDisabled = false;
        commitVariant = 'success';
    }

    return (
        <>
            <Modal show={show} onHide={onHide}>
                <Modal.Header closeButton>
                    <Modal.Title>Commit Changes</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    <Form className="mb-2" onSubmit={(e) => {
                        onSubmit();
                        e.preventDefault();
                    }}>
                        <Form.Group controlId="message">
                            <Form.Control type="text" placeholder="Commit Message" ref={textRef}/>
                        </Form.Group>

                        {metadataFields.map((f, i) => {
                            return (
                                <Form.Group controlId="message" key={`commit-metadata-field-${f.key}-${f.value}-${i}`}>
                                    <Row>
                                        <Col md={{span: 5}}>
                                            <Form.Control type="text" placeholder="Key"  defaultValue={f.key} onChange={(e) => {
                                                metadataFields[i].key = e.currentTarget.value;
                                                setMetadataFields(metadataFields);
                                            }}/>
                                        </Col>
                                        <Col md={{ span: 5}}>
                                            <Form.Control type="text" placeholder="Value"  defaultValue={f.value}  onChange={(e) => {
                                                metadataFields[i].value = e.currentTarget.value;
                                                setMetadataFields(metadataFields);
                                            }}/>
                                        </Col>
                                        <Col md={{ span: 1}}>
                                            <Form.Text>
                                                <Button size="sm" variant="outline-secondary" onClick={() => {
                                                    setMetadataFields([...metadataFields.slice(0,i), ...metadataFields.slice(i+1)]);
                                                }}>
                                                    <Octicon icon={X}/>
                                                </Button>
                                            </Form.Text>
                                        </Col>
                                    </Row>
                                </Form.Group>
                            )
                        })}

                        <Button onClick={() => {
                            setMetadataFields([...metadataFields, {key: "", value: ""}]);
                        }} size="sm" variant="outline-secondary">
                            <Octicon icon={Plus}/>{' '}
                            Add Metadata field
                        </Button>
                    </Form>
                    {(!!commitState.error) ? (<Alert variant="danger">{commitState.error}</Alert>) : (<span/>)}
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" disabled={disabled} onClick={onHide}>
                        Cancel
                    </Button>
                    <Button variant="success" disabled={disabled} onClick={onSubmit}>
                        Commit Changes
                    </Button>
                </Modal.Footer>
            </Modal>
            <Button disabled={commitDisabled} variant={commitVariant} onClick={() => { setShow(true); }}>
                <Octicon icon={GitCommit}/> Commit Changes{' '}
                {!commitDisabled &&
                <>
                    <Badge variant="light">{changes}</Badge>
                    <span className="sr-only">uncommited changes</span>
                </>
                }
            </Button>
        </>
    );
});


const TreePage = ({repo, refId, path, list, listTree, listTreePaginate, diff, resetDiff, diffResults, uploadState, deleteObject, deleteState, commitState}) => {
    const history = useHistory();
    const location = useLocation();

    useEffect(() => {
        listTree(repo.id, refId.id, path);
        if (refId.type === 'branch') {
            diff(repo.id, refId.id, refId.id);
        } else {
            resetDiff();
        }
    }, [repo.id, refId, path, listTree, diff, resetDiff, uploadState.done, commitState.done, deleteState.done]);

    const paginator = (!list.loading && !!list.payload && list.payload.pagination && list.payload.pagination.has_more);
    const changes = diffResults.payload ? diffResults.payload.results.length : 0;
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
                    <UploadButton refId={refId} repo={repo} path={path}/>
                    <CommitButton refId={refId} repo={repo} changes={changes}/>
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
                diffResults={diffResults}
                list={list}
                path={path}/>

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
    ({ objects, refs, commits }) => ({
        list: objects.list,
        diffResults: refs.diff,
        uploadState: objects.upload,
        deleteState: objects.delete,
        commitState: commits.commit,
    }),
    ({ listTree, listTreePaginate, diff, resetDiff, deleteObject })
)(TreePage);
