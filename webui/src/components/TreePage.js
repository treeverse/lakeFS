import React, {useEffect, useState, useRef} from "react";
import {useHistory, useLocation} from "react-router-dom";
import {connect} from "react-redux";

import ButtonToolbar from "react-bootstrap/ButtonToolbar";
import Button from "react-bootstrap/Button";

import Octicon, {GitCommit, Plus} from "@primer/octicons-react";

import {listTree, upload, uploadDone} from "../actions/objects";
import {diff, resetDiff} from "../actions/refs";
import RefDropdown from "./RefDropdown";
import Tree from "./Tree";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import {doCommit, resetCommit} from "../actions/commits";


const CompareToolbar = ({repo, refId, compare}) => {
    const history = useHistory();
    const location = useLocation();

    return  (
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

                params.delete('compareCommit');
                params.delete('compareBranch');
                history.push({...location, search: params.toString()})
            }}/>

            <RefDropdown
                repo={repo} 
                selected={compare}
                prefix={'Compared to '}
                emptyText={'Compare with...'}
                withWorkspace={false}
                onCancel={() => {
                    const params = new URLSearchParams(location.search);
                    params.delete('compareBranch');
                    params.delete('compareCommit');
                    history.push({...location, search: params.toString()})
                }}
                selectRef={(ref) => {
                    const params = new URLSearchParams(location.search);
                    if (ref.type === 'branch') {
                        params.set('compareBranch', ref.id);
                        params.delete('compareCommit'); // if we explicitly selected a branch, remove an existing commit if any
                    } else {
                        params.set('compareCommit', ref.id);
                        params.delete('compareBranch'); // if we explicitly selected a commit, remove an existing branch if any
                    }
                    history.push({...location, search: params.toString()})
                }}/>

        </ButtonToolbar>
    );
};

const UploadButton = connect(
    ({ objects }) => ({ uploadState: objects.upload }),
    ({ upload, uploadDone })
)(({ repo, refId, path, uploadState, upload, uploadDone }) => {
    const [show, setShow] = useState(false);
    const textRef = useRef(null);
    const fileRef = useRef(null);

    console.log('STATE: uploadState.done', uploadState);

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
)(({ repo, refId, commitState, doCommit, resetCommit }) => {

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

    return (
        <>
            <Modal show={show} onHide={onHide}>
                <Modal.Header closeButton>
                    <Modal.Title>Commit Changes</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    <Form onSubmit={(e) => {
                        onSubmit();
                        e.preventDefault();
                    }}>
                        <Form.Group controlId="message">
                            <Form.Control type="text" placeholder="Commit Message" ref={textRef}/>
                        </Form.Group>

                        {metadataFields.map((f, i) => {
                            return (
                                <Form.Group controlId="message" key={`commit-metadata-field-${i}`}>
                                    <Row>
                                        <Col md={{span: 6}}>
                                            <Form.Control type="text" placeholder="Key"  defaultValue={f.key} onChange={(e) => {
                                                metadataFields[i].key = e.currentTarget.value;
                                                setMetadataFields(metadataFields);
                                            }}/>
                                        </Col>
                                        <Col md={{ span: 6}}>
                                            <Form.Control type="text" placeholder="Value"  defaultValue={f.value}  onChange={(e) => {
                                                metadataFields[i].value = e.currentTarget.value;
                                                setMetadataFields(metadataFields);
                                            }}/>
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
            <Button variant="success" onClick={() => { setShow(true); }}>
                <Octicon icon={GitCommit}/> Commit Changes
            </Button>
        </>
    );
});


const TreePage = ({repo, refId, compareRef, path, list, listTree, diff, resetDiff, diffResults, uploadState}) => {
    const history = useHistory();
    const location = useLocation();

    let compare;
    if (!!compareRef) {
        compare = compareRef;
    }

    const compareId = (!!compare) ? compare.id : "";

    useEffect(() => {
        listTree(repo.id, refId.id, path);
    }, [repo.id, refId.id, path, listTree, uploadState.done]);

    useEffect(() => {
        if (!!compare) {
            diff(repo.id, refId.id, compare.id);
        } else {
            resetDiff();
        }
        // (compareId is computed from compare which is not included in the deps list)
        // eslint-disable-next-line
    },[repo.id, refId.id, listTree, diff, compareId, uploadState]);

    return (
        <div className="mt-3">
            <div className="action-bar">
                <CompareToolbar refId={refId} repo={repo} compare={compare}/>
                <ButtonToolbar className="float-right mb-2">
                    <UploadButton refId={refId} repo={repo} path={path}/>
                    <CommitButton refId={refId} repo={repo}/>
                </ButtonToolbar>
            </div>

            <Tree
                repo={repo}
                refId={refId}
                onNavigate={(path) => {
                    const params = new URLSearchParams(location.search);
                    params.set('path', path);
                    history.push({...location, search: params.toString()});
                }}
                diffResults={diffResults}
                list={list}
                path={path}/>
        </div>
    );
};

export default connect(
    ({ objects, refs }) => ({ list: objects.list, diffResults: refs.diff, uploadState: objects.upload }),
    ({ listTree, diff, resetDiff })
)(TreePage);