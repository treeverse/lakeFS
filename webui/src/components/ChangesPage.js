import React, {useCallback, useEffect, useRef, useState} from "react";
import {useLocation, useHistory} from "react-router-dom";
import {HistoryIcon, GitCommitIcon, PlusIcon, SyncIcon, XIcon} from "@primer/octicons-react";
import RefDropdown from "./RefDropdown";
import Changes from "./Changes";
import {connect} from "react-redux";
import {diff, diffPaginate, PAGINATION_AMOUNT} from "../actions/refs";
import {Button, ButtonToolbar, Col, Form, Modal, OverlayTrigger, Row, Tooltip} from "react-bootstrap";
import {resetRevertBranch, revertBranch} from "../actions/branches";
import ConfirmationModal from "./ConfirmationModal";
import {doCommit, resetCommit} from "../actions/commits";
import Alert from "react-bootstrap/Alert";

const CommitButton = connect(
    ({ commits }) => ({ commitState: commits.commit }),
    ({ doCommit, resetCommit })
)(({ repo, refId, commitState, doCommit, resetCommit, disabled }) => {

    const textRef = useRef(null);

    const [show, setShow] = useState(false);
    const [metadataFields, setMetadataFields] = useState([]);

    const commitDisabled = commitState.inProgress;

    const onHide = () => {
        if (commitDisabled) return;
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
        if (commitDisabled) return;
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
                                                <Button size="sm" variant="secondary" onClick={() => {
                                                    setMetadataFields([...metadataFields.slice(0,i), ...metadataFields.slice(i+1)]);
                                                }}>
                                                    <XIcon/>
                                                </Button>
                                            </Form.Text>
                                        </Col>
                                    </Row>
                                </Form.Group>
                            )
                        })}

                        <Button onClick={() => {
                            setMetadataFields([...metadataFields, {key: "", value: ""}]);
                        }} size="sm" variant="secondary">
                            <PlusIcon/>{' '}
                            Add Metadata field
                        </Button>
                    </Form>
                    {(!!commitState.error) ? (<Alert variant="danger">{commitState.error}</Alert>) : (<span/>)}
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" disabled={commitDisabled} onClick={onHide}>
                        Cancel
                    </Button>
                    <Button variant="success" disabled={commitDisabled} onClick={onSubmit}>
                        Commit Changes
                    </Button>
                </Modal.Footer>
            </Modal>
            <Button variant="success" disabled={disabled} onClick={() => { setShow(true); }}>
                <GitCommitIcon/> Commit Changes{' '}
            </Button>
        </>
    );
});

const RevertButton = connect(
    ({ branches }) => ({ status: branches.revert }),
    ({ revertBranch, resetRevertBranch })
)(({ repo, refId, status, revertBranch, resetRevertBranch, disabled }) => {
    if (!refId || refId.type !== 'branch') {
        return null;
    }
    const [show, setShow] = useState(false);
    const submitDisabled = status.inProgress;

    const onHide = () => {
        if (submitDisabled) return;
        setShow(false);
    };

    useEffect(() => {
        if (status.error) {
            window.alert(status.error);
            resetRevertBranch();
        } else if (status.done) {
            setShow(false);
            resetRevertBranch();
        }
    }, [status, resetRevertBranch]);

    const onSubmit = () => {
        if (submitDisabled) return;
        revertBranch(repo.id, refId.id, {type: "reset"});
        setShow(false);
    };

    return (
        <>
            <ConfirmationModal show={show} onHide={onHide} msg="Are you sure you want to revert all uncommitted changes?" onConfirm={onSubmit} />
            <Button variant="light" disabled={disabled} onClick={() => { setShow(true) }}>
                <HistoryIcon/> Revert
            </Button>
        </>
    );
});

const ChangesPage = ({repo, refId, path, diff, diffPaginate, diffResults, commitState, revertState}) => {
    const history = useHistory();
    const location = useLocation();

    const refreshData   = useCallback(() => {
        diff(repo.id, refId.id, refId.id);
    }, [repo.id, refId.id, diff]);

    useEffect(() => {
        refreshData();
    },[repo.id, refId.id, refreshData,  commitState.done, revertState.done]);

    const paginator =(!diffResults.loading && !!diffResults.payload && diffResults.payload.pagination && diffResults.payload.pagination.has_more);
    const hasNoChanges = !diffResults.payload || !diffResults.payload.results || diffResults.payload.results.length === 0;
    return(
        <>
        <div className="mt-3 mb-5">
            <div className="action-bar">
                <ButtonToolbar className="float-left mb-2">
                    <RefDropdown repo={repo} selected={refId} withCommits={false} selectRef={(ref) => {
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
                        <Button variant="light" onClick={refreshData}><SyncIcon/></Button>
                    </OverlayTrigger>

                    <RevertButton refId={refId} repo={repo} disabled={hasNoChanges} />
                    <CommitButton refId={refId} repo={repo} disabled={hasNoChanges}/>
                </ButtonToolbar>

            </div>

            <>
                <Changes
                    repo={repo}
                    refId={refId}
                    showActions={true}
                    list={diffResults}
                    path={path}
                />

                {paginator &&
                <p className="tree-paginator">
                    <Button variant="outline-primary" onClick={() => {
                        diffPaginate(repo.id, refId.id, refId.id, diffResults.payload.pagination.next_offset, PAGINATION_AMOUNT);
                    }}>
                        Load More
                    </Button>
                </p>
                }
            </>
        </div>

        </>
    );
};


export default connect(
    ({refs, commits, branches}) => ({
        diffResults: refs.diff,
        commitState: commits.commit,
        revertState: branches.revert,
    }),
    ({diff, diffPaginate})
)(ChangesPage);
