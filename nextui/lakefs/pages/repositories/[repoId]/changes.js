import React, {useRef, useState} from "react";
import {useRouter} from "next/router";

import {
    GitCommitIcon,
    HistoryIcon,
    PlusIcon,
    SyncIcon,
    XIcon
} from "@primer/octicons-react";

import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Alert from "react-bootstrap/Alert";
import Table from "react-bootstrap/Table";
import Card from "react-bootstrap/Card";
import {OverlayTrigger} from "react-bootstrap";
import Tooltip from "react-bootstrap/Tooltip";
import Button from "react-bootstrap/Button";

import {refs, branches, commits} from "../../../rest/api";
import {useAPIWithPagination} from "../../../rest/hooks";
import {useRepoAndRef} from "../../../lib/hooks/repo";
import {ConfirmationModal} from "../../../lib/components/modals";
import {ActionGroup, ActionsBar, Error, Loading} from "../../../lib/components/controls";
import RefDropdown from "../../../lib/components/repository/refDropdown";
import {RepositoryPageLayout} from "../../../lib/components/repository/layout";
import {formatAlertText} from "../../../lib/components/repository/errors";
import {ChangeEntryRow} from "../../../lib/components/repository/changes";


const CommitButton = ({ repo, onCommit, enabled = false }) => {

    const textRef = useRef(null);

    const [committing, setCommitting] = useState(false)
    const [show, setShow] = useState(false)
    const [metadataFields, setMetadataFields] = useState([])

    const hide = () => {
        if (committing) return;
        setShow(false)
    }

    const onSubmit = () => {
        const message = textRef.current.value;
        const metadata = {};
        metadataFields.forEach(pair => metadata[pair.key] = pair.value)
        setCommitting(true)
        onCommit({message, metadata}, () => {
            setCommitting(false)
            setShow(false);
        })
    };

    const alertText = formatAlertText(repo.id, null);
    return (
        <>
            <Modal show={show} onHide={hide} size="lg">
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
                    {(alertText) ? (<Alert variant="danger">{alertText}</Alert>) : (<span/>)}
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" disabled={committing} onClick={hide}>
                        Cancel
                    </Button>
                    <Button variant="success" disabled={committing} onClick={onSubmit}>
                        Commit Changes
                    </Button>
                </Modal.Footer>
            </Modal>
            <Button variant="success" disabled={!enabled} onClick={() => setShow(true)}>
                <GitCommitIcon/> Commit Changes{' '}
            </Button>
        </>
    );
}


const RevertButton =({ onRevert, enabled = false }) => {
    const [show, setShow] = useState(false)
    const hide = () => setShow(false)

    return (
        <>
            <ConfirmationModal
                show={show}
                onHide={hide}
                msg="Are you sure you want to revert all uncommitted changes?"
                onConfirm={() => {
                    onRevert()
                    hide()
                }} />
            <Button variant="light" disabled={!enabled} onClick={() => setShow(true)}>
                <HistoryIcon/> Revert
            </Button>
        </>
    );
}

const ChangesContainer = ({ repo, reference, onSelectRef, showActions = true }) => {
    const [actionError, setActionError] = useState(null)
    const [internalRefresh, setInternalRefresh] = useState(true)

    const { results, error, loading, paginate, hasMore } = useAPIWithPagination(async (after) => {
        return refs.changes(repo.id, reference.id, after, 1)
    }, [repo.id, reference.id, internalRefresh])

    const refresh = () => setInternalRefresh(!internalRefresh)

    if (!!error) return <Error error={error}/>
    if (loading) return <Loading/>

    const actionErrorDisplay = (!!actionError) ?
        <Error error={actionError} onDismiss={() => setActionError(null)}/> : <></>

    const paginationButton = (hasMore) ? (
        <p className="tree-paginator mt-3">
            <Button variant="outline-primary" onClick={paginate}>Load More</Button>
        </p>
    ) : <></>

    return (
        <>
            <ActionsBar>
                <ActionGroup orientation="left">
                    <RefDropdown
                        emptyText={'Select Branch'}
                        repo={repo}
                        selected={(!!reference) ? reference : null}
                        withCommits={false}
                        withWorkspace={false}
                        selectRef={onSelectRef}
                    />
                </ActionGroup>

                <ActionGroup orientation="right">

                    <OverlayTrigger placement="bottom"
                                    overlay={<Tooltip id="refreshTooltipId">Refresh</Tooltip>}>
                        <Button variant="light" onClick={refresh}>
                            <SyncIcon/>
                        </Button>
                    </OverlayTrigger>

                    <RevertButton enabled={results.length > 0} onRevert={() => {
                        branches.revert(repo.id, reference.id, {type: 'reset'})
                            .then(refresh)
                            .catch(error => setActionError(error))
                    }}/>
                    <CommitButton repo={repo} enabled={results.length > 0} onCommit={(commitDetails, done) => {
                        commits
                            .commit(repo.id, reference.id, commitDetails.message, commitDetails.metadata)
                            .then(() => {
                                done()
                                refresh()
                            })
                    }}/>
                </ActionGroup>
            </ActionsBar>

            {actionErrorDisplay}

            <div className="tree-container">
                    {(results.length === 0) ? <Alert variant="info">No changes</Alert> : (
                        <Card>
                            <Table borderless size="sm">
                                <tbody>
                                {results.map(entry => (
                                    <ChangeEntryRow key={entry.path} entry={entry} showActions={showActions} onRevert={(entry) => {
                                        branches
                                            .revert(repo.id, reference.id, {type: 'object', path: entry.path})
                                            .then(() => {
                                                setInternalRefresh(!internalRefresh)
                                            })
                                            .catch(error => {
                                                setActionError(error)
                                            })
                                        }}/>
                                ))}
                                </tbody>
                            </Table>
                        </Card>
                    )}

                {paginationButton}
            </div>
        </>
    )
}

const RefContainer = ({ repoId, refId, onSelectRef }) => {
    const {loading, error, response} = useRepoAndRef(repoId, refId)
    if (loading) return <Loading/>
    if (!!error) return <Error error={error}/>
    const { repo, ref } = response
    return (
        <ChangesContainer repo={repo} reference={ref} onSelectRef={onSelectRef}/>
    )
}

const RepositoryChangesPage = () => {
    const router = useRouter()
    const { repoId, ref } = router.query;

    return (
        <RepositoryPageLayout repoId={repoId} activePage={'changes'}>
            {(!repoId) ?
                <Loading/> :
                <RefContainer
                    repoId={repoId}
                    refId={ref}
                    onSelectRef={ref => router.push({
                        pathname: `/repositories/[repoId]/changes`,
                        query: {repoId, ref: ref.id}
                    })}
                />
            }
        </RepositoryPageLayout>
    )
}

export default RepositoryChangesPage;