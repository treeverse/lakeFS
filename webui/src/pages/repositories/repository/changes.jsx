import React, {useRef, useState} from "react";

import {
    GitCommitIcon,
    HistoryIcon,
    PlusIcon,
    XIcon
} from "@primer/octicons-react";

import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Alert from "react-bootstrap/Alert";
import Table from "react-bootstrap/Table";
import Card from "react-bootstrap/Card";
import Button from "react-bootstrap/Button";

import {refs, branches, commits} from "../../../lib/api";
import {useAPIWithPagination} from "../../../lib/hooks/api";
import {RefContextProvider, useRefs} from "../../../lib/hooks/repo";
import {ConfirmationModal} from "../../../lib/components/modals";
import {ActionGroup, ActionsBar, Error, Loading, RefreshButton} from "../../../lib/components/controls";
import RefDropdown from "../../../lib/components/repository/refDropdown";
import {RepositoryPageLayout} from "../../../lib/components/repository/layout";
import {formatAlertText} from "../../../lib/components/repository/errors";
import {ChangeEntryRow, TreeItem} from "../../../lib/components/repository/changes";
import {Paginator} from "../../../lib/components/pagination";
import {useRouter} from "../../../lib/hooks/router";
import {URINavigator} from "../../../lib/components/repository/tree";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import {ToggleButton} from "react-bootstrap";
import {Link} from "../../../lib/components/nav";


const CommitButton = ({repo, onCommit, enabled = false}) => {

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
                                            <Form.Control type="text" placeholder="Key" defaultValue={f.key}
                                                          onChange={(e) => {
                                                              metadataFields[i].key = e.currentTarget.value;
                                                              setMetadataFields(metadataFields);
                                                          }}/>
                                        </Col>
                                        <Col md={{span: 5}}>
                                            <Form.Control type="text" placeholder="Value" defaultValue={f.value}
                                                          onChange={(e) => {
                                                              metadataFields[i].value = e.currentTarget.value;
                                                              setMetadataFields(metadataFields);
                                                          }}/>
                                        </Col>
                                        <Col md={{span: 1}}>
                                            <Form.Text>
                                                <Button size="sm" variant="secondary" onClick={() => {
                                                    setMetadataFields([...metadataFields.slice(0, i), ...metadataFields.slice(i + 1)]);
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


const RevertButton = ({onRevert, enabled = false}) => {
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
                }}/>
            <Button variant="light" disabled={!enabled} onClick={() => setShow(true)}>
                <HistoryIcon/> Revert
            </Button>
        </>
    );
}

const ChangesBrowser = ({repo, reference, after, prefix, view, onSelectRef, onPaginate}) => {
    const [actionError, setActionError] = useState(null);
    const [internalRefresh, setInternalRefresh] = useState(true);

    const radios = [
        {name: 'Flat', value: 'flat', selected: false},
        {name: 'Directory', value: 'dir', selected: false},
        {name: 'Tree', value: 'tree', selected: false},
    ];

    let delimiter = ""
    switch (view) {
        case "dir":
            delimiter = "/";
            radios[1].selected = true;
            break;
        case "tree":
            delimiter = "/";
            radios[2].selected = true;
            break;
        default:
            delimiter = "";
            radios[0].selected = true;
            break;
    }

    const {results, error, loading, nextPage} = useAPIWithPagination(async () => {
        if (!repo) return
        return refs.changes(repo.id, reference.id, after, prefix, delimiter)
    }, [repo.id, reference.id, internalRefresh, after, prefix, delimiter])

    const refresh = () => setInternalRefresh(!internalRefresh)

    if (!!error) return <Error error={error}/>
    if (loading) return <Loading/>

    let onRevert = async (entry) => {
        branches
            .revert(repo.id, reference.id, {type: entry.path_type, path: entry.path})
            .then(() => {
                setInternalRefresh(!internalRefresh)
            })
            .catch(error => {
                setActionError(error)
            })
    }

    let tablebody;
    if (view === 'tree') {
        tablebody =
            <tbody>
            {results.map(entry => (
                <TreeItem key={entry.path + "-tree-item"} entry={entry} repo={repo} reference={reference}
                          internalReferesh={internalRefresh}
                          onRevert={onRevert} delimiter={delimiter} after={after} relativeTo={""}
                          getMore={(afterUpdated, path) => {
                              return refs.changes(repo.id, reference.id, afterUpdated, path, delimiter)
                          }}/>
            ))}
            </tbody>
    } else {
        tablebody = <tbody>
        {results.map(entry => (
            <ChangeEntryRow
                key={entry.path + "-change-entry"}
                entry={entry}
                relativeTo={prefix}
                showActions={true}
                onRevert={(entry) => {
                    branches
                        .revert(repo.id, reference.id, {type: entry.path_type, path: entry.path})
                        .then(() => {
                            setInternalRefresh(!internalRefresh)
                        })
                        .catch(error => {
                            setActionError(error)
                        })
                }}
                onNavigate={entry => {
                    return {
                        pathname: '/repositories/:repoId/changes',
                        params: {repoId: repo.id},
                        query: {
                            prefix: entry.path,
                            ref: reference.id,
                            view: "dir"
                        }
                    }
                }}
            />
        ))}
        </tbody>
    }

    const actionErrorDisplay = (!!actionError) ?
        <Error error={actionError} onDismiss={() => setActionError(null)}/> : <></>

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

                    <RefreshButton onClick={refresh}/>

                    <RevertButton enabled={results.length > 0} onRevert={() => {
                        branches.revert(repo.id, reference.id, {type: 'reset'})
                            .then(refresh)
                            .catch(error => setActionError(error))
                    }}/>
                    <CommitButton repo={repo} enabled={results.length > 0} onCommit={async (commitDetails, done) => {
                        try {
                            await commits.commit(repo.id, reference.id, commitDetails.message, commitDetails.metadata);
                            setActionError(null);
                            refresh();
                        } catch (err) {
                            setActionError(err);
                        }
                        done();
                    }}/>
                </ActionGroup>
            </ActionsBar>

            {actionErrorDisplay}
            <div className="tree-container">
                {(results.length === 0) ? <Alert variant="info">No changes</Alert> : (
                    <Card>
                        <Card.Header>
                        <span className="float-left">
                            {(delimiter !== "") && (
                                <URINavigator path={prefix} reference={reference} repo={repo}
                                              relativeTo={`${reference.id} workspace`}
                                              pathURLBuilder={(params, query) => {
                                                  return {
                                                      pathname: '/repositories/:repoId/changes',
                                                      params: params,
                                                      query: {delimiter: "/", ref: reference.id},
                                                  }
                                              }}/>
                            )}
                        </span>
                            <span className="float-right">
                            <Form>
                              <ButtonGroup className="view-options">
                                {radios.map((radio, idx) => (
                                    <div key={idx}>
                                        <Link href={{
                                            pathname: '/repositories/:repoId/changes',
                                            params: {repoId: repo.id},
                                            query: {
                                                prefix: "",
                                                ref: reference.id,
                                                view: radio.value,
                                            }
                                        }}>
                                            <ToggleButton className="view-options"
                                                id={`radio-${idx}`}
                                                key={`radio-${idx}`}
                                                type="radio"
                                                variant="secondary"
                                                name="radio"
                                                value={radio.value}
                                                checked={radio.selected}>
                                                {radio.name}
                                            </ToggleButton>
                                        </Link>
                                    </div>
                                ))}
                              </ButtonGroup>
                            </Form>
                        </span>
                        </Card.Header>
                        <Card.Body>
                            <Table borderless size="sm">
                                {tablebody}
                            </Table>
                        </Card.Body>
                    </Card>
                )}
                <Paginator onPaginate={onPaginate} nextPage={nextPage} after={after}/>
            </div>
        </>
    )
}

const ChangesContainer = () => {
    const router = useRouter();
    const {repo, reference, loading, error} = useRefs()
    const {after, prefix, view} = router.query

    if (loading) return <Loading/>
    if (!!error) return <Error error={error}/>

    return (
        <ChangesBrowser
            after={(!!after) ? after : ""}
            prefix={(!!prefix) ? prefix : ""}
            view={(!!view) ? view : ""}
            repo={repo}
            reference={reference}
            onPaginate={after => router.push({
                pathname: `/repositories/:repoId/changes`,
                params: {repoId: repo.id},
                query: {
                    ref: reference.id,
                    after: (!!after) ? after : "",
                    prefix: (!!prefix) ? prefix : "",
                    view: (!!view) ? view : "",
                }
            })}
            onSelectRef={ref => router.push({
                pathname: `/repositories/:repoId/changes`,
                params: {repoId: repo.id},
                query: {
                    ref: ref.id,
                    delimiter: (view) ? view : "",
                }
            })}
        />
    )
}


const RepositoryChangesPage = () => {
    return (
        <RefContextProvider>
            <RepositoryPageLayout activePage={'changes'}>
                <ChangesContainer/>
            </RepositoryPageLayout>
        </RefContextProvider>
    )
}

export default RepositoryChangesPage;