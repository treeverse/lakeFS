import React, {useRef, useState} from "react";

import {
    ArrowLeftIcon,
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
import {TreeEntryPaginator, TreeItem} from "../../../lib/components/repository/changes";
import {useRouter} from "../../../lib/hooks/router";
import {URINavigator} from "../../../lib/components/repository/tree";
import {RepoError} from "./error";
import {TableDiff} from "../../../lib/components/repository/TableDiff";


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
                        <Form.Group controlId="message" className="mb-3">
                            <Form.Control type="text" placeholder="Commit Message" ref={textRef}/>
                        </Form.Group>

                        {metadataFields.map((f, i) => {
                            return (
                                <Form.Group controlId="message" key={`commit-metadata-field-${f.key}-${f.value}-${i}`} className="mb-3">
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

export async function appendMoreResults(resultsState, prefix, afterUpdated, setAfterUpdated, setResultsState, getMore) {
    let resultsFiltered = resultsState.results
    if (resultsState.prefix !== prefix) {
        // prefix changed, need to delete previous results
        setAfterUpdated("")
        resultsFiltered = []
    }

    if (resultsFiltered.length > 0 && resultsFiltered.at(-1).path > afterUpdated) {
        // results already cached
        return {prefix: prefix, results: resultsFiltered, pagination: resultsState.pagination}
    }

    const {results, pagination} = await getMore()
    setResultsState({prefix: prefix, results: resultsFiltered.concat(results), pagination: pagination})
    return {results: resultsState.results, pagination: pagination}
}

const ChangesBrowser = ({repo, reference, prefix, onSelectRef, }) => {
    const [actionError, setActionError] = useState(null);
    const [internalRefresh, setInternalRefresh] = useState(true);
    const [afterUpdated, setAfterUpdated] = useState(""); // state of pagination of the item's children
    const [resultsState, setResultsState] = useState({prefix: prefix, results:[], pagination:{}}); // current retrieved children of the item
    const [tableDiffState, setTableDiffState] = useState({isExpanded: false, expandedTablePath: ""});

    const delimiter = '/'

    const { error, loading, nextPage } = useAPIWithPagination(async () => {
        if (!repo) return
        return await appendMoreResults(resultsState, prefix, afterUpdated, setAfterUpdated, setResultsState,
            () => refs.changes(repo.id, reference.id, afterUpdated, prefix, delimiter));
    }, [repo.id, reference.id, internalRefresh, afterUpdated, delimiter, prefix])

    const results = resultsState.results
    const committedRef = reference.id + "@"
    const uncommittedRef = reference.id

    const refresh = () => {
        setResultsState({prefix: prefix, results:[], pagination:{}})
        setInternalRefresh(!internalRefresh)
    }

    if (error) return <Error error={error}/>
    if (loading) return <Loading/>

    let onRevert = async (entry) => {
        branches
            .revert(repo.id, reference.id, {type: entry.path_type, path: entry.path})
            .then(refresh)
            .catch(error => {
                setActionError(error)
            })
    }

    let onNavigate = (entry) => {
        return {
            pathname: `/repositories/:repoId/changes`,
            params: {repoId: repo.id},
            query: {
                ref: reference.id,
                prefix: entry.path,
            }
        }
    }

   const actionErrorDisplay = (actionError) ?
        <Error error={actionError} onDismiss={() => setActionError(null)}/> : <></>

    return (
        <>
            <ActionsBar>
                <ActionGroup orientation="left">
                    <RefDropdown
                        emptyText={'Select Branch'}
                        repo={repo}
                        selected={(reference) ? reference : null}
                        withCommits={false}
                        withWorkspace={false}
                        withTags={false}
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
                    <>
                    {tableDiffState.isExpanded
                        ?  <Button className="action-bar"
                                   variant="secondary"
                                   disabled={false}
                                   onClick={() => setTableDiffState( {isExpanded: false, expandedTablePath: ""})}>
                            <ArrowLeftIcon/> Back to object comparison
                        </Button>
                        : ""}
                    <Card>
                        <Card.Header>
                        <span className="float-start">
                            {(delimiter !== "") && (
                                <URINavigator path={prefix} reference={reference} repo={repo}
                                              relativeTo={`${reference.id} workspace`}
                                              pathURLBuilder={(params, query) => {
                                                  return {
                                                      pathname: '/repositories/:repoId/changes',
                                                      params: params,
                                                      query: {ref: reference.id, prefix: query.path ?? ""},
                                                  }
                                              }}/>
                            )}
                        </span>
                        </Card.Header>
                        <Card.Body>
                            <Table borderless size="sm">
                                <tbody>
                                {tableDiffState.isExpanded
                                    ? <TableDiff repo={repo} leftRef={committedRef} rightRef={uncommittedRef} tablePath={tableDiffState.expandedTablePath}/>
                                    :
                                    results.map(entry => {
                                    return (
                                        <TreeItem key={entry.path + "-tree-item"} entry={entry} repo={repo}
                                                  reference={reference}
                                                  leftDiffRefID={committedRef} rightDiffRefID={uncommittedRef}
                                                  internalReferesh={internalRefresh}
                                                  onNavigate={onNavigate} onRevert={onRevert} delimiter={delimiter}
                                                  relativeTo={prefix}
                                                  getMore={(afterUpdated, path, useDelimiter= true, amount = -1) => {
                                                      return refs.changes(repo.id, reference.id, afterUpdated, path, useDelimiter ? delimiter : "", amount > 0 ? amount : undefined)
                                                  }}
                                                  setTableDiffExpanded={() => setTableDiffState({isExpanded: true,  expandedTablePath: entry.path})}
                                        />
                                        )
                                })}
                                { !!nextPage &&
                                    <TreeEntryPaginator path={""} loading={loading} nextPage={nextPage} setAfterUpdated={setAfterUpdated}/>
                                }
                                </tbody>
                            </Table>
                        </Card.Body>
                    </Card></>
                )}
            </div>
        </>
    )
}

const ChangesContainer = () => {
    const router = useRouter();
    const {repo, reference, loading, error} = useRefs()
    const {prefix} = router.query

    if (loading) return <Loading/>
    if (error) return <RepoError error={error}/>

    return (
        <ChangesBrowser
            prefix={(prefix) ? prefix : ""}
            repo={repo}
            reference={reference}
            onSelectRef={ref => router.push({
                pathname: `/repositories/:repoId/changes`,
                params: {repoId: repo.id},
                query: {
                    ref: ref.id,
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