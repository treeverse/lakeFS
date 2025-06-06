import React, {useEffect, useRef, useState} from "react";
import { useOutletContext } from "react-router-dom";
import {GitCommitIcon, HistoryIcon,} from "@primer/octicons-react";

import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";

import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";

import {branches, commits, refs} from "../../../lib/api";
import {useAPIWithPagination} from "../../../lib/hooks/api";
import {useRefs} from "../../../lib/hooks/repo";
import {ConfirmationModal} from "../../../lib/components/modals";
import {ActionGroup, ActionsBar, AlertError, Loading, RefreshButton} from "../../../lib/components/controls";
import RefDropdown from "../../../lib/components/repository/refDropdown";
import {formatAlertText} from "../../../lib/components/repository/errors";
import {ChangesTreeContainer, MetadataFields} from "../../../lib/components/repository/changes";
import {useRouter} from "../../../lib/hooks/router";
import {URINavigator} from "../../../lib/components/repository/tree";
import {RepoError} from "./error";
import { Link } from "../../../lib/components/nav";
import Card from "react-bootstrap/Card";
import {PlusIcon} from "@primer/octicons-react";


/**
 * A component to display when there are no changes in the repository.
 * This is used specifically for the uncommitted changes page.
 * 
 * @param repo Repository
 * @param reference commitID / branch
 */
export const EmptyChangesState = ({ repo, reference }) => {
    return (
        <div className="tree-container">
            <Card className="border-0 shadow-sm">
                <Card.Body className="text-center p-5">
                    <h3 className="mb-3">No Changes Yet</h3>
                    <p className="text-muted mb-4">
                        No uncommitted changes on <code>{reference.id}</code>!
                        <br />Upload or modify files to see them appear here.
                    </p>
                    <Link 
                        href={{
                            pathname: "/repositories/:repoId/objects",
                            params: { repoId: repo.id },
                            query: { ref: reference.id, upload: true }
                        }}
                        className="btn btn-primary"
                    >
                        <PlusIcon className="me-1" /> Upload Files
                    </Link>
                </Card.Body>
            </Card>
        </div>
    );
};

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

                        <MetadataFields metadataFields={metadataFields} setMetadataFields={setMetadataFields}/>
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

    const delimiter = '/'

    const getMoreUncommittedChanges = (afterUpdated, path, useDelimiter= true, amount = -1) => {
        return refs.changes(repo.id, reference.id, afterUpdated, path, useDelimiter ? delimiter : "", amount > 0 ? amount : undefined)
    }

    const { error, loading, nextPage } = useAPIWithPagination(async () => {
        if (!repo) return
        return await appendMoreResults(resultsState, prefix, afterUpdated, setAfterUpdated, setResultsState,
            () => refs.changes(repo.id, reference.id, afterUpdated, prefix, delimiter));
    }, [repo.id, reference.id, internalRefresh, afterUpdated, delimiter, prefix])

    const results = resultsState.results

    const refresh = () => {
        setResultsState({prefix: prefix, results:[], pagination:{}})
        setInternalRefresh(!internalRefresh)
    }


    if (error) return <AlertError error={error}/>
    if (loading) return <Loading/>

    let onReset = async (entry) => {
        branches
            .reset(repo.id, reference.id, {type: entry.path_type, path: entry.path})
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

    const uriNavigator =  <URINavigator path={prefix} reference={reference} repo={repo}
                                      pathURLBuilder={(params, query) => {
                                          return {
                                              pathname: '/repositories/:repoId/changes',
                                              params: params,
                                              query: {ref: reference.id, prefix: query.path ?? ""},
                                          }}}/>
    const changesTreeMessage = <p>Showing changes for branch <strong>{reference.id}</strong></p>
    const committedRef = reference.id + "@"
    const uncommittedRef = reference.id

   const actionErrorDisplay = (actionError) ?
        <AlertError error={actionError} onDismiss={() => setActionError(null)}/> : <></>

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

                    <RevertButton enabled={results.length > 0 && !repo?.read_only} onRevert={() => {
                        branches.reset(repo.id, reference.id, {type: 'reset'})
                            .then(refresh)
                            .catch(error => setActionError(error))
                    }}/>
                    <CommitButton repo={repo} enabled={results.length > 0 && !repo?.read_only} onCommit={async (commitDetails, done) => {
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
            <ChangesTreeContainer results={results} delimiter={delimiter}
                                  uriNavigator={uriNavigator} leftDiffRefID={committedRef} rightDiffRefID={uncommittedRef}
                                  repo={repo} reference={reference} internalReferesh={internalRefresh} prefix={prefix}
                                  getMore={getMoreUncommittedChanges}
                                  loading={loading} nextPage={nextPage} setAfterUpdated={setAfterUpdated}
                                  onNavigate={onNavigate} onRevert={onReset} changesTreeMessage={changesTreeMessage}
                                  noChangesText="No changes - you can modify this branch by uploading data using the UI or any of the supported SDKs"
                                  emptyStateComponent={<EmptyChangesState repo={repo} reference={reference} />}
                                  />
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
  const [setActivePage] = useOutletContext();
  useEffect(() => setActivePage('changes'), [setActivePage]);
  return <ChangesContainer />;
}

export default RepositoryChangesPage;
