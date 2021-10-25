import React, {useState} from "react";

import {RepositoryPageLayout} from "../../../lib/components/repository/layout";
import {ActionGroup, ActionsBar, Error, Loading, RefreshButton} from "../../../lib/components/controls";
import {RefContextProvider, useRefs} from "../../../lib/hooks/repo";
import RefDropdown from "../../../lib/components/repository/refDropdown";
import {ArrowLeftIcon, GitMergeIcon} from "@primer/octicons-react";
import {useAPIWithPagination} from "../../../lib/hooks/api";
import {refs} from "../../../lib/api";
import Alert from "react-bootstrap/Alert";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import {ChangeEntryRow, TreeItem} from "../../../lib/components/repository/changes";
import {Paginator} from "../../../lib/components/pagination";
import {ConfirmationButton} from "../../../lib/components/modals";
import {useRouter} from "../../../lib/hooks/router";
import {URINavigator} from "../../../lib/components/repository/tree";
import Form from "react-bootstrap/Form";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import {Link} from "../../../lib/components/nav";
import {ToggleButton} from "react-bootstrap";


const CompareList = ({ repo, reference, compareReference, after, view, prefix, onSelectRef, onSelectCompare, onPaginate }) => {
    const [internalRefresh, setInternalRefresh] = useState(true);
    const [mergeError, setMergeError] = useState(null);
    const [merging, setMerging] = useState(false);

    const refresh = () => {
        setInternalRefresh(!internalRefresh)
        setMergeError(null)
    }

    const radios = [
        { name: 'Flat', value: 'flat' , selected:false},
        { name: 'Directory', value: 'dir', selected:false },
        { name: 'Tree', value: 'tree', selected:false },
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

    const { results, error, loading, nextPage } = useAPIWithPagination(async () => {
        if (compareReference.id !== reference.id)
            return refs.diff(repo.id, reference.id, compareReference.id, after, prefix, delimiter);
        return {pagination: {has_more: false}, results: []}; // nothing to compare here.
    }, [repo.id, reference.id, compareReference.id, internalRefresh, after, prefix, delimiter]);

    let content;

    const relativeTitle = (from, to) => {
        let fromId = from.id;
        let toId = to.id;
        if (from.type === 'commit') {
            fromId = fromId.substr(0, 12);
        }
        if (to.type === 'commit') {
            toId = toId.substr(0, 12);
        }

        return `${fromId}...${toId}`
    }

    let tablebody;
    if (view === 'tree'){
        tablebody =
            <tbody>
            {results.map(entry => (
                <TreeItem key={entry.path+"-item"} entry={entry} repo={repo} reference={reference} internalReferesh={internalRefresh}
                          delimiter={delimiter} after={after} relativeTo={""}
                          getMore={(afterUpdated, path) => refs.diff(repo.id, reference.id, compareReference.id, afterUpdated, path, delimiter)}
                />
            ))}
            </tbody>
    } else {
        tablebody = <tbody>
        {results.map(entry => (
            <ChangeEntryRow
                key={entry.path}
                entry={entry}
                relativeTo={prefix}
                showActions={true}
                onNavigate={entry => {
                    return {
                        pathname: '/repositories/:repoId/compare',
                        params: {repoId: repo.id},
                        query: {
                            compare: compareReference === null ? "" : compareReference.id,
                            ref: reference === null ? "" : reference.id,
                            prefix: entry.path,
                            view: "dir"
                        }
                    }
                }}
            />
        ))}
        </tbody>
    }

    if (loading) content = <Loading/>
    else if (!!error) content = <Error error={error}/>
    else if (compareReference.id === reference.id) content = (
        <Alert variant="warning">
            <Alert.Heading>There isn’t anything to compare.</Alert.Heading>
            You’ll need to use two different sources to get a valid comparison.
        </Alert>
    )
    else content = (
            <div className="tree-container">
                {(results.length === 0) ? <Alert variant="info">No changes</Alert> : (
                    <Card>
                        <Card.Header>
                        <span className="float-left">
                            {(delimiter !== "") && (
                                <URINavigator
                                    path={prefix}
                                    reference={reference}
                                    relativeTo={relativeTitle(reference, compareReference)}
                                    repo={repo}
                                    pathURLBuilder={(params, query) => {
                                        const q = {
                                            delimiter: "/",
                                            prefix: query.path,
                                        };
                                        if (compareReference) q.compare = compareReference.id;
                                        if (reference) q.ref = reference.id;
                                        return {
                                            pathname: '/repositories/:repoId/compare',
                                            params: {repoId: repo.id},
                                            query: q
                                        }
                                    }}
                                />
                            )}
                        </span>
                        <span className="float-right">
                            <Form>
                              <ButtonGroup className={"view-options"}>
                                {radios.map((radio, idx) => (
                                    <div key={idx}>
                                    <Link href={{
                                            pathname: '/repositories/:repoId/compare',
                                            params: {repoId: repo.id},
                                            query: {
                                                compare: compareReference === null ? "" : compareReference.id,
                                                ref: reference === null ? "" : reference.id,
                                                view: radio.value,
                                            }
                                    }}>
                                        <ToggleButton className={"view-options"}
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
    )

    const emptyDiff = (!loading && !error && !!results && results.length === 0);

    return (
        <>
            <ActionsBar>
                <ActionGroup orientation="left">
                    <RefDropdown
                        prefix={'Base '}
                        repo={repo}
                        selected={(!!reference) ? reference : null}
                        withCommits={true}
                        withWorkspace={false}
                        selectRef={onSelectRef}/>

                    <ArrowLeftIcon className="mr-2 mt-2" size="small" verticalAlign="middle"/>

                    <RefDropdown
                        prefix={'Compared to '}
                        emptyText={'Compare with...'}
                        repo={repo}
                        selected={(!!compareReference) ? compareReference : null}
                        withCommits={true}
                        withWorkspace={false}
                        selectRef={onSelectCompare}/>
                </ActionGroup>

                <ActionGroup orientation="right">

                    <RefreshButton onClick={refresh}/>

                    {(compareReference.type === 'branch' && reference.type === 'branch') &&
                    <ConfirmationButton
                        variant="success"
                        disabled={((compareReference.id === reference.id) || merging || emptyDiff)}
                        msg={`Are you sure you'd like to merge '${compareReference.id}' into '${reference.id}'?`}
                        tooltip={`merge '${compareReference.id}' into '${reference.id}'`}
                        onConfirm={hide => {
                            setMerging(true)
                            hide()
                            refs.merge(repo.id, compareReference.id, reference.id)
                                .then(() => {
                                    setMergeError(null)
                                    setMerging(false)
                                    refresh()
                                })
                                .catch(err => {
                                    setMergeError(err)
                                    setMerging(false)
                                })
                        }}>
                        <GitMergeIcon/> {(merging) ? 'Merging...' : 'Merge'}
                    </ConfirmationButton>
                    }
                </ActionGroup>
            </ActionsBar>

            {mergeError && <Error error={mergeError}/>}
            {content}
        </>
    );
};


const CompareContainer = () => {
    const router = useRouter();
    const { loading, error, repo, reference, compare } = useRefs();

    const { after, prefix, view } = router.query;

    if (loading) return <Loading/>;
    if (!!error) return <Error error={error}/>;

    const route = query => router.push({pathname: `/repositories/:repoId/compare`, params: {repoId: repo.id}, query: {
        ...query,
        view: (view) ? view : "",
    }});

    return (
        <CompareList
            repo={repo}
            after={(!!after) ? after : ""}
            view={(!!view) ? view : ""}
            prefix={(!!prefix) ? prefix : ""}
            reference={reference}
            onSelectRef={reference => route(compare ? {ref: reference.id, compare: compare.id} : {ref: reference.id})}
            compareReference={compare}
            onSelectCompare={compare => route(reference ? {ref: reference.id, compare: compare.id} : {compare: compare.id})}
            onPaginate={after => {
                const query = {
                    after: (after) ? after : "",
                    prefix: (prefix) ? prefix : "",
                    view: (view) ? view : ""
                };
                if (compare) query.compare = compare.id;
                if (reference) query.ref = reference.id;
                route(query);
            }}
        />
    );
};

const RepositoryComparePage = () => {
    return (
        <RefContextProvider>
            <RepositoryPageLayout activePage={'compare'}>
                <CompareContainer/>
            </RepositoryPageLayout>
        </RefContextProvider>
    );
};

export default RepositoryComparePage;