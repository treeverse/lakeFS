import React, {useState} from "react";

import {RepositoryPageLayout} from "../../../lib/components/repository/layout";
import {
    ActionGroup,
    ActionsBar,
    Error,
    ExperimentalOverlayTooltip,
    Loading,
    RefreshButton
} from "../../../lib/components/controls";
import {RefContextProvider, useRefs} from "../../../lib/hooks/repo";
import RefDropdown from "../../../lib/components/repository/refDropdown";
import {ArrowLeftIcon, DiffIcon, GitMergeIcon} from "@primer/octicons-react";
import {useAPIWithPagination} from "../../../lib/hooks/api";
import {refs, statistics} from "../../../lib/api";
import Alert from "react-bootstrap/Alert";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import {TreeEntryPaginator, TreeItem} from "../../../lib/components/repository/changes";
import {useRouter} from "../../../lib/hooks/router";
import {URINavigator} from "../../../lib/components/repository/tree";
import {appendMoreResults} from "./changes";
import {RefTypeBranch, RefTypeCommit} from "../../../constants";
import Button from "react-bootstrap/Button";
import {FormControl, FormHelperText, InputLabel, MenuItem, Select} from "@mui/material";
import Modal from "react-bootstrap/Modal";
import {RepoError} from "./error";
import {ComingSoonModal} from "../../../lib/components/modals";

const CompareList = ({ repo, reference, compareReference, prefix, onSelectRef, onSelectCompare, onNavigate }) => {
    const [internalRefresh, setInternalRefresh] = useState(true);
    const [afterUpdated, setAfterUpdated] = useState(""); // state of pagination of the item's children
    const [resultsState, setResultsState] = useState({prefix: prefix, results:[], pagination:{}}); // current retrieved children of the item
    const [showDeltaDiffButton, setShowDeltaDiffButton] = useState(false);
    const [showComingSoonModal, setShowComingSoonModal] = useState(false);
    const sendDeltaDiffStats = async () => {
        const deltaDiffStatEvents = [
            {
                "class": "experimental-feature",
                "name": "delta-diff",
                "count": 1,
            }
        ];
        await statistics.sendStats(deltaDiffStatEvents);
    }

    const refresh = () => {
        setResultsState({prefix: prefix, results:[], pagination:{}})
        setInternalRefresh(!internalRefresh)
        setShowDeltaDiffButton(false);
    }

    const delimiter = "/"

    const { error, loading, nextPage } = useAPIWithPagination(async () => {
        if (!repo) return
        if (compareReference.id === reference.id)
            return {pagination: {has_more: false}, results: []}; // nothing to compare here.

        return await appendMoreResults(resultsState, prefix, afterUpdated, setAfterUpdated, setResultsState,
            () => refs.diff(repo.id, reference.id, compareReference.id, afterUpdated, prefix, delimiter));
    }, [repo.id, reference.id, internalRefresh, afterUpdated, delimiter, prefix])

    let results = resultsState.results
    let content;

    const relativeTitle = (from, to) => {
        let fromId = from.id;
        let toId = to.id;
        if (from.type === RefTypeCommit) {
            fromId = fromId.substr(0, 12);
        }
        if (to.type === RefTypeCommit) {
            toId = toId.substr(0, 12);
        }

        return `${fromId}...${toId}`
    }

    if (loading) content = <Loading/>
    else if (error) content = <Error error={error}/>
    else if (compareReference.id === reference.id) content = (
        <Alert variant="warning">
            <Alert.Heading>There isn’t anything to compare.</Alert.Heading>
            You’ll need to use two different sources to get a valid comparison.
        </Alert>
    )
    else content = (
            <div className="tree-container">
                {(results.length === 0) ? <Alert variant="info">No changes</Alert> : (
                    <>
                        <ComingSoonModal display={showComingSoonModal}
                                         onCancel={() => setShowComingSoonModal(false)}>
                            <div>lakeFS Delta Lake tables diff is under development</div>
                        </ComingSoonModal>
                        <ExperimentalOverlayTooltip show={showDeltaDiffButton}>
                            <Button className="action-bar"
                                    variant="primary"
                                    disabled={false}
                                    onClick={() => {
                                        setShowComingSoonModal(true);
                                        sendDeltaDiffStats();
                                    }}>
                                <DiffIcon/> Compare Delta Lake tables
                            </Button>
                        </ExperimentalOverlayTooltip>
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
                                                if (compareReference)
                                                    q.compare = compareReference.id;
                                                if (reference)
                                                    q.ref = reference.id;
                                                return {
                                                    pathname: '/repositories/:repoId/compare',
                                                    params: {repoId: repo.id},
                                                    query: q
                                                };
                                            }}/>
                                    )}
                                </span>
                            </Card.Header>
                            <Card.Body>
                                <Table borderless size="sm">
                                    <tbody>
                                    {results.map(entry => {
                                        if(!showDeltaDiffButton && entry.path.includes("_delta_log")) {
                                            setShowDeltaDiffButton(true)
                                        }
                                        let leftCommittedRef = reference.id;
                                        let rightCommittedRef = compareReference.id;
                                        if (reference.type === RefTypeBranch) {
                                            leftCommittedRef += "@";
                                        }
                                        if (compareReference.type === RefTypeBranch) {
                                            rightCommittedRef += "@";
                                        }
                                        return (
                                            <TreeItem key={entry.path + "-item"} entry={entry} repo={repo}
                                                      reference={reference}
                                                      internalReferesh={internalRefresh} leftDiffRefID={leftCommittedRef}
                                                      rightDiffRefID={rightCommittedRef} delimiter={delimiter}
                                                      relativeTo={prefix}
                                                      onNavigate={onNavigate}
                                                      getMore={(afterUpdatedChild, path, useDelimiter = true, amount = -1) => {
                                                          return refs.diff(repo.id, reference.id, compareReference.id, afterUpdatedChild, path, useDelimiter ? delimiter : "", amount > 0 ? amount : undefined);
                                                      }}/>);
                                    })}
                                    {!!nextPage &&
                                        <TreeEntryPaginator path={""} loading={loading} nextPage={nextPage}
                                                            setAfterUpdated={setAfterUpdated}/>}
                                    </tbody>
                                </Table>
                            </Card.Body>
                    </Card></>
                )}
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
                        selected={(reference) ? reference : null}
                        withCommits={true}
                        withWorkspace={false}
                        selectRef={onSelectRef}/>

                    <ArrowLeftIcon className="mr-2 mt-2" size="small" verticalAlign="middle"/>

                    <RefDropdown
                        prefix={'Compared to '}
                        emptyText={'Compare with...'}
                        repo={repo}
                        selected={(compareReference) ? compareReference : null}
                        withCommits={true}
                        withWorkspace={false}
                        selectRef={onSelectCompare}/>
                </ActionGroup>

                <ActionGroup orientation="right">

                    <RefreshButton onClick={refresh}/>

                    {(compareReference.type === RefTypeBranch && reference.type === RefTypeBranch) &&
                        <MergeButton
                            repo={repo}
                            disabled={((compareReference.id === reference.id) || emptyDiff)}
                            source={compareReference.id}
                            dest={reference.id}
                            onDone={refresh}
                        />
                    }
                </ActionGroup>
            </ActionsBar>

            {content}
        </>
    );
};

const MergeButton = ({repo, onDone, source, dest, disabled = false}) => {
    const initialMerge = {
        merging: false,
        show: false,
        err: null,
        strategy: "none",
    }
    const [mergeState, setMergeState] = useState(initialMerge);

    const onStrategyChange = (event) => {
        setMergeState({merging: mergeState.merging, err: mergeState.err, show: mergeState.show, strategy: event.target.value});
    }
    const hide = () => {
        if (mergeState.merging) return;
        setMergeState(initialMerge);
    }

    const onSubmit = async () => {
        let strategy = mergeState.strategy;
        if (strategy === "none") {
            strategy = "";
        }
        setMergeState({merging: true, show: mergeState.show, err: mergeState.err, strategy: mergeState.strategy})
        try {
            await refs.merge(repo.id, source, dest, strategy);
            setMergeState({merging: mergeState.merging, show: mergeState.show, err: null, strategy: mergeState.strategy})
            onDone();
            hide();
        } catch (err) {
            setMergeState({merging: mergeState.merging, show: mergeState.show, err: err, strategy: mergeState.strategy})
        }
    }

    return (
        <>
            <Modal show={mergeState.show} onHide={hide} size="lg">
                <Modal.Header closeButton>
                    <Modal.Title>Merge branch {source} into {dest}</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    <FormControl sx={{ m: 1, minWidth: 120 }}>
                        <InputLabel id="demo-select-small">Strategy</InputLabel>
                        <Select
                            labelId="demo-select-small"
                            id="demo-simple-select-helper"
                            value={mergeState.strategy}
                            label="Strategy"
                            onChange={onStrategyChange}
                        >
                            <MenuItem value={"none"}>Default</MenuItem>
                            <MenuItem value={"source-wins"}>source-wins</MenuItem>
                            <MenuItem value={"dest-wins"}>dest-wins</MenuItem>
                        </Select>
                    </FormControl>
                    <FormHelperText>In case of a merge conflict, this option will force the merge process
                        to automatically favor changes from <b>{dest}</b> (&rdquo;dest-wins&rdquo;) or
                        from <b>{source}</b> (&rdquo;source-wins&rdquo;). In case no selection is made,
                        the merge process will fail in case of a conflict.</FormHelperText>
                    {(mergeState.err) ? (<Error error={mergeState.err}/>) : (<></>)}
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" disabled={mergeState.merging} onClick={hide}>
                        Cancel
                    </Button>
                    <Button variant="success" disabled={mergeState.merging} onClick={onSubmit}>
                        {(mergeState.merging) ? 'Merging...' : 'Merge'}
                    </Button>
                </Modal.Footer>
            </Modal>
            <Button variant="success" disabled={disabled} onClick={() => setMergeState({merging: mergeState.merging,
                err: mergeState.err, show: true, strategy: mergeState.strategy})}>
                <GitMergeIcon/> Merge
            </Button>
        </>
    );
}

const CompareContainer = () => {
    const router = useRouter();
    const { loading, error, repo, reference, compare } = useRefs();

    const { prefix } = router.query;

    if (loading) return <Loading/>;
    if (error) return <RepoError error={error}/>;

    const route = query => router.push({pathname: `/repositories/:repoId/compare`, params: {repoId: repo.id}, query: {
        ...query,
    }});

    return (
        <CompareList
            repo={repo}
            prefix={(prefix) ? prefix : ""}
            reference={reference}
            onSelectRef={reference => route(compare ? {ref: reference.id, compare: compare.id} : {ref: reference.id})}
            compareReference={compare}
            onSelectCompare={compare => route(reference ? {ref: reference.id, compare: compare.id} : {compare: compare.id})}
            onNavigate={entry => {
                return {
                    pathname: `/repositories/:repoId/compare`,
                    params: {repoId: repo.id},
                    query: {
                        ref: reference.id,
                        compare: compare.id,
                        prefix: entry.path,
                    }
                }
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
