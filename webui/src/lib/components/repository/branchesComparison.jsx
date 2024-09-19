import React, {useCallback, useRef, useState} from "react";
import {refs, refs as refsAPI} from "../../../lib/api";
import Alert from "react-bootstrap/Alert";
import {RefTypeBranch, RefTypeCommit} from "../../../constants";
import {useAPIWithPagination} from "../../hooks/api";
import {appendMoreResults} from "../../../pages/repositories/repository/changes";
import {ActionGroup, ActionsBar, AlertError, Loading, RefreshButton} from "../controls";
import {ChangesTreeContainer, defaultGetMoreChanges, MetadataFields} from "./changes";
import {URINavigator} from "./tree";
import {useRouter} from "../../hooks/router";
import RefDropdown from "./refDropdown";
import {ArrowLeftIcon, ArrowSwitchIcon, GitMergeIcon} from "@primer/octicons-react";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import Button from "react-bootstrap/Button";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import {FormControl, FormHelperText, InputLabel, MenuItem, Select} from "@mui/material";

const BranchComparison = (
    {repo, reference, compareReference, showActionsBar, prefix = "", onSelectRef, onSelectCompare}
) => {
    const [internalRefresh, setInternalRefresh] = useState(true);

    const [afterUpdated, setAfterUpdated] = useState(""); // state of pagination of the item's children
    const [resultsState, setResultsState] = useState({prefix, results: [], pagination: {}}); // current retrieved children of the item

    const delimiter = "/"

    const {nextPage, loading, error} = useAPIWithPagination(async () => {
        if (!repo) return

        if (compareReference.id === reference.id) {
            return {pagination: {has_more: false}, results: []}; // nothing to compare here.
        }

        const getMoreResults = () =>
            refsAPI.diff(repo.id, reference.id, compareReference.id, afterUpdated, prefix, delimiter);
        return await appendMoreResults(resultsState, prefix, afterUpdated, setAfterUpdated, setResultsState, getMoreResults);
    }, [repo.id, reference.id, internalRefresh, afterUpdated, delimiter, prefix])

    const {results} = resultsState;
    const apiResult = {results, loading, error, nextPage};

    const isEmptyDiff = (!loading && !error && !!results && results.length === 0);

    const doRefresh = () => {
        setResultsState({prefix, results: [], pagination: {}})
        setInternalRefresh(!internalRefresh)
    }

    return (
        <>
            {showActionsBar &&
                <CompareActionsBar
                    repo={repo}
                    reference={reference}
                    onSelectRef={onSelectRef}
                    compareReference={compareReference}
                    onSelectCompare={onSelectCompare}
                    doRefresh={doRefresh}
                    isEmptyDiff={isEmptyDiff}
                />
            }
            <BranchChangesList
                apiResult={apiResult}
                repo={repo}
                reference={reference}
                compareReference={compareReference}
                prefix={prefix}
                delimiter={delimiter}
                refresh={internalRefresh}
                setAfterUpdated={setAfterUpdated}
            />
        </>
    );
};

const CompareActionsBar = (
    {repo, reference, onSelectRef, compareReference, onSelectCompare, doRefresh, isEmptyDiff}
) => {
    const router = useRouter();
    const handleSwitchRefs = useCallback((e) => {
        e.preventDefault();
        router.push({
            pathname: `/repositories/:repoId/compare`, params: {repoId: repo.id},
            query: {ref: compareReference.id, compare: reference.id}
        });
    }, []);

    return <ActionsBar>
        <ActionGroup orientation="left">
            <RefDropdown
                prefix={'Base '}
                repo={repo}
                selected={(reference) ? reference : null}
                withCommits={true}
                withWorkspace={false}
                selectRef={onSelectRef}/>

            <ArrowLeftIcon className="me-2 mt-2" size="small" verticalAlign="middle"/>

            <RefDropdown
                prefix={'Compared to '}
                emptyText={'Compare with...'}
                repo={repo}
                selected={(compareReference) ? compareReference : null}
                withCommits={true}
                withWorkspace={false}
                selectRef={onSelectCompare}/>

            <OverlayTrigger placement="bottom" overlay={
                <Tooltip>Switch directions</Tooltip>
            }>
                    <span>
                        <Button variant={"link"}
                                onClick={handleSwitchRefs}>
                            <ArrowSwitchIcon className="me-2 mt-2" size="small" verticalAlign="middle"/>
                        </Button>
                    </span>
            </OverlayTrigger>&#160;&#160;
        </ActionGroup>

        <ActionGroup orientation="right">

            <RefreshButton onClick={doRefresh}/>

            {(compareReference.type === RefTypeBranch && reference.type === RefTypeBranch) &&
                <MergeButton
                    repo={repo}
                    disabled={((compareReference.id === reference.id) || isEmptyDiff || repo?.read_only)}
                    source={compareReference.id}
                    dest={reference.id}
                    onDone={doRefresh}
                />
            }
        </ActionGroup>
    </ActionsBar>;
};

const MergeButton = ({repo, onDone, source, dest, disabled = false}) => {
    const textRef = useRef(null);
    const [metadataFields, setMetadataFields] = useState([])
    const initialMerge = {
        merging: false,
        show: false,
        err: null,
        strategy: "none",
    }
    const [mergeState, setMergeState] = useState(initialMerge);

    const onClickMerge = useCallback(() => {
            setMergeState({merging: mergeState.merging, err: mergeState.err, show: true, strategy: mergeState.strategy})
        }
    );

    const onStrategyChange = (event) => {
        setMergeState({
            merging: mergeState.merging,
            err: mergeState.err,
            show: mergeState.show,
            strategy: event.target.value
        });
    }
    const hide = () => {
        if (mergeState.merging) return;
        setMergeState(initialMerge);
        setMetadataFields([])
    }

    const onSubmit = async () => {
        const message = textRef.current.value;
        const metadata = {};
        metadataFields.forEach(pair => metadata[pair.key] = pair.value)

        let strategy = mergeState.strategy;
        if (strategy === "none") {
            strategy = "";
        }
        setMergeState({merging: true, show: mergeState.show, err: mergeState.err, strategy: mergeState.strategy})
        try {
            await refs.merge(repo.id, source, dest, strategy, message, metadata);
            setMergeState({
                merging: mergeState.merging,
                show: mergeState.show,
                err: null,
                strategy: mergeState.strategy
            })
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
                    <Form className="mb-2">
                        <Form.Group controlId="message" className="mb-3">
                            <Form.Control type="text" placeholder="Commit Message (Optional)" ref={textRef}/>
                        </Form.Group>

                        <MetadataFields metadataFields={metadataFields} setMetadataFields={setMetadataFields}/>
                    </Form>
                    <FormControl sx={{m: 1, minWidth: 120}}>
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
                    {(mergeState.err) ? (<AlertError error={mergeState.err}/>) : (<></>)}
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
            <Button variant="success" disabled={disabled} onClick={() => onClickMerge()}>
                <GitMergeIcon/> {"Merge"}
            </Button>
        </>
    );
}

const BranchChangesList = (
    {apiResult, repo, reference, compareReference, prefix, delimiter, refresh, setAfterUpdated}
) => {
    const {results, loading, error, nextPage} = apiResult;

    if (loading) return <Loading/>;
    if (error) return <AlertError error={error}/>;

    const changesTreeMessage =
        <p>
            Showing changes between <strong>{reference.id}</strong> {""}
            and <strong>{compareReference.id}</strong>
        </p>

    if (compareReference.id === reference.id) {
        return <Alert variant="warning">
            <Alert.Heading>There isn’t anything to compare.</Alert.Heading>
            You’ll need to use two different sources to get a valid comparison.
        </Alert>;
    }

    return <ChangesTreeContainer
        results={results}
        reference={reference}
        leftDiffRefID={getRefID(reference)}
        rightDiffRefID={getRefID(compareReference)}
        repo={repo}
        delimiter={delimiter}
        uriNavigator={CompareURINavigator(prefix, reference, compareReference, repo)}
        internalReferesh={refresh}
        prefix={prefix}
        getMore={defaultGetMoreChanges(repo, reference.id, compareReference.id, delimiter)}
        nextPage={nextPage}
        setAfterUpdated={setAfterUpdated}
        onNavigate={getNavigatorToComparePage(repo, reference, compareReference)}
        changesTreeMessage={changesTreeMessage}
    />;
};

function getURINavigatorRelativeTitle(from, to) {
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

const CompareURINavigator = (prefix, reference, compareReference, repo) =>
    <URINavigator
        path={prefix}
        reference={reference}
        relativeTo={getURINavigatorRelativeTitle(reference, compareReference)}
        repo={repo}
        pathURLBuilder={(params, query) => {
            const q = {
                delimiter: "/",
                prefix: query.path,
            };
            if (compareReference) {
                q.compare = compareReference.id;
            }
            if (reference) {
                q.ref = reference.id;
            }
            return {
                pathname: '/repositories/:repoId/compare',
                params: {repoId: repo.id},
                query: q
            };
        }}/>;

const getNavigatorToComparePage = (repo, ref, compareRef) => entry => ({
    pathname: `/repositories/:repoId/compare`,
    params: {repoId: repo.id},
    query: {
        ref: ref.id,
        compare: compareRef.id,
        prefix: entry.path,
    }
});

function getRefID(reference) {
    let refID = reference.id;
    if (reference.type === RefTypeBranch) {
        refID += "@";
    }
    return refID;
}

export default BranchComparison;
