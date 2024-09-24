import React, {useState} from "react";
import {refs as refsAPI} from "../../../lib/api";
import Alert from "react-bootstrap/Alert";
import {RefTypeBranch, RefTypeCommit} from "../../../constants";
import {useAPIWithPagination} from "../../hooks/api";
import {appendMoreResults} from "../../../pages/repositories/repository/changes";
import {AlertError, Loading} from "../controls";
import {ChangesTreeContainer, defaultGetMoreChanges} from "./changes";
import {URINavigator} from "./tree";
import CompareBranchesActionsBar from "./compareBranchesActionBar";

const CompareBranches = (
    {repo, reference, compareReference, showActionsBar, prefix = "", baseSelectURL}
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
                <CompareBranchesActionsBar
                    repo={repo}
                    reference={reference}
                    compareReference={compareReference}
                    baseSelectURL={baseSelectURL}
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

export default CompareBranches;
