import React, {useState} from "react";
import {refs as refsAPI} from "../../../lib/api";
import Alert from "react-bootstrap/Alert";
import {RefTypeBranch, RefTypeCommit} from "../../../constants";
import {useAPIWithPagination} from "../../hooks/api";
import {appendMoreResults} from "../../../pages/repositories/repository/changes";
import {AlertError, Loading} from "../controls";
import {ChangesTreeContainer, defaultGetMoreChanges} from "./changes";
import {URINavigator} from "./tree";

const BranchComparison = ({repo, reference: ref, compareReference: compareRef, prefix = "", refresh, onResultsFetched}) => {
    const [afterUpdated, setAfterUpdated] = useState(""); // state of pagination of the item's children
    const [resultsState, setResultsState] = useState({prefix, results: [], pagination: {}}); // current retrieved children of the item

    const delimiter = "/"

    const {nextPage, error, loading} = useAPIWithPagination(async () => {
        if (!repo) return

        if (compareRef.id === ref.id) {
            return {pagination: {has_more: false}, results: []}; // nothing to compare here.
        }

        const getMoreResults = () => refsAPI.diff(repo.id, ref.id, compareRef.id, afterUpdated, prefix, delimiter);
        return await appendMoreResults(resultsState, prefix, afterUpdated, setAfterUpdated, setResultsState, getMoreResults);
    }, [repo.id, ref.id, refresh, afterUpdated, delimiter, prefix])

    if (onResultsFetched) {
        onResultsFetched(resultsState.results, loading, error);
    }

    if (loading) return <Loading/>;
    if (error) return <AlertError error={error}/>;

    let results = resultsState.results;

    const changesTreeMessage =
        <p>
            Showing changes between <strong>{ref.id}</strong> {""}
            and <strong>{compareRef.id}</strong>
        </p>

    if (compareRef.id === ref.id) {
        return <Alert variant="warning">
            <Alert.Heading>There isn’t anything to compare.</Alert.Heading>
            You’ll need to use two different sources to get a valid comparison.
        </Alert>;
    }

    return <ChangesTreeContainer
        results={results}
        refID={ref.id}
        leftDiffRefID={getRefID(ref)}
        rightDiffRefID={getRefID(compareRef)}
        repo={repo}
        delimiter={delimiter}
        uriNavigator={CompareURINavigator(prefix, ref, compareRef, repo)}
        internalReferesh={refresh}
        prefix={prefix}
        getMore={defaultGetMoreChanges(repo, ref.id, compareRef.id, delimiter)}
        loading={loading}
        nextPage={nextPage}
        setAfterUpdated={setAfterUpdated}
        onNavigate={getNavigatorToComparePage(repo, ref, compareRef)}
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
