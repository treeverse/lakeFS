import React, {useState} from "react";
import {refs as refsAPI} from "../../../lib/api";
import Alert from "react-bootstrap/Alert";
import {RefTypeBranch, RefTypeCommit} from "../../../constants";
import {useAPIWithPagination} from "../../hooks/api";
import {appendMoreResults} from "../../../pages/repositories/repository/changes";
import {AlertError, Loading} from "../controls";
import {RepoError} from "../../../pages/repositories/repository/error";
import {ChangesTreeContainer, defaultGetMoreChanges} from "./changes";
import {useRefs} from "../../hooks/repo";
import {useRouter} from "../../hooks/router";
import {URINavigator} from "./tree";

const CompareURINavigator = (prefix, reference, compareReference, repo) => {
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

    return <URINavigator
        path={prefix}
        reference={reference}
        relativeTo={relativeTitle(reference, compareReference)}
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
};

const CompareList = ({repo, reference, compareReference, prefix}) => {
    const [internalRefresh] = useState(true);
    // TODO: handle refresh
    // const [internalRefresh, setInternalRefresh] = useState(true);
    const [afterUpdated, setAfterUpdated] = useState(""); // state of pagination of the item's children
    const [resultsState, setResultsState] = useState({prefix, results: [], pagination: {}}); // current retrieved children of the item

    const delimiter = "/"

    const {nextPage, error, loading} = useAPIWithPagination(async () => {
        if (!repo) return

        if (compareReference.id === reference.id) {
            return {pagination: {has_more: false}, results: []}; // nothing to compare here.
        }

        console.log({resultsState, prefix, ref: reference.id, compare: compareReference.id, delimiter})
        return await appendMoreResults(
            resultsState,
            prefix,
            afterUpdated,
            setAfterUpdated,
            setResultsState,
            () => refsAPI.diff(repo.id, reference.id, compareReference.id, afterUpdated, prefix, delimiter));
    }, [repo.id, reference.id, internalRefresh, afterUpdated, delimiter, prefix])

    if (loading) return <Loading/>;
    if (error) return <AlertError error={error}/>;

    let results = resultsState.results
    console.log({results});

    const uriNavigator = CompareURINavigator(prefix, reference, compareReference, repo);

    const changesTreeMessage =
        <p>
            Showing changes between <strong>{reference.id}</strong> {""}
            and <strong>{compareReference.id}</strong>
        </p>

    let leftCommittedRef = reference.id;
    let rightCommittedRef = compareReference.id;
    if (reference.type === RefTypeBranch) {
        leftCommittedRef += "@";
    }
    if (compareReference.type === RefTypeBranch) {
        rightCommittedRef += "@";
    }

    if (compareReference.id === reference.id) {
        return <Alert variant="warning">
            <Alert.Heading>There isn’t anything to compare.</Alert.Heading>
            You’ll need to use two different sources to get a valid comparison.
        </Alert>;
    }

    const onNavigate = entry => ({
        pathname: `/repositories/:repoId/compare`,
        params: {repoId: repo.id},
        query: {
            ref: reference.id,
            compare: compareReference.id,
            prefix: entry.path,
        }
    })

    return <ChangesTreeContainer
        results={results}
        leftDiffRefID={leftCommittedRef}
        rightDiffRefID={rightCommittedRef}
        repo={repo}
        delimiter={delimiter}
        uriNavigator={uriNavigator}
        reference={reference}
        internalReferesh={internalRefresh}
        prefix={prefix}
        getMore={defaultGetMoreChanges(repo, reference.id, compareReference.id, delimiter)}
        loading={loading}
        nextPage={nextPage}
        setAfterUpdated={setAfterUpdated}
        onNavigate={onNavigate}
        changesTreeMessage={changesTreeMessage}
    />;
};

const BranchComparison = ({reference, compareReference}) => {
    const router = useRouter();
    const {repo, loading, error} = useRefs();

    const {prefix} = router.query;

    if (loading) return <Loading/>;
    if (error) return <RepoError error={error}/>;

    return (
        <CompareList
            repo={repo}
            reference={reference}
            compareReference={compareReference}
            prefix={prefix || ""}
        />
    );
};

export default BranchComparison;
