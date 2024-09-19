import React, {useEffect, useState} from "react";
import {AlertError, Loading} from "../../../../../lib/components/controls";
import {useRefs} from "../../../../../lib/hooks/repo";
import {useAPI, useAPIWithPagination} from "../../../../../lib/hooks/api";
import {commits, refs} from "../../../../../lib/api";
import {ChangesTreeContainer, defaultGetMoreChanges} from "../../../../../lib/components/repository/changes";
import {useRouter} from "../../../../../lib/hooks/router";
import {URINavigator} from "../../../../../lib/components/repository/tree";
import {appendMoreResults} from "../../changes";
import {CommitInfoCard} from "../../../../../lib/components/repository/commits";
import { useOutletContext } from "react-router-dom";



const ChangeList = ({ repo, commit, prefix, onNavigate }) => {
    const [actionError, setActionError] = useState(null);
    const [afterUpdated, setAfterUpdated] = useState(""); // state of pagination of the item's children
    const [resultsState, setResultsState] = useState({prefix: prefix, results:[], pagination:{}}); // current retrieved children of the item

    const delimiter = "/"

    const { error, loading, nextPage } = useAPIWithPagination(async () => {
        if (!repo) return
        if (!commit.parents || commit.parents.length === 0) return {results: [], pagination: {has_more: false}};

        return await appendMoreResults(resultsState, prefix, afterUpdated, setAfterUpdated, setResultsState,
            () => refs.diff(repo.id, commit.parents[0], commit.id, afterUpdated, prefix, delimiter));
    }, [repo.id, commit.id, afterUpdated, prefix])

    const results = resultsState.results

    if (error) return <AlertError error={error}/>
    if (loading) return <Loading/>

    const actionErrorDisplay = (actionError) ?
        <AlertError error={actionError} onDismiss={() => setActionError(null)}/> : <></>

    const commitSha = commit.id.substring(0, 12);
    const uriNavigator = <URINavigator path={prefix} reference={commit} repo={repo}
                                       relativeTo={`${commitSha}`}
                                       pathURLBuilder={(params, query) => {
                                           return {
                                               pathname: '/repositories/:repoId/commits/:commitId',
                                               params: {repoId: repo.id, commitId: commit.id},
                                               query: {prefix: query.path}
                                           }
                                       }}/>
    const changesTreeMessage = <p>Showing changes for commit <strong>{commitSha}</strong></p>
    return (
        <>
            {actionErrorDisplay}
            <ChangesTreeContainer results={results} delimiter={delimiter} uriNavigator={uriNavigator} leftDiffRefID={commit.parents[0]}
                                  rightDiffRefID={commit.id} repo={repo} refID={commit.id} prefix={prefix}
                                  getMore={defaultGetMoreChanges(repo, commit.parents[0], commit.id, delimiter)}
                                  loading={loading} nextPage={nextPage} setAfterUpdated={setAfterUpdated} onNavigate={onNavigate}
                                  changesTreeMessage={changesTreeMessage}/>
        </>
    )
};

const CommitView = ({ repo, commitId, onNavigate, view, prefix }) => {
    // pull commit itself
    const {response, loading, error} = useAPI(async () => {
        return await commits.get(repo.id, commitId);
    }, [repo.id, commitId]);

    if (loading) return <Loading/>;
    if (error) return <AlertError error={error}/>;

    const commit = response;

    return (
        <div className="mb-5 mt-3">
            <CommitInfoCard repo={repo} commit={commit}/>
            <div className="mt-4">
                <ChangeList
                    prefix={prefix}
                    view={(view) ? view : ""}
                    repo={repo}
                    commit={commit}
                    onNavigate={onNavigate}
                />
            </div>
        </div>
    );
};

const CommitContainer = () => {
    const router = useRouter();
    const { repo, loading, error } = useRefs();
    const { prefix } = router.query;
    const { commitId } = router.params;

    if (loading) return <Loading/>;
    if (error) return <AlertError error={error}/>;

    return (
        <CommitView
            repo={repo}
            prefix={(prefix) ? prefix : ""}
            commitId={commitId}
            onNavigate={(entry) => {
                return {
                    pathname: '/repositories/:repoId/commits/:commitId',
                    params: {repoId: repo.id, commitId: commitId},
                    query: {
                        prefix: entry.path,
                    }
                }
            }}
        />
    )
}

const RepositoryCommitPage = () => {
    const [setActivePage] = useOutletContext();
    useEffect(() => setActivePage('commits'), [setActivePage]);

    return <CommitContainer/>;
}

export default RepositoryCommitPage;
