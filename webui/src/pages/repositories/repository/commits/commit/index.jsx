import React, {useState} from "react";
import {RepositoryPageLayout} from "../../../../../lib/components/repository/layout";
import {ClipboardButton,Error, LinkButton, Loading} from "../../../../../lib/components/controls";
import {RefContextProvider, useRefs} from "../../../../../lib/hooks/repo";
import Card from "react-bootstrap/Card";
import {useAPI, useAPIWithPagination} from "../../../../../lib/hooks/api";
import {commits, refs} from "../../../../../lib/api";
import dayjs from "dayjs";
import Table from "react-bootstrap/Table";
import {ChangesTreeContainer} from "../../../../../lib/components/repository/changes";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import {BrowserIcon, LinkIcon, PackageIcon, PlayIcon} from "@primer/octicons-react";
import {Link} from "../../../../../lib/components/nav";
import {useRouter} from "../../../../../lib/hooks/router";
import {URINavigator} from "../../../../../lib/components/repository/tree";
import {appendMoreResults} from "../../changes";

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

    if (error) return <Error error={error}/>
    if (loading) return <Loading/>

    const actionErrorDisplay = (actionError) ?
        <Error error={actionError} onDismiss={() => setActionError(null)}/> : <></>

    const uriNavigator = <URINavigator path={prefix} reference={commit} repo={repo}
                                       relativeTo={`${commit.id.substring(0, 12)}`}
                                       pathURLBuilder={(params, query) => {
                                           return {
                                               pathname: '/repositories/:repoId/commits/:commitId',
                                               params: {repoId: repo.id, commitId: commit.id},
                                               query: {prefix: query.path}
                                           }
                                       }}/>
    return (
        <>
            {actionErrorDisplay}
            <ChangesTreeContainer results={results} delimiter={delimiter} uriNavigator={uriNavigator} leftDiffRefID={commit.parents[0]}
                                  rightDiffRefID={commit.id} repo={repo} reference={commit} prefix={prefix}
                                  getMore={(afterUpdated, path, useDelimiter = true, amount = -1) => {
                                      return refs.diff(repo.id, commit.parents[0], commit.id, afterUpdated, path, useDelimiter ? delimiter : "", amount > 0 ? amount : undefined)
                                  }} loading={loading} nextPage={nextPage} setAfterUpdated={setAfterUpdated} onNavigate={onNavigate}/>
        </>
    )
};

const CommitActions = ({ repo, commit }) => {

    const buttonVariant = "outline-dark";

    return (
        <div>
            <ButtonGroup className="commit-actions">
                <LinkButton
                    buttonVariant="outline-dark"
                    href={{pathname: '/repositories/:repoId/objects', params: {repoId: repo.id}, query: {ref: commit.id}}}
                    tooltip="Browse commit objects">
                    <BrowserIcon/>
                </LinkButton>
                <LinkButton
                    buttonVariant={buttonVariant}
                    href={{pathname: '/repositories/:repoId/actions', params: {repoId: repo.id}, query: {commit: commit.id}}}
                    tooltip="View Commit Action runs">
                    <PlayIcon/>
                </LinkButton>
                <ClipboardButton variant={buttonVariant} text={commit.id} tooltip="Copy ID to clipboard"/>
                <ClipboardButton variant={buttonVariant} text={`lakefs://${repo.id}/${commit.id}`} tooltip="Copy URI to clipboard" icon={<LinkIcon/>}/>
                <ClipboardButton variant={buttonVariant} text={`s3://${repo.id}/${commit.id}`} tooltip="Copy S3 URI to clipboard" icon={<PackageIcon/>}/>
            </ButtonGroup>
        </div>
    );
};


const CommitMetadataTable = ({ commit }) => {
    if (!commit.metadata) return <></>
    const keys = Object.getOwnPropertyNames(commit.metadata)
    if (keys.length === 0) return <></>

    return (
        <>
        <Table>
            <thead>
                <tr>
                    <th>Metadata Key</th>
                    <th>Value</th>
                </tr>
            </thead>
            <tbody>
            {keys.map(key => (
                <tr key={key}>
                    <td><code>{key}</code></td>
                    <td><code>{commit.metadata[key]}</code></td>
                </tr>
            ))}
            </tbody>
        </Table>
        </>
    );
};

const CommitLink = ({ repoId, commitId }) => {
    return (
        <>
            <Link href={{
                pathname: '/repositories/:repoId/commits/:commitId',
                params: {repoId, commitId}
            }}>
                <code>{commitId}</code>
            </Link>
            <br/>
        </>
    );
}

const CommitInfo = ({ repo, commit }) => {
    return (
        <Table size="sm" borderless hover>
            <tbody>
            <tr>
                <td><strong>ID</strong></td>
                <td>
                    <CommitLink repoId={repo.id} commitId={commit.id}/>
                </td>
            </tr>
            <tr>
                <td><strong>Committer</strong></td>
                <td>{commit.committer}</td>
            </tr>
            <tr>
                <td><strong>Creation Date</strong></td>
                <td>
                    {dayjs.unix(commit.creation_date).format("MM/DD/YYYY HH:mm:ss")} ({dayjs.unix(commit.creation_date).fromNow()})
                </td>
            </tr>
            {(commit.parents) ? (
            <tr>
                <td>
                    <strong>Parents</strong></td>
                <td>
                    {commit.parents.map(cid => (
                        <CommitLink key={cid} repoId={repo.id} commitId={cid}/>
                    ))}
                </td>
            </tr>
            ) : <></>}
            </tbody>
        </Table>
    );
};

const CommitView = ({ repo, commitId, onNavigate, view, prefix }) => {
    // pull commit itself
    const {response, loading, error} = useAPI(async () => {
        return await commits.get(repo.id, commitId);
    }, [repo.id, commitId]);

    if (loading) return <Loading/>;
    if (error) return <Error error={error}/>;

    const commit = response;

    return (
        <div className="mb-5 mt-3">
            <Card>
                <Card.Body>
                    <div className="clearfix">
                        <div className="float-start">
                            <Card.Title>{commit.message}</Card.Title>
                        </div>
                        <div className="float-end">
                            <CommitActions repo={repo} commit={commit}/>
                        </div>
                    </div>

                    <div className="mt-4">
                        <CommitInfo repo={repo} commit={commit}/>
                        <CommitMetadataTable commit={commit}/>
                    </div>
                </Card.Body>
            </Card>

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
    if (error) return <Error error={error}/>;

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
    return (
        <RefContextProvider>
            <RepositoryPageLayout activePage={'commits'}>
                <CommitContainer/>
            </RepositoryPageLayout>
        </RefContextProvider>
    )
}

export default RepositoryCommitPage;
