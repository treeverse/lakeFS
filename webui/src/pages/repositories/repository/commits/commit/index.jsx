import React from "react";
import {RepositoryPageLayout} from "../../../../../lib/components/repository/layout";
import {
    ClipboardButton,
    Error, LinkButton,
    Loading, ToggleSwitch
} from "../../../../../lib/components/controls";
import {RefContextProvider, useRefs} from "../../../../../lib/hooks/repo";
import Card from "react-bootstrap/Card";
import {useAPI, useAPIWithPagination} from "../../../../../lib/hooks/api";
import {commits, refs} from "../../../../../lib/api";
import moment from "moment";
import Table from "react-bootstrap/Table";
import Alert from "react-bootstrap/Alert";
import {ChangeEntryRow} from "../../../../../lib/components/repository/changes";
import {Paginator} from "../../../../../lib/components/pagination";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import {BrowserIcon, LinkIcon, PackageIcon, PlayIcon} from "@primer/octicons-react";
import {Link} from "../../../../../lib/components/nav";
import {useRouter} from "../../../../../lib/hooks/router";
import {URINavigator} from "../../../../../lib/components/repository/tree";


const ChangeList = ({ repo, commit, after, prefix, delimiter, onPaginate }) => {
    const {results, loading, error, nextPage} = useAPIWithPagination(async() => {
        if (!commit.parents || commit.parents.length === 0) return {results: [], pagination: {has_more: false}};
        return refs.diff(repo.id, commit.id, commit.parents[0], after, prefix, delimiter);
    }, [repo.id, commit.id, after, prefix, delimiter]);

    if (!!loading) return <Loading/>;
    if (!!error) return <Error error={error}/>;

    const { push } = useRouter();

    return (
        <div className="tree-container">
            {(results.length === 0) ? <Alert variant="info">No changes</Alert> : (
                <Card>
                    <Card.Header>
                        <span className="float-left">
                            {(delimiter !== "") && (
                                <URINavigator
                                    path={prefix}
                                    reference={{type: 'commit', id: commit.id.substr(0, 12)}}
                                    relativeTo={commit.id.substr(0, 12)}
                                    repo={repo}
                                    pathURLBuilder={(params, query) => {
                                        return {
                                            pathname: '/repositories/:repoId/commits/:commitId',
                                            params: {repoId: repo.id, commitId: commit.id},
                                            query: {delimiter, prefix: query.path}
                                        }
                                    }}
                                />
                            )}
                        </span>
                        <span className="float-right">
                            <ToggleSwitch
                                label="Directory View"
                                id="changes-directory-view-toggle"
                                defaultChecked={(delimiter !== "")}
                                onChange={(checked) => {
                                    push({
                                        pathname: '/repositories/:repoId/commits/:commitId',
                                        params: {repoId: repo.id, commitId: commit.id},
                                        query: {delimiter: (checked) ? "/" : ""},
                                    })
                                }}
                            />
                        </span>
                    </Card.Header>
                    <Card.Body>
                        <Table borderless size="sm">
                            <tbody>
                            {results.map(entry => (
                                <ChangeEntryRow
                                    repo={repo}
                                    reference={commit}
                                    relativeTo={prefix}
                                    key={entry.path}
                                    entry={entry}
                                    showActions={false}
                                    onNavigate={entry => {
                                        return {
                                            pathname: '/repositories/:repoId/commits/:commitId',
                                            params: {repoId: repo.id, commitId: commit.id},
                                            query: {delimiter: "/", prefix: entry.path}
                                        }
                                    }}
                                />
                            ))}
                            </tbody>
                        </Table>
                    </Card.Body>
                </Card>
            )}

            <Paginator onPaginate={onPaginate} nextPage={nextPage} after={after}/>
        </div>
    );
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
                    {moment.unix(commit.creation_date).format("MM/DD/YYYY HH:mm:ss")} ({moment.unix(commit.creation_date).fromNow()})
                </td>
            </tr>
            {(!!commit.parents) ? (
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

const CommitView = ({ repo, commitId, onPaginate, after, prefix, delimiter }) => {

    // pull commit itself
    const {response, loading, error} = useAPI(async () => {
        return await commits.get(repo.id, commitId);
    }, [repo.id, commitId]);

    if (loading) return <Loading/>;
    if (!!error) return <Error error={error}/>;

    const commit = response;

    return (
        <div className="mb-5 mt-3">
            <Card>
                <Card.Body>
                    <div className="clearfix">
                        <div className="float-left">
                            <Card.Title>{commit.message}</Card.Title>
                        </div>
                        <div className="float-right">
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
                <ChangeList repo={repo} commit={commit} onPaginate={onPaginate} after={after} prefix={prefix} delimiter={delimiter}/>
            </div>
        </div>
    );
};

const CommitContainer = () => {
    const router = useRouter();
    const { repo, loading, error } = useRefs();
    const { after, prefix, delimiter } = router.query;
    const { commitId } = router.params;

    if (loading) return <Loading/>;
    if (!!error) return <Error error={error}/>;

    return (
        <CommitView
            repo={repo}
            after={(!!after) ? after : ""}
            prefix={(!!prefix) ? prefix : ""}
            delimiter={(!!delimiter) ? delimiter : ""}
            commitId={commitId}
            onPaginate={after => router.push({
                pathname: '/repositories/:repoId/commits/:commitId',
                params: {repoId: repo.id, commitId},
                query: {
                    after: (after) ? after : "",
                    prefix: (prefix) ? prefix : "",
                    delimiter: (delimiter) ? delimiter : "",
                }
            })}
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
