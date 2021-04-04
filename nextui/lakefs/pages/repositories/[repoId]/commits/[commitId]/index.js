import {useRouter} from "next/router";
import {RepositoryPageLayout} from "../../../../../lib/components/repository/layout";
import {
    ClipboardButton,
    Error, LinkButton,
    Loading
} from "../../../../../lib/components/controls";
import React from "react";
import {useRepoAndRef} from "../../../../../lib/hooks/repo";
import Card from "react-bootstrap/Card";
import {useAPI, useAPIWithPagination} from "../../../../../rest/hooks";
import { commits, refs} from "../../../../../rest/api";
import moment from "moment";
import Link from 'next/link';
import Table from "react-bootstrap/Table";
import Alert from "react-bootstrap/Alert";
import {ChangeEntryRow} from "../../../../../lib/components/repository/changes";
import {Paginator} from "../../../../../lib/components/pagination";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import {BrowserIcon, LinkIcon, PackageIcon, PlayIcon} from "@primer/octicons-react";


const ChangeList = ({ repo, commit, after, onPaginate }) => {

    const {results, loading, error, nextPage} = useAPIWithPagination(async() => {
        if (!commit.parents || commit.parents.length === 0) return {results: [], pagination: {has_more: false}}
        return refs.diff(repo.id, commit.id, commit.parents[(commit.parents.length === 2) ? 1 : 0], after)
    }, [repo.id, commit.id, after])

    if (!!loading) return <Loading/>
    if (!!error) return <Error error={error}/>

    return (
        <div className="tree-container">
            {(results.length === 0) ? <Alert variant="info">No changes</Alert> : (
                <Card>
                    <Card.Body>
                        <Table borderless size="sm">
                            <tbody>
                            {results.map(entry => (
                                <ChangeEntryRow key={entry.path} entry={entry} showActions={false}/>
                            ))}
                            </tbody>
                        </Table>
                    </Card.Body>
                </Card>
            )}

            <Paginator onPaginate={onPaginate} nextPage={nextPage} after={after}/>
        </div>
    )
}

const CommitActions = ({ repo, commit }) => {

    const buttonVariant = "primary"

    return (
        <div>
            <ButtonGroup className="commit-actions">
                <LinkButton
                    buttonVariant="outline-primary"
                    href={{pathname: '/repositories/[repoId]/objects', query: {repoId: repo.id, ref: commit.id}}}
                    tooltip="Browse objects at this commit">
                    <BrowserIcon/>
                </LinkButton>
                <LinkButton
                    buttonVariant={buttonVariant}
                    href={{pathname: '/repositories/[repoId]/actions', query: {repoId: repo.id, commit: commit.id}}}
                    tooltip="View Commit Action runs">
                    <PlayIcon/>
                </LinkButton>
                <ClipboardButton variant={buttonVariant} text={commit.id} tooltip="copy ID to clipboard"/>
                <ClipboardButton variant={buttonVariant} text={`lakefs://${repo.id}@${commit.id}`} tooltip="copy URI to clipboard" icon={<LinkIcon/>}/>
                <ClipboardButton variant={buttonVariant} text={`s3://${repo.id}/${commit.id}`} tooltip="copy S3 URI to clipboard" icon={<PackageIcon/>}/>
            </ButtonGroup>
        </div>
    )
}


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
    )
}

const CommitLink = ({ repoId, commitId }) => {
    return (
        <Link href={{
            pathname: '/repositories/[repoId]/commits/[commitId]',
            query: {repoId, commitId}
        }}>
            <a><code>{commitId}</code></a>
        </Link>
    )
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
                        <>
                            <CommitLink key={cid} repoId={repo.id} commitId={cid}/>
                            <br/>
                        </>
                    ))}
                </td>
            </tr>
            ) : <></>}
            </tbody>
        </Table>
    )
}

const CommitContainer = ({ repo, reference, onPaginate, after }) => {

    // pull commit itself
    const {response, loading, error} = useAPI(async () => {
        return await commits.get(repo.id, reference.id)
    }, [repo.id, reference.id])

    if (loading) return <Loading/>
    if (!!error) return <Error error={error}/>

    const commit = response

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
                <ChangeList repo={repo} commit={commit} onPaginate={onPaginate} after={after}/>
            </div>
        </div>
    )

}

const RefContainer = ({ repoId, refId, onPaginate, after }) => {
    const {loading, error, response} = useRepoAndRef(repoId, refId)
    if (loading) return <Loading/>
    if (!!error) return <Error error={error}/>
    const { repo, ref } = response
    return (
        <CommitContainer repo={repo} reference={ref} onPaginate={onPaginate} after={after}/>
    )
}


const RepositoryCommitPage = () => {
    const router = useRouter()
    const { repoId, commitId, after } = router.query;

    return (
        <RepositoryPageLayout repoId={repoId} activePage={'commits'}>
            {(!repoId) ?
                <Loading/> :
                <RefContainer
                    repoId={repoId}
                    refId={commitId}
                    onPaginate={after => {
                        router.push({
                            pathname: '/repositories/[repoId]/commits/[commitId]',
                            query: {repoId, commitId, after}
                        })
                    }}
                    after={(!!after) ? after : ""}/>
            }
        </RepositoryPageLayout>
    )
}

export default RepositoryCommitPage;