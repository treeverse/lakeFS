import {useRouter} from "next/router";
import {RepositoryPageLayout} from "../../../lib/components/repository/layout";
import {ClipboardButton, Error, Loading} from "../../../lib/components/controls";
import React, {useState} from "react";
import {useRepoAndRef} from "../../../lib/hooks/repo";
import {useAPIWithPagination} from "../../../rest/hooks";
import {commits} from "../../../rest/api";
import ListGroupItem from "react-bootstrap/ListGroupItem";
import Table from "react-bootstrap/Table";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import {OverlayTrigger} from "react-bootstrap";
import Link from "next/link";
import Tooltip from "react-bootstrap/Tooltip";
import Button from "react-bootstrap/Button";
import {DiffIcon, LinkExternalIcon, LinkIcon, PlayIcon} from "@primer/octicons-react";
import moment from "moment";


const CommitWidget = ({repo, commit, previous}) => {

    const buttonVariant = "secondary";

    return (
        <ListGroupItem>
            <div className="clearfix">
                <div className="float-left">
                    <h6>{commit.message}</h6>
                    <p>
                        <small>
                            <strong>{commit.committer}</strong> committed at <strong>{moment.unix(commit.creation_date).format("MM/DD/YYYY HH:mm:ss")}</strong> ({moment.unix(commit.creation_date).fromNow()})
                        </small>
                    </p>
                    {(!!commit.metadata) ? (
                        <Table className="commit-metadata" size="sm" hover>
                            <thead>
                            <tr>
                                <th>Metadata Key</th>
                                <th>Value</th>
                            </tr>
                            </thead>
                            <tbody>
                            {Object.getOwnPropertyNames(commit.metadata).map((key, i) => {
                                return (
                                    <tr key={`commit-${commit.id}-metadata-${i}`}>
                                        <td><code>{key}</code></td>
                                        <td><code>{commit.metadata[key]}</code></td>
                                    </tr>
                                );
                            })}
                            </tbody>
                        </Table>
                    ) : (<span/>)}
                </div>
                <div className="float-right">
                    <ButtonGroup className="commit-actions">
                        <ClipboardButton variant={buttonVariant} text={`s3://${repo.id}/${commit.id}`} tooltip="copy S3 URI to clipboard" icon={<LinkExternalIcon/>}/>
                        <ClipboardButton variant={buttonVariant} text={`lakefs://${repo.id}@${commit.id}`} tooltip="copy URI to clipboard" icon={<LinkIcon/>}/>
                        <ClipboardButton variant={buttonVariant} text={commit.id} tooltip="copy ID to clipboard"/>
                        <OverlayTrigger placement="bottom" overlay={<Tooltip>View commit runs</Tooltip>}>
                            <Link href={{pathname: '/repositories/[repoId]/actions', query: {repoId: repo.id, commit: commit.id}}}>
                            <Button variant={buttonVariant} as="a">
                                <PlayIcon/>
                            </Button>
                            </Link>
                        </OverlayTrigger>
                    </ButtonGroup>
                </div>
            </div>
        </ListGroupItem>
    );
}


const CommitsContainer = ({ repo, reference, onSelectRef }) => {

    const [refresh, setRefresh] = useState(true)

    const { results, error, loading, paginate, hasMore } = useAPIWithPagination(async (after) => {
        return commits.log(repo.id, reference.id, after, 1)
    }, [repo.id, reference.id, refresh])

    if (loading) return <Loading/>
    if (!!error) return <Error error={error}/>

    return (
        <div>
            {results.map(commit => (
                <CommitWidget repo={repo} commit={commit}/>
            ))}
        </div>
    )


}


const RefContainer = ({ repoId, refId, onSelectRef }) => {
    const {loading, error, response} = useRepoAndRef(repoId, refId)
    if (loading) return <Loading/>
    if (!!error) return <Error error={error}/>
    const { repo, ref } = response
    return (
        <CommitsContainer repo={repo} reference={ref} onSelectRef={onSelectRef}/>
    )
}


const RepositoryCommitsPage = () => {
    const router = useRouter()
    const { repoId, ref } = router.query;

    return (
        <RepositoryPageLayout repoId={repoId} activePage={'commits'}>
            {(!repoId) ?
                <Loading/> :
                <RefContainer
                    repoId={repoId}
                    refId={ref}
                    onSelectRef={ref => router.push({
                        pathname: `/repositories/[repoId]/changes`,
                        query: {repoId, ref: ref.id}
                    })}
                />
            }
        </RepositoryPageLayout>
    )
}

export default RepositoryCommitsPage;