import React, {useState} from "react";

import {useRouter} from "next/router";
import Link from "next/link";
import moment from "moment";
import {BrowserIcon, LinkIcon, PackageIcon, PlayIcon, SyncIcon} from "@primer/octicons-react";

import {commits} from "../../../../rest/api";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import {OverlayTrigger} from "react-bootstrap";
import Tooltip from "react-bootstrap/Tooltip";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import ListGroup from "react-bootstrap/ListGroup";

import {
    ActionGroup,
    ActionsBar,
    ClipboardButton,
    Error,
    LinkButton,
    Loading
} from "../../../../lib/components/controls";
import {RepositoryPageLayout} from "../../../../lib/components/repository/layout";
import {RefContextProvider, useRefs, useRepoAndRef} from "../../../../lib/hooks/repo";
import {useAPIWithPagination} from "../../../../rest/hooks";
import {Paginator} from "../../../../lib/components/pagination";
import RefDropdown from "../../../../lib/components/repository/refDropdown";


const CommitWidget = ({ repo, commit }) => {

    const buttonVariant = "primary"

    return (
        <ListGroup.Item>
            <div className="clearfix">
                <div className="float-left">
                    <h6>
                        <Link href={{
                            pathname: '/repositories/[repoId]/commits/[commitId]',
                            query: {repoId: repo.id, commitId: commit.id}
                        }}>
                            <a>{commit.message}</a>
                        </Link>
                    </h6>
                    <p>
                        <small>
                            <strong>{commit.committer}</strong> committed at <strong>{moment.unix(commit.creation_date).format("MM/DD/YYYY HH:mm:ss")}</strong> ({moment.unix(commit.creation_date).fromNow()})
                        </small>
                    </p>
                </div>
                <div className="float-right">
                    <ButtonGroup className="commit-actions">
                        <LinkButton
                            buttonVariant="outline-primary"
                            href={{
                                pathname: '/repositories/[repoId]/commits/[commitId]',
                                query: {repoId: repo.id, commitId: commit.id}
                            }}>
                            <code>{commit.id.substr(0, 16)}</code>
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

                    <div className="float-right ml-2">
                        <ButtonGroup className="commit-actions">
                            <LinkButton
                                buttonVariant="primary"
                                href={{pathname: '/repositories/[repoId]/objects', query: {repoId: repo.id, ref: commit.id}}}
                                tooltip="Browse objects at this commit">
                                <BrowserIcon/>
                            </LinkButton>
                        </ButtonGroup>
                    </div>
                </div>
            </div>
        </ListGroup.Item>
    );
}


const CommitsBrowser = ({ repo, reference, after, onPaginate, onSelectRef }) => {

    const [refresh, setRefresh] = useState(true)
    const { results, error, loading, nextPage } = useAPIWithPagination(async () => {
        return commits.log(repo.id, reference.id, after)
    }, [repo.id, reference.id, refresh, after])

    if (loading) return <Loading/>
    if (!!error) return <Error error={error}/>

    return (
        <div className="mb-5">

            <ActionsBar>
                <ActionGroup orientation="left">
                    <RefDropdown
                        repo={repo}
                        selected={(!!reference) ? reference : null}
                        withCommits={true}
                        withWorkspace={false}
                        selectRef={onSelectRef}
                    />
                </ActionGroup>

                <ActionGroup orientation="right">
                    <OverlayTrigger placement="bottom"
                                    overlay={<Tooltip id="refreshTooltipId">Refresh</Tooltip>}>
                        <Button variant="light" onClick={() =>  setRefresh(!refresh) }>
                            <SyncIcon/>
                        </Button>
                    </OverlayTrigger>
                </ActionGroup>
            </ActionsBar>

            <Card>
                <ListGroup variant="flush">
                {results.map(commit => (
                    <CommitWidget key={commit.id} repo={repo} commit={commit}/>
                ))}
                </ListGroup>
            </Card>
            <Paginator onPaginate={onPaginate} nextPage={nextPage} after={after}/>
        </div>
    )


}


const CommitsContainer = () => {
    const router = useRouter()
    const { after } = router.query
    const { repo, reference, loading ,error } = useRefs()

    if (loading) return <Loading/>
    if (!!error) return <Error error={error}/>

    return (
        <CommitsBrowser
            repo={repo}
            reference={reference}
            onSelectRef={ref => router.push({
                pathname: `/repositories/[repoId]/commits`,
                query: {repoId: repo.id, ref: ref.id}
            })}
            after={(!!after) ? after : ""}
            onPaginate={after => router.push({
                    pathname: `/repositories/[repoId]/commits`,
                    query: {repoId: repo.id, ref: reference.id, after}
                })}
        />
    )
}


const RepositoryCommitsPage = () => {
    return (
        <RefContextProvider>
            <RepositoryPageLayout activePage={'commits'}>
                <CommitsContainer/>
            </RepositoryPageLayout>
        </RefContextProvider>
    )
}

export default RepositoryCommitsPage;