import React, {useState} from "react";

import moment from "moment";
import {BrowserIcon, LinkIcon, PackageIcon, PlayIcon} from "@primer/octicons-react";

import {commits} from "../../../../lib/api";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Card from "react-bootstrap/Card";
import ListGroup from "react-bootstrap/ListGroup";

import {
    ActionGroup,
    ActionsBar,
    ClipboardButton,
    Error,
    LinkButton,
    Loading, RefreshButton
} from "../../../../lib/components/controls";
import {RepositoryPageLayout} from "../../../../lib/components/repository/layout";
import {RefContextProvider, useRefs} from "../../../../lib/hooks/repo";
import {useAPIWithPagination} from "../../../../lib/hooks/api";
import {Paginator} from "../../../../lib/components/pagination";
import RefDropdown from "../../../../lib/components/repository/refDropdown";
import {Link} from "../../../../lib/components/nav";
import {useRouter} from "../../../../lib/hooks/router";
import {Route, Switch} from "react-router-dom";
import RepositoryCommitPage from "./commit";


const CommitWidget = ({ repo, commit }) => {

    const buttonVariant = "outline-dark";

    return (
        <ListGroup.Item>
            <div className="clearfix">
                <div className="float-left">
                    <h6>
                        <Link href={{
                            pathname: '/repositories/:repoId/commits/:commitId',
                            params: {repoId: repo.id, commitId: commit.id}
                        }}>
                            {commit.message}
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
                            buttonVariant="outline-dark"
                            href={{
                                pathname: '/repositories/:repoId/commits/:commitId',
                                params: {repoId: repo.id, commitId: commit.id}
                            }}>
                            <code>{commit.id.substr(0, 16)}</code>
                        </LinkButton>
                        <LinkButton
                            buttonVariant={buttonVariant}
                            href={{pathname: '/repositories/:repoId/actions', query: {commit: commit.id}, params: {repoId: repo.id}}}
                            tooltip="View Commit Action runs">
                            <PlayIcon/>
                        </LinkButton>
                        <ClipboardButton variant={buttonVariant} text={commit.id} tooltip="Copy ID to clipboard"/>
                        <ClipboardButton variant={buttonVariant} text={`lakefs://${repo.id}/${commit.id}`} tooltip="Copy URI to clipboard" icon={<LinkIcon/>}/>
                        <ClipboardButton variant={buttonVariant} text={`s3://${repo.id}/${commit.id}`} tooltip="Copy S3 URI to clipboard" icon={<PackageIcon/>}/>

                    </ButtonGroup>

                    <div className="float-right ml-2">
                        <ButtonGroup className="commit-actions">
                            <LinkButton
                                buttonVariant="outline-dark"
                                href={{pathname: '/repositories/:repoId/objects', params: {repoId: repo.id}, query: {ref: commit.id}}}
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
                    <RefreshButton onClick={() => { setRefresh(!refresh); }}/>
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
    const router = useRouter();
    const { after } = router.query;
    const { repo, reference, loading ,error } = useRefs();

    if (loading) return <Loading/>;
    if (!!error) return <Error error={error}/>;

    const params = {repoId: repo.id};

    return (
        <CommitsBrowser
            repo={repo}
            reference={reference}
            onSelectRef={ref => router.push({
                pathname: `/repositories/:repoId/commits`,
                query: {ref: ref.id},
                params
            })}
            after={(!!after) ? after : ""}
            onPaginate={after => router.push({
                    pathname: `/repositories/:repoId/commits`,
                    query: {ref: reference.id, after},
                    params
                })}
        />
    );
};


const RepositoryCommitsPage = () => {
    return (
        <RefContextProvider>
            <RepositoryPageLayout activePage={'commits'}>
                <CommitsContainer/>
            </RepositoryPageLayout>
        </RefContextProvider>
    );
};

const RepositoryCommitsIndexPage = () => {
    return (
        <Switch>
            <Route exact path="/repositories/:repoId/commits">
                <RepositoryCommitsPage/>
            </Route>
            <Route path="/repositories/:repoId/commits/:commitId">
                <RepositoryCommitPage/>
            </Route>
        </Switch>
    )
}

export default RepositoryCommitsIndexPage;