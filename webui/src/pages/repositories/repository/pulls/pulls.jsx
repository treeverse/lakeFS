import React, {useEffect, useState} from "react";
import {useOutletContext} from "react-router-dom";
import Card from "react-bootstrap/Card";
import ListGroup from "react-bootstrap/ListGroup";

import {
    ActionGroup,
    ActionsBar,
    AlertError,
    Loading,
    PrefixSearchWidget,
    RefreshButton
} from "../../../../lib/components/controls";
import {pulls} from "../../../../lib/api";
import {useRefs} from "../../../../lib/hooks/repo";
import {useAPIWithPagination} from "../../../../lib/hooks/api";
import {Paginator} from "../../../../lib/components/pagination";
import {useRouter} from "../../../../lib/hooks/router";
import Alert from "react-bootstrap/Alert";
import {RepoError} from "../error";
import {Tab, Tabs} from "react-bootstrap";
import {Link} from "../../../../lib/components/nav";
import Button from "react-bootstrap/Button";
import TimeAgo from "react-timeago";


const PullWidget = ({repo, pull}) => {
    const authorLink =
        <Link href={{
            // TODO: to where this link should lead?
            pathname: '/repositories/:repoId/user/:userId',
            params: {repoId: repo.id, userId: pull.author}
        }}>
            {pull.author}
        </Link>;
    return (
        <ListGroup.Item className="pull-row">
            <div className="clearfix">
                <div className="float-start pt-1 pb-2">
                    <Link className="pull-title fs-4"
                          href={{
                              pathname: '/repositories/:repoId/pulls/:pullId',
                              params: {repoId: repo.id, pullId: pull.id},
                          }}
                    >
                        {pull.id}
                    </Link>
                    <div className="pull-info mt-1">
                        Opened <TimeAgo date={new Date(pull.created_at * 1000)}/> ago by {authorLink}
                    </div>
                </div>
                <div className="pull-branches mt-3 float-end">
                    <Button variant="secondary" size="sm" disabled={true}>{pull.source_branch}</Button>
                    <span className="m-2">&#8680;</span>
                    <Button variant="secondary" size="sm" disabled={true}>{pull.destination_branch}</Button>
                </div>
            </div>
        </ListGroup.Item>
    );
};

const PullStatus = {
    open: "open",
    closed: "closed",
    merged: "merged",
}

const PullsList = ({repo, after, prefix, onPaginate}) => {
    const router = useRouter()
    const [refresh, setRefresh] = useState(true);
    // TODO: pullState should be persistent in the url and saved as a url param?
    const [pullsState, setPullsState] = useState(PullStatus.open);
    const {results, error, loading, nextPage} = useAPIWithPagination(async () => {
        return pulls.list(repo.id, pullsState, prefix, after);
    }, [repo.id, pullsState, prefix, refresh, after]);

    const doRefresh = () => setRefresh(true);

    let content;

    if (loading) content = <Loading/>;
    else if (error) content = <AlertError error={error}/>;
    else content = (results && !!results.length ?
                <>
                    <Card>
                        <ListGroup variant="flush">
                            {results.map(pull => (
                                <PullWidget key={pull.id} repo={repo} pull={pull}/>
                            ))}
                        </ListGroup>
                    </Card>
                    <Paginator onPaginate={onPaginate} nextPage={nextPage} after={after}/>
                </> : <Alert variant="info">There aren&apos;t any pull requests yet.</Alert>
        )

    return (
        <div className="mb-5">
            <ActionsBar>
                <ActionGroup>
                    <Tabs
                        defaultActiveKey={pullsState}
                        id="pulls-tabs"
                        onSelect={key => setPullsState(key)}
                        className="mb-3"
                    >
                        <Tab eventKey={PullStatus.open} title="Open"/>
                        <Tab eventKey={PullStatus.closed} title="Closed"/>
                    </Tabs>
                </ActionGroup>
                <ActionGroup orientation="right">
                    <PrefixSearchWidget
                        defaultValue={router.query.prefix}
                        text="Find Pull Request"
                        onFilter={prefix => router.push({
                            pathname: '/repositories/:repoId/pulls',
                            params: {repoId: repo.id},
                            query: {prefix}
                        })}/>

                    <RefreshButton onClick={doRefresh}/>
                    <Button variant="success">Create Pull Request</Button>
                </ActionGroup>
            </ActionsBar>
            {content}
        </div>
    );
};


const PullsContainer = () => {
    const router = useRouter()
    const {repo, loading, error} = useRefs();
    const {after} = router.query;
    const routerPfx = router.query.prefix || "";

    if (loading) return <Loading/>;
    if (error) return <RepoError error={error}/>;

    return (
        <PullsList
            repo={repo}
            after={after || ""}
            prefix={routerPfx}
            onPaginate={after => {
                const query = {after};
                if (router.query.prefix) {
                    query.prefix = router.query.prefix;
                }
                router.push({
                    pathname: '/repositories/:repoId/pulls',
                    params: {repoId: repo.id},
                    query
                });
            }}/>
    );
};


const RepositoryPullsPage = () => {
    const [setActivePage] = useOutletContext();
    useEffect(() => setActivePage("pulls"), [setActivePage]);
    return <PullsContainer/>;
}

export default RepositoryPullsPage;
