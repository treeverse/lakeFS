import React, {useEffect, useState} from "react";
import {useOutletContext} from "react-router-dom";
import Card from "react-bootstrap/Card";
import ListGroup from "react-bootstrap/ListGroup";
import Alert from "react-bootstrap/Alert";
import {Tab, Tabs} from "react-bootstrap";
import Button from "react-bootstrap/Button";
import dayjs from "dayjs";

import {ActionGroup, AlertError, Loading} from "../../../../lib/components/controls";
import {pulls as pullsAPI} from "../../../../lib/api";
import {useRefs} from "../../../../lib/hooks/repo";
import {useAPIWithPagination} from "../../../../lib/hooks/api";
import {Paginator} from "../../../../lib/components/pagination";
import {useRouter} from "../../../../lib/hooks/router";
import {RepoError} from "../error";
import {Link} from "../../../../lib/components/nav";
import {PullStatus} from "../../../../constants";
import {ArrowLeftIcon, GitMergeIcon, GitPullRequestClosedIcon, GitPullRequestIcon} from "@primer/octicons-react";


const PullIcon = ({status}) => {
    switch (status) {
        case PullStatus.open:
            return <GitPullRequestIcon className="text-success"/>;
        case PullStatus.closed:
            return <GitPullRequestClosedIcon className="text-secondary"/>;
        case PullStatus.merged:
            return <GitMergeIcon className="text-primary"/>;
        default:
            return null;
    }
}

const PullWidget = ({repo, pull}) => {
    const getDescription = () => {
        if (pull.status === PullStatus.open) {
            return <>Opened {dayjs(pull.creation_date).fromNow()} by <strong>{pull.author}</strong>.</>;
        } else if (pull.status === PullStatus.closed || pull.status === PullStatus.merged) {
            const statusDesc = pull.status === PullStatus.closed ? "Closed" : "Merged";
            return <>{statusDesc} {dayjs(pull.closed_date).fromNow()} by <strong>{pull.author}</strong>.</>;
        }
    };

    return (
        <ListGroup.Item className="pull-row pt-3 pb-3 clearfix" id={pull.id}>
            <div className="float-start">
                <h6>
                    <PullIcon status={pull.status}/>
                    {" "}
                    <Link className="pull-title" href={{
                        pathname: '/repositories/:repoId/pulls/:pullId',
                        params: {repoId: repo.id, pullId: pull.id}
                    }}>
                        {pull.title}
                    </Link>
                </h6>
                <small className="pull-description">{getDescription()}</small>
            </div>
            <div className="float-end mt-2">
                <div className="btn btn-light btn-sm">{pull.destination_branch}</div>
                <ArrowLeftIcon className="m-1" size="small" verticalAlign="middle"/>
                <div className="btn btn-light btn-sm">{pull.source_branch}</div>
            </div>
        </ListGroup.Item>
    );
};

const PullsList = ({repo, after, prefix, onPaginate}) => {
    const router = useRouter()
    // TODO: pullState should be persistent in the url and saved as a url param?
    const [pullsState, setPullsState] = useState(PullStatus.open);
    const {results, error, loading, nextPage} = useAPIWithPagination(async () => {
        return pullsAPI.list(repo.id, pullsState, prefix, after);
    }, [repo.id, pullsState, prefix, after]);

    let content;

    if (loading) content = <Loading/>;
    else if (error) content = <AlertError error={error}/>;
    else content = (results && !!results.length ?
                <>
                    <Card>
                        <ListGroup variant="flush" className="pulls-list">
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
            <div className="position-relative clearfix">
                <div className="">
                    <Tabs
                        defaultActiveKey={pullsState}
                        id="pulls-tabs"
                        onSelect={key => setPullsState(key)}
                        className="mb-3 pt-2"
                    >
                        <Tab eventKey={PullStatus.open} title="Open"/>
                        <Tab eventKey={PullStatus.closed} title="Closed"/>
                    </Tabs>
                </div>
                <ActionGroup orientation="right" className="position-absolute top-0 end-0 pb-2">
                    <Button variant="success"
                            onClick={() => router.push({
                                pathname: '/repositories/:repoId/pulls/create',
                                params: {repoId: repo.id},
                            })}
                    >
                        Create Pull Request
                    </Button>
                </ActionGroup>
            </div>
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

const RepositoryPullsListPage = () => {
    const [setActivePage] = useOutletContext();
    useEffect(() => setActivePage("pulls"), [setActivePage]);
    return <PullsContainer/>;
}

export default RepositoryPullsListPage;
