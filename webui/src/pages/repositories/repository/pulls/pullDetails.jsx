import React, {useEffect, useState} from "react";
import {useOutletContext} from "react-router-dom";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import {GitMergeIcon, GitPullRequestClosedIcon, GitPullRequestIcon} from "@primer/octicons-react";
import dayjs from "dayjs";
import Markdown from 'react-markdown'
import {backOff} from "exponential-backoff";

import {AlertError, Loading} from "../../../../lib/components/controls";
import {useRefs} from "../../../../lib/hooks/repo";
import {useRouter} from "../../../../lib/hooks/router";
import {RepoError} from "../error";
import {pulls as pullsAPI, refs as refsAPI} from "../../../../lib/api";
import {useAPI} from "../../../../lib/hooks/api";
import {Link} from "../../../../lib/components/nav";
import CompareBranches from "../../../../lib/components/repository/compareBranches";
import {PullStatus, RefTypeBranch} from "../../../../constants";

const BranchLink = ({repo, branch}) =>
    <Link href={{
        pathname: '/repositories/:repoId/objects',
        params: {repoId: repo.id},
        query: {ref: branch}
    }}>
        {branch}
    </Link>;

const StatusBadge = ({status}) => {
    const text = <span className="text-capitalize">{status}</span>;
    switch (status) {
        case PullStatus.open:
            return <Badge pill bg={"success"}>{<GitPullRequestIcon/>} {text}</Badge>;
        case PullStatus.closed:
            return <Badge pill bg={"secondary"}>{<GitPullRequestClosedIcon/>} {text}</Badge>;
        case PullStatus.merged:
            return <Badge pill bg={"primary"}>{<GitMergeIcon/>} {text}</Badge>;
        default:
            return <Badge pill bg={"light"}>{text}</Badge>;
    }
};

const PullDetailsContent = ({repo, pull}) => {
    let [loading, setLoading] = useState(false);
    let [error, setError] = useState(null);

    const mergePullRequest = async () => {
        setError(null);
        setLoading(true);
        try {
            await refsAPI.merge(repo.id, pull.source_branch, pull.destination_branch);
        } catch (error) {
            setError(`Failed to merge pull request: ${error.message}`);
            setLoading(false);
            return;
        }
        try {
            await backOff(() => pullsAPI.update(repo.id, pull.id, {status: PullStatus.merged}));
        } catch (error) {
            setError(`Failed to update pull request status: ${error.message}`);
            setLoading(false);
        }
        window.location.reload(); // TODO (gilo): replace with a more elegant solution
    }

    const changePullStatus = (status) => async () => {
        setError(null);
        setLoading(true);
        try {
            await pullsAPI.update(repo.id, pull.id, {status});
            window.location.reload(); // TODO (gilo): replace with a more elegant solution
        } catch (error) {
            setError(`Failed to change pull-request status to ${status}: ${error.message}`);
            setLoading(false);
        }
    }

    const createdAt = dayjs(pull.creation_date);

    const isPullOpen = () => pull.status === PullStatus.open;

    return (
        <div className="pull-details w-75 mb-5">
            <h1>{pull.title}</h1>
            <div className="pull-info mt-3">
                <StatusBadge status={pull.status}/>
                <span className="ms-2">
                    <strong>{pull.author}</strong> wants to merge {""}
                    <BranchLink repo={repo} branch={pull.source_branch}/> {""}
                    into <BranchLink repo={repo} branch={pull.destination_branch}/>.
                </span>
            </div>
            <Card className="mt-4">
                <Card.Header>
                    Opened on {createdAt.format("MMM D, YYYY")} ({createdAt.fromNow()}).
                </Card.Header>
                <Card.Body className="description">
                    <Markdown>{pull.description}</Markdown>
                </Card.Body>
            </Card>
            <div className="bottom-buttons-row mt-4 clearfix">
                {error && <AlertError error={error} onDismiss={() => setError(null)}/>}
                <div className="bottom-buttons-group float-end">
                    {isPullOpen() &&
                        <Button variant="outline-secondary"
                                className="text-secondary-emphasis me-2"
                                disabled={loading}
                                onClick={changePullStatus(PullStatus.closed)}>
                            {loading ?
                                <span className="spinner-border spinner-border-sm text-light" role="status"/> :
                                <>Close pull request</>
                            }
                        </Button>
                    }
                    {isPullOpen() &&
                        <Button variant="success"
                                disabled={loading}
                                onClick={mergePullRequest}>
                            {loading ?
                                <span className="spinner-border spinner-border-sm text-light" role="status"/> :
                                <><GitMergeIcon/> Merge pull request</>
                            }
                        </Button>
                    }
                </div>
            </div>
            <hr className="mt-5 mb-4"/>
            <CompareBranches
                repo={repo}
                reference={{id: pull.destination_branch, type: RefTypeBranch}}
                compareReference={{id: pull.source_branch, type: RefTypeBranch}}
            />
        </div>
    );
};

const PullDetails = ({repo, pullId}) => {
    const {response: pull, error, loading} = useAPI(async () => {
        return pullsAPI.get(repo.id, pullId);
    }, [repo.id, pullId]);

    if (loading) return <Loading/>;
    if (error) return <AlertError error={error}/>;

    return <PullDetailsContent repo={repo} pull={pull}/>;
}

const PullDetailsContainer = () => {
    const router = useRouter()
    const {repo, loading, error} = useRefs();
    const {pullId} = router.params;

    if (loading) return <Loading/>;
    if (error) return <RepoError error={error}/>;

    return <PullDetails repo={repo} pullId={pullId}/>;
};


const RepositoryPullDetailsPage = () => {
    const [setActivePage] = useOutletContext();
    useEffect(() => setActivePage("pulls"), [setActivePage]);
    return <PullDetailsContainer/>;
}

export default RepositoryPullDetailsPage;
