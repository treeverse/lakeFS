import React, {useEffect} from "react";
import {useOutletContext} from "react-router-dom";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import {GitMergeIcon, GitPullRequestClosedIcon, GitPullRequestIcon} from "@primer/octicons-react";
import dayjs from "dayjs";

import {AlertError, Loading} from "../../../../lib/components/controls";
import {useRefs} from "../../../../lib/hooks/repo";
import {useRouter} from "../../../../lib/hooks/router";
import {RepoError} from "../error";
import {pulls as pullsAPI} from "../../../../lib/api";
import {useAPI} from "../../../../lib/hooks/api";
import {Link} from "../../../../lib/components/nav";
import BranchComparison from "../../../../lib/components/repository/branchesComparison";
import {PullStatus, RefTypeBranch} from "../../../../constants";

const BranchLink = ({repo, branch}) =>
    <Link href={{
        pathname: '/repositories/:repoId/objects',
        params: {repoId: repo.id},
        query: {ref: branch}
    }}>
        {branch}
    </Link>;

const getStatusBadgeParams = status => {
    switch (status) {
        case PullStatus.open:
            return {bgColor: "success", icon: <GitPullRequestIcon/>};
        case PullStatus.closed:
            return {bgColor: "purple", icon: <GitPullRequestClosedIcon/>};
        case PullStatus.merged:
            return {bgColor: "danger", icon: <GitMergeIcon/>};
        default:
            return {bgColor: "secondary", icon: null};
    }
};

const StatusBadge = ({status}) => {
    const {bgColor, icon} = getStatusBadgeParams(status);
    return <Badge pill bg={bgColor}>
        {icon} <span className="text-capitalize">{status}</span>
    </Badge>;
};

const PullDetailsContent = ({repo, pull}) => {
    const createdAt = dayjs.unix(pull.creation_date);

    return (
        <div className="pull-details mb-5">
            <h1>{pull.title} <span className="fs-5 text-secondary">{pull.id}</span></h1>
            <div className="mt-3">
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
                    {pull.description}
                </Card.Body>
            </Card>
            <div className="bottom-buttons-row mt-4 clearfix">
                <div className="bottom-buttons-group float-end">
                    <Button variant="outline-secondary" className="text-secondary-emphasis me-2">
                        Close pull request
                    </Button>
                    <Button variant="success">
                        <GitMergeIcon/> Merge pull request
                    </Button>
                </div>
            </div>
            <hr className="mt-5 mb-4"/>
            <div className="w-75">
                <BranchComparison
                    repo={repo}
                    reference={{id: pull.destination_branch, type: RefTypeBranch}}
                    compareReference={{id: pull.source_branch, type: RefTypeBranch}}
                />
            </div>
        </div>
    );
};

const PullDetails = ({repo, pullId}) => {
    const {response: pull, error, loading} = useAPI(async () => {
        console.log({repo, pullId});
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
