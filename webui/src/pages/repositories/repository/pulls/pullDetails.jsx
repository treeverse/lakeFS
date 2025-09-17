import React, {useContext, useEffect, useState} from "react";
import {useOutletContext} from "react-router-dom";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import {GitMergeIcon, GitPullRequestClosedIcon, GitPullRequestIcon} from "@primer/octicons-react";
import dayjs from "dayjs";
import Markdown from 'react-markdown'

import {AlertError, Loading} from "../../../../lib/components/controls";
import {useRefs} from "../../../../lib/hooks/repo";
import {useRouter} from "../../../../lib/hooks/router";
import {RepoError} from "../error";
import {pulls as pullsAPI} from "../../../../lib/api";
import {useAPI} from "../../../../lib/hooks/api";
import {Link} from "../../../../lib/components/nav";
import CompareBranches from "../../../../lib/components/repository/compareBranches";
import {PullStatus, RefTypeBranch} from "../../../../constants";
import {DiffContext, WithDiffContext} from "../../../../lib/hooks/diffContext";
import Alert from "react-bootstrap/Alert";

const PullDetailsContent = ({repo, pull}) => {
    let [loading, setLoading] = useState(false);
    let [error, setError] = useState(null);

    const {state: {results: diffResults, loading: diffLoading, error: diffError}} = useContext(DiffContext);
    const isEmptyDiff = (!diffLoading && !diffError && !!diffResults && diffResults.length === 0);
    const formattedDiffError = getFormattedDiffError(diffError);

    const mergePullRequest = async () => {
        setError(null);
        setLoading(true);
        try {
            await pullsAPI.merge(repo.id, pull.id);
        } catch (error) {
            setError(error.message);
            setLoading(false);
            return;
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

    const isPullOpen = () => pull.status === PullStatus.open;
    const headerDate = isPullOpen() ? dayjs(pull.creation_date) : dayjs(pull.closed_date);

    return (
        <div className="pull-details w-75 mb-5">
            <h1>{pull.title}</h1>
            <div className="pull-info mt-3">
                <PullInfo repo={repo} pull={pull}/>
            </div>
            <Card className="mt-4">
                <Card.Header>
                    <span className="text-capitalize">{pull.status}</span> {""}
                    on {headerDate.format("MMM D, YYYY")} ({headerDate.fromNow()}).
                </Card.Header>
                <Card.Body className="description">
                    <Markdown>{pull.description}</Markdown>
                </Card.Body>
            </Card>
            <div className="bottom-buttons-row mt-4 clearfix">
                {error && <AlertError error={error} onDismiss={() => setError(null)}/>}
                {formattedDiffError && <AlertError error={formattedDiffError}/>}
                {isPullOpen() &&
                    <>
                        <div className="bottom-buttons-group float-end">
                            <ClosePullButton onClick={changePullStatus(PullStatus.closed)} loading={loading}/>
                            {!formattedDiffError &&
                                <MergePullButton
                                    onClick={mergePullRequest} isEmptyDiff={isEmptyDiff} loading={loading}/>
                            }
                        </div>
                        {isEmptyDiff &&
                            <Alert variant="warning" className="mt-4">
                                Merging is disabled for pull requests without changes.
                            </Alert>
                        }
                    </>
                }
            </div>
            {isPullOpen() && !formattedDiffError && // in case of diff error, we show an error message above instead.
                <>
                    <hr className="mt-5 mb-4"/>
                    <CompareBranches
                        repo={repo}
                        reference={{id: pull.destination_branch, type: RefTypeBranch}}
                        compareReference={{id: pull.source_branch, type: RefTypeBranch}}
                    />
                </>
            }
            {pull.status === PullStatus.merged && pull.merged_commit_id &&
                <MergedCommitDetails repo={repo} pull={pull}/>
            }
        </div>
    );
};

// message example: "<author> wants to merge <source-branch> into <destination-branch>."
const PullInfo = ({repo, pull}) => <>
    <StatusBadge status={pull.status}/>
    <span className="ms-2">
        <strong>{pull.author}</strong> {`${getActionText(pull.status)} `}
        <BranchLink repo={repo} branch={pull.source_branch}/> {""}
        into <BranchLink repo={repo} branch={pull.destination_branch}/>.
    </span>
</>;

function getActionText(status) {
    switch (status) {
        case PullStatus.open:
            return "wants to merge";
        case PullStatus.closed:
            return "wanted to merge";
        case PullStatus.merged:
            return "merged";
        default:
            return ""; // shouldn't happen
    }
}

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

const ClosePullButton = ({onClick, loading}) =>
    <Button variant="outline-secondary"
            className="text-secondary-emphasis"
            disabled={loading}
            onClick={onClick}>
        {loading ?
            <span className="spinner-border spinner-border-sm text-light" role="status"/> :
            <>Close pull request</>
        }
    </Button>;

const MergePullButton = ({onClick, isEmptyDiff, loading}) =>
    <Button variant="success"
            className="ms-2"
            disabled={loading || isEmptyDiff}
            onClick={onClick}>
        {loading ?
            <span className="spinner-border spinner-border-sm text-light" role="status"/> :
            <><GitMergeIcon/> Merge pull request</>
        }
    </Button>;

// message example: "<author> merged commit <commit-id> into master 2 days ago."
const MergedCommitDetails = ({repo, pull}) => <div>
    <strong>{pull.author}</strong> merged {""}
    <Link href={{
        pathname: '/repositories/:repoId/commits/:commitId',
        params: {repoId: repo.id, commitId: pull.merged_commit_id},
    }}>
        commit {pull.merged_commit_id.substring(0, 7)}
    </Link> {""}
    into <BranchLink repo={repo} branch={pull.destination_branch}/> {""}
    {dayjs(pull.closed_date).fromNow()}.
</div>;

const BranchLink = ({repo, branch}) =>
    <Link href={{
        pathname: '/repositories/:repoId/objects',
        params: {repoId: repo.id},
        query: {ref: branch}
    }}>
        {branch}
    </Link>;

// this is pretty hacky, but there seem to be no other way to detect this specific error
function getFormattedDiffError(error) {
    const notFoundSuffix = ": not found";
    if (error?.message?.endsWith(notFoundSuffix)) {
        const {message} = error;
        for (let getCommitPrefix of ["get commit by ref ", "get commit by branch "]) {
            if (message.startsWith(getCommitPrefix)) {
                const branch = message.split(getCommitPrefix)[1].split(notFoundSuffix)[0];
                return `Branch '${branch}' not found.`
            }
        }
    }
    return error?.message;
}

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
    return <WithDiffContext>
        <PullDetailsContainer/>
    </WithDiffContext>;
}

export default RepositoryPullDetailsPage;
