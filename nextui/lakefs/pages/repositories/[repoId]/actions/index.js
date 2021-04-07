import {useRouter} from "next/router";
import {RepositoryPageLayout} from "../../../../lib/components/repository/layout";
import {
    ActionGroup,
    ActionsBar,
    Error,
    FormattedDate,
    Loading, Na,
    TooltipButton
} from "../../../../lib/components/controls";
import React, {useState} from "react";
import {useRepo} from "../../../../lib/hooks/repo";
import {useAPIWithPagination} from "../../../../rest/hooks";
import {actions} from "../../../../rest/api";
import {OverlayTrigger} from "react-bootstrap";
import Tooltip from "react-bootstrap/Tooltip";
import Button from "react-bootstrap/Button";
import {
    CheckCircleFillIcon,
    FilterIcon,
    StopwatchIcon,
    SyncIcon,
    XCircleFillIcon,
    XCircleIcon, XIcon
} from "@primer/octicons-react";
import Table from "react-bootstrap/Table";
import Link from 'next/link';
import {Paginator} from "../../../../lib/components/pagination";
import {ActionStatusIcon} from "../../../../lib/components/repository/actions";


const RunRow = ({ repo, run, onFilterBranch, onFilterCommit }) => {

    return (
        <tr>
            <td>
                <ActionStatusIcon className="mr-2" status={run.status}/>
                {' '}
                <Link href={{
                    pathname: '/repositories/[repoId]/actions/[runId]',
                    query: {repoId: repo.id, runId: run.run_id}
                }}>
                    <a>{run.run_id}</a>
                </Link>
            </td>
            <td>{run.event_type}</td>
            <td>
                <Link href={{
                    pathname: '/repositories/[repoId]/objects',
                    query: {repoId: repo.id, ref: run.branch}
                }}>
                    <a className="mr-2">{run.branch}</a>
                </Link>
                <TooltipButton
                    onClick={() => onFilterBranch(run.branch)}
                    variant="link"
                    tooltip="filter by branch"
                    className="row-hover"
                    size="sm">
                    <FilterIcon size="small"/>
                </TooltipButton>
            </td>
            <td><FormattedDate dateValue={run.start_time}/></td>
            <td>
                {(!run.end_time) ? <Na/> :<FormattedDate dateValue={run.end_time}/>}
            </td>
            <td>
                {(!run.commit_id) ? <Na/> : (
                    <>
                        <Link href={{
                            pathname: '/repositories/[repoId]/commits/[commitId]',
                            query: {repoId: repo.id, commitId: run.commit_id}
                        }}>
                            <a className="mr-2" >
                                <code>{run.commit_id.substr(0, 12)}</code>
                            </a>
                        </Link>
                        <TooltipButton
                            onClick={() => onFilterCommit(run.commit_id)}
                            variant="link"
                            tooltip="filter by commit ID"
                            className="row-hover"
                            size="sm">
                            <FilterIcon size="small"/>
                        </TooltipButton>
                    </>
                )}
            </td>
        </tr>
    )
}

const RunTable = ({ repo, runs, nextPage, after, onPaginate, onFilterBranch, onFilterCommit }) => {
    return (
        <>
            <Table>
                <thead>
                    <tr>
                        <th>Run ID</th>
                        <th>Event</th>
                        <th>Branch</th>
                        <th>Start Time</th>
                        <th>End Time</th>
                        <th>Commit ID</th>
                    </tr>
                </thead>
                <tbody>
                {runs.map(run => <RunRow
                    key={run.run_id}
                    repo={repo}
                    run={run}
                    onFilterBranch={onFilterBranch}
                    onFilterCommit={onFilterCommit}/>)}
                </tbody>
            </Table>
            <Paginator onPaginate={onPaginate} after={after} nextPage={nextPage}/>
        </>
    )
}

const ActionsContainer = ({ repo, after, onPaginate, branch, commit, onFilterBranch, onFilterCommit }) => {

    const [refresh, setRefresh] = useState(false)
    const {results, loading, error, nextPage} = useAPIWithPagination(async () => {
        return await actions.listRuns(repo.id, branch, commit, after)
    }, [repo.id, after, refresh, branch, commit])

    const doRefresh = () => setRefresh(!refresh)

    let content;
    if (!!error) content = <Error error={error}/>
    else if (loading) content = <Loading/>
    else content = (
            <RunTable
                repo={repo}
                runs={results}
                nextPage={nextPage}
                after={after}
                onPaginate={onPaginate}
                onFilterCommit={onFilterCommit}
                onFilterBranch={onFilterBranch}
            />
    )

    let filters = [];
    if (!!branch) {
        filters = [<TooltipButton key="branch" variant="light" tooltip="remove branch filter" onClick={() => onFilterBranch("")}>
            <XIcon/> {branch}
        </TooltipButton>]
    }
    if (!!commit) {
        filters = [...filters, <TooltipButton key="commit" variant="light" tooltip="remove commit filter" onClick={() => onFilterCommit("")}>
            <XIcon/>  {commit.substr(0, 12)}
        </TooltipButton> ]
    }

    return (
        <div className="mb-5">
            <ActionsBar>
                <ActionGroup orientation="left">
                    {filters}
                </ActionGroup>

                <ActionGroup orientation="right">
                    <TooltipButton tooltip="Refresh" onClick={doRefresh} variant="light">
                        <SyncIcon/>
                    </TooltipButton>
                </ActionGroup>
            </ActionsBar>

            {content}
        </div>
    )
}


const RepoContainer = ({ repoId, after, onPaginate, branch, commit, onFilterBranch, onFilterCommit }) => {
    const {loading, error, response} = useRepo(repoId)
    if (loading) return <Loading/>
    if (!!error) return <Error error={error}/>
    return (
        <ActionsContainer
            repo={response}
            after={after}
            onPaginate={onPaginate}
            branch={branch}
            commit={commit}
            onFilterBranch={onFilterBranch}
            onFilterCommit={onFilterCommit}
        />
    )
}

const RepositoryActionsPage = () => {
    const router = useRouter()
    const { repoId, after } = router.query;

    const commit = (!!router.query.commit) ? router.query.commit : ""
    const branch = (!!router.query.branch) ? router.query.branch : ""

    return (
        <RepositoryPageLayout repoId={repoId} activePage={'actions'}>
            {(!repoId) ?
                <Loading/> :
                <RepoContainer
                    repoId={repoId}
                    after={(!!after) ? after : ""}
                    commit={commit}
                    branch={branch}
                    onFilterCommit={commit => {
                        const query = {repoId} // will reset pagination
                        if (!!commit) query.commit = commit
                        router.push({pathname: `/repositories/[repoId]/actions`, query})
                    }}
                    onFilterBranch={branch => {
                        const query = {repoId} // will reset pagination
                        if (!!branch) query.branch = branch
                        router.push({pathname: `/repositories/[repoId]/actions`, query})
                    }}
                    onPaginate={after => {
                        const query = {repoId, after}
                        if (!!commit) query.commit = commit
                        if (!!branch) query.branch = branch
                        router.push({pathname: `/repositories/[repoId]/actions`, query})
                    }}
                />
            }
        </RepositoryPageLayout>
    )
}

export default RepositoryActionsPage;