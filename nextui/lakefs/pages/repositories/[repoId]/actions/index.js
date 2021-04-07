import {useRouter} from "next/router";
import {RepositoryPageLayout} from "../../../../lib/components/repository/layout";
import {
    ActionGroup,
    ActionsBar,
    Error,
    FormattedDate,
    Loading,
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


const RunRow = ({ repo, run, onFilterBranch, onFilterCommit }) => {

    let status = <StopwatchIcon fill="yellow"/>
    switch (run.status) {
        case "completed":
            status = <CheckCircleFillIcon fill="green"/>
            break
        case "failed":
            status = <XCircleFillIcon fill="red"/>
    }

    status = (
        <OverlayTrigger placement="bottom" overlay={<Tooltip id={`${run.run_id}-status-tooltip`}>{run.status}</Tooltip>}>
            <span className="mr-2">{status}</span>
        </OverlayTrigger>
    )

    return (
        <tr>
            <td>
                {status} {' '}
                <Link href={{
                    pathname: '/repositories/[repoId]/actions/[actionId]',
                    query: {repoId: repo.id, actionId: run.run_id}
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
                    variant="outline-info"
                    tooltip="filter by branch"
                    className="row-hover"
                    size="sm">
                    <FilterIcon/>
                </TooltipButton>
            </td>
            <td><FormattedDate dateValue={run.start_time}/></td>
            <td>
                {(!!run.end_time) ?
                    <FormattedDate dateValue={run.end_time}/> :
                    <span>&mdash;</span>
                }
            </td>
            <td>
                {(!!run.commit_id) ?
                    (<>
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
                            variant="outline-info"
                            tooltip="filter by commit ID"
                            className="row-hover"
                            size="sm">
                            <FilterIcon/>
                        </TooltipButton>
                    </>) : (<span>&mdash;</span>)}
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

const ActionsContainer = ({ repo, after, onPaginate }) => {

    const [refresh, setRefresh] = useState(false)
    const [commitFilter, setCommitFilter] = useState("")
    const [branchFilter, setBranchFilter] = useState("")

    const {results, loading, error, nextPage} = useAPIWithPagination(async () => {
        return await actions.listRuns(repo.id, branchFilter, commitFilter, after)
    }, [repo.id, after, refresh, commitFilter, branchFilter])

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
                onFilterCommit={commitId => {
                    setCommitFilter(commitId)
                }}
                onFilterBranch={branchId => {
                    setBranchFilter(branchId)
                }}
            />
    )

    let filters = [];
    if (!!branchFilter) {
        filters = [<TooltipButton variant="light" tooltip="remove branch filter" onClick={() => setBranchFilter("")}>
            <XIcon/> {branchFilter}
        </TooltipButton>]
    }
    if (!!commitFilter) {
        filters = [...filters, <TooltipButton variant="light" tooltip="remove commit filter" onClick={() => setCommitFilter("")}>
            <XIcon/>  {commitFilter.substr(0, 12)}
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


const RefContainer = ({ repoId, after, onPaginate }) => {
    const {loading, error, response} = useRepo(repoId)
    if (loading) return <Loading/>
    if (!!error) return <Error error={error}/>
    return (
        <ActionsContainer repo={response} after={after} onPaginate={onPaginate}/>
    )
}

const RepositoryActionsPage = () => {
    const router = useRouter()
    const { repoId, after } = router.query;

    return (
        <RepositoryPageLayout repoId={repoId} activePage={'actions'}>
            {(!repoId) ?
                <Loading/> :
                <RefContainer
                    repoId={repoId}
                    after={(!!after) ? after : ""}
                    onPaginate={after => {
                        const query = {repoId, after}
                        if (!!router.query.prefix) query.prefix = router.query.prefix
                        router.push({pathname: `/repositories/[repoId]/actions`, query})
                    }}
                />
            }
        </RepositoryPageLayout>
    )
}

export default RepositoryActionsPage;