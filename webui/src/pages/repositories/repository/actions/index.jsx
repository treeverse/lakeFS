import React, {useState} from "react";
import {RepositoryPageLayout} from "../../../../lib/components/repository/layout";
import {
    ActionGroup,
    ActionsBar,
    AlertError,
    FormattedDate,
    Loading, Na, RefreshButton,
    TooltipButton
} from "../../../../lib/components/controls";
import {RefContextProvider, useRefs} from "../../../../lib/hooks/repo";
import {useAPIWithPagination} from "../../../../lib/hooks/api";
import {actions} from "../../../../lib/api";
import {
    FilterIcon,
    XIcon
} from "@primer/octicons-react";
import {Table} from "react-bootstrap";
import {Paginator} from "../../../../lib/components/pagination";
import {ActionStatusIcon} from "../../../../lib/components/repository/actions";
import {Route, Routes} from "react-router-dom";
import {Link} from "../../../../lib/components/nav";
import {useRouter} from "../../../../lib/hooks/router";
import RepositoryActionPage from "./run";
import Alert from "react-bootstrap/Alert";
import {RepoError} from "../error";


const RunRow = ({ repo, run, onFilterBranch, onFilterCommit }) => {
    return (
        <tr>
            <td>
                <ActionStatusIcon className="me-2" status={run.status}/>
                {' '}
                <Link href={{
                    pathname: '/repositories/:repoId/actions/:runId',
                    params: {repoId: repo.id, runId: run.run_id}
                }}>
                    {run.run_id}
                </Link>
            </td>
            <td>{run.event_type}</td>
            <td>
                <Link className="me-2" href={{
                    pathname: '/repositories/:repoId/objects',
                    params: {repoId: repo.id},
                    query: {ref: run.branch}
                }}>
                    {run.branch}
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
                        <Link className="me-2" href={{
                            pathname: '/repositories/:repoId/commits/:commitId',
                            params: {repoId: repo.id, commitId: run.commit_id}
                        }}>
                            <code>{run.commit_id.substr(0, 12)}</code>
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

const ActionsList = ({ repo, after, onPaginate, branch, commit, onFilterBranch, onFilterCommit }) => {

    const [refresh, setRefresh] = useState(false)
    const {results, loading, error, nextPage} = useAPIWithPagination(async () => {
        return await actions.listRuns(repo.id, branch, commit, after)
    }, [repo.id, after, refresh, branch, commit])

    const doRefresh = () => setRefresh(!refresh)

    let content;
    if (error) content = <AlertError error={error}/>

    else if (loading) content = <Loading/>
    else if (results.length === 0 && !nextPage) content = <Alert variant="info" className={"mt-3"}>No action runs have been logged yet.</Alert>
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
    if (branch) {
        filters = [<TooltipButton key="branch" variant="light" tooltip="remove branch filter" onClick={() => onFilterBranch("")}>
            <XIcon/> {branch}
        </TooltipButton>]
    }
    if (commit) {
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
                    <RefreshButton onClick={doRefresh}/>
                </ActionGroup>
            </ActionsBar>
            {content}
            <div>
                {/* eslint-disable-next-line react/jsx-no-target-blank */}
                Actions can be configured to run when predefined events occur. <a href="https://docs.lakefs.io/setup/hooks.html" target="_blank">Learn more.</a>
            </div>
        </div>
    )
}


const ActionsContainer = () => {
    const router = useRouter();
    const { after } = router.query;
    const commit = (router.query.commit) ? router.query.commit : "";
    const branch = (router.query.branch) ? router.query.branch : "";

    const { repo, loading, error } = useRefs();

    if (loading) return <Loading/>;
    if (error) return <RepoError error={error}/>;

    const params = {repoId: repo.id};

    return (
        <ActionsList
            repo={repo}
            after={after}
            onPaginate={after => {
                const query = {after};
                if (commit) query.commit = commit;
                if (branch) query.branch = branch;
                router.push({pathname: `/repositories/:repoId/actions`, query, params})
            }}
            branch={branch}
            commit={commit}
            onFilterBranch={branch => {
                const query = {}; // will reset pagination
                if (branch) query.branch = branch;
                router.push({pathname: `/repositories/:repoId/actions`, query, params})
            }}
            onFilterCommit={commit => {
                const query = {} // will reset pagination
                if (commit) query.commit = commit;
                router.push({pathname: `/repositories/:repoId/actions`, query, params})
            }}
        />
    );
};

const RepositoryActionsPage = () => {
    return (
        <RefContextProvider>
            <RepositoryPageLayout activePage={'actions'}>
                <ActionsContainer/>
            </RepositoryPageLayout>
        </RefContextProvider>
    );
};

const RepositoryActionsIndexPage = () => {
    return (
        <Routes>
            <Route path="" element={<RepositoryActionsPage/>} />
            <Route path=":runId" element={<RepositoryActionPage/>} />
        </Routes>
    );
};

export default RepositoryActionsIndexPage;
