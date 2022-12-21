import React, {useState} from "react";

import {RepositoryPageLayout} from "../../../../../lib/components/repository/layout";
import {Error, FormattedDate, Loading, Na} from "../../../../../lib/components/controls";
import {RefContextProvider, useRefs} from "../../../../../lib/hooks/repo";
import {useAPI} from "../../../../../lib/hooks/api";
import {actions} from "../../../../../lib/api";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import ListGroup from "react-bootstrap/ListGroup";
import {
    ChevronDownIcon, ChevronRightIcon,
    HomeIcon,
    PlayIcon,
} from "@primer/octicons-react";
import Button from "react-bootstrap/Button";
import moment from "moment";
import {ActionStatusIcon} from "../../../../../lib/components/repository/actions";
import Table from "react-bootstrap/Table";
import {Link} from "../../../../../lib/components/nav";
import {useRouter} from "../../../../../lib/hooks/router";


const RunSummary = ({ repo, run }) => {
    return (
        <Table size="lg">
            <tbody>
                <tr>
                    <td><strong>ID</strong></td>
                    <td>{run.run_id}</td>
                </tr>
                <tr>
                    <td><strong>Event Type</strong></td>
                    <td>{run.event_type}</td>
                </tr>
                <tr>
                    <td><strong>Status</strong></td>
                    <td>{run.status}</td>
                </tr>
                <tr>
                    <td><strong>Branch</strong></td>
                    <td>
                    {(!run.branch) ? <Na/> :
                        <Link className="mr-2" href={{
                            pathname: '/repositories/:repoId/objects',
                            params: {repoId: repo.id},
                            query: {ref: run.branch}
                        }}>
                            {run.branch}
                        </Link>
                    }
                    </td>
                </tr>
                <tr>
                    <td><strong>Commit</strong></td>
                    <td>
                        {(!run.commit_id) ? <Na/> : <Link className="mr-2" href={{
                        pathname: '/repositories/:repoId/commits/:commitId',
                        params: {repoId: repo.id, commitId: run.commit_id}
                        }}>
                            <code>{run.commit_id.substr(0, 12)}</code>
                        </Link>
                        }
                    </td>
                </tr>
                <tr>
                    <td><strong>Start Time</strong></td>
                    <td>{(!run.start_time) ? <Na/> :<FormattedDate dateValue={run.start_time}/>}</td>
                </tr>
                <tr>
                    <td><strong>End Time</strong></td>
                    <td>{(!run.end_time) ? <Na/> :<FormattedDate dateValue={run.end_time}/>}</td>
                </tr>
            </tbody>
        </Table>
    );
};


const HookLog = ({ repo, run, execution }) => {
    const [expanded, setExpanded] = useState(false);
    const {response, loading, error} = useAPI(() => {
        if (!expanded) return '';
        return actions.getRunHookOutput(repo.id, run.run_id, execution.hook_run_id);
    }, [repo.id, execution.hook_id, execution.hook_run_id, expanded]);

    let content = <></>;
    if (expanded) {
        if (loading) {
            content = <pre>Loading...</pre>;
        } else if (error) {
            content = <Error error={error}/>;
        } else {
            content = <pre>{response}</pre>;
        }
    }

    let duration = '(running)';
    if (execution.status === 'completed' || execution.status === 'failed') {
        const endTs = moment(execution.end_time);
        const startTs = moment(execution.start_time);
        const diff = moment.duration(endTs.diff(startTs)).asSeconds();
        duration = `(${execution.status} in ${diff}s)`;
    } else if (execution.status === 'skipped') {
        duration = '(skipped)'
    }

    return (
            <div className="hook-log">

                <p className="mb-3 hook-log-title">
                    <Button variant="link" onClick={() => {setExpanded(!expanded)}} disabled={execution.status === "skipped"}>
                        {(expanded) ?  <ChevronDownIcon size="small"/> : <ChevronRightIcon size="small"/>}
                    </Button>
                    {' '}
                    <ActionStatusIcon status={execution.status}/>
                    {' '}
                    {execution.hook_id}

                    <small>
                        {duration}
                    </small>
                </p>

                <div className="hook-log-content">
                    {content}
                </div>
            </div>
    );
}

const ExecutionsExplorer = ({ repo, run, executions }) => {
    return (
        <div className="hook-logs">
            {executions.map(exec => (
                <HookLog key={`${exec.hook_id}-${exec.hook_run_id}`} repo={repo} run={run} execution={exec}/>
            ))}
        </div>
    );
};

const ActionBrowser = ({ repo, run, hooks, onSelectAction, selectedAction = null }) => {

    const hookRuns = hooks.results;

    // group by action
    const actionNames = {};
    hookRuns.forEach(hookRun => { actionNames[hookRun.action] = true });
    const actions = Object.getOwnPropertyNames(actionNames).sort();

    let content = <RunSummary repo={repo} run={run}/>
    if (selectedAction !== null) {
        // we're looking at a specific action, let's filter
        const actionRuns = hookRuns
            .filter(hook => hook.action === selectedAction)
            .sort((a, b) => {
                if (a.hook_run_id > b.hook_run_id) return 1;
                else if (a.hook_run_id < b.hook_run_id) return -1;
                return 0;
            })
        content = <ExecutionsExplorer run={run} repo={repo} executions={actionRuns}/>;
    }

    return (
        <Row className="mt-3">
            <Col md={{span: 3}}>
                <ListGroup variant="flush">
                    <ListGroup.Item action
                        onClick={() => onSelectAction(null)}>
                        <HomeIcon/> Summary
                    </ListGroup.Item>
                </ListGroup>

                <div className="mt-3">

                    <h6>Actions</h6>

                    <ListGroup>
                        {actions.map(actionName => (
                            <ListGroup.Item action
                                key={actionName}
                                onClick={() => onSelectAction(actionName)}>
                                <PlayIcon/> {actionName}
                            </ListGroup.Item>
                        ))}
                    </ListGroup>
                </div>
            </Col>
            <Col md={{span: 9}}>
                {content}
            </Col>
        </Row>
    );
};


const RunContainer = ({ repo, runId, onSelectAction, selectedAction }) => {
    const {response, error, loading} = useAPI(async () => {
        const [ run, hooks ] = await Promise.all([
            actions.getRun(repo.id, runId),
            actions.listRunHooks(repo.id, runId)
        ]);
        return {run, hooks};
    }, [repo.id, runId]);

    if (loading) return <Loading/>;
    if (error) return <Error error={error}/>;

    return (
        <ActionBrowser
            repo={repo}
            run={response.run}
            hooks={response.hooks}
            onSelectAction={onSelectAction}
            selectedAction={selectedAction}
        />
    )
}

const ActionContainer = () => {
    const router = useRouter();
    const { action } = router.query;
    const { runId } = router.params;
    const {loading, error, repo} = useRefs();

    if (loading) return <Loading/>;
    if (error) return <Error error={error}/>;

    const params = {repoId: repo.id, runId};

    return <RunContainer
        repo={repo}
        runId={runId}
        selectedAction={(action) ? action : null}
        onSelectAction={action => {
            const query = {};
            if (action) query.action = action;
            router.push({
                pathname: '/repositories/:repoId/actions/:runId', query, params
            });
        }}
    />
}

const RepositoryActionPage = () => {
    return (
        <RefContextProvider>
            <RepositoryPageLayout activePage={'actions'} fluid>
                <ActionContainer/>
            </RepositoryPageLayout>
        </RefContextProvider>
    );
};

export default RepositoryActionPage;
