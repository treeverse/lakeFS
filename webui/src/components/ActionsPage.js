import {connect} from "react-redux";
import {listActionsRuns, listActionsRunHooks, getActionsRun, getActionsRunHookOutput, resetActionsRunHookOutput} from '../actions/actions';
import {Link, useHistory, useParams} from "react-router-dom";
import React, {useCallback, useEffect, useState} from "react";
import {SyncIcon} from "@primer/octicons-react";
import {PaginatedEntryList} from "./auth/entities";
import * as moment from "moment";
import {
    Alert,
    Container,
    Breadcrumb,
    Card,
    Button,
    Form,
    ButtonToolbar,
    Accordion
} from "react-bootstrap";
import {CheckCircleFillIcon, XCircleFillIcon} from "@primer/octicons-react";
import ClipboardButton from "./ClipboardButton";

export const ActionsRunsPage = connect(
    ({ actions }) => ({
        runs: actions.runs,
    }),
    ({ listActionsRuns })
)(
({repo, runs, listActionsRuns }) => {
    const [update, setUpdate] = useState(Date.now());

    const listActionsRunsFn = useCallback((after, amount) => {
        listActionsRuns(repo.id, after, amount);
    }, [repo.id, listActionsRuns]);

    return (
        <div className="mt-3">
            <div className="action-bar">
                <ButtonToolbar className="float-right mb-2 pr-1">
                    <Button variant="outline-dark" onClick={() => {
                        setUpdate(Date.now())
                    }}>
                        <SyncIcon/>
                    </Button>
                </ButtonToolbar>
            </div>
            <Form>
                <div className="actions-runs-list">
                <PaginatedEntryList
                    listFn={listActionsRunsFn}
                    entities={runs}
                    entityToKey={entity => entity.run_id}
                    emptyState={"No actions found"}
                    update={update}
                    fields={["Run ID", "Status", "Event", "Branch", "Start Time", "End Time", "Commit ID"]}
                    entityToRow={
                        entity => {
                            return [
                                (<Link to={`/repositories/${repo.id}/actions/${entity.run_id}`}>{entity.run_id.substr(4)}</Link>),
                                (<strong style={{'color': (entity.status === "completed") ? 'green':'red'}}>{entity.status}</strong>),
                                entity.event_type,
                                (<Link to={`/repositories/${repo.id}/tree?branch=${entity.branch}`}>{entity.branch}</Link>),
                                moment(entity.start_time).format("MM/DD/YYYY HH:mm:ss"),
                                moment(entity.end_time).format("MM/DD/YYYY HH:mm:ss"),
                                entity.commit_id && (<>
                                    <Link to={`/repositories/${repo.id}/tree?commit=${entity.commit_id}`}>{entity.commit_id.substr(0, 16)}</Link>
                                    <span className={"clipboard-copy"}><ClipboardButton variant="link" text={entity.commit_id} tooltip={"Copy Commit ID"}/></span>
                                </>),
                            ]
                        }
                    }
                />
                </div>
            </Form>
        </div>
    );
});


export const ActionsRunPage = connect(
    ({ actions }) => ({
        run: actions.run,
        runHooks: actions.runHooks,
        runHookOutput: actions.runHookOutput,
    }),
    ({ listActionsRunHooks, getActionsRun, getActionsRunHookOutput, resetActionsRunHookOutput })
)(
    ({repo, run, runHooks, getActionsRun, listActionsRunHooks, runHookOutput, getActionsRunHookOutput, resetActionsRunHookOutput}) => {
        // const {runId, hookRunId: hookRunIdParam} = useParams();
        // const [hookRunId, setHookRunId] = useState(hookRunIdParam);
        const {runId, hookRunId} = useParams();
        const history = useHistory();

        useEffect(() => {
            getActionsRun(repo.id, runId)
            listActionsRunHooks(repo.id, runId, '', -1);
            if (hookRunId) {
                getActionsRunHookOutput(repo.id, runId, hookRunId);
            } else {
                resetActionsRunHookOutput();
            }
        },[getActionsRun, listActionsRunHooks, repo.id, runId, hookRunId, getActionsRunHookOutput, resetActionsRunHookOutput]);

        if (run.loading || runHooks.loading) {
            return <p>Loading...</p>;
        }
        if (run.error) {
            return <Alert variant={"danger"}>{run.error}</Alert>;
        }
        if (runHooks.error) {
            return <Alert variant={"danger"}>{runHooks.error}</Alert>;
        }

        if (runHooks.payload.pagination.results === 0) {
            return <Alert variant="warning">No hooks</Alert>;
        }

        const hook = hookRunId && runHooks.payload.results.find(x => x.hook_run_id === hookRunId);
        if (hookRunId && !hook) {
            const href = `/repositories/${repo.id}/actions/${runId}`;
            return <Alert variant="warning">
                Can't find hook run ID '{hookRunId}' for run ID <Alert.Link href={href}>{runId}</Alert.Link>
            </Alert>;
        }

        return (
            <Container className="mt-3">
                <Breadcrumb>
                    <Breadcrumb.Item href={`/repositories/${repo.id}/actions`}>Actions</Breadcrumb.Item>
                    <Breadcrumb.Item active={!hookRunId} href={`/repositories/${repo.id}/actions/${runId}`}>{runId}</Breadcrumb.Item>
                    {hookRunId &&
                        <Breadcrumb.Item active={!!hookRunId} href={`/repositories/${repo.id}/actions/${runId}/${hookRunId}`}>{hook.action} / {hook.hook_id}</Breadcrumb.Item>
                    }
                </Breadcrumb>
                <RunDetails repo={repo} runId={runId} runDetails={run.payload} />
                <Accordion defaultActiveKey={hookRunId}>
                    {runHooks.payload.results.map(hook => {
                        return (
                            <Card key={"hook_"+hook.hook_run_id}>
                                <Card.Header>
                                    <Accordion.Toggle as={Button} variant="link" eventKey={hook.hook_run_id}>
                                        {hook.status === "completed"
                                            ? <span style={{color: "green"}}><CheckCircleFillIcon/> </span>
                                            : <span style={{color: "red"}}><XCircleFillIcon/> </span>
                                        }
                                        &nbsp;{hook.action} / {hook.hook_id}
                                    </Accordion.Toggle>
                                </Card.Header>
                                <Accordion.Collapse
                                    eventKey={hook.hook_run_id}
                                    onEnter={() => history.replace(`/repositories/${repo.id}/actions/${runId}/${hook.hook_run_id}`)}
                                    onExit={() => history.replace(`/repositories/${repo.id}/actions/${runId}`)}
                                >
                                    <Card.Body>
                                        <Card.Text>
                                            <strong>Status:</strong> <strong style={{'color': (hook.status === 'completed') ? 'green':'red'}}>{hook.status}</strong><br/>
                                            <strong>Action:</strong> {hook.action}<br/>
                                            <strong>Hook ID:</strong> {hook.hook_id}<br/>
                                            <strong>Start time:</strong> {moment(hook.start_time).format("MM/DD/YYYY HH:mm:ss")}<br/>
                                            <strong>End time:</strong> {moment(hook.end_time).format("MM/DD/YYYY HH:mm:ss")}<br/>
                                            <strong>Hook Run ID:</strong> {hook.hook_run_id}<br/>
                                            <hr/>
                                        </Card.Text>
                                        {runHookOutput.error
                                            ? <Alert variant="warning">Failed to load hook output</Alert>
                                            : runHookOutput.inProgress
                                                ? <p>Loading...</p>
                                                : runHookOutput.done
                                                    ? <pre>{runHookOutput.payload}</pre>
                                                    : ''
                                        }
                                    </Card.Body>
                                </Accordion.Collapse>
                            </Card>
                        );
                    })}
                </Accordion>
            </Container>
        );
    });


const RunDetails = ({ runDetails }) => {
    return (
        <Card border={runDetails.status !== 'completed' && 'danger'}>
            <Card.Header>
                <strong>Run ID:</strong> {runDetails.run_id}<br/>
                <strong>Branch:</strong> {runDetails.branch}<br/>
            </Card.Header>
            <Card.Body>
                <strong>Status:</strong> <strong style={{'color': (runDetails.status === 'completed') ? 'green':'red'}}>{runDetails.status}</strong><br/>
                <strong>Event:</strong> {runDetails.event_type}<br/>
                <strong>Start Time:</strong> {runDetails.start_time}<br/>
                <strong>End Time:</strong> {runDetails.end_time}<br/>
                {runDetails.commit_id && <><strong>Commit ID:</strong> {runDetails.commit_id}<br/></>}
            </Card.Body>
        </Card>
    )
};
