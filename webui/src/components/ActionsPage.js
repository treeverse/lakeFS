import {connect} from "react-redux";
import {listActionsRuns, listActionsRunHooks, getActionsRun, getActionsRunHookOutput} from '../actions/actions';
import {Link, useHistory, useParams} from "react-router-dom";
import React, {useCallback, useEffect, useState} from "react";
import {SyncIcon} from "@primer/octicons-react";
import {PaginatedEntryList} from "./auth/entities";
import * as moment from "moment";
import {Alert, Container, Breadcrumb, Row, Col, Card, Button, Form, Nav, ButtonToolbar} from "react-bootstrap";
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
                                (<Button variant="link" href={`/repositories/${repo.id}/tree?branch=${entity.branch}`}>
                                    {entity.branch}
                                </Button>),
                                moment(entity.start_time).format("MM/DD/YYYY HH:mm:ss"),
                                moment(entity.end_time).format("MM/DD/YYYY HH:mm:ss"),
                                entity.commit_id && (<>
                                    <Button variant="link" href={`/repositories/${repo.id}/tree?commit=${entity.commit_id}`}>
                                        {entity.commit_id.substr(0, 16)}
                                    </Button>
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

const HooksPage = ({ repo, runId, hookRunId, runHooks }) => {
        const history = useHistory();

        const onSelect = (key, e) => {
            e.preventDefault();
            const href = `/repositories/${repo.id}/actions/${runId}/${key}`;
            history.push(href);
        };
        return (
            <Card>
                <Card.Header>
                    Hooks
                </Card.Header>
                <Card.Body>
                    <Nav variant="pills" className="flex-column" defaultActiveKey={hookRunId}>
                        {runHooks.payload.results.map((hook, number) => {
                            return (
                                <Nav.Item key={'hook_'+number}>
                                    <Nav.Link eventKey={hook.hook_run_id} onSelect={onSelect}>
                                        {hook.status === "completed"
                                            ? <span style={{color: "green"}}><CheckCircleFillIcon/> </span>
                                            : <span style={{color: "red"}}><XCircleFillIcon/> </span>
                                        }
                                        {hook.action} {hook.hook_id}
                                    </Nav.Link>
                                </Nav.Item>
                            );
                        })}
                    </Nav>
                </Card.Body>
            </Card>
        );
    };

const HookPage = connect(
    ({ actions }) => ({
        run: actions.run,
        runHooks: actions.runHooks,
        runHookOutput: actions.runHookOutput,
    }),
    ({ listActionsRunHooks, getActionsRun, getActionsRunHookOutput })
)(
    ({repo, runId, hookRunId, runHooks, runHookOutput, run, getActionsRun, getActionsRunHookOutput }) => {
        useEffect(() => {
            if (hookRunId) {
                getActionsRunHookOutput(repo.id, runId, hookRunId);
            } else {
                getActionsRun(repo.id, runId);
            }
        },[getActionsRunHookOutput, getActionsRun, repo.id, runId, hookRunId]);

        if (!hookRunId) {
            // Run Summary
            if (run.error) {
                return <Alert variant={"danger"}>Action run information not found!</Alert>;
            }
            if (run.loading) {
                return <p>Loading...</p>;
            }
            return (
                <Card border={run.payload.status !== 'completed' && 'danger'}>
                    <Card.Header>
                        <strong>Run ID:</strong> {run.payload.run_id}<br/>
                        <strong>Branch:</strong> {run.payload.branch}<br/>
                    </Card.Header>
                    <Card.Body>
                        <strong>Status:</strong> <strong style={{'color': (run.payload.status === 'completed') ? 'green':'red'}}>{run.payload.status}</strong><br/>
                        <strong>Event:</strong> {run.payload.event_type}<br/>
                        <strong>Start Time:</strong> {run.payload.start_time}<br/>
                        <strong>End Time:</strong> {run.payload.end_time}<br/>
                        {run.payload.commit_id && <><strong>Commit ID:</strong> {run.payload.commit_id}<br/></>}
                    </Card.Body>
                </Card>
            )
        }

        // Hook information
        const hook = runHooks.payload.results.find(x => x.hook_run_id === hookRunId);
        if (!hook) {
            return <Alert variant={"danger"}>Hook information not found!</Alert>;
        }

        if (runHookOutput.loading) {
            return <p>Loading...</p>;
        }

        if (runHookOutput.error) {
            return <Alert variant={"danger"}>{runHookOutput.error}</Alert>;
        }
        const completed = hook.status === "completed";
        return (
            <Card border={!completed && 'danger'}>
                <Card.Header>
                    <strong>Hook Run ID:</strong> {hook.hook_run_id}<br/>
                    <strong>Action:</strong> {hook.action}<br/>
                    <strong>Hook ID:</strong> {hook.hook_id}<br/>
                    <strong>Start Time:</strong> {moment(hook.start_time).format("MM/DD/YYYY HH:mm:ss")}<br/>
                    <strong>End Time:</strong> {moment(hook.end_time).format("MM/DD/YYYY HH:mm:ss")}<br/>
                    <strong>Status:</strong> <strong style={{'color': completed ? 'green':'red'}}>{hook.status}</strong>
                </Card.Header>
                <Card.Body>
                    <pre>{runHookOutput.payload}</pre>
                </Card.Body>
            </Card>
        );
    });

export const ActionsRunPage = connect(
    ({ actions }) => ({
        runHooks: actions.runHooks,
    }),
    ({ listActionsRunHooks })
)(
    ({repo, runHooks, listActionsRunHooks }) => {
        const {runId, hookRunId} = useParams();

        useEffect(() => {
            listActionsRunHooks(repo.id, runId, '', -1);
        },[listActionsRunHooks, repo.id, runId]);

        if (runHooks.loading) {
            return <p>Loading...</p>;
        }

        if (runHooks.error) {
            return <Alert variant={"danger"}>{runHooks.error}</Alert>;
        }

        if (runHooks.payload.pagination.results === 0) {
            return <Alert variant="warning">No hooks</Alert>;
        }

        const hrefRunId = `/repositories/${repo.id}/actions/${runId}`;
        const hook = runHooks.payload.results.find(x => x.hook_run_id === hookRunId);
        if (hookRunId && !hook) {
            return <Alert variant="warning">
                Can't find hook run ID '{hookRunId}' for run ID <Alert.Link href={hrefRunId}>{runId}</Alert.Link>
            </Alert>;
        }

        return (
            <Container className="mt-3">
                <Breadcrumb>
                    <Breadcrumb.Item href={`/repositories/${repo.id}/actions`}>Actions</Breadcrumb.Item>
                    <Breadcrumb.Item active={!hookRunId} href={`/repositories/${repo.id}/actions/${runId}`}>{runId}</Breadcrumb.Item>
                    {hookRunId &&
                    <Breadcrumb.Item active={!!hookRunId} href={hrefRunId+'/'+hookRunId}>{hook.action} / {hook.hook_id}</Breadcrumb.Item>
                    }
                </Breadcrumb>
                <Row>
                    <Col md={3}>
                        <HooksPage repo={repo} runId={runId} hookRunId={hookRunId} runHooks={runHooks}/>
                    </Col>
                    <Col md={9}>
                        <HookPage repo={repo} runId={runId} hookRunId={hookRunId} runHooks={runHooks}/>
                    </Col>
                </Row>
            </Container>
        );
    });
