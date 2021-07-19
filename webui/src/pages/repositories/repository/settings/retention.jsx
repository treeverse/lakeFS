import React, {useState} from "react";
import {RepositoryPageLayout} from "../../../../lib/components/repository/layout";
import {
    ActionGroup,
    ActionsBar,
    Error,
    Loading,
    RefreshButton,
    ToggleSwitch
} from "../../../../lib/components/controls";
import Button from "react-bootstrap/Button";
import {NotFoundError, retention} from "../../../../lib/api";
import {useAPI} from "../../../../lib/hooks/api";
import {RefContextProvider, useRefs} from "../../../../lib/hooks/repo";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import {SettingsLayout} from "./layout";
import {Container, Row} from "react-bootstrap";
import Col from "react-bootstrap/Col";
import {PolicyEditor} from "../../../../lib/components/policy";

const exampleJson = (defaultBranch) => {
    return {
        "default_retention_days": 21,
        "branches":
            [
                {"branch_id": defaultBranch, "retention_days": 28},
            ]
    }
}

const GetStarted = ({repo, onSubmit}) => {
    const [showCreate, setShowCreate] = useState(false);
    return (
        <>
            <Container className="m-4 mb-5">
                <h2 className="mt-2">No Garbage collection policy found</h2>
                <br/>
                This policy determines for how long objects are kept in the storage after they are deleted in lakeFS.
                <br/>
                <Row>
                    <Col md={{span: 5, offset: 3}}>
                        <br/>
                        <Button variant="success" onClick={() => setShowCreate(true)}>Create your first policy</Button>
                    </Col>
                </Row>
                <br/>
                <Row className="pt-2 ml-2">
                    For detailed information about garbage collection see the&nbsp;<a
                    href="https://docs.lakefs.io/reference/garbage-collection.html" target="_blank"
                    rel="noopener noreferrer">docs</a>
                </Row>
            </Container>
            <PolicyEditor
                noID
                isCreate
                policy={exampleJson(repo.default_branch)}
                onSubmit={onSubmit}
                onHide={() => setShowCreate(false)}
                show={showCreate}
            />
        </>
    )
}

const GCPolicy = ({repo}) => {
    const [refresh, setRefresh] = useState(true);
    const [jsonView, setJsonView] = useState(false);
    const [showCreate, setShowCreate] = useState(false);

    const {response, error, loading} = useAPI(async () => {
        return await retention.getGCPolicy(repo.id)
    }, [repo, refresh])
    const doRefresh = () => setRefresh(!refresh);
    const onSubmit = (policy) => {

        return retention.setGCPolicy(repo.id, policy).then(() => {
            setShowCreate(false);
            setRefresh(!refresh);
        })
    };
    const policy = response
    let content;

    if (loading) content = <Loading/>;
    else if (!!error) content = error instanceof NotFoundError ? <GetStarted repo={repo} onSubmit={onSubmit}/> :  <Error error={error}/>;
    else {
        content = (
                    <>
                        <ActionsBar>
                            <ActionGroup><h4>Garbage Collection Policy</h4></ActionGroup>
                            <ActionGroup orientation="right">
                                <RefreshButton onClick={doRefresh}/>
                                <Button variant="success" onClick={() => setShowCreate(true)}>Edit Policy</Button>
                            </ActionGroup>
                        </ActionsBar>
                        <ActionsBar>
                            <ActionGroup orientation="right">
                                <ToggleSwitch label={"JSON view"} id={"policy-json-switch"} onChange={setJsonView}
                                              defaultChecked={jsonView}/>
                            </ActionGroup>
                        </ActionsBar>
                        {jsonView ? (
                            <pre className={"policy-body"}>{JSON.stringify(policy, null, 4)}</pre>
                        ) : (
                            <>
                                <Table borderless>
                                    <tbody>
                                    <tr key={'branch-default'}>
                                        <td><code>Default retention days: {policy.default_retention_days}</code></td>
                                    </tr>
                                    </tbody>
                                </Table>
                                <Card>
                                    {policy.branches && <Table>
                                        <thead>
                                        <tr>
                                            <th width={"80%"}>Branch</th>
                                            <th>Retention Days</th>
                                        </tr>
                                        </thead>
                                        <tbody>
                                        {policy.branches.map((branch, i) => {
                                            return (
                                                <tr key={`branch-${i}`}>
                                                    <td><code>{branch.branch_id}</code></td>
                                                    <td><code>{branch.retention_days}</code></td>
                                                </tr>
                                            );
                                        })}
                                        </tbody>
                                    </Table>}
                                </Card>
                                <br/>
                                <br/>
                                <Row className="pt-2 ml-2">
                                    For information about how to run the garbage collection see the&nbsp;<a
                                    href="https://docs.lakefs.io/reference/garbage-collection.html" target="_blank"
                                    rel="noopener noreferrer">docs</a>.
                                </Row>
                            </>
                        )}

                        <PolicyEditor
                            policy={policy}
                            onSubmit={onSubmit}
                            onHide={() => setShowCreate(false)}
                            show={showCreate}
                        />
                    </>
                );
    }
    return (
        <div className="mb-5">
            <ActionsBar>
            </ActionsBar>
            {content}
        </div>
    );
};


const RetentionContainer = () => {
    const {repo, loading, error} = useRefs();
    if (loading) return <Loading/>;
    if (!!error) return <Error error={error}/>;

    return (
        <GCPolicy repo={repo}/>
    );
}

const RepositoryRetentionPage = () => {
    return (
        <RefContextProvider>
            <RepositoryPageLayout activePage={'settings'}>
                <SettingsLayout activeTab={"retention"}>
                    <RetentionContainer/>
                </SettingsLayout>
            </RepositoryPageLayout>
        </RefContextProvider>
    );
};

export default RepositoryRetentionPage;
