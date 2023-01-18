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
import {PolicyEditor} from "../../../../lib/components/policy";
import Alert from "react-bootstrap/Alert";

const exampleJson = (defaultBranch) => {
    return {
        "default_retention_days": 21,
        "branches":
            [
                {"branch_id": defaultBranch, "retention_days": 28},
            ]
    }
}

const GCPolicy = ({repo}) => {
    const [refresh, setRefresh] = useState(true);
    const [jsonView, setJsonView] = useState(false);
    const [showCreate, setShowCreate] = useState(false);
    const [isActionsDisabled, setIsActionsDisabled] = useState(false);

    const {response, error, loading} = useAPI(async () => {
        return await retention.getGCPolicy(repo.id)
    }, [repo, refresh])

    const doRefresh = () => setRefresh(!refresh);
    const onDelete = async () => {
        setIsActionsDisabled(true);
        try {
            await retention.deleteGCPolicy(repo.id);
        }
        catch (err) {
            setIsActionsDisabled(false);
            throw err;
        }
        setRefresh(!refresh);
        setIsActionsDisabled(false);
    }

    const onSubmit = async (policy) => {
        setIsActionsDisabled(true);
        try {
            await retention.setGCPolicy(repo.id, policy);
        }
        catch (err) {
            setIsActionsDisabled(false);
            throw err;
        }
        setShowCreate(false);
        setRefresh(!refresh);
        setIsActionsDisabled(false);
    };

    const jsonToggleBar = <ActionsBar>
        <ActionGroup orientation="right">
            <ToggleSwitch label={"JSON view"} id={"policy-json-switch"} onChange={setJsonView}
                          defaultChecked={jsonView}/>
        </ActionGroup>
    </ActionsBar>
    const isPolicyNotSet = error && error instanceof NotFoundError
    const policy = response
    let content;
    if (loading) {
        content = <Loading/>;
    } else if (error) {
        content = isPolicyNotSet ? <Alert variant="info" className={"mt-3"}>A garbage collection policy is not set yet.</Alert> :  <Error error={error}/>;
    } else if (jsonView) {
        content = <>
            <pre className={"policy-body"}>{JSON.stringify(policy, null, 4)}</pre>
            {jsonToggleBar}
            </>
    } else {
        content = <>
            <Table borderless>
                <tbody>
                <tr key={'branch-default'}>
                    <td><code>Default retention days: {policy.default_retention_days}</code></td>
                </tr>
                </tbody>
            </Table>
            <Card className={"mb-3"}>
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
            {jsonToggleBar}
        </>
    }
    let editorProps = {
        policy: policy
    };
    if (!!error  && error instanceof NotFoundError) {
        editorProps = {
            noID: true,
            isCreate: true,
            policy: exampleJson(repo.default_branch),
        }
    }
    return <div className="mt-3 mb-5">
        <div className={"section-title"}>
            <h4 className={"mb-0"}>
                <div className={"ms-1 me-1 pl-0 d-flex"}>
                    <div className="flex-grow-1">Garbage collection policy</div>
                    <RefreshButton className={"ms-1"} onClick={doRefresh}/>
                    {!error && !loading && !isPolicyNotSet &&
                        <Button className={"ms-2 btn-secondary"} disabled={isActionsDisabled} onClick={onDelete}>Delete
                            Policy</Button>}
                    <Button className={"ms-2"} disabled={isActionsDisabled} onClick={() => setShowCreate(true)}>Edit Policy</Button>
                </div>
            </h4>
        </div>
        <div>
            {/* eslint-disable-next-line react/jsx-no-target-blank */}
            This policy determines for how long objects are kept in the storage after they are deleted in lakeFS. <a
            href="https://docs.lakefs.io/reference/garbage-collection.html" target="_blank">Learn more.</a>
        </div>
        <div className={"mt-3"}>
            {content}
        </div>
        <PolicyEditor
            onSubmit={onSubmit}
            onHide={() => setShowCreate(false)}
            show={showCreate}
            {...editorProps}
        />
    </div>
};


const RetentionContainer = () => {
    const {repo, loading, error} = useRefs();
    if (loading) return <Loading/>;
    if (error) return <Error error={error}/>;

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
