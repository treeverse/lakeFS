import React, {useState} from "react";

import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import {PencilIcon} from "@primer/octicons-react";

import {AuthLayout} from "../../../lib/components/auth/layout";
import {PolicyHeader} from "../../../lib/components/auth/nav";
import {useAPI} from "../../../lib/hooks/api";
import {auth} from "../../../lib/api";
import {PolicyDisplay, PolicyEditor} from "../../../lib/components/policy";
import {
    ActionGroup,
    ActionsBar,
    Loading,
    AlertError,
} from "../../../lib/components/controls";
import {useRouter} from "../../../lib/hooks/router";


const PolicyView = ({ policyId }) => {
    const [jsonView, setJsonView] = useState(false);
    const [showEditor, setShowEditor] = useState(false);
    const [refresh, setRefresh] = useState(false);

    const {response, loading, error} = useAPI(() => {
        return auth.getPolicy(policyId);
    }, [policyId, refresh]);

    const policy = response;

    let content;
    if (loading) content = <Loading/>;
    else if (error) content=  <AlertError error={error}/>;
    else content = (
        <PolicyDisplay policy={policy} asJSON={jsonView}/>
    );

    return (
        <>
            <PolicyHeader policyId={policyId}/>

            <ActionsBar>
                <ActionGroup orientation="left">
                    <Button variant="primary" onClick={() => setShowEditor(true)}>
                        <PencilIcon/> Edit
                    </Button>
                </ActionGroup>
                <ActionGroup orientation="right">
                    <Form>
                    <Form.Switch
                        label="JSON View"
                        id="policy-json-switch"
                        onChange={e => setJsonView(e.target.checked)}
                    />
                    </Form>
                </ActionGroup>
            </ActionsBar>

            <div className="mt-2">
                {content}
            </div>

            <PolicyEditor
                policy={policy}
                show={showEditor}
                onSubmit={(policyBody) => {
                    return auth.editPolicy(policyId, policyBody).then(() => {
                        setShowEditor(false);
                        setRefresh(!refresh);
                    });
                }}
                onHide={() => { setShowEditor(false) }}
            />
        </>
    );
}


const PolicyContainer = () => {
    const router = useRouter();
    const { policyId } = router.params;
    return (!policyId) ? <></> : <PolicyView policyId={policyId}/>;
}

const PolicyPage = () => {
    return (
        <AuthLayout activeTab="policies">
            <PolicyContainer/>
        </AuthLayout>
    );
}

export default PolicyPage;
