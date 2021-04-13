import {AuthLayout} from "../../../lib/components/auth/layout";
import {useRouter} from "next/router";
import {PolicyHeader} from "../../../lib/components/auth/nav";
import {
    ActionGroup,
    ActionsBar,
    Loading,
    Error,
} from "../../../lib/components/controls";
import Button from "react-bootstrap/Button";
import {useAPI} from "../../../rest/hooks";
import {auth} from "../../../rest/api";
import {useState} from "react";
import {CopyIcon, PencilIcon} from "@primer/octicons-react";
import Form from "react-bootstrap/Form";
import {PolicyDisplay, PolicyEditor} from "../../../lib/components/auth/policy";


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
    else if (!!error) content=  <Error error={error}/>;
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
    const { policyId } = router.query;
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