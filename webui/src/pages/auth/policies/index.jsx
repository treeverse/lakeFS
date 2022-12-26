import React, {useEffect, useState} from "react";

import Button from "react-bootstrap/Button";

import {auth} from "../../../lib/api";
import {useAPIWithPagination} from "../../../lib/hooks/api";
import {AuthLayout} from "../../../lib/components/auth/layout";
import {ConfirmationButton} from "../../../lib/components/modals";
import {Paginator} from "../../../lib/components/pagination";
import {PolicyEditor} from "../../../lib/components/policy";
import {
    ActionGroup,
    ActionsBar,
    Checkbox,
    DataTable,
    Error, FormattedDate,
    Loading,
    RefreshButton
} from "../../../lib/components/controls";
import {useRouter} from "../../../lib/hooks/router";
import {Link} from "../../../lib/components/nav";
import {Route, Routes} from "react-router-dom";
import PolicyPage from "./policy";
import { disallowPercentSign, INVALID_POLICY_ID_ERROR_MESSAGE } from "../validation";


const PoliciesContainer = () => {
    const [selected, setSelected] = useState([]);
    const [deleteError, setDeleteError] = useState(null);
    const [showCreate, setShowCreate] = useState(false);
    const [refresh, setRefresh] = useState(false);
    const [createModalError, setCreateModalError] = useState(null);

    const router = useRouter();
    const after = (router.query.after) ? router.query.after : "";
    const { results, loading, error, nextPage } =  useAPIWithPagination(() => {
        return auth.listPolicies("", after);
    }, [after, refresh]);

    useEffect(() => { setSelected([]); }, [after, refresh]);

    if (error) return <Error error={error}/>;
    if (loading) return <Loading/>;

    return (
        <>
            <ActionsBar>
                <ActionGroup orientation="left">
                    <Button
                        variant="success"
                        onClick={() => setShowCreate(true)}>
                        Create Policy
                    </Button>

                    <ConfirmationButton
                        onConfirm={() => {
                            auth.deletePolicies(selected.map(p => p.id))
                                .catch(err => setDeleteError(err))
                                .then(() => {
                                    setSelected([]);
                                    setRefresh(!refresh);
                                });
                        }}
                        disabled={(selected.length === 0)}
                        variant="danger"
                        msg={`Are you sure you'd like to delete ${selected.length} policies?`}>
                        Delete Selected
                    </ConfirmationButton>
                </ActionGroup>
                <ActionGroup orientation="right">
                    <RefreshButton onClick={() => setRefresh(!refresh)}/>
                </ActionGroup>
            </ActionsBar>
            <div className="auth-learn-more">
                A policy defines the permissions of a user or a group. <a href="https://docs.lakefs.io/reference/authorization.html#authorization" target="_blank" rel="noopener noreferrer">Learn more.</a>
            </div>

            {(!!deleteError) && <Error error={deleteError}/>}

            <PolicyEditor
                onSubmit={(policyId, policyBody) => {
                    return auth.createPolicy(policyId, policyBody).then(() => {
                        setSelected([]);
                        setCreateModalError(null);
                        setShowCreate(false);
                        setRefresh(!refresh);
                    }).catch((err) => {
                        setCreateModalError(err.message);
                    })
                }}
                onHide={() => {
                    setCreateModalError(null);
                    setShowCreate(false)
                }}
                show={showCreate}
                validationFunction={disallowPercentSign(INVALID_POLICY_ID_ERROR_MESSAGE)}
                externalError={createModalError}
            />

            <DataTable
                results={results}
                headers={['', 'Policy ID', 'Created At']}
                keyFn={policy => policy.id}
                rowFn={policy => [
                    <Checkbox
                        name={policy.id}
                        onAdd={() => setSelected([...selected, policy])}
                        onRemove={() => setSelected(selected.filter(p => p !== policy))}
                    />,
                    <Link href={{pathname: '/auth/policies/:policyId', params: {policyId: policy.id}}}>
                        {policy.id}
                    </Link>,
                    <FormattedDate dateValue={policy.creation_date}/>
                ]}/>

            <Paginator
                nextPage={nextPage}
                after={after}
                onPaginate={after => router.push({pathname: '/auth/policies', query: {after}})}
            />
        </>
    );
};


const PoliciesPage = () => {
    return (
        <AuthLayout activeTab="policies">
            <PoliciesContainer/>
        </AuthLayout>
    );
};

const PoliciesIndexPage = () => {
    return (
        <Routes>
            <Route exact path="" element={<PoliciesPage/>} />
            <Route path=":policyId" element={<PolicyPage/>} />
        </Routes>
    )
}

export default PoliciesIndexPage;
