import React, { useEffect, useState } from "react";
import { useOutletContext } from "react-router-dom";
import Button from "react-bootstrap/Button";

import { auth } from "../../../lib/api";
import { useAPIWithPagination } from "../../../lib/hooks/api";
import { ConfirmationButton } from "../../../lib/components/modals";
import { Paginator } from "../../../lib/components/pagination";
import { PolicyEditor } from "../../../lib/components/policy";
import {
    ActionGroup,
    ActionsBar,
    Checkbox,
    DataTable,
    AlertError,
    FormattedDate,
    Loading,
    RefreshButton,
    Warning,
    useDebouncedState,
    SearchInput
} from "../../../lib/components/controls";
import { useRouter } from "../../../lib/hooks/router";
import { useLoginConfigContext } from "../../../lib/hooks/conf";
import { Link } from "../../../lib/components/nav";
import { disallowPercentSign, INVALID_POLICY_ID_ERROR_MESSAGE } from "../validation";


const PoliciesContainer = () => {
    const [selected, setSelected] = useState([]);
    const [deleteError, setDeleteError] = useState(null);
    const [showCreate, setShowCreate] = useState(false);
    const [refresh, setRefresh] = useState(false);
    const [createModalError, setCreateModalError] = useState(null);

    const router = useRouter();
    const prefix = router.query.prefix ? router.query.prefix : "";
    const after = router.query.after ? router.query.after : "";

    const [searchPrefix, setSearchPrefix] = useDebouncedState(
        prefix,
        search => router.push({ pathname: '/auth/policies', query: { prefix: search } })
    );

    const { results, loading, error, nextPage } = useAPIWithPagination(() => {
        return auth.listPolicies(prefix, after);
    }, [refresh, prefix, after]);

    useEffect(() => { setSelected([]); }, [after, refresh]);

    const { RBAC: rbac } = useLoginConfigContext();

    if (error) return <AlertError error={error} />;
    if (loading) return <Loading />;

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
                    <SearchInput
                        searchPrefix={searchPrefix}
                        setSearchPrefix={setSearchPrefix}
                        placeholder="Find a Policy..."
                    />
                    <RefreshButton onClick={() => setRefresh(!refresh)} />
                </ActionGroup>
            </ActionsBar>
            {rbac === 'simplified' && <Warning>
                <b>Deprecation Notice:</b> RBAC (Role-Based Access Control) is being deprecated
                and will be replaced by ACL (Access Control Lists) in future releases.
                For more information on the transition from RBAC to ACL, please visit
                our <a href="https://docs.lakefs.io/posts/security_update.html">documentation page</a>.
            </Warning>}
            <div className="auth-learn-more">
                A policy defines the permissions of a user or a group. <a href="https://docs.lakefs.io/reference/authorization.html#authorization" target="_blank" rel="noopener noreferrer">Learn more.</a>
            </div>

            {(!!deleteError) && <AlertError error={deleteError} />}

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
                keyFn={policy => policy.id}
                rowFn={policy => [
                    <Checkbox
                        key={policy.id}
                        name={policy.id}
                        onAdd={() => setSelected([...selected, policy])}
                        onRemove={() => setSelected(selected.filter(p => p !== policy))}
                        defaultChecked={selected.some(selectedPolicy => selectedPolicy.id === policy.id)}
                    />,
                    <Link key={policy.id} href={{ pathname: '/auth/policies/:policyId', params: { policyId: policy.id } }}>
                        {policy.id}
                    </Link>,
                    <FormattedDate key={policy.id} dateValue={policy.creation_date} />
                ]}
                headers={['', 'Policy ID', 'Created At']}
                firstFixedCol={true}
            />

            <Paginator
                nextPage={nextPage}
                after={after}
                onPaginate={after => router.push({ pathname: "/auth/policies", query: { prefix, after } })}
            />
        </>
    );
};


const PoliciesPage = () => {
    const [setActiveTab] = useOutletContext();
    useEffect(() => setActiveTab("policies"), [setActiveTab]);
    return <PoliciesContainer />;
};

export default PoliciesPage;
