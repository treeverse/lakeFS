import React, {useEffect, useState} from "react";

import Button from "react-bootstrap/Button";

import {AuthLayout} from "../../../../lib/components/auth/layout";
import {GroupHeader} from "../../../../lib/components/auth/nav";
import {useAPIWithPagination} from "../../../../lib/hooks/api";
import {auth} from "../../../../lib/api";
import {Paginator} from "../../../../lib/components/pagination";
import {AttachModal} from "../../../../lib/components/auth/forms";
import {ConfirmationButton} from "../../../../lib/components/modals";
import {
    ActionGroup,
    ActionsBar,
    DataTable,
    FormattedDate,
    Loading,
    Error,
    RefreshButton
} from "../../../../lib/components/controls";
import {Link} from "../../../../lib/components/nav";
import {useRouter} from "../../../../lib/hooks/router";


const GroupPoliciesList = ({ groupId, after, onPaginate }) => {
    const [refresh, setRefresh] = useState(false);
    const [showAddModal, setShowAddModal] = useState(false);
    const [attachError, setAttachError] = useState(null);

    const {results, loading, error, nextPage} = useAPIWithPagination(() => {
        return auth.listGroupPolicies(groupId, after);
    }, [groupId, after, refresh]);

    useEffect(() => { setAttachError(null); }, [refresh, after])

    let content;
    if (loading) content = <Loading/>;
    else if (!!error) content=  <Error error={error}/>;
    else content = (
            <>
                {attachError && <Error error={attachError}/>}

                <DataTable
                    keyFn={policy => policy.id}
                    rowFn={policy => [
                        <Link href={{pathname: '/auth/policies/:policyId', params: {policyId: policy.id}}}>{policy.id}</Link>,
                        <FormattedDate dateValue={policy.creation_date}/>
                    ]}
                    headers={['Policy ID', 'Created At']}
                    actions={[{
                        key: 'Detach',
                        buttonFn: policy => <ConfirmationButton
                            size="sm"
                            variant="outline-danger"
                            modalVariant="danger"
                            msg={<span>Are you sure you{'\''}d like to detach policy <strong>{policy.id}</strong>?</span>}
                            onConfirm={() => {
                                auth.detachPolicyFromGroup(groupId, policy.id)
                                    .catch(error => alert(error))
                                    .then(() => { setRefresh(!refresh) })
                            }}>
                            Remove
                        </ConfirmationButton>
                    }]}
                    results={results}
                    emptyState={'No policies found'}
                />

                <Paginator onPaginate={onPaginate} after={after} nextPage={nextPage}/>


                {showAddModal && <AttachModal
                    show={showAddModal}
                    emptyState={'No policies found'}
                    filterPlaceholder={'Find Policy...'}
                    modalTitle={'Attach Policies'}
                    addText={'Attach Policies'}
                    searchFn={prefix => auth.listPolicies(prefix, "", 5).then(res => res.results)}
                    onHide={() => setShowAddModal(false)}
                    onAttach={(selected) => {
                        Promise.all(selected.map(policyId => auth.attachPolicyToGroup(groupId, policyId)))
                            .then(() => { setRefresh(!refresh); setAttachError(null) })
                            .catch(error => { setAttachError(error) })
                            .finally(() => { setShowAddModal(false) })
                    }}/>
                }
            </>
        );

    return (
        <>
            <GroupHeader groupId={groupId} page={'policies'}/>

            <ActionsBar>
                <ActionGroup orientation="left">
                    <Button variant="success" onClick={() => setShowAddModal(true)}>Attach Policy</Button>
                </ActionGroup>

                <ActionGroup orientation="right">
                    <RefreshButton onClick={() => setRefresh(!refresh)}/>
                </ActionGroup>
            </ActionsBar>

            <div className="mt-2">
                {content}
            </div>
        </>
    );
};

const GroupPoliciesContainer = () => {
    const router = useRouter();
    const { after } = router.query;
    const { groupId } = router.params;
    return (!groupId) ? <></> : <GroupPoliciesList
        groupId={groupId}
        after={(!!after) ? after : ""}
        onPaginate={after => router.push({pathname: '/auth/groups/:groupId/policies', params: {groupId}, query: {after}})}
    />;
};

const GroupPoliciesPage = () => {
    return (
        <AuthLayout activeTab="groups">
            <GroupPoliciesContainer/>
        </AuthLayout>
    );
};

export default GroupPoliciesPage;