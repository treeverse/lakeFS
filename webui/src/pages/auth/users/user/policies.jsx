import React from "react";

import {AuthLayout} from "../../../../lib/components/auth/layout";
import {UserHeaderWithContext} from "./userHeaderWithContext";
import {
    ActionGroup,
    ActionsBar,
    DataTable,
    FormattedDate,
    Loading,
    AlertError,
    RefreshButton
} from "../../../../lib/components/controls";
import Button from "react-bootstrap/Button";
import {useAPIWithPagination} from "../../../../lib/hooks/api";
import {auth} from "../../../../lib/api";
import {Paginator} from "../../../../lib/components/pagination";
import {useState} from "react";
import {AttachModal} from "../../../../lib/components/auth/forms";
import {ConfirmationButton} from "../../../../lib/components/modals";
import {Link} from "../../../../lib/components/nav";
import {useRouter} from "../../../../lib/hooks/router";


const UserPoliciesList = ({ userId, after, onPaginate }) => {
    const [refresh, setRefresh] = useState(false);
    const [showAddModal, setShowAddModal] = useState(false);
    const [attachError, setAttachError] = useState(null);

    const {results, loading, error, nextPage} = useAPIWithPagination(() => {
        return auth.listUserPolicies(userId, false, after);
    }, [userId, after, refresh]);

    let content;
    if (loading) content = <Loading/>;
    else if (error) content=  <AlertError error={error}/>;
    else content = (
            <>
                {attachError && <AlertError error={attachError}/>}
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
                                auth.detachPolicyFromUser(userId, policy.id)
                                    .catch(error => alert(error))
                                    .then(() => { setRefresh(!refresh) })
                            }}>
                            Detach
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
                        Promise.all(selected.map(policyId => auth.attachPolicyToUser(userId, policyId)))
                            .then(() => { setRefresh(!refresh); setAttachError(null) })
                            .catch(error => { setAttachError(error) })
                            .finally(() => { setShowAddModal(false) });
                    }}/>
                }
            </>
        )

    return (
        <>
            <UserHeaderWithContext userId={userId} page={'policies'}/>

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
}

const UserPoliciesContainer = () => {
    const router = useRouter();
    const { after } = router.query;
    const { userId } = router.params;
    return (!userId) ? <></> : <UserPoliciesList
        userId={userId}
        after={(after) ? after : ""}
        onPaginate={after => router.push({pathname: '/auth/users/:userId/policies', params: {userId}, query: {after}})}
    />;
};

const UserPoliciesPage = () => {
    return (
        <AuthLayout activeTab="users">
            <UserPoliciesContainer />
        </AuthLayout>
    );
};

export default UserPoliciesPage;
