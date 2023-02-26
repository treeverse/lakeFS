import React from "react";

import {AuthLayout} from "../../../../lib/components/auth/layout";
import {UserHeaderWithContext} from "./userHeaderWithContext";
import {
    ActionGroup,
    ActionsBar,
    DataTable,
    FormattedDate,
    Loading,
    Error,
    RefreshButton
} from "../../../../lib/components/controls";
import Button from "react-bootstrap/Button";
import {useAPIWithPagination} from "../../../../lib/hooks/api";
import {auth} from "../../../../lib/api";
import {Paginator} from "../../../../lib/components/pagination";
import {useState} from "react";
import {AttachModal} from "../../../../lib/components/auth/forms";
import {ConfirmationButton} from "../../../../lib/components/modals";
import {useRouter} from "../../../../lib/hooks/router";
import {Link} from "../../../../lib/components/nav";


const UserGroupsList = ({ userId, after, onPaginate }) => {
    const [refresh, setRefresh] = useState(false);
    const [showAddModal, setShowAddModal] = useState(false);
    const [attachError, setAttachError] = useState(null);

    const {results, loading, error, nextPage} = useAPIWithPagination(() => {
        return auth.listUserGroups(userId, after);
    }, [userId, after, refresh]);

    let content;
    if (loading) content = <Loading/>;
    else if (error) content=  <Error error={error}/>;
    else content = (
        <>
            {attachError && <Error error={attachError}/>}

            <DataTable
                keyFn={group => group.id}
                rowFn={group => [
                    <Link href={{pathname: '/auth/groups/:groupId', params: {groupId: group.id}}}>{group.id}</Link>,
                    <FormattedDate dateValue={group.creation_date}/>
                ]}
                headers={['Group ID', 'Created At']}
                actions={[{
                    key: 'Remove',
                    buttonFn: group => <ConfirmationButton
                        size="sm"
                        variant="outline-danger"
                        msg={<span>Are you sure you{'\''}d like to remove user <strong>{userId}</strong> from group <strong>{group.id}</strong>?</span>}
                        onConfirm={() => {
                            auth.removeUserFromGroup(userId, group.id)
                                .catch(error => alert(error))
                                .then(() => { setRefresh(!refresh) })
                        }}>
                        Remove
                    </ConfirmationButton>
                }]}
                results={results}
                emptyState={'No groups found'}
            />

            <Paginator onPaginate={onPaginate} after={after} nextPage={nextPage}/>

            {showAddModal && <AttachModal
                show={showAddModal}
                emptyState={'No groups found'}
                filterPlaceholder={'Find Group...'}
                modalTitle={'Add to Groups'}
                addText={'Add to Groups'}
                searchFn={prefix => auth.listGroups(prefix, "", 1000).then(res => res.results)}
                onHide={() => setShowAddModal(false)}
                onAttach={(selected) => {
                    Promise.all(selected.map(groupId => auth.addUserToGroup(userId, groupId)))
                        .then(() => { setRefresh(!refresh); setAttachError(null) })
                        .catch(error => { setAttachError(error) })
                        .finally(() => { setShowAddModal(false) })
                }}/>
            }
        </>
    );

    return (
        <>
            <UserHeaderWithContext userId={userId} page={'groups'}/>

            <ActionsBar>
                <ActionGroup orientation="left">
                    <Button variant="success" onClick={() => setShowAddModal(true)}>Add User to Group</Button>
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

const UserGroupsContainer = () => {
    const router = useRouter();
    const { after } = router.query;
    const { userId } = router.params;
    return (!userId) ? <></> : <UserGroupsList
        userId={userId}
        after={(after) ? after : ""}
        onPaginate={after => router.push({pathname: '/auth/users/:userId/groups', params: {userId}, query: {after}})}
    />;
};

const UserGroupsPage = () => {
    return (
        <AuthLayout activeTab="users">
            <UserGroupsContainer />
        </AuthLayout>
    );
};

export default UserGroupsPage;