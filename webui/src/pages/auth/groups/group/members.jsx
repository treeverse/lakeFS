import React, {useEffect, useState} from "react";
import { useOutletContext } from "react-router-dom";
import Button from "react-bootstrap/Button";

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
    AlertError,
    RefreshButton
} from "../../../../lib/components/controls";
import {useRouter} from "../../../../lib/hooks/router";
import {Link} from "../../../../lib/components/nav";
import {resolveDisplayName} from "../../../../lib/utils";


const GroupMemberList = ({ groupId, after, onPaginate }) => {
    const [refresh, setRefresh] = useState(false);
    const [showAddModal, setShowAddModal] = useState(false);
    const [attachError, setAttachError] = useState(null);

    const {results, loading, error, nextPage} = useAPIWithPagination(() => {
        return auth.listGroupMembers(groupId, after);
    }, [groupId, after, refresh]);

    useEffect(() => {
        setAttachError(null);
    }, [refresh]);

    let content;
    if (loading) content = <Loading/>;
    else if (error) content=  <AlertError error={error}/>;
    else content = (
            <>
                {attachError && <AlertError error={attachError}/>}

                <DataTable
                    keyFn={user => user.id}
                    rowFn={user => [
                        <Link href={{pathname: '/auth/users/:userId', params: {userId: user.id}}}>{resolveDisplayName(user)}</Link>,
                        <FormattedDate dateValue={user.creation_date}/>
                    ]}
                    headers={['User ID', 'Created At']}
                    actions={[{
                        key: 'Remove',
                        buttonFn: user => <ConfirmationButton
                            size="sm"
                            variant="outline-danger"
                            msg={<span>Are you sure you{'\''}d like to remove user <strong>{resolveDisplayName(user)}</strong> from group <strong>{groupId}</strong>?</span>}
                            onConfirm={() => {
                                auth.removeUserFromGroup(user.id, groupId)
                                    .catch(error => alert(error))
                                    .then(() => { setRefresh(!refresh) });
                            }}>
                            Remove
                        </ConfirmationButton>
                    }]}
                    results={results}
                    emptyState={'No users found'}
                />

                <Paginator onPaginate={onPaginate} after={after} nextPage={nextPage}/>

                {showAddModal && <AttachModal
                    show={showAddModal}
                    emptyState={'No users found'}
                    filterPlaceholder={'Find User...'}
                    modalTitle={'Add to Group'}
                    addText={'Add to Group'}
                    searchFn={prefix => auth.listUsers(prefix, "", 5).then(res => res.results)}
                    onHide={() => setShowAddModal(false)}
                    onAttach={(selected) => {
                        Promise.all(selected.map(user => auth.addUserToGroup(user.id, groupId)))
                            .then(() => { setRefresh(!refresh); setAttachError(null) })
                            .catch(error => { setAttachError(error) })
                            .finally(() => { setShowAddModal(false) });
                    }}/>
                }
            </>
        );

    return (
        <>
            <GroupHeader groupId={groupId} page={'members'}/>

            <ActionsBar>
                <ActionGroup orientation="left">
                    <Button variant="success" onClick={() => setShowAddModal(true)}>Add Members</Button>
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

const GroupMembersContainer = () => {
    const router = useRouter();
    const { after } = router.query;
    const { groupId } = router.params;
    return groupId && <GroupMemberList
        groupId={groupId}
        after={(after) ? after : ""}
        onPaginate={after => router.push({pathname: '/auth/groups/:groupId/members', params: {groupId},query: {after}})}
    />;
};

const GroupMembersPage = () => {
    const [setActiveTab] = useOutletContext();
    useEffect(() => setActiveTab('groups'), [setActiveTab]);
    return <GroupMembersContainer/>;
};

export default GroupMembersPage;
