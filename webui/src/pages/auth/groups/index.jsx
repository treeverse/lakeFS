import React, {useEffect, useState} from "react";

import Button from "react-bootstrap/Button";

import {AuthLayout} from "../../../lib/components/auth/layout";
import {useAPIWithPagination} from "../../../lib/hooks/api";
import {auth} from "../../../lib/api";
import {ConfirmationButton} from "../../../lib/components/modals";
import {Paginator} from "../../../lib/components/pagination";
import {
    ActionGroup,
    ActionsBar,
    Checkbox,
    DataTable,
    Error, FormattedDate,
    Loading,
    RefreshButton
} from "../../../lib/components/controls";
import {Route, Routes} from "react-router-dom";
import {useRouter} from "../../../lib/hooks/router";
import {Link} from "../../../lib/components/nav";
import GroupPage from "./group";
import {EntityActionModal} from "../../../lib/components/auth/forms";
import { disallowPercentSign, INVALID_GROUP_NAME_ERROR_MESSAGE } from "../validation";


const GroupsContainer = () => {
    const [selected, setSelected] = useState([]);
    const [deleteError, setDeleteError] = useState(null);
    const [showCreate, setShowCreate] = useState(false);
    const [refresh, setRefresh] = useState(false);

    const router = useRouter();
    const after = (router.query.after) ? router.query.after : "";
    const { results, loading, error, nextPage } =  useAPIWithPagination(() => {
        return auth.listGroups(after);
    }, [after, refresh]);

    useEffect(() => {
        setSelected([]);
    }, [after, refresh]);

    if (error) return <Error error={error}/>;
    if (loading) return <Loading/>;

    return (
        <>
            <ActionsBar>
                <ActionGroup orientation="left">
                    <Button
                        variant="success"
                        onClick={() => setShowCreate(true)}>
                        Create Group
                    </Button>

                    <ConfirmationButton
                        onConfirm={() => {
                            auth.deleteGroups(selected.map(g => g.id))
                                .catch(err => setDeleteError(err))
                                .then(() => {
                                    setSelected([]);
                                    setRefresh(!refresh)
                                })
                        }}
                        disabled={(selected.length === 0)}
                        variant="danger"
                        msg={`Are you sure you'd like to delete ${selected.length} groups?`}>
                        Delete Selected
                    </ConfirmationButton>
                </ActionGroup>
                <ActionGroup orientation="right">
                    <RefreshButton onClick={() => setRefresh(!refresh)}/>
                </ActionGroup>
            </ActionsBar>
            <div className="auth-learn-more">
                A group is a collection of users. <a href="https://docs.lakefs.io/reference/authorization.html#authorization" target="_blank" rel="noopener noreferrer">Learn more.</a>
            </div>


            {(!!deleteError) && <Error error={deleteError}/>}

            <EntityActionModal
                show={showCreate}
                onHide={() => setShowCreate(false)}
                onAction={groupId => {
                    return auth.createGroup(groupId).then(() => {
                        setSelected([]);
                        setShowCreate(false);
                        setRefresh(!refresh);
                    });
                }}
                title="Create Group"
                placeholder="Group Name (e.g. 'DataTeam')"
                actionName={"Create"}
                validationFunction={disallowPercentSign(INVALID_GROUP_NAME_ERROR_MESSAGE)}
            />

            <DataTable
                results={results}
                headers={['', 'Group ID', 'Created At']}
                keyFn={group => group.id}
                rowFn={group => [
                    <Checkbox
                        name={group.id}
                        onAdd={() => setSelected([...selected, group])}
                        onRemove={() => setSelected(selected.filter(g => g !== group))}
                    />,
                    <Link href={{pathname: '/auth/groups/:groupId', params: {groupId: group.id}}}>
                        {group.id}
                    </Link>,
                    <FormattedDate dateValue={group.creation_date}/>
                ]}/>

            <Paginator
                nextPage={nextPage}
                after={after}
                onPaginate={after => router.push({pathname: '/auth/groups', query: {after}})}
            />
        </>
    );
};

const GroupsPage = () => {
    return (
        <AuthLayout activeTab="groups">
            <GroupsContainer/>
        </AuthLayout>
    );
};

const GroupsIndexPage = () => {
    return (
        <Routes>
            <Route path=":groupId/*" element={<GroupPage/>} />
            <Route path="" element={<GroupsPage/>} />
        </Routes>
    )
}

export default GroupsIndexPage;
