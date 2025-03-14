import React, {createContext, useCallback, useEffect, useState} from "react";
import {Outlet} from "react-router-dom";
import { useOutletContext } from "react-router-dom";

import Button from "react-bootstrap/Button";

import {useAPI, useAPIWithPagination} from "../../../lib/hooks/api";
import {auth} from "../../../lib/api";
import useUser from "../../../lib/hooks/user";
import {ConfirmationButton} from "../../../lib/components/modals";
import {EntityActionModal} from "../../../lib/components/auth/forms";
import {Paginator} from "../../../lib/components/pagination";
import {useRouter} from "../../../lib/hooks/router";
import {Link} from "../../../lib/components/nav";
import {
    ActionGroup,
    ActionsBar,
    Checkbox,
    DataTable,
    AlertError,
    FormattedDate,
    Loading,
    RefreshButton
} from "../../../lib/components/controls";
import validator from "validator/es";
import { disallowPercentSign, INVALID_USER_NAME_ERROR_MESSAGE } from "../validation";
import { resolveUserDisplayName } from "../../../lib/utils";

const USER_NOT_FOUND = "unknown";
export const GetUserEmailByIdContext = createContext();


const UsersContainer = ({nextPage, refresh, setRefresh, error, loading, userListResults}) => {
    const { user } = useUser();
    const currentUser = user;

    const router = useRouter();
    const after = (router.query.after) ? router.query.after : "";
    const [selected, setSelected] = useState([]);
    const [deleteError, setDeleteError] = useState(null);
    const [showCreate, setShowCreate] = useState(false);
    const [showInvite, setShowInvite] = useState(false);
    
    

    useEffect(() => { setSelected([]); }, [refresh, after]);

    const authCapabilities = useAPI(() => auth.getAuthCapabilities());
    if (error) return <AlertError error={error}/>;
    if (loading) return <Loading/>;
    if (authCapabilities.loading) return <Loading/>;

    const canInviteUsers = !authCapabilities.error && authCapabilities.response && authCapabilities.response.invite_user;

    return (
        <>
            <ActionsBar>
                <UserActionsActionGroup canInviteUsers={canInviteUsers} selected={selected}
                                        onClickInvite={() => setShowInvite(true)} onClickCreate={() => setShowCreate(true)}
                                        onConfirmDelete={() => {
                                            auth.deleteUsers(selected.map(u => u.id))
                                                .catch(err => setDeleteError(err))
                                                .then(() => {
                                                    setSelected([]);
                                                    setRefresh(!refresh);
                                                })}}/>
                <ActionGroup orientation="right">
                    <RefreshButton onClick={() => setRefresh(!refresh)}/>
                </ActionGroup>
            </ActionsBar>
            <div className="auth-learn-more">
                Users are entities that access and use lakeFS. <a href="https://docs.lakefs.io/reference/authentication.html" target="_blank" rel="noopener noreferrer">Learn more.</a>
            </div>

            {(!!deleteError) && <AlertError error={deleteError}/>}

            <EntityActionModal
                show={showCreate}
                onHide={() => setShowCreate(false)}
                onAction={userId => {
                    return auth.createUser(userId).then(() => {
                        setSelected([]);
                        setShowCreate(false);
                        setRefresh(!refresh);
                    });
                }}
                title={canInviteUsers ? "Create API User" : "Create User"}
                placeholder={canInviteUsers ? "Name (e.g. Spark)" : "Username (e.g. 'jane.doe')"}
                actionName={"Create"}
                validationFunction={disallowPercentSign(INVALID_USER_NAME_ERROR_MESSAGE)}
            />

            <EntityActionModal
                show={showInvite}
                onHide={() => setShowInvite(false)}
                onAction={async (userEmail) => {
                    if (!validator.isEmail(userEmail)) {
                    throw new Error("Invalid email address");
                }
                    await auth.createUser(userEmail, true);
                    setSelected([]);
                    setShowInvite(false);
                    setRefresh(!refresh);
                }}
                title={"Invite User"}
                placeholder={"Email"}
                actionName={"Invite"}
            />

            <DataTable
                results={userListResults}
                headers={['', 'User ID', 'Created At']}
                keyFn={user => user.id}
                rowFn={user => [
                    <Checkbox
                        disabled={(!!currentUser && currentUser.id === user.id)}
                        name={user.id}
                        onAdd={() => setSelected([...selected, user])}
                        onRemove={() => setSelected(selected.filter(u => u !== user))}
                    />,
                    <Link href={{pathname: '/auth/users/:userId', params: {userId: user.id}}}>
                        { resolveUserDisplayName(user) }
                    </Link>,
                    <FormattedDate dateValue={user.creation_date}/>
                ]}
                firstFixedCol={true}
            />

            <Paginator
                nextPage={nextPage}
                after={after}
                onPaginate={after => router.push({pathname: '/auth/users', query: {after}})}
            />
        </>
    );
};

const UserActionsActionGroup = ({canInviteUsers, selected, onClickInvite, onClickCreate, onConfirmDelete }) => {

    return (
        <ActionGroup orientation="left">
            <Button
                hidden={!canInviteUsers}
                variant="primary"
                onClick={onClickInvite}>
                Invite User
            </Button>

            <Button
                variant="success"
                onClick={onClickCreate}>
                {canInviteUsers ? "Create API User" : "Create User"}
            </Button>
            <ConfirmationButton
                onConfirm={onConfirmDelete}
                disabled={(selected.length === 0)}
                variant="danger"
                msg={`Are you sure you'd like to delete ${selected.length} users?`}>
                Delete Selected
            </ConfirmationButton>
        </ActionGroup>
    );
}

export const UsersPage = () => {
    const { setActiveTab, refresh, loading, error, nextPage, setRefresh, usersList } = useOutletContext();
    useEffect(() => setActiveTab("users"), [setActiveTab]);
    return (
        <UsersContainer
            refresh={refresh}
            loading={loading}
            error={error}
            nextPage={nextPage}
            setRefresh={setRefresh}
            userListResults={usersList}
        />
    );
};

const UsersIndexPage = () => {
    const [setActiveTab] = useOutletContext();
    const [refresh, setRefresh] = useState(false);
    const [usersList, setUsersList] = useState([]);
    const router = useRouter();
    const after = (router.query.after) ? router.query.after : "";
    const { results, loading, error, nextPage } =  useAPIWithPagination(() => {
        return auth.listUsers('', after);
    }, [after, refresh]);

    useEffect(() => {
        setUsersList(results);
    }, [results, refresh]);

    const getUserEmailById = useCallback((id) => {
        const userRecord = usersList.find(user => user.id === id);
        // return something, so we don't completely break the state
        // this can help us track down issues later on
        if (!userRecord) {
            return USER_NOT_FOUND;
        }

        return userRecord.email || userRecord.id;
    }, [usersList]);

    return (
        <GetUserEmailByIdContext.Provider value={getUserEmailById}>
            <Outlet context={{setActiveTab, refresh, loading, error, nextPage, setRefresh, usersList, getUserEmailById}} />
        </GetUserEmailByIdContext.Provider>
    )
}

export default UsersIndexPage;
