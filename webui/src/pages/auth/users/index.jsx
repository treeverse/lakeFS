import React, {createContext, useCallback, useEffect, useState} from "react";
import {Outlet, useOutletContext} from "react-router-dom";

import Button from "react-bootstrap/Button";

import {useAPI} from "../../../lib/hooks/api";
import {auth} from "../../../lib/api";
import useUser from "../../../lib/hooks/user";
import {ConfirmationButton} from "../../../lib/components/modals";
import {EntityActionModal} from "../../../lib/components/auth/forms";
import {Paginator} from "../../../lib/components/pagination";
import {Link} from "../../../lib/components/nav";
import {
    ActionGroup,
    ActionsBar,
    AlertError,
    Checkbox,
    DataTable,
    FormattedDate,
    Loading,
    RefreshButton,
    SearchInput,
    useDebouncedState
} from "../../../lib/components/controls";
import validator from "validator/es";
import {disallowPercentSign, INVALID_USER_NAME_ERROR_MESSAGE} from "../validation";
import {resolveUserDisplayName} from "../../../lib/utils";
import {allUsersFromLakeFS} from "../../../lib/components/auth/users";
import {useRouter} from "../../../lib/hooks/router";

const DEFAULT_LISTING_AMOUNT = 100;
const DECIMAL_RADIX = 10;
const USER_NOT_FOUND = "unknown";
export const GetUserEmailByIdContext = createContext();

const UsersContainer = ({ refresh, setRefresh, allUsers, loading, error }) => {
    const { user } = useUser();
    const currentUser = user;

    const router = useRouter();
    const prefix = router.query.prefix ? router.query.prefix : "";
    const afterParsedToInt = parseInt(router.query.after, DECIMAL_RADIX);
    const after = isNaN(afterParsedToInt) ? 0 : afterParsedToInt;

    const [selected, setSelected] = useState([]);
    const [deleteError, setDeleteError] = useState(null);
    const [showCreate, setShowCreate] = useState(false);
    const [showInvite, setShowInvite] = useState(false);
    const [paginationData, setPaginationData] = useState(
        { hasMorePages: false, paginatedFilteredUsers: []});
    const [searchPrefix, setSearchPrefix] = useDebouncedState(
        prefix,
        search => {
            return navigateToUsersPage(search, 0);
        }
    );

    useEffect(() => { setSelected([]); }, [refresh, after]);

    const authCapabilities = useAPI(() => auth.getAuthCapabilities());

    // The function is placed inside useEffect to prevent calling the filter() function on every render of the component,
    // especially when state updates occur, such as searchPrefix updating with each typed letter.
    useEffect(() => {
        if (allUsers) {
            const filtered = allUsers.filter((user) => resolveUserDisplayName(user).startsWith(prefix));
            const hasMorePages = after + DEFAULT_LISTING_AMOUNT < filtered.length;
            const paginatedFilteredUsers = filtered.slice(after, after + DEFAULT_LISTING_AMOUNT);
            setPaginationData({ hasMorePages, paginatedFilteredUsers });
        }
    }, [allUsers, prefix, after]);

    const afterForPagination = after === 0 ? "" : String(after);
    const nextPage = paginationData.hasMorePages ? String(after + DEFAULT_LISTING_AMOUNT) : null;

    function navigateToUsersPage(prefix, after) {
        return router.push({ pathname: "/auth/users", query: { prefix, after } });
    }

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
                    <SearchInput
                        searchPrefix={searchPrefix}
                        setSearchPrefix={setSearchPrefix}
                        placeholder="Find a User..."
                    />
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
                results={paginationData.paginatedFilteredUsers}
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
                after={afterForPagination}
                onPaginate={after => { return navigateToUsersPage(prefix, after); }}
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
    const { setActiveTab, refresh, setRefresh, allUsers, loading, error } = useOutletContext();
    useEffect(() => setActiveTab("users"), [setActiveTab]);
    return (
        <UsersContainer
            refresh={refresh}
            setRefresh={setRefresh}
            allUsers={allUsers}
            loading={loading}
            error={error}
        />
    );
};

const UsersIndexPage = () => {
    const [setActiveTab] = useOutletContext();
    const [refresh, setRefresh] = useState(false);

    const { response: allUsers, loading, error } = useAPI(() => {
        return allUsersFromLakeFS(resolveUserDisplayName);
    }, [refresh]);

    const getUserEmailById = useCallback((id) => {
        const userRecord = allUsers?.find(user => user.id === id);
        // return something, so we don't completely break the state
        // this can help us track down issues later on
        if (!userRecord) {
            return USER_NOT_FOUND;
        }

        return userRecord.email || userRecord.id;
    }, [allUsers]);

    return (
        <GetUserEmailByIdContext.Provider value={getUserEmailById}>
            <Outlet context={{ setActiveTab, refresh, setRefresh, allUsers, loading, error, getUserEmailById }} />
        </GetUserEmailByIdContext.Provider>
    )
}

export default UsersIndexPage;

