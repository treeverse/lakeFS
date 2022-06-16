import React, {useEffect, useState} from "react";
import {Route, Switch} from "react-router-dom";

import Button from "react-bootstrap/Button";

import {AuthLayout} from "../../../lib/components/auth/layout";
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
    Error, FormattedDate,
    Loading,
    RefreshButton
} from "../../../lib/components/controls";
import UserPage from "./user";
import validator from "validator/es";


const UsersContainer = () => {
    const { user } = useUser();
    const currentUser = user;

    const [selected, setSelected] = useState([]);
    const [deleteError, setDeleteError] = useState(null);
    const [showCreate, setShowCreate] = useState(false);
    const [showInvite, setShowInvite] = useState(false);
    const [refresh, setRefresh] = useState(false);

    const router = useRouter();
    const after = (router.query.after) ? router.query.after : "";
    const { results, loading, error, nextPage } =  useAPIWithPagination(() => {
        return auth.listUsers('', after);
    }, [after, refresh]);

    useEffect(() => { setSelected([]); }, [refresh, after]);

    const authCapabilities = useAPI(() => auth.getAuthCapabilities());
    if (error) return <Error error={error}/>;
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

            {(!!deleteError) && <Error error={deleteError}/>}

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
                title={canInviteUsers ? "Create Integration User" : "Create User"}
                placeholder={canInviteUsers ? "Integration Name (e.g. Spark)" : "Username (e.g. 'jane.doe')"}
                actionName={"Create"}
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
                results={results}
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
                        {user.email || user.id}
                    </Link>,
                    <FormattedDate dateValue={user.creation_date}/>
                ]}/>

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
                {canInviteUsers ? "Create Integration User" : "Create User"}
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

const UsersPage = () => {
    return (
        <AuthLayout activeTab="users">
            <UsersContainer/>
        </AuthLayout>
    );
};

const UsersIndexPage = () => {
    return (
        <Switch>
            <Route path="/auth/users/:userId">
                <UserPage/>
            </Route>
            <Route path="/auth/users">
                <UsersPage/>
            </Route>
        </Switch>
    )
}

export default UsersIndexPage;
