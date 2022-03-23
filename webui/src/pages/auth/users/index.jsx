import React, {useEffect, useState} from "react";
import {Route, Switch} from "react-router-dom";

import Button from "react-bootstrap/Button";

import {AuthLayout} from "../../../lib/components/auth/layout";
import {useAPIWithPagination} from "../../../lib/hooks/api";
import {auth} from "../../../lib/api";
import useUser from "../../../lib/hooks/user";
import {ConfirmationButton} from "../../../lib/components/modals";
import {EntityCreateModal} from "../../../lib/components/auth/forms";
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


const UsersContainer = () => {
    const { user } = useUser();
    const currentUser = user;

    const [selected, setSelected] = useState([]);
    const [deleteError, setDeleteError] = useState(null);
    const [showCreate, setShowCreate] = useState(false);
    const [refresh, setRefresh] = useState(false);

    const router = useRouter();
    const after = (!!router.query.after) ? router.query.after : "";
    const { results, loading, error, nextPage } =  useAPIWithPagination(() => {
        return auth.listUsers('', after);
    }, [after, refresh]);

    useEffect(() => { setSelected([]); }, [refresh, after]);

    if (!!error) return <Error error={error}/>;
    if (loading) return <Loading/>;

    return (
        <>
            <ActionsBar>
                <ActionGroup orientation="left">
                    <Button
                        variant="success"
                        onClick={() => setShowCreate(true)}>
                        Create User
                    </Button>

                    <ConfirmationButton
                        onConfirm={() => {
                            auth.deleteUsers(selected.map(u => u.id))
                                .catch(err => setDeleteError(err))
                                .then(() => {
                                    setSelected([]);
                                    setRefresh(!refresh);
                                })
                        }}
                        disabled={(selected.length === 0)}
                        variant="danger"
                        msg={`Are you sure you'd like to delete ${selected.length} users?`}>
                        Delete Selected
                    </ConfirmationButton>
                </ActionGroup>
                <ActionGroup orientation="right">
                    <RefreshButton onClick={() => setRefresh(!refresh)}/>
                </ActionGroup>
            </ActionsBar>
            <div className="auth-learn-more">
                Users are entities that access and use lakeFS. <a href="https://docs.lakefs.io/reference/authorization.html#authorization" target="_blank" rel="noopener noreferrer">Learn more.</a>
            </div>

            {(!!deleteError) && <Error error={deleteError}/>}

            <EntityCreateModal
                show={showCreate}
                onHide={() => setShowCreate(false)}
                onCreate={userId => {
                    return auth.createUser(userId).then(() => {
                        setSelected([]);
                        setShowCreate(false);
                        setRefresh(!refresh);
                    });
                }}
                title="Create User"
                idPlaceholder="Username (e.g. 'jane.doe')"
            />

            <DataTable
                results={results}
                headers={['', 'User ID', 'Created At','Email']}
                keyFn={user => user.id}
                rowFn={user => [
                    <Checkbox
                        disabled={(!!currentUser && currentUser.id === user.id)}
                        name={user.id}
                        onAdd={() => setSelected([...selected, user])}
                        onRemove={() => setSelected(selected.filter(u => u !== user))}
                    />,
                    <Link href={{pathname: '/auth/users/:userId', params: {userId: user.id}}}>
                        {user.id}
                    </Link>,
                    <FormattedDate dateValue={user.creation_date}/>,
                    <div>{user.email}</div>
                ]}/>

            <Paginator
                nextPage={nextPage}
                after={after}
                onPaginate={after => router.push({pathname: '/auth/users', query: {after}})}
            />
        </>
    );
};

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
