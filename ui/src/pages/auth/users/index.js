import {AuthLayout} from "../../../lib/components/auth/layout";
import {
    ActionGroup,
    ActionsBar,
    Checkbox,
    DataTable,
    Error,
    Loading,
    RefreshButton
} from "../../../lib/components/controls";
import {useAPIWithPagination} from "../../../rest/hooks";
import {auth} from "../../../rest/api";
import {useRouter} from "next/router";
import {useState} from "react";
import Link from 'next/link';
import moment from "moment";

import Button from "react-bootstrap/Button";

import useUser from "../../../lib/hooks/user";
import {ConfirmationButton} from "../../../lib/components/modals";
import {EntityCreateModal} from "../../../lib/components/auth/forms";
import {Paginator} from "../../../lib/components/pagination";


const UsersContainer = () => {
    const { user } = useUser()
    const currentUser = user

    const [selected, setSelected] = useState([])
    const [deleteError, setDeleteError] = useState(null)
    const [showCreate, setShowCreate] = useState(false)
    const [refresh, setRefresh] = useState(false)

    const router = useRouter()
    const after = (!!router.query.after) ? router.query.after : ""
    const { results, loading, error, nextPage } =  useAPIWithPagination(() => {
        return auth.listUsers(after)
    }, [after, refresh])

    if (!!error) return <Error error={error}/>
    if (loading) return <Loading/>

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
                                    setSelected([])
                                    setRefresh(!refresh)
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

            {(!!deleteError) && <Error error={deleteError}/>}

            <EntityCreateModal
                show={showCreate}
                onHide={() => setShowCreate(false)}
                onCreate={userId => {
                    return auth.createUser(userId).then(() => {
                        setShowCreate(false)
                        setRefresh(!refresh)
                    })
                }}
                title="Create User"
                idPlaceholder="Username (e.g. 'jane.doe')"
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
                    <Link href={{pathname: '/auth/users/[userId]', query: {userId: user.id}}}>
                        {user.id}
                    </Link>,
                    moment.unix(user.creation_date).format()
                ]}/>

            <Paginator
                nextPage={nextPage}
                after={after}
                onPaginate={after => router.push({pathname: '/auth/users', query: {after}})}
            />
        </>
    )
}

const UsersPage = () => {
    return (
        <AuthLayout activeTab="users">
            <UsersContainer/>
        </AuthLayout>
    )
}

export default UsersPage