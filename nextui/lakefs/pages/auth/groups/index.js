import {AuthLayout} from "../../../lib/components/auth/layout";
import {
    ActionGroup,
    ActionsBar,
    Checkbox,
    DataTable,
    Error, FormattedDate,
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


const GroupsContainer = () => {
    const [selected, setSelected] = useState([])
    const [deleteError, setDeleteError] = useState(null)
    const [showCreate, setShowCreate] = useState(false)
    const [refresh, setRefresh] = useState(false)

    const router = useRouter()
    const after = (!!router.query.after) ? router.query.after : ""
    const { results, loading, error, nextPage } =  useAPIWithPagination(() => {
        return auth.listGroups(after)
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
                        Create Group
                    </Button>

                    <ConfirmationButton
                        onConfirm={() => {
                            auth.deleteGroups(selected.map(g => g.id))
                                .catch(err => setDeleteError(err))
                                .then(() => {
                                    setSelected([])
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

            {(!!deleteError) && <Error error={deleteError}/>}

            <EntityCreateModal
                show={showCreate}
                onHide={() => setShowCreate(false)}
                onCreate={groupId => {
                    return auth.createGroup(groupId).then(() => {
                        setShowCreate(false)
                        setRefresh(!refresh)
                    })
                }}
                title="Create Group"
                idPlaceholder="Group Name (e.g. 'DataTeam')"
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
                    <Link href={{pathname: '/auth/groups/[groupId]', query: {groupId: group.id}}}>
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
    )
}

const GroupsPage = () => {
    return (
        <AuthLayout activeTab="groups">
            <GroupsContainer/>
        </AuthLayout>
    )
}

export default GroupsPage