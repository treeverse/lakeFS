import React, {useEffect, useState} from "react";
import Button from "react-bootstrap/Button";
import Dropdown from "react-bootstrap/Dropdown";

import {useAPIWithPagination} from "../../../lib/hooks/api";
import {auth} from "../../../lib/api";
import {ConfirmationButton} from "../../../lib/components/modals";
import {Paginator} from "../../../lib/components/pagination";
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
import {useRouter} from "../../../lib/hooks/router";
import {Link} from "../../../lib/components/nav";
import {EntityActionModal} from "../../../lib/components/auth/forms";
import { disallowPercentSign, INVALID_GROUP_NAME_ERROR_MESSAGE } from "../validation";
import {useLoginConfigContext} from "../../../lib/hooks/conf";
import {useAuthOutletContext} from "../../../lib/components/auth/layout";

interface PermissionTypes {
    Read: string;
    Write: string;
    Super: string;
    Admin: string;
}

type PermissionType = keyof PermissionTypes;


const permissions: PermissionTypes = {
    'Read': 'Read repository data and metadata, and manage own credentials.',
    'Write': 'Read and write repository data and metadata, and manage own credentials.',
    'Super': 'Perform all operations on repository, and manage own credentials.',
    'Admin': 'Do anything.',
};

type ACLPermissionButtonProps = {
    initialValue?: string;
    onSelect?: (newPermission: string) => unknown;
    variant?: string;
}

const ACLPermission: React.FC<ACLPermissionButtonProps> = ({initialValue, onSelect, variant}) => {
    const [value, setValue] = useState<string|undefined>(initialValue);
    const [title, setTitle] = useState<string>("");
    variant ||= 'secondary';

    useEffect(() => {
        if (!initialValue) {
            setTitle('(unknown)');
            return;
        }

        if (Object.keys(permissions).includes(initialValue)) {
            setTitle(permissions[initialValue as PermissionType]);
        } else {
            setTitle('(unknown)');
        }
    }, [initialValue]);

    return (<Dropdown onSelect={
        (p: PermissionType) => {
            if (value !== p) {
                if (onSelect) { onSelect(p); }
                setValue(p);
                setTitle(permissions[p]);
            }
        }}>
        <Dropdown.Toggle variant={variant} title={title}>{value}</Dropdown.Toggle>
        <Dropdown.Menu>
        {Object.entries(permissions).map(([key, text]) =>
            <Dropdown.Item key={key} eventKey={key}>
            <div><b>{key}</b><br/>{text}</div>
            </Dropdown.Item>
        )}
            </Dropdown.Menu>
        </Dropdown>);
};

const getACLMaybe = async (groupId: string) => {
    try {
        return await auth.getACL(groupId);
    } catch (e) {
        if (e.message.toLowerCase().includes('no acl')) {
            return null;
        }
        throw e;
    }
}

const GroupsContainer = () => {
    const [selected, setSelected] = useState([]);
    const [deleteError, setDeleteError] = useState(null);
    const [putACLError, setPutACLError] = useState(null);
    const [showCreate, setShowCreate] = useState(false);
    const [refresh, setRefresh] = useState(false);

    const router = useRouter();
    const after = (router.query.after) ? router.query.after : "";
    const lc = useLoginConfigContext();
    const simplified = lc.RBAC === 'simplified';
    const { results, loading, error, nextPage } =  useAPIWithPagination(async () => {
        const groups = await auth.listGroups("", after);
        const enrichedResults = await Promise.all(groups?.results.map(async group => ({...group, acl: simplified && await getACLMaybe(group.id)})));
        return {...groups, results: enrichedResults};
    }, [after, refresh, lc.RBAC]);

    useEffect(() => {
        setSelected([]);
    }, [after, refresh]);

    if (error) return <AlertError error={error}/>;
    if (loading) return <Loading/>;
    const headers = simplified ? ['', 'Group Name', 'Permission', 'Created At'] : ['', 'Group Name', 'Created At'];

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
                            // eslint-disable-next-line @typescript-eslint/no-explicit-any
                            auth.deleteGroups(selected.map(g => (g as any).id))
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


            {(!!deleteError) && <AlertError error={deleteError}/>}
            {(!!putACLError) && <AlertError error={putACLError}/>}

            <EntityActionModal
                show={showCreate}
                onHide={() => setShowCreate(false)}
                onAction={(groupName, groupDesc) => {
                    return auth.createGroup(groupName, groupDesc).then(() => {
                        setSelected([]);
                        setShowCreate(false);
                        setRefresh(!refresh);
                    });
                }}
                title="Create Group"
                placeholder="Group Name (e.g. 'DataTeam')"
                actionName={"Create"}
                validationFunction={disallowPercentSign(INVALID_GROUP_NAME_ERROR_MESSAGE)}
                showExtraField={true}
                extraPlaceholder="Group Description (optional)"
            />

            <DataTable
                results={results}
                headers={headers}
                keyFn={group => group.id}
                rowFn={group => {
                    const elements = [
                        <Checkbox
                            name={group.id}
                            onAdd={() => setSelected([...selected, group])}
                            onRemove={() => setSelected(selected.filter(g => g !== group))}
                        />,
                        <Link href={{pathname: '/auth/groups/:groupId', params: {groupId: group.id}}}>
                            {group.name}
                        </Link>]
                    simplified && elements.push(group.acl ? <ACLPermission initialValue={group.acl.permission} onSelect={
                            ((permission) => auth.putACL(group.id, {...group.acl, permission})
                                .then(() => setPutACLError(null), (e) => setPutACLError(e)))
                        }/> : <></>)
                    elements.push(<FormattedDate dateValue={group.creation_date}/>)

                    return elements;
                }}/>

            <Paginator
                nextPage={nextPage}
                after={after}
                onPaginate={after => router.push({pathname: '/auth/groups', query: {after}, params: {}})}
            />
        </>
    );
};

export const GroupsPage = () => {
    const [setActiveTab] = useAuthOutletContext();
    useEffect(() => setActiveTab('groups'), [setActiveTab]);
    return <GroupsContainer/>;
};

export default GroupsPage;
