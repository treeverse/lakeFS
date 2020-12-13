import {connect} from "react-redux";
import {
    addUserToGroup, attachPolicyToUser, createCredentials,
    createUser, deleteCredentials, deleteUsers, detachPolicyFromUser, listCredentials,
    listGroups, listPolicies, listUserEffectivePolicies, listUserGroups, listUserPolicies,
    listUsers, removeUserFromGroup,
    resetAddUserToGroup, resetAttachPolicyToUser, resetCreateCredentials,
    resetCreateUser, resetDeleteCredentials, resetDeleteUsers, resetDetachPolicyFromUser, resetRemoveUserFromGroup
} from "../../actions/auth";
import React, {useCallback, useEffect, useRef, useState} from "react";
import {Link, useHistory, useParams} from "react-router-dom";
import {Alert, Button, ButtonToolbar, Col, Table} from "react-bootstrap";
import {EntityCreateButton, PaginatedEntryList} from "./entities";
import Form from "react-bootstrap/Form";
import {SyncIcon} from "@primer/octicons-react";
import * as moment from "moment";
import Breadcrumb from "react-bootstrap/Breadcrumb";
import Tabs from "react-bootstrap/Tabs";
import Tab from "react-bootstrap/Tab";
import ClipboardButton from "../ClipboardButton";
import ConfirmationModal from "../ConfirmationModal";


export const UsersPage = connect(
    ({ auth }) => ({
        usersList: auth.usersList,
        createUserStatus: auth.userCreation,
        deletionStatus: auth.userDeletion
    }),
    ({ listUsers, createUser, resetCreateUser, deleteUsers, resetDeleteUsers })
)(({ listUsers, usersList, createUser, deleteUsers, resetDeleteUsers, deletionStatus,  createUserStatus, resetCreateUser }) => {

    const [update, setUpdate] = useState(new Date());
    const history = useHistory();

    const createFn = useCallback((formData) => {
        createUser(formData.id)
    }, [createUser]);

    const [checkedUsers, setCheckedUsers] = useState([]);

    const deleteSelectedUsers = () => {
        deleteUsers(checkedUsers);
        handleClose();
    };

    useEffect(() => {
        if (deletionStatus.done) {
            resetDeleteUsers();
            setCheckedUsers([]);
            listUsers();
        }
    }, [deletionStatus, resetDeleteUsers, listUsers]);

    const [show, setShow] = useState(false);  
    const handleClose = () => setShow(false);
    const handleShow = () => setShow(true);
    const deleteConfirmMsg = `are you sure you'd like to delete users: ${checkedUsers.join(', ')}?`;

    return (
        <Col lg={9}>
            <div className={"mb-2"}>
                <div className="action-bar borderless">
                    <ButtonToolbar className="float-left mb-2 pl-1">
                        <EntityCreateButton
                            variant={'success'}
                            buttonText={'Create User'}
                            modalTitle={'Create User'}
                            createFn={createFn}
                            resetFn={resetCreateUser}
                            idPlaceholderText={'Username (e.g. "jane.doe")'}
                            onDone={() => {
                                history.push(`/auth/users/${createUserStatus.payload.id}`);
                            }}
                            status={createUserStatus}>
                            <Form.Group>
                                <Form.Control type="text" name="id" placeholder="Username (e.g. 'jane.doe')"/>
                            </Form.Group>
                        </EntityCreateButton>
                        <Button variant="danger" disabled={checkedUsers.length < 1} onClick={handleShow}>
                            Delete Selected
                        </Button>
                    </ButtonToolbar>
                    <ButtonToolbar className="float-right mb-2 pr-1">
                        <Button variant="outline-dark" onClick={() => { setUpdate(new Date()); }}>
                            <SyncIcon/>
                        </Button>
                    </ButtonToolbar>
                    <ConfirmationModal show={show} onHide={handleClose} msg={deleteConfirmMsg} onConfirm={deleteSelectedUsers}/>
                </div>
            </div>

            <Form>
                <PaginatedEntryList
                    listFn={listUsers}
                    entities={usersList}
                    emptyState={"No users found"}
                    update={update}
                    fields={["", "User ID", "Created At"]}
                    entityToRow={entity => {
                        return [
                            (
                                <Form.Group>
                                    <Form.Check type="checkbox" name={entity.id} onChange={(e) => {
                                        if (e.currentTarget.checked) {
                                            // add it
                                            if (checkedUsers.indexOf(entity.id) === -1) {
                                                setCheckedUsers([...checkedUsers, entity.id]);
                                            }
                                        } else {
                                            // remove it
                                            setCheckedUsers(checkedUsers.filter(id => id !== entity.id));
                                        }
                                    }}/>
                                </Form.Group>
                            ),
                            (<Link to={`/auth/users/${entity.id}`}>{entity.id}</Link>),
                            moment.unix(entity.creation_date).toISOString()
                        ]
                    }}
                />
            </Form>
        </Col>
    );
});


const UserMembershipModalButton = connect(
    ({ auth }) => ({ currentGroups: auth.userGroupsList, allGroups: auth.groupsList,  status: auth.groupMembershipCreation }),
    ({ listGroups, addUserToGroup, resetAddUserToGroup })
)(({ userId, currentGroups, allGroups, status, listGroups, addUserToGroup, resetAddUserToGroup, onDone }) => {

    const createFn = useCallback((form) => {
        addUserToGroup(userId, form.group);
    }, [userId, addUserToGroup])
    const resetFn = resetAddUserToGroup;

    return (
        <EntityCreateButton
            variant={'success'}
            buttonText={'Add user to Group'}
            modalTitle={'Select Group'}
            createButtonText={'Add To Group'}
            createFn={createFn}
            resetFn={resetFn}
            onDone={onDone}
            status={status}>
            <Form.Group>
                <PaginatedEntryList
                    listFn={listGroups}
                    entities={allGroups}
                    emptyState={"No groups found"}
                    fields={["Add", "Group ID", "Created At"]}
                    entityToRow={entity => {
                        return [
                            (<Form.Check type="radio" value={entity.id} name="group"/>),
                            (<Link to={`/auth/groups/${entity.id}`}>{entity.id}</Link>),
                            (moment.unix(entity.creation_date).toISOString())
                        ]
                    }}
                />
            </Form.Group>
        </EntityCreateButton>
    );
});

const UserPolicyModalButton = connect(
    ({ auth }) => ({  policies: auth.policiesList,  status: auth.policyUserAttachment }),
    ({ attachPolicyToUser, resetAttachPolicyToUser, listPolicies })
)(({ userId, policies, status, listPolicies, attachPolicyToUser, resetAttachPolicyToUser, onDone }) => {

    const createFn = useCallback((form) => {
        attachPolicyToUser(userId, form.policy);
    }, [userId, attachPolicyToUser])

    const resetFn = resetAttachPolicyToUser;

    return (
        <EntityCreateButton
            variant={'success'}
            buttonText={'Attach Policy'}
            modalTitle={'Select Policy'}
            createButtonText={'Attach to User'}
            createFn={createFn}
            resetFn={resetFn}
            onDone={onDone}
            status={status}>
            <Form.Group>
                <PaginatedEntryList
                    listFn={listPolicies}
                    entities={policies}
                    emptyState={"No policies found"}
                    fields={["Add", "Policy ID", "Created At"]}
                    entityToRow={entity => {
                        return [
                            (<Form.Check type="radio" value={entity.id} name="policy"/>),
                            (<Link to={`/auth/policies/${entity.id}`}>{entity.id}</Link>),
                            (moment.unix(entity.creation_date).toISOString())
                        ]
                    }}
                />
            </Form.Group>
        </EntityCreateButton>
    );
});


const UserGroupPane = connect(
    ({ auth }) => ({
        groups: auth.userGroupsList,
        groupMembershipDeletion: auth.groupMembershipDeletion,
    }),
    ({ listUserGroups, removeUserFromGroup, resetRemoveUserFromGroup })
)(({ userId, listUserGroups, groups, removeUserFromGroup, resetRemoveUserFromGroup, groupMembershipDeletion }) => {

    const groupDetachForm = useRef(null);

    const listGroupsFn = useCallback((after, amount) => {
        listUserGroups(userId, after, amount)
    }, [listUserGroups, userId]);

    const detachGroupFn = useCallback((groupId) => {
        removeUserFromGroup(userId, groupId);
        handleClose();
    }, [removeUserFromGroup, userId])

    useEffect(() => {
        if (groupMembershipDeletion.done) {
            resetRemoveUserFromGroup();
            listGroupsFn()
        }
    }, [groupMembershipDeletion, resetRemoveUserFromGroup, listGroupsFn, userId])

    const [show, setShow] = useState(false);  
    const handleClose = () => setShow(false);
    const handleShow = () => setShow(true);
    const [entityId,setEntityId] = useState('');
    const detachConfirmMsg = `are you sure you'd like to remove user '${userId}' from group '${entityId}'?`;

    return (
        <>
            <div className="action-bar borderless">
                <ButtonToolbar className="float-left mb-2 pl-1">
                    <UserMembershipModalButton userId={userId} onDone={() => { listGroupsFn() }}/>
                </ButtonToolbar>
                <ButtonToolbar className="float-right mb-2 pr-1">
                    <Button variant="outline-dark" onClick={() => { listGroupsFn() }}>
                        <SyncIcon/>
                    </Button>
                </ButtonToolbar>
            </div>

            <Form ref={groupDetachForm}>
                <PaginatedEntryList
                    listFn={listGroupsFn}
                    entities={groups}
                    emptyState={"No group memberships found"}
                    fields={["Group ID", "Created At", ""]}
                    entityToRow={entity => {
                        return [
                            (<Link to={`/auth/groups/${entity.id}`}>{entity.id}</Link>),
                            (moment.unix(entity.creation_date).toISOString()),
                            (<Button size={"sm"} variant="outline-danger" onClick={() => {
                                setEntityId(entity.id);
                                handleShow();
                            }}>Remove</Button>)
                        ]
                    }}
                />
            </Form>
            <ConfirmationModal show={show} onHide={handleClose} msg={detachConfirmMsg} 
                onConfirm={() => {
                    detachGroupFn(entityId);
                }}/>     
        </>
    );
});


const UserAttachedPoliciesPane = connect(
    ({ auth }) => ({
        policies: auth.userPoliciesList,
        policyUserDetachment: auth.policyUserDetachment,
    }),
    ({ listUserPolicies, detachPolicyFromUser, resetDetachPolicyFromUser })
)(({ userId, listUserPolicies, detachPolicyFromUser, resetDetachPolicyFromUser, policies, policyUserDetachment }) => {

    const detachPolicyFn = useCallback((policyId) => {
        detachPolicyFromUser(userId, policyId);
        handleClose();
    },[detachPolicyFromUser, userId])

    const listPoliciesFn = useCallback((after, amount) => {
        listUserPolicies(userId, after, amount)
    }, [userId, listUserPolicies])

    useEffect(() => {
        if (policyUserDetachment.done) {
            resetDetachPolicyFromUser();
            listPoliciesFn()
        }
    }, [policyUserDetachment, resetDetachPolicyFromUser, listPoliciesFn, userId])

    const [show, setShow] = useState(false);  
    const handleClose = () => setShow(false);
    const handleShow = () => setShow(true);
    const [entityId,setEntityId] = useState('');
    const detachConfirmMsg = `are you sure you'd like to detach policy '${entityId}' from user '${userId}'?`;
    return (
        <>
            <div className="action-bar borderless">
                <ButtonToolbar className="float-left mb-2 pl-1">
                    <UserPolicyModalButton userId={userId} onDone={() => { listPoliciesFn() }}/>
                </ButtonToolbar>
                <ButtonToolbar className="float-right mb-2 pr-1">
                    <Button variant="outline-dark" onClick={() => { listPoliciesFn() }}>
                        <SyncIcon/>
                    </Button>
                </ButtonToolbar>
            </div>

            <PaginatedEntryList
                listFn={listPoliciesFn}
                entities={policies}
                emptyState={"No attached policies found"}
                fields={["Policy ID", "Created At", "Detach"]}
                entityToRow={entity => {
                    return [
                        (<Link to={`/auth/policies/${entity.id}`}>{entity.id}</Link>),
                        (moment.unix(entity.creation_date).toISOString()),
                        (<Button size={"sm"} variant="outline-danger" onClick={() => {
                            setEntityId(entity.id);
                            handleShow();
                        }}>Detach</Button>)
                    ]
                }}
            />
            <ConfirmationModal show={show} onHide={handleClose} msg={detachConfirmMsg} 
                onConfirm={() => {
                    detachPolicyFn(entityId);
                }}/>
        </>
    );
});


const UserEffectivePoliciesPane = connect(
    ({ auth }) => ({
        effectivePolicies: auth.userEffectivePoliciesList,
        policyUserDetachment: auth.policyUserDetachment,
    }),
    ({ listUserEffectivePolicies })
)(({ userId, effectivePolicies, listUserEffectivePolicies }) => {

    const listEffectivePoliciesFn = useCallback((after, amount) => {
        listUserEffectivePolicies(userId, after, amount)
    }, [userId, listUserEffectivePolicies])

    return (
        <>
            <div className="action-bar borderless">
                <ButtonToolbar className="float-left mb-2 pl-1">
                    <div><small>All policies attached to this user, through direct attachment or via group memberships</small></div>
                </ButtonToolbar>
                <ButtonToolbar className="float-right mb-2 pr-1">
                    <Button variant="outline-dark" onClick={() => { listEffectivePoliciesFn() }}>
                        <SyncIcon/>
                    </Button>
                </ButtonToolbar>
            </div>

            <PaginatedEntryList
                listFn={listEffectivePoliciesFn}
                entities={effectivePolicies}
                emptyState={"No effective policies found"}
                fields={["Policy ID", "Created At"]}
                entityToRow={entity => {
                    return [
                        (<Link to={`/auth/policies/${entity.id}`}>{entity.id}</Link>),
                        moment.unix(entity.creation_date).toISOString()
                    ]
                }}
            />
        </>
    );
});


const CredentialsCreateButton = connect(
    ({ auth }) => ({ creationStatus: auth.credentialsCreation }),
    ({ createCredentials, resetCreateCredentials })
)(({createCredentials, creationStatus, resetCreateCredentials, userId, onDone }) => {

    const createFn = useCallback((formData) => {
        createCredentials(userId);
    }, [createCredentials, userId]);

    return (
        <EntityCreateButton
            variant={'success'}
            modalSize={"lg"}
            buttonText={'Create Access Key'}
            modalTitle={'Create Access Key'}
            createFn={createFn}
            resetFn={resetCreateCredentials}
            status={creationStatus}
            onDone={onDone}
            createView={(payload) => {
                return (
                    <>
                        <Table>
                            <tbody>
                            <tr>
                                <th>Access Key ID</th>
                                <td><code>{payload.access_key_id}</code></td>
                                <td>
                                    <ClipboardButton variant="secondary" text={payload.access_key_id}/>
                                </td>
                            </tr>
                            <tr>
                                <th>Access Secret Key</th>
                                <td><code>{payload.access_secret_key}</code></td>
                                <td>
                                    <ClipboardButton variant="secondary" text={payload.access_secret_key}/>
                                </td>
                            </tr>
                            </tbody>
                        </Table>
                        <Alert variant="warning">
                            Copy the Access Secret Key and store it somewhere safe.
                            You will not be able to access it again.
                        </Alert>
                    </>
                )
            }}
        >
            <p>
                Create a new Access Key for user <strong>{userId}</strong>?
            </p>
        </EntityCreateButton>
    )
});

export const UserCredentialsPane = connect(
    ({ auth }) => ({
        currentUser: auth.user,
        credentials: auth.credentialsList,
        credentialsDeletionStatus: auth.credentialsDeletion
    }),
    ({ listCredentials, deleteCredentials, resetDeleteCredentials })
)(({ userId, currentUser, credentials, credentialsDeletionStatus, listCredentials, deleteCredentials, resetDeleteCredentials }) => {
    const listCredentialsFn = useCallback((after, amount) => {
        listCredentials(userId, after, amount);
    }, [userId, listCredentials]);

    const deleteCredentialsFn = useCallback((accessKeyId) => {
        deleteCredentials(userId, accessKeyId);
        handleClose();
    }, [deleteCredentials, userId]);

    useEffect(() => {
        if (credentialsDeletionStatus.done) {
            resetDeleteCredentials()
            listCredentialsFn()
        }
    }, [userId, listCredentialsFn, credentialsDeletionStatus, resetDeleteCredentials])
    const [show, setShow] = useState(false);  
    const handleClose = () => setShow(false);
    const handleShow = () => setShow(true);
    const [entityId,setEntityId] = useState('');
    const deleteConfirmMsg = `Are you sure you'd like to delete access key '${entityId}?'`;

    return (
        <>
            <div className="action-bar borderless">
                <ButtonToolbar className="float-left mb-2 pl-1">
                    <CredentialsCreateButton userId={userId} onDone={() => { listCredentialsFn(); }}/>
                </ButtonToolbar>
                <ButtonToolbar className="float-right mb-2 pr-1">
                    <Button variant="outline-dark" onClick={() => { listCredentialsFn() }}>
                        <SyncIcon/>
                    </Button>
                </ButtonToolbar>
            </div>

            <PaginatedEntryList
                listFn={listCredentialsFn}
                entities={credentials}
                emptyState={"No Access Keys found"}
                fields={["Access Key ID", "Issued At", ""]}
                entityToKey={entity => entity.access_key_id}
                entityToRow={entity => {
                    return [
                        (<span>
                                    <code>{entity.access_key_id}</code>
                            {(currentUser.accessKeyId === entity.access_key_id) ?
                                (<strong>{' '} (current)</strong>) : (<span/>)}
                                </span>),
                        (moment.unix(entity.creation_date).toISOString()),
                        (<Button
                            size={"sm"}
                            variant="outline-danger"
                            onClick={() => { setEntityId(entity.access_key_id);
                                             handleShow(); }}
                            disabled={(currentUser.accessKeyId === entity.access_key_id)}
                        >Revoke</Button>)
                    ]
                }}
            />
            <ConfirmationModal show={show} onHide={handleClose} msg={deleteConfirmMsg} 
                onConfirm={() => {
                    deleteCredentialsFn(entityId);
                }}/>
        </>
    )
});


export const UserPage = () => {
    let { userId } = useParams();

    return (
        <Col md={9}>

            <Breadcrumb>
                <Breadcrumb.Item href={`/auth/users`}>Users</Breadcrumb.Item>
                <Breadcrumb.Item active href={`/auth/users/${userId}`}>{userId}</Breadcrumb.Item>
            </Breadcrumb>

            <h2>{userId}</h2>

            <Tabs defaultActiveKey="groups" unmountOnExit={true}>
                <Tab eventKey="groups" title="Group Memberships">
                    <UserGroupPane userId={userId}/>
                </Tab>
                <Tab eventKey="attachedPolicies" title="Directly Attached Policies">
                    <UserAttachedPoliciesPane userId={userId}/>
                </Tab>
                <Tab eventKey="effectivePolicies" title="Effective Attached Policies">
                    <UserEffectivePoliciesPane userId={userId}/>
                </Tab>
                <Tab eventKey="credentials" title="Access Keys">
                    <UserCredentialsPane userId={userId}/>
                </Tab>
            </Tabs>
        </Col>
    )
};
