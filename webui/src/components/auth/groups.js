import {connect} from "react-redux";
import {
    addUserToGroup, attachPolicyToGroup,
    createGroup,
    deleteGroups, detachPolicyFromGroup,
    listGroupMembers,
    listGroupPolicies,
    listGroups,
    listPolicies,
    listUsers,
    removeUserFromGroup,
    resetAddUserToGroup, resetAttachPolicyToGroup,
    resetCreateGroup,
    resetDeleteGroups, resetDetachPolicyFromGroup,
    resetRemoveUserFromGroup
} from "../../actions/auth";
import React, {useCallback, useEffect, useRef, useState} from "react";
import {Link, useHistory, useParams} from "react-router-dom";
import {Button, ButtonToolbar, Col} from "react-bootstrap";
import {EntityCreateButton, PaginatedEntryList} from "./entities";
import Form from "react-bootstrap/Form";
import {SyncIcon} from "@primer/octicons-react";
import * as moment from "moment";
import Breadcrumb from "react-bootstrap/Breadcrumb";
import Tabs from "react-bootstrap/Tabs";
import Tab from "react-bootstrap/Tab";
import ConfirmationModal from "../ConfirmationModal";


export const GroupsPage = connect(
    ({auth }) => ({
        groupsList: auth.groupsList,
        createGroupStatus: auth.groupCreation,
        deletionStatus: auth.groupDeletion
    }),
    ({ listGroups, resetCreateGroup, createGroup, deleteGroups, resetDeleteGroups })
)(({ listGroups, createGroup, groupsList, createGroupStatus, resetCreateGroup, deleteGroups, resetDeleteGroups, deletionStatus }) => {

    const [update, setUpdate] = useState(new Date());
    const [checkedGroups, setCheckedGroups] = useState([]);

    const history = useHistory();

    const createFn = useCallback((formData) => {
        createGroup(formData.id)
    }, [createGroup])

    const deleteSelectedGroups = () => {
        deleteGroups(checkedGroups);
        handleClose();
    }

    useEffect(() => {
        if (deletionStatus.done) {
            resetDeleteGroups();
            setCheckedGroups([]);
            listGroups();
        }
    }, [deletionStatus, resetDeleteGroups, listGroups]);

    const [show, setShow] = useState(false);  
    const handleClose = () => setShow(false);
    const handleShow = () => setShow(true);
    const deleteConfirmMsg = `are you sure you'd like to delete groups: ${checkedGroups.join(', ')}?`;

    return (
        <Col lg={9}>
            <div className={"mb-2"}>
                <div className="action-bar borderless">
                    <ButtonToolbar className="float-left mb-2 pl-1">
                        <EntityCreateButton
                            variant={'success'}
                            buttonText={'Add A Group'}
                            modalTitle={'Add a new Group'}
                            createFn={createFn}
                            resetFn={resetCreateGroup}
                            onDone={() => {
                                history.push(`/auth/groups/${createGroupStatus.payload.id}`);
                            }}
                            status={createGroupStatus}>
                            <Form.Group>
                                <Form.Control type="text" name="id" placeholder="Group ID (i.e. 'Developers')"/>
                            </Form.Group>
                        </EntityCreateButton>
                        <Button variant="danger" disabled={checkedGroups.length < 1} onClick={handleShow}>
                            Delete Selected
                        </Button>
                    </ButtonToolbar>
                    <ButtonToolbar className="float-right mb-2 pr-1">
                        <Button variant="outline-dark" onClick={() => { setUpdate(new Date()); }}>
                            <SyncIcon/>
                        </Button>
                    </ButtonToolbar>
                    <ConfirmationModal show={show} onHide={handleClose} msg={deleteConfirmMsg} onConfirm={deleteSelectedGroups}/>
                </div>
            </div>

            <Form>
                <PaginatedEntryList
                    listFn={listGroups}
                    entities={groupsList}
                    emptyState={"No groups found"}
                    update={update}
                    fields={["", "Group ID", "Created At"]}
                    entityToRow={entity => {
                        return [
                            (
                                <Form.Group>
                                    <Form.Check type="checkbox" name={entity.id} onChange={(e) => {
                                        if (e.currentTarget.checked) {
                                            // add it
                                            if (checkedGroups.indexOf(entity.id) === -1) {
                                                setCheckedGroups([...checkedGroups, entity.id]);
                                            }
                                        } else {
                                            // remove it
                                            setCheckedGroups(checkedGroups.filter(id => id !== entity.id));
                                        }
                                    }}/>
                                </Form.Group>
                            ),
                            (<Link to={`/auth/groups/${entity.id}`}>{entity.id}</Link>),
                            moment.unix(entity.creation_date).toISOString()
                        ]
                    }}
                />
            </Form>
        </Col>
    );
});


const GroupPolicyModalButton = connect(
    ({ auth }) => ({  policies: auth.policiesList,  status: auth.policyGroupAttachment }),
    ({ attachPolicyToGroup, resetAttachPolicyToGroup, listPolicies })
)(({ groupId, policies, status, listPolicies, attachPolicyToGroup, resetAttachPolicyToGroup, onDone }) => {

    const createFn = useCallback((form) => {
        attachPolicyToGroup(groupId, form.policy);
    }, [groupId, attachPolicyToGroup])

    return (
        <EntityCreateButton
            variant={'success'}
            buttonText={'Attach Policy'}
            modalTitle={'Select Policy'}
            createButtonText={'Attach to Group'}
            createFn={createFn}
            resetFn={resetAttachPolicyToGroup}
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



const GroupMembershipModalButton = connect(
    ({ auth }) => ({ currentMembers: auth.groupMemberList, allUsers: auth.usersList,  status: auth.groupMembershipCreation }),
    ({ listUsers, addUserToGroup, resetAddUserToGroup })
)(({ groupId, currentMembers, allUsers, status, listUsers, addUserToGroup, resetAddUserToGroup, onDone }) => {

    const createFn = useCallback((form) => {
        addUserToGroup(form.user, groupId);
    }, [groupId, addUserToGroup])

    return (
        <EntityCreateButton
            variant={'success'}
            buttonText={'Add user to Group'}
            modalTitle={'Select Group'}
            createButtonText={'Add To Group'}
            createFn={createFn}
            resetFn={resetAddUserToGroup}
            onDone={onDone}
            status={status}>
            <Form.Group>
                <PaginatedEntryList
                    listFn={listUsers}
                    entities={allUsers}
                    emptyState={"No users found"}
                    fields={["Add", "User ID", "Created At"]}
                    entityToRow={entity => {
                        return [
                            (<Form.Check type="radio" value={entity.id} name="user"/>),
                            (<Link to={`/auth/users/${entity.id}`}>{entity.id}</Link>),
                            (moment.unix(entity.creation_date).toISOString())
                        ]
                    }}
                />
            </Form.Group>
        </EntityCreateButton>
    );
});


const GroupMembersPane = connect(
    ({ auth }) => ({
        members: auth.groupMemberList,
        groupMembershipDeletion: auth.groupMembershipDeletion,
    }),
    ({ listGroupMembers, removeUserFromGroup, resetRemoveUserFromGroup })
)(({ groupId, listGroupMembers, removeUserFromGroup, resetRemoveUserFromGroup, members, groupMembershipDeletion }) => {

    const userDetachForm = useRef(null);

    const listMembersFn = useCallback((after, amount) => {
        listGroupMembers(groupId, after, amount)
    }, [groupId, listGroupMembers]);

    const detachUserFn = useCallback((userId) => {
        removeUserFromGroup(userId, groupId);
        handleClose();
    }, [removeUserFromGroup, groupId])


    useEffect(() => {
        if (groupMembershipDeletion.done) {
            resetRemoveUserFromGroup();
            listMembersFn()
        }
    }, [groupMembershipDeletion, resetRemoveUserFromGroup, listMembersFn, groupId])

    const [show, setShow] = useState(false);  
    const handleClose = () => setShow(false);
    const handleShow = () => setShow(true);
    const [entityId , setEntityId] = useState('');
    const detachConfirmMsg  = `are you sure you'd like to remove user '${entityId}' from group '${groupId}'?`;
 
    return (
        <>
            <div className="action-bar borderless">
                <ButtonToolbar className="float-left mb-2 pl-1">
                    <GroupMembershipModalButton groupId={groupId} onDone={() => { listMembersFn() }}/>
                </ButtonToolbar>
                <ButtonToolbar className="float-right mb-2 pr-1">
                    <Button variant="outline-dark" onClick={() => { listMembersFn() }}>
                        <SyncIcon/>
                    </Button>
                </ButtonToolbar>
            </div>

            <Form ref={userDetachForm}>
                <PaginatedEntryList
                    listFn={listMembersFn}
                    entities={members}
                    emptyState={"No group members found"}
                    fields={["User ID", "Created At", ""]}
                    entityToRow={entity => {
                        return [
                            (<Link to={`/auth/users/${entity.id}`}>{entity.id}</Link>),
                            moment.unix(entity.creation_date).toISOString(),
                            (<Button size={"sm"} variant="outline-danger" 
                            onClick={() => {
                                handleShow();
                                setEntityId(entity.id);
                            }}>
                            Remove</Button>)
                        ]
                    }}
                />
            </Form>
            <ConfirmationModal show={show} onHide={handleClose} 
                                msg={detachConfirmMsg }
                                onConfirm = {() => {
                                detachUserFn(entityId);
                            }}/>
        </>
    );
});

const GroupPoliciesPane = connect(
    ({ auth }) => ({
        policies: auth.groupPoliciesList,
        policyGroupDetachment: auth.policyGroupDetachment,
    }),
    ({ listGroupPolicies, detachPolicyFromGroup, resetDetachPolicyFromGroup })
)(({ groupId, listGroupPolicies, detachPolicyFromGroup, resetDetachPolicyFromGroup, policies, policyGroupDetachment }) => {

    const listPoliciesFn = useCallback((after, amount) => {
        listGroupPolicies(groupId, after, amount)
    }, [groupId, listGroupPolicies]);

    const detachPolicyFn = useCallback((policyId) => {
        detachPolicyFromGroup(groupId, policyId);
        handleClose();
    },[detachPolicyFromGroup, groupId])

    useEffect(() => {
        if (policyGroupDetachment.done) {
            resetDetachPolicyFromGroup();
            listPoliciesFn()
        }
    }, [policyGroupDetachment, resetDetachPolicyFromGroup, listPoliciesFn, groupId])

    const [show, setShow] = useState(false);  
    const handleClose = () => setShow(false);
    const handleShow = () => setShow(true);
    const [entityId , setEntityId] = useState('');
    
    const detachConfirmMsg = `are you sure you'd like to detach policy '${entityId}' from group '${groupId}'?`;

    return (
        <>
            <div className="action-bar borderless">
                <ButtonToolbar className="float-left mb-2 pl-1">
                    <GroupPolicyModalButton groupId={groupId} onDone={() => { listPoliciesFn() }}/>
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
                        moment.unix(entity.creation_date).toISOString(),
                        (<Button size={"sm"} variant="outline-danger" 
                        onClick={() => {
                            handleShow();
                            setEntityId(entity.id);
                        }}>
                        Detach</Button>)
                    ]
                }}
            />
            <ConfirmationModal show={show} onHide={handleClose} 
                            msg={detachConfirmMsg} 
                            onConfirm = {() => {
                            detachPolicyFn(entityId);
                        }}/>
        </>


    );
});

export const GroupPage = () => {
    let { groupId } = useParams();

    return (
        <Col md={9}>

            <Breadcrumb>
                <Breadcrumb.Item href={`/auth/groups`}>Groups</Breadcrumb.Item>
                <Breadcrumb.Item active href={`/auth/groups/${groupId}`}>{groupId}</Breadcrumb.Item>
            </Breadcrumb>

            <h2>{groupId}</h2>

            <Tabs defaultActiveKey="members" unmountOnExit={true}>
                <Tab eventKey="members" title="Members">
                    <GroupMembersPane groupId={groupId}/>
                </Tab>
                <Tab eventKey="policies" title="Attached Policies">
                    <GroupPoliciesPane groupId={groupId}/>
                </Tab>
            </Tabs>
        </Col>
    );
};
