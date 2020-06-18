import {connect} from "react-redux";

import React, {useEffect, useState, useRef, useCallback} from "react";
import {Row, Col, Nav, Card, Table, ButtonToolbar, Button, Alert} from "react-bootstrap";
import {SyncIcon} from "@primer/octicons-react";
import {Link} from "react-router-dom";

import * as moment from "moment"

import {Switch, Route, useHistory, useLocation, useParams, Redirect} from "react-router-dom";
import {
    listUsers,
    listGroups,
    listPolicies,
    listCredentials,
    listUserGroups,
    listUserPolicies,
    listUserEffectivePolicies,
    getPolicy,
    listGroupPolicies,
    createUser,
    resetCreateUser,
    createCredentials,
    resetCreateCredentials,
    resetCreateGroup,
    createGroup,
    listGroupMembers,
    resetAddUserToGroup,
    addUserToGroup,
    removeUserFromGroup,
    resetRemoveUserFromGroup,
    attachPolicyToUser,
    resetAttachPolicyToUser,
    resetDetachPolicyFromUser, detachPolicyFromUser, resetDeleteCredentials, deleteCredentials
} from "../actions/auth";
import Breadcrumb from "react-bootstrap/Breadcrumb";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import ClipboardButton from "./ClipboardButton";
import Tabs from "react-bootstrap/Tabs";
import Tab from "react-bootstrap/Tab";

export const AuthManagementPage = () => {
    return (
        <div className="mt-5 auth-page">
            <Row>
                <AuthNav/>
                <Switch>
                    <Route exact path={"/auth"}>
                        <Redirect to={"/auth/credentials"}/>
                    </Route>
                    <Route path={"/auth/users/:userId"}>
                        <UserPage/>
                    </Route>
                    <Route path={"/auth/users"}>
                        <UsersPage/>
                    </Route>
                    <Route path={"/auth/groups/:groupId"}>
                        <GroupPage/>
                    </Route>
                    <Route path={"/auth/groups"}>
                        <GroupsPage/>
                    </Route>
                    <Route path={"/auth/policies/:policyId"}>
                        <PolicyPage/>
                    </Route>
                    <Route path={"/auth/policies"}>
                        <PoliciesPage/>
                    </Route>
                    <Route path={"/auth/credentials"}>
                        <CredentialsPage/>
                    </Route>
                </Switch>
            </Row>
        </div>
    );
};


const AuthNav = () => {

    const location = useLocation();
    const history = useHistory();
    const isActive = prefix => location.pathname.indexOf(prefix) === 0;
    const onSelect = (href, e) => {
        e.preventDefault();
        history.push(href);
    };

    return (
        <Col lg={3}>
            <Card>
                <Card.Header>
                    Access Control
                </Card.Header>
                <Card.Body>

                    <Nav variant="pills" className="flex-column">
                        <Nav.Item>
                            <Nav.Link href={"/auth/credentials"} onSelect={onSelect} active={isActive("/auth/credentials")}>My Credentials</Nav.Link>
                        </Nav.Item>
                    </Nav>

                    <hr/>

                    <Nav variant="pills" className="flex-column">
                        <Nav.Item>
                            <Nav.Link href={"/auth/users"} onSelect={onSelect} active={isActive("/auth/users")}>Users</Nav.Link>
                        </Nav.Item>
                        <Nav.Item>
                            <Nav.Link href={"/auth/groups"} onSelect={onSelect} active={isActive("/auth/groups")}>Groups</Nav.Link>
                        </Nav.Item>
                        <Nav.Item>
                            <Nav.Link href={"/auth/policies"} onSelect={onSelect} active={isActive("/auth/policies")}>Policies</Nav.Link>
                        </Nav.Item>
                    </Nav>

                </Card.Body>
            </Card>
        </Col>
    );
}

const EntityCreateButton = ({ createFn, resetFn, status, variant, buttonText, modalTitle, children, onDone = null, createView = null, modalSize="md", onShow = null, createButtonText = "Create" }) => {
   const [show, setShow] = useState(false);
   const [showCreateView, setShowCreateView] = useState(false);

    const formRef = useRef(null);

    const disabled = (status.inProgress);

    const onHide = () => {
        if (disabled) return;
        setShow(false);
        setShowCreateView(false);
        resetFn();
    };

    const onSubmit = () => {
        if (disabled) return;
        createFn(serializeForm(formRef));
    };



    useEffect(() => {
        if (status.done) {
            if (onDone !== null)
                onDone();

            if (createView !== null) {
                setShowCreateView(true);
            } else {
                setShow(false);
            }
            resetFn();
        }
    }, [resetFn, createView, onDone, status.done]);


    const content = (createView !== null &&  showCreateView) ? createView(status.payload) : children;
    const showCreateButton = ((createView !== null && !showCreateView) || createView === null);

    const serializeForm = () => {
        const elements = formRef.current.elements;
        const formData = {};
        for (let i = 0; i < elements.length; i++) {
            const current = elements[i];
            if ((current.type === "radio" || current.type === "checkbox") && !current.checked) {
               continue;
            }
            formData[current.name] = current.value;
        }
        return formData;
    }

   return (
       <>
           <Button variant={variant} onClick={() => {
               if (!show && onShow !== null) onShow();
               setShow(!show)
           }} disabled={disabled}>{buttonText}</Button>
           <Modal enforceFocus={false} show={show} onHide={onHide} size={modalSize}>
               <Modal.Header closeButton>
                   <Modal.Title>{modalTitle}</Modal.Title>
               </Modal.Header>
               <Modal.Body>
                   <Form ref={formRef} onSubmit={(e) => {
                       onSubmit();
                       e.preventDefault();
                   }}>
                       {content}
                   </Form>
                   {(!!status.error) ? (<Alert variant="danger">{status.error}</Alert>) : (<span/>)}
               </Modal.Body>

               {(showCreateButton) ? (
                   <Modal.Footer>
                       <Button variant="secondary" disabled={disabled} onClick={onHide}>
                           Cancel
                       </Button>
                       <Button variant="success" disabled={disabled} onClick={onSubmit}>
                           {createButtonText}
                       </Button>
                   </Modal.Footer>
               ) : (<></>)}
           </Modal>
       </>
   );
}

const UsersPage = connect(
    ({ auth }) => ({ usersList: auth.usersList, createUserStatus: auth.userCreation }),
    ({ listUsers, createUser, resetCreateUser })
)(({ listUsers, usersList, createUser, createUserStatus, resetCreateUser }) => {

    const [update, setUpdate] = useState(new Date());
    const history = useHistory();

    const createFn = useCallback((formData) => {
        createUser(formData.id)
    }, [createUser])

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
                            idPlaceholderText={'User ID (e.g. "jane.doe")'}
                            onDone={() => {
                                history.push(`/auth/users/${createUserStatus.payload.id}`);
                            }}
                            status={createUserStatus}>
                                <Form.Group>
                                    <Form.Control type="text" name="id" placeholder="User ID (e.g. 'jane.doe')"/>
                                </Form.Group>
                        </EntityCreateButton>

                    </ButtonToolbar>
                    <ButtonToolbar className="float-right mb-2 pr-1">
                        <Button variant="outline-dark" onClick={() => { setUpdate(new Date()); }}>
                            <SyncIcon/>
                        </Button>
                    </ButtonToolbar>
                </div>
            </div>

            <PaginatedEntryList
                listFn={listUsers}
                entities={usersList}
                emptyState={"No users found"}
                update={update}
                fields={["User ID", "Created At"]}
                entityToRow={entity => {
                    return [
                        (<Link to={`/auth/users/${entity.id}`}>{entity.id}</Link>),
                        moment.unix(entity.creation_date).toISOString()
                    ]
                }}
            />
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


const UserPage = connect(
    ({auth}) => ({
        currentUser: auth.user,
        policies: auth.userPoliciesList,
        effectivePolicies: auth.userEffectivePoliciesList,
        groups: auth.userGroupsList,
        credentials: auth.credentialsList,
        groupMembershipDeletion: auth.groupMembershipDeletion,
        policyUserDetachment: auth.policyUserDetachment,
        credentialsDeletionStatus: auth.credentialsDeletion
    }),
    ({ listUserGroups, removeUserFromGroup, resetRemoveUserFromGroup, listUserPolicies, listUserEffectivePolicies, listCredentials, detachPolicyFromUser, resetDetachPolicyFromUser, deleteCredentials, resetDeleteCredentials }))
(({ listUserGroups, removeUserFromGroup, resetRemoveUserFromGroup, listUserPolicies, listUserEffectivePolicies,
      listCredentials, effectivePolicies, policies, groups, credentials, groupMembershipDeletion,
      detachPolicyFromUser, resetDetachPolicyFromUser, policyUserDetachment,
      deleteCredentials, credentialsDeletionStatus, currentUser }) => {
    let { userId } = useParams();

    const listGroupsFn = useCallback((after, amount) => {
        listUserGroups(userId, after, amount)
    }, [listUserGroups, userId])

    const listPoliciesFn = useCallback((after, amount) => {
        listUserPolicies(userId, after, amount)
    }, [userId, listUserPolicies])

    const listEffectivePoliciesFn = useCallback((after, amount) => {
        listUserEffectivePolicies(userId, after, amount)
    }, [userId, listUserEffectivePolicies])

    const listCredentialsFn = useCallback((after, amount) => {
        listCredentials(userId, after, amount);
    }, [userId, listCredentials]);

    const detachGroupFn = useCallback((groupId) => {
        if (window.confirm(`are you sure you'd like to remove user '${userId}' from group '${groupId}'?`))
            removeUserFromGroup(userId, groupId)
    }, [removeUserFromGroup, userId])

    const detachPolicyFn = useCallback((policyId) => {
        if (window.confirm(`are you sure you'd like to detach policy '${policyId}' from user '${userId}'?`))
            detachPolicyFromUser(userId, policyId)
    },[detachPolicyFromUser, userId])

    useEffect(() => {
        if (groupMembershipDeletion.done) {
            resetRemoveUserFromGroup();
            listGroupsFn()
            listEffectivePoliciesFn()
        }
    }, [groupMembershipDeletion, resetRemoveUserFromGroup, listEffectivePoliciesFn, listGroupsFn, userId])


    useEffect(() => {
        if (policyUserDetachment.done) {
            resetDetachPolicyFromUser();
            listPoliciesFn()
            listEffectivePoliciesFn()
        }
    }, [policyUserDetachment, resetDetachPolicyFromUser, listEffectivePoliciesFn, listPoliciesFn, userId])

    const deleteCredentialsFn = useCallback((accessKeyId) => {
        if (window.confirm(`Are you sure you'd like to delete key pair '${accessKeyId}?'`))
            deleteCredentials(userId, accessKeyId);
    }, [userId]);

    useEffect(() => {
        if (credentialsDeletionStatus.done) {
            resetDeleteCredentials()
            listCredentialsFn()
        }
    }, [userId, credentialsDeletionStatus, resetDeleteCredentials])

    const groupDetachForm = useRef(null);



    return (
        <Col md={9}>

            <Breadcrumb>
                <Breadcrumb.Item href={`/auth/users`}>Users</Breadcrumb.Item>
                <Breadcrumb.Item active href={`/auth/users/${userId}`}>{userId}</Breadcrumb.Item>
            </Breadcrumb>

            <h2>{userId}</h2>

            <Tabs defaultActiveKey="groups">
                <Tab eventKey="groups" title="Group Memberships">

                    <div className="action-bar borderless">
                        <ButtonToolbar className="float-left mb-2 pl-1">
                            <UserMembershipModalButton userId={userId} onDone={() => {
                                listGroupsFn()
                                listEffectivePoliciesFn()
                            }}/>
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
                            fields={["Group ID", "Created At", "Remove"]}
                            entityToRow={entity => {
                                return [
                                    (<Link to={`/auth/groups/${entity.id}`}>{entity.id}</Link>),
                                    (moment.unix(entity.creation_date).toISOString()),
                                    (<Button size={"sm"} variant="outline-danger" onClick={() => {
                                        detachGroupFn(entity.id)
                                    }}>Detach</Button>)
                                ]
                            }}
                        />
                    </Form>
                </Tab>
                <Tab eventKey="attachedPolicies" title="Directly Attached Policies">

                    <div className="action-bar borderless">
                        <ButtonToolbar className="float-left mb-2 pl-1">
                            <UserPolicyModalButton userId={userId} onDone={() => {
                                listPoliciesFn()
                                listEffectivePoliciesFn()
                            }}/>
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
                                    detachPolicyFn(entity.id)
                                }}>Detach</Button>)
                            ]
                        }}
                    />
                </Tab>
                <Tab eventKey="effectivePolicies" title="Effective Attached Policies">

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
                </Tab>
                <Tab eventKey="credentials" title="Key Pairs">
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
                        emptyState={"No Key Pairs found"}
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
                                    onClick={() => { deleteCredentialsFn(entity.access_key_id); }}
                                    disabled={(currentUser.accessKeyId === entity.access_key_id)}
                                >Revoke</Button>)
                            ]
                        }}
                    />
                </Tab>
            </Tabs>
        </Col>
    )
});


const GroupsPage = connect(
    ({auth }) => ({ groupsList: auth.groupsList, createGroupStatus: auth.groupCreation }),
    ({ listGroups, resetCreateGroup, createGroup })
)(({ listGroups, createGroup, groupsList, createGroupStatus, resetCreateGroup }) => {

    const [update, setUpdate] = useState(new Date());

    const history = useHistory();

    const createFn = useCallback((formData) => {
        createGroup(formData.id)
    }, [createGroup])

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
                    </ButtonToolbar>
                    <ButtonToolbar className="float-right mb-2 pr-1">
                        <Button variant="outline-dark" onClick={() => { setUpdate(new Date()); }}>
                            <SyncIcon/>
                        </Button>
                    </ButtonToolbar>
                </div>
            </div>

            <PaginatedEntryList
                listFn={listGroups}
                entities={groupsList}
                emptyState={"No groups found"}
                update={update}
                fields={["Group ID", "Created At"]}
                entityToRow={entity => {
                    return [
                        (<Link to={`/auth/groups/${entity.id}`}>{entity.id}</Link>),
                        moment.unix(entity.creation_date).toISOString()
                    ]
                }}
            />
        </Col>
    );
});

const GroupPage = connect(
    ({auth}) => ({
        policies: auth.groupPoliciesList,
        members: auth.groupMemberList
    }),
    ({ listGroupPolicies, listGroupMembers }))
(({ listGroupPolicies, policies, listGroupMembers, members }) => {
    let { groupId } = useParams();

    const listPoliciesFn = useCallback((after, amount) => {
        listGroupPolicies(groupId, after, amount)
    }, [groupId, listGroupPolicies])

    const listMembersFn = useCallback((after, amount) => {
        listGroupMembers(groupId, after, amount)
    }, [groupId, listGroupMembers])

    return (
        <Col md={9}>

            <Breadcrumb>
                <Breadcrumb.Item href={`/auth/groups`}>Groups</Breadcrumb.Item>
                <Breadcrumb.Item active href={`/auth/groups/${groupId}`}>{groupId}</Breadcrumb.Item>
            </Breadcrumb>

            <h2>{groupId}</h2>

            <Tabs defaultActiveKey="members">
                <Tab eventKey="members" title="Members">
                    <PaginatedEntryList
                        listFn={listMembersFn}
                        entities={members}
                        emptyState={"No group members found"}
                        fields={["User ID", "Created At"]}
                        entityToRow={entity => {
                            return [
                                (<Link to={`/auth/users/${entity.id}`}>{entity.id}</Link>),
                                moment.unix(entity.creation_date).toISOString()
                            ]
                        }}
                    />

                </Tab>
                <Tab eventKey="policies" title="Attached Policies">
                    <PaginatedEntryList
                        listFn={listPoliciesFn}
                        entities={policies}
                        emptyState={"No attached policies found"}
                        fields={["Policy ID", "Created At"]}
                        entityToRow={entity => {
                            return [
                                (<Link to={`/auth/policies/${entity.id}`}>{entity.id}</Link>),
                                moment.unix(entity.creation_date).toISOString()
                            ]
                        }}
                    />
                </Tab>
            </Tabs>
        </Col>
    )
});



const PoliciesPage = connect(
    ({ auth }) => ({ policiesList: auth.policiesList }),
    ({ listPolicies })
)(({ listPolicies, policiesList }) => {

    const [update, setUpdate] = useState(new Date());

    return (
        <Col lg={9}>
            <div className={"mb-2"}>
                <div className="action-bar borderless">
                    <ButtonToolbar className="float-left mb-2 pl-1">
                        <Button variant={'success'}>
                            Add Policy
                        </Button>
                    </ButtonToolbar>
                    <ButtonToolbar className="float-right mb-2 pr-1">
                        <Button variant="outline-dark" onClick={() => { setUpdate(new Date()); }}>
                            <SyncIcon/>
                        </Button>
                    </ButtonToolbar>
                </div>
            </div>

            <PaginatedEntryList
                listFn={listPolicies}
                entities={policiesList}
                emptyState={"No policies found"}
                update={update}
                fields={["Policy ID", "Created At"]}
                entityToRow={entity => {
                    return [
                        (<Link to={`/auth/policies/${entity.id}`}>{entity.id}</Link>),
                        moment.unix(entity.creation_date).toISOString()
                    ]
                }}
            />
        </Col>
    );
});

const PolicyPage = connect(
    ({auth}) => ({
        policy: auth.policy,
    }),
    ({ getPolicy }))
(({ getPolicy, policy }) => {
    let { policyId } = useParams();

    useEffect(() => {
        getPolicy(policyId);
    }, [getPolicy, policyId]);

    const [asJsonToggle, setAsJsonToggle] = useState(false);

    return (
        <Col md={9}>
            <Breadcrumb>
                <Breadcrumb.Item href={`/auth/policies`}>Policies</Breadcrumb.Item>
                <Breadcrumb.Item active href={`/auth/policies/${policyId}`}>{policyId}</Breadcrumb.Item>
            </Breadcrumb>

            <h2>
                {policyId}
                <div className={"float-right"}>
                    <Button size={"sm"} onClick={(e) => {
                        e.preventDefault();
                        setAsJsonToggle(!asJsonToggle);
                        e.target.blur();
                    }} variant={"secondary"}>{(asJsonToggle) ? "Table View" : "JSON Document View"}</Button>
                </div>
            </h2>

            <hr/>

            <div>
                {(policy.loading) ? (
                    <span>Loading...</span>
                ) : (
                    <PolicyDisplay policy={policy.payload} asJsonToggle={asJsonToggle}/>
                )}
            </div>
        </Col>
    )
});

const PolicyDisplay = ({ policy, asJsonToggle }) => {

      let childComponent;
    if (asJsonToggle) {
        childComponent = (<pre className={"policy-body"}>{JSON.stringify(policy, null, 4)}</pre>);
    } else {
        childComponent = (
            <div>

                <p>
                    <strong>Created At: </strong>
                    {moment.unix(policy.creation_date).toISOString()}
                </p>
                <Table>
                    <thead>
                    <tr>
                        <th>Actions</th>
                        <th>Resource</th>
                        <th>Effect</th>
                    </tr>
                    </thead>
                    <tbody>
                    {policy.statement.map((statement, i) => {
                        return (
                            <tr key={`statement-${i}`}>
                                <td><code>{statement.action.join(", ")}</code></td>
                                <td><code>{statement.resource}</code></td>
                                <td><strong style={{'color': (statement.effect === "Allow") ? 'green':'red'}}>{statement.effect}</strong></td>
                            </tr>
                        );
                    })}
                    </tbody>
                </Table>
            </div>
        );
    }

    return (
        <div>
            {childComponent}
        </div>
    );
}


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
            buttonText={'Create Key Pair'}
            modalTitle={'Create Key Pair'}
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
                Create a new Key Pair for user <strong>{userId}</strong>?
            </p>
        </EntityCreateButton>
    )
});


const CredentialsPage = connect(
    ({ auth }) => ({
        credentialsList: auth.credentialsList,
        user: auth.user,
        deletionStatus: auth.credentialsDeletion
    }),
    ({ listCredentials, deleteCredentials, resetDeleteCredentials })
)(({ listCredentials, deleteCredentials, resetDeleteCredentials, credentialsList, user, deletionStatus }) => {

    const [update, setUpdate] = useState(new Date());
    const userId = user.id;

    const listFn = useCallback((after, amount) => {
        listCredentials(userId, after, amount)
    }, [listCredentials, userId]);

    const deleteCredentialsFn = useCallback((accessKeyId) => {
        if (window.confirm(`Are you sure you'd like to delete key pair '${accessKeyId}?'`))
        deleteCredentials(userId, accessKeyId);
    }, [userId]);

    useEffect(() => {
        if (deletionStatus.done) {
            resetDeleteCredentials()
            listFn()
        }
    }, [userId, deletionStatus, resetDeleteCredentials])

    return (
        <Col lg={9}>

            <div className={"mb-2"}>
                <div className="action-bar borderless">
                    <ButtonToolbar className="float-left mb-2 pl-1">
                        <CredentialsCreateButton userId={userId} onDone={() => { listFn(); }}/>
                    </ButtonToolbar>
                    <ButtonToolbar className="float-right mb-2 pr-1">
                        <Button variant="outline-dark" onClick={() => { setUpdate(new Date()); }}>
                            <SyncIcon/>
                        </Button>
                    </ButtonToolbar>
                </div>
            </div>

            <PaginatedEntryList
                listFn={listFn}
                entities={credentialsList}
                emptyState={"No Key Pairs found"}
                update={update}
                fields={["Access Key ID", "Issued At", ""]}
                entityToKey={entity => entity.access_key_id}
                entityToRow={entity => {
                    return [
                        (<span>
                            <code>{entity.access_key_id}</code>
                                {(user.accessKeyId === entity.access_key_id) ?
                                    (<strong>{'  '}(current)</strong>) : (<span/>)}
                        </span>),
                        (moment.unix(entity.creation_date).toISOString()),
                        (<Button
                            size={"sm"}
                            variant="outline-danger"
                            onClick={() => { deleteCredentialsFn(entity.access_key_id); }}
                            disabled={(user.accessKeyId === entity.access_key_id)}
                        >Revoke</Button>)
                    ]
                }}
            />
        </Col>
    );
});


const PaginatedEntryList = ({ listFn, entities, entityToRow, entityToKey, emptyState, fields, update}) => {
    const [offsets, setOffsets] = useState([""]);
    const [currentOffset, setCurrentOffset] = useState("");

    useEffect(() => {
        listFn(currentOffset);
    },[listFn, currentOffset, update]);


    const addNextOffset = (offset) => {
        if (offsets.indexOf(offset) === -1) {
            const updated = [...offsets, offset];
            offsets.sort();
            return updated;
        }
        return offsets;
    }

    const getPreviousOffset = () => {
        const offsetIndex = offsets.indexOf(currentOffset);
        if (offsetIndex === -1 || offsetIndex === 0) {
            return null;
        }
        return offsets[offsetIndex-1];
    }

    return <EntityList
        entities={entities}
        emptyState={emptyState}
        offsets={offsets}
        fields={fields}
        entityToRow={entityToRow}
        entityToKey={entityToKey}
        onNext={(nextOffset) => {
            setOffsets(addNextOffset(nextOffset));
            setCurrentOffset(nextOffset);
        }}
        hasPrevious={getPreviousOffset() !== null}
        onPrevious={() => {
            setCurrentOffset(getPreviousOffset);
        }}
    />
}

const EntityList = ({ entities, fields, entityToRow, entityToKey = entity => entity.id, emptyState, onNext, onPrevious, hasPrevious }) => {

    if (!entityToKey) {
        entityToKey = entity => entity.id;
    } 
    
    if (entities.loading) {
        return <p>Loading...</p>;
    }

    if (!!entities.error) {
        return <Alert variant={"danger"}>{entities.error}</Alert>
    }

    if (entities.payload.pagination.results === 0) {
        return <Alert variant="warning">{emptyState}</Alert>
    }

    return (
        <div>
            <Table>
                <thead>
                    <tr>
                        {(fields.map(field  => {
                            return <th key={field}>{field}</th>
                        }))}
                    </tr>
                </thead>
                <tbody>
                {entities.payload.results.map(entity => {
                    return (
                        <tr key={entityToKey(entity)}>
                            {entityToRow(entity).map((cell, i) => {
                                return <td key={`${entityToKey(entity)}-cell-${i}`}>{cell}</td>
                            })}
                        </tr>
                    );
                })}
                </tbody>
            </Table>


                <p className="tree-paginator">
                    {(hasPrevious) ? (
                        <Button variant="outline-primary" onClick={() => {onPrevious() }}>
                            &lt;&lt; Previous Page
                        </Button>
                    ) : (null)}
                    {'  '}
                    {(entities.payload.pagination.has_more) ? (
                    <Button variant="outline-primary" onClick={() => {
                        onNext(entities.payload.pagination.next_offset)
                    }}>
                        Next Page &gt;&gt;
                    </Button>) : (null)}
                </p>


        </div>
    )
}