import {connect} from "react-redux";
import {
    getPolicy,
    listPolicies,
    deletePolicies,
    resetDeletePolicies,
    createPolicy,
    resetCreatePolicy, resetEditPolicy, editPolicy
} from "../../actions/auth";
import React, {useCallback, useEffect, useRef, useState} from "react";
import {Button, ButtonToolbar, Col, Modal, Table} from "react-bootstrap";
import {SyncIcon, CopyIcon, PencilIcon} from "@primer/octicons-react";
import {EntityCreateButton, PaginatedEntryList} from "./entities";
import {Link, useParams, useHistory} from "react-router-dom";
import * as moment from "moment";
import Breadcrumb from "react-bootstrap/Breadcrumb";
import Form from "react-bootstrap/Form";
import Alert from "react-bootstrap/Alert";
import ConfirmationModal from "../ConfirmationModal";


export const PoliciesPage = connect(
    ({ auth }) => ({
        policiesList: auth.policiesList,
        deletionStatus: auth.policyDeletion,
        creationStatus: auth.policyCreation
    }),
    ({ listPolicies, deletePolicies, createPolicy, resetCreatePolicy, resetDeletePolicies })
)(({ listPolicies, policiesList, deletionStatus, deletePolicies, resetDeletePolicies, createPolicy, resetCreatePolicy, creationStatus }) => {

    const [update, setUpdate] = useState(new Date());

    const history = useHistory();

    const [checkedPolicies, setCheckedPolicies] = useState([]);

    const createFn = useCallback((formData) => {
        createPolicy(formData.id, formData.policy)
    }, [createPolicy])

    const deleteSelectedPolicies = () => {
        deletePolicies(checkedPolicies);
        handleClose();
    }

    useEffect(() => {
        if (deletionStatus.done) {
            resetDeletePolicies();
            setCheckedPolicies([]);
            listPolicies();
        }
    }, [deletionStatus, resetDeletePolicies, listPolicies]);

    const [show, setShow] = useState(false);  
    const handleClose = () => setShow(false);
    const handleShow = () => setShow(true);
    const confirmDeleteMsg = `are you sure you'd like to delete policies: ${checkedPolicies.join(', ')}?`;

    return (
        <Col lg={9}>
            <div className={"mb-2"}>
                <div className="action-bar borderless">
                    <ButtonToolbar className="float-left mb-2 pl-1">
                        <EntityCreateButton
                            variant={'success'}
                            buttonText={'Create Policy'}
                            modalTitle={'Create a new Policy'}
                            createFn={createFn}
                            resetFn={resetCreatePolicy}
                            onDone={() => {
                                history.push(`/auth/policies/${creationStatus.payload.id}`);
                            }}
                            status={creationStatus}>
                            <Form.Group>
                                <Form.Control type="text" name="id" placeholder="Policy ID (i.e. 'RepoReaders')"/>
                            </Form.Group>
                            <Form.Group>
                                <Form.Control as="textarea" rows="15" name="policy" placeholder="Policy JSON Document" className="policy-document"/>
                            </Form.Group>
                        </EntityCreateButton>
                        <Button variant="danger" disabled={checkedPolicies.length < 1} onClick={handleShow}>
                            Delete Selected
                        </Button>
                    </ButtonToolbar>

                    <ButtonToolbar className="float-right mb-2 pr-1">
                        <Button variant="outline-dark" onClick={() => { setUpdate(new Date()); }}>
                            <SyncIcon/>
                        </Button>
                    </ButtonToolbar>
                    <ConfirmationModal show={show} onHide={handleClose} msg={confirmDeleteMsg} onConfirm={deleteSelectedPolicies}/>
                </div>
            </div>

            <Form>
                <PaginatedEntryList
                    listFn={listPolicies}
                    entities={policiesList}
                    emptyState={"No policies found"}
                    update={update}
                    fields={["", "Policy ID", "Created At"]}
                    entityToRow={entity => {
                        return [
                            (
                                <Form.Group>
                                    <Form.Check type="checkbox" name={entity.id} onChange={(e) => {
                                        if (e.currentTarget.checked) {
                                            // add it
                                            if (checkedPolicies.indexOf(entity.id) === -1) {
                                                setCheckedPolicies([...checkedPolicies, entity.id]);
                                            }
                                        } else {
                                            // remove it
                                            setCheckedPolicies(checkedPolicies.filter(id => id !== entity.id));
                                        }
                                    }}/>
                                </Form.Group>
                            ),
                            (<Link to={`/auth/policies/${entity.id}`}>{entity.id}</Link>),
                            moment.unix(entity.creation_date).toISOString()
                        ]
                    }}
                />
            </Form>
        </Col>
    );
});

export const PolicyPage = connect(
    ({auth}) => ({
        policy: auth.policy,
        editStatus: auth.policyEdit,
        createStatus: auth.policyCreation,
    }),
    ({ getPolicy, editPolicy, createPolicy, resetCreatePolicy, resetEditPolicy }))
(({ getPolicy, policy, editPolicy, resetEditPolicy, editStatus, createStatus, createPolicy, resetCreatePolicy }) => {
    let { policyId } = useParams();
    let history = useHistory();

    useEffect(() => {
        getPolicy(policyId);
    }, [getPolicy, policyId]);

    const [asJsonToggle, setAsJsonToggle] = useState(false);
    const [showEditor, setShowEditor] = useState(false);
    const [editMode, setEditMode] = useState('edit');

    const editor = useRef(null);
    const idEditor = useRef(null);
    const editValue = (!!policy.payload) ? JSON.stringify({statement: policy.payload.statement}, null, 4) : "";

    const onEdit = () => {
        if (editMode === 'edit') {
            editPolicy(policyId, editor.current.value);
        } else {
            createPolicy(idEditor.current.value, editor.current.value);
        }
    }

    useEffect(() => {
        if (createStatus.done) {
            setShowEditor(false);
            resetCreatePolicy();
            history.push(`/auth/policies/${createStatus.payload.id}`);
        }
    }, [createStatus, resetCreatePolicy, history])

    useEffect(() => {
        if (editStatus.done) {
            resetEditPolicy();
            setShowEditor(false);
            getPolicy(policyId);
        }
    }, [policyId, editStatus, resetEditPolicy, getPolicy])

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
                    }} variant="outline-dark">
                        {(asJsonToggle) ? "Table View" : "JSON Document View"}
                    </Button>
                    {' '}
                    <Button size={"sm"} onClick={(e) => {
                        e.preventDefault();
                        setEditMode('edit');
                        setShowEditor(true);
                        e.target.blur();
                    }} variant="primary"><PencilIcon/> Edit</Button>
                    {' '}
                    <Button size={"sm"} onClick={(e) => {
                        e.preventDefault();
                        setEditMode('clone');
                        setShowEditor(true);
                        e.target.blur();
                    }} variant="success">
                        <CopyIcon/>
                        Clone
                    </Button>
                </div>
            </h2>

            <Modal show={showEditor} onHide={() => { setShowEditor(false) }}>
                <Modal.Header>
                    {(editMode === 'clone') ? "Clone Policy": "Edit Policy"}
                </Modal.Header>
                <Modal.Body>
                    <Form onSubmit={onEdit}>
                        {(editMode === 'clone') ? (
                        <Form.Group>
                            <Form.Control name="policyId" placeholder="Policy ID, e.g. ('MyRepoRead')" defaultValue={`CopyOf${policyId}`} ref={idEditor}/>
                        </Form.Group>
                        ) : null}
                        <Form.Group>
                            <Form.Control as="textarea" rows="15" name="policy" placeholder="Policy JSON Document" defaultValue={editValue} ref={editor} className="policy-document"/>
                        </Form.Group>
                    </Form>
                    {(!!editStatus.error) ? (
                        <Alert variant="danger">{editStatus.error}</Alert>
                    ) : null}
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" onClick={() => { setShowEditor(false) }}>
                        Cancel
                    </Button>
                    <Button variant="success" onClick={onEdit}>
                        Save
                    </Button>
                </Modal.Footer>
            </Modal>


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
        childComponent = (<pre className={"policy-body"}>{JSON.stringify({statement: policy.statement}, null, 4)}</pre>);
    } else {
        childComponent = (
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
                            <td><strong style={{'color': (statement.effect === "allow") ? 'green':'red'}}>{statement.effect}</strong></td>
                        </tr>
                    );
                })}
                </tbody>
            </Table>

        );
    }

    return (
        <div>
            <p>
                <strong>Created At: </strong>
                {moment.unix(policy.creation_date).toISOString()}
            </p>
            {childComponent}
        </div>
    );
}
