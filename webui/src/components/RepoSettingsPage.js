import React, {useRef, useState, useEffect} from "react";
import {connect} from "react-redux";
import Form from "react-bootstrap/Form";
import {Container} from "react-bootstrap";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import {TrashIcon} from "@primer/octicons-react";
import Modal from "react-bootstrap/Modal";
import {deleteRepository, listRepositories} from "../actions/repositories";
import {useHistory} from "react-router-dom";


const DeleteRepositoryModal = ({repo, show, onSubmit, onCancel}) => {

    const [isDisabled, setIsDisabled] = useState(true);
    const repoNameField = useRef(null);

    const compareRepoName = () => {
        if (repoNameField.current.value === repo) {
            setIsDisabled(false);
        }
        else {
            setIsDisabled(true);
        }
    }

    return (
        <Modal show={show} onHide={onCancel} size="lg">
            <Modal.Header closeButton>
                <Modal.Title>Delete Repository</Modal.Title>
            </Modal.Header>
            <Modal.Body>
                Are you sure you wish to delete repository <strong>{repo}</strong>? <br />
                This action cannot be undone. This will delete the following: <br /> <br />

                <ul>
                    <li>All commits</li>
                    <li>All branches</li>
                    <li>All tags</li>
                    <li>All repository configuration</li>
                </ul>
                
                Data in the underlying object store will not be deleted by this action. <br /> <br />

                Please type <strong>{repo}</strong> to confirm: <br />
                <Form.Control className="mt-2" type="text" autoFocus ref={repoNameField} onChange={compareRepoName}/>  
            </Modal.Body>
            <Modal.Footer>
                <Button disabled={isDisabled} variant="danger" onClick={onSubmit}>I understand the consequences, delete this repository</Button>
            </Modal.Footer>
        </Modal>
    );
};

const RepoSettingsPage = connect(
    ({ repositories }) => ({ deleteStatus: repositories.delete }),
    ({ deleteRepository, listRepositories })
)(({repo, deleteStatus, deleteRepository, listRepositories}) => {

    const [showingDeleteModal, setShowDeleteModal] = useState(false);
    const history = useHistory();

    const deleteRepo = () => {
        if (deleteStatus.inProgress) {
            return;
        }
        setShowDeleteModal(false);
        deleteRepository(repo.id);
    }

    useEffect(()=> {
        if (deleteStatus.done) {
            history.push('/repositories');
        }
        listRepositories();
    }, [listRepositories, deleteStatus.done, history]);

    const body = (
        <>
            <div className="section-title"><h4>General</h4></div>
            <Container>
                <Row>
                    <Form.Label column md={{span:2}} className="mb-3">Repository name</Form.Label>
                    <Col md={{span:4}}><Form.Control readOnly value={repo.id} type="text"/></Col>
                </Row>
                <Row>
                    <Form.Label column md={{span:2}} className="mb-3">Storage namespace</Form.Label>
                    <Col md={{span:4}}><Form.Control readOnly value={repo.storage_namespace} type="text"/></Col>
                </Row>
                <Row>
                    <Form.Label column md={{span:2}} className="mb-3">Default branch</Form.Label>
                    <Col md={{span:4}}><Form.Control readOnly value={repo.default_branch} type="text"/></Col>
                </Row>
            </Container>
            <Button className="float-left mt-3" onClick={() => {setShowDeleteModal(true);}} variant="danger"><TrashIcon/> Delete this repository</Button>
            <DeleteRepositoryModal
                repo={repo.id}
                show={showingDeleteModal}
                onSubmit={deleteRepo}
                onCancel={() => {setShowDeleteModal(false)}}/>
        </>
    );
    return (
        <div className="mt-3 mb-5">
            <div className="action-bar">
                <h3>Settings</h3>
            </div>

            {body}
        </div>
    );
});

export default connect()(RepoSettingsPage);
