import React, {useRef, useState} from "react";

import {RepositoryPageLayout} from "../../../lib/components/repository/layout";
import {RefContextProvider, useRefs} from "../../../lib/hooks/repo";
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import {TrashIcon} from "@primer/octicons-react";
import Col from "react-bootstrap/Col";
import {Error, Loading} from "../../../lib/components/controls";
import Modal from "react-bootstrap/Modal";
import {repositories} from "../../../lib/api";
import {useRouter} from "../../../lib/hooks/router";


const DeleteRepositoryModal = ({repo, show, onSubmit, onCancel}) => {
    const [isDisabled, setIsDisabled] = useState(true);
    const repoNameField = useRef(null);

    const compareRepoName = () => {
        setIsDisabled(repoNameField.current.value !== repo.id);
    };

    return (
        <Modal show={show} onHide={onCancel} size="lg">
            <Modal.Header closeButton>
                <Modal.Title>Delete Repository</Modal.Title>
            </Modal.Header>
            <Modal.Body>
                Are you sure you wish to delete repository <strong>{repo.id}</strong>? <br />
                This action cannot be undone. This will delete the following: <br /> <br />

                <ul>
                    <li>All commits</li>
                    <li>All branches</li>
                    <li>All tags</li>
                    <li>All repository configuration</li>
                </ul>

                Data in the underlying object store will not be deleted by this action. <br /> <br />

                Please type <strong>{repo.id}</strong> to confirm: <br />
                <Form.Control className="mt-2" placeholder="Enter repository name to confirm" type="text" autoFocus ref={repoNameField} onChange={compareRepoName}/>
            </Modal.Body>
            <Modal.Footer>
                <Button disabled={isDisabled} variant="danger" onClick={onSubmit}>I understand the consequences, delete this repository</Button>
            </Modal.Footer>
        </Modal>
    );
};

const SettingsContainer = () => {
    const router = useRouter();
    const { repo, loading, error} = useRefs();
    const [showingDeleteModal, setShowDeleteModal] = useState(false);
    const [ deletionError, setDeletionError ] = useState(null);

    if (loading) return <Loading/>;
    if (!!error) return <Error error={error}/>;
    if (!!deletionError) return <Error error={deletionError}/>;

    return (
        <div className="mt-3 mb-5">

            <div className="section-title">
                <h4>General</h4>
            </div>

            <Container>
                <Row>
                    <Form.Label column md={{span:2}} className="mb-3">
                        Repository name
                    </Form.Label>
                    <Col md={{span:4}}>
                        <Form.Control readOnly value={repo.id} type="text"/>
                    </Col>
                </Row>
                <Row>
                    <Form.Label column md={{span:2}} className="mb-3">
                        Storage namespace
                    </Form.Label>
                    <Col md={{span:4}}>
                        <Form.Control readOnly value={repo.storage_namespace} type="text"/>
                    </Col>
                </Row>
                <Row>
                    <Form.Label column md={{span:2}} className="mb-3">
                        Default branch
                    </Form.Label>
                    <Col md={{span:4}}>
                        <Form.Control readOnly value={repo.default_branch} type="text"/>
                    </Col>
                </Row>
            </Container>

            <Button variant="danger" className="mt-3" onClick={() => setShowDeleteModal(!showingDeleteModal)}>
                <TrashIcon/> Delete this repository
            </Button>

            <DeleteRepositoryModal
                repo={repo}
                onCancel={() => { setShowDeleteModal(false) }}
                onSubmit={() => {
                    repositories.delete(repo.id).catch(err => {
                        setDeletionError(err)
                    }).then(() => {
                        return router.push('/repositories')
                    })
                }}
                show={showingDeleteModal}/>
        </div>
    );
};


const RepositorySettingsPage = () => {
    return (
        <RefContextProvider>
            <RepositoryPageLayout activePage={'settings'}>
                <SettingsContainer/>
            </RepositoryPageLayout>
        </RefContextProvider>
    )
}



export default RepositorySettingsPage;