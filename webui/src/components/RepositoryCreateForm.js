import React, {useRef, useState} from 'react';

import Form from "react-bootstrap/Form";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import {connect} from "react-redux";

const DEFAULT_BLOCKSTORE_TYPE = "s3";

export const RepositoryCreateForm = connect(
  ({ repositories, config }) => {
    const {create} = repositories;
    return {
      create,
      config: config.config,
    };
  })(({ error, onSubmit, onCancel, create, sm = 6, config }) => {
    const fieldNameOffset = 3;

    const [formValid, setFormValid] = useState(false);
    const storageNamespaceField = useRef(null);
    const defaultBranchField = useRef(null);
    const repoIdField = useRef(null);

    const checkValidity = () => {
        if (repoIdField.current.value.length === 0 ||
            storageNamespaceField.current.value.length === 0 ||
            defaultBranchField.current.value.length === 0) {
            setFormValid(false);
            return;
        }
        setFormValid(true);
    };

    let blockstoreType = config.payload == null ? DEFAULT_BLOCKSTORE_TYPE : config.payload['blockstore.type']

    return (
        <Form className={"mt-5"} onSubmit={(e) => {
            e.preventDefault();
            if (!formValid) {
                return;
            }
            onSubmit({
                id: repoIdField.current.value,
                storage_namespace: storageNamespaceField.current.value,
                default_branch: defaultBranchField.current.value
            });
        }}>
            <Form.Group as={Row} controlId="id">
                <Form.Label column sm={fieldNameOffset}>Repository ID</Form.Label>
                <Col sm={sm}>
                    <Form.Control type="text" autoFocus ref={repoIdField} onChange={checkValidity}/>
                </Col>
            </Form.Group>

            <Form.Group as={Row}>
                <Form.Label column sm={fieldNameOffset}>Storage Namespace</Form.Label>
                    <Col sm={sm}>
                        <Form.Control type="text" ref={storageNamespaceField} placeholder={`e.g. ${blockstoreType}://example-bucket/`} onChange={checkValidity}/>
                    </Col>
            </Form.Group>
            <Form.Group as={Row} controlId="defaultBranch">
                <Form.Label column sm={fieldNameOffset}>Default Branch</Form.Label>
                <Col sm={sm}>
                    <Form.Control type="text" ref={defaultBranchField} placeholder="defaultBranch" defaultValue={"master"} onChange={checkValidity}/>
                </Col>
            </Form.Group>

            {error &&
                <Row>
                    <Col md={{span: sm, offset: fieldNameOffset}} >
                        <Alert variant={"danger"}>{error}</Alert>
                    </Col>
                </Row>}

            <Row>
                <Col md={{span: sm, offset: fieldNameOffset}} >
                    <Button variant="success" type="submit" className="mr-2" disabled={!formValid || create.inProgress}>
                        { create.inProgress ? 'Creating...' : 'Create Repository' }
                    </Button>
                    <Button variant="secondary" onClick={(e) => {
                        e.preventDefault();
                        onCancel();
                    }}>Cancel</Button>
                </Col>
            </Row>
        </Form>
    );
});