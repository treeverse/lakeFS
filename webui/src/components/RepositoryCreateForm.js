import React, {useRef, useState, useEffect} from 'react';

import Form from "react-bootstrap/Form";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import {connect} from "react-redux";
import {getConfig} from "../actions/config";


const DEFAULT_BLOCKSTORE_TYPE = "s3";

export const RepositoryCreateForm = connect(
  ({ repositories, config }) => {
    const {create} = repositories;
    return {create, config: config.config};
  }, {getConfig})(({ error, onSubmit, onCancel, create, config, getConfig, sm = 6 }) => {
    const fieldNameOffset = 3;
    const repoValidityRegex = /^[a-z0-9][a-z0-9-]{2,62}$/;

    const [formValid, setFormValid] = useState(false);
    const [repoValid, setRepoValid] = useState(true);
    const [storageNamespaceValid, setStorageNamespaceValid] = useState(true);
    const [defaultBranchValid, setDefaultBranchValid] = useState(true);
    const storageNamespaceField = useRef(null);
    const defaultBranchField = useRef(null);
    const repoNameField = useRef(null);

    const checkRepoValidity = () => {
        const isRepoValid = repoValidityRegex.test(repoNameField.current.value)
        setRepoValid(isRepoValid);
        setFormValid(isRepoValid && storageNamespaceValid && defaultBranchValid);
    };

    const checkStorageNamespaceValidity = () => {
        const isStorageNamespaceValid = storageNamespaceValidityRegex.test(storageNamespaceField.current.value)
        setStorageNamespaceValid(isStorageNamespaceValid);
        setFormValid(isStorageNamespaceValid && defaultBranchValid && repoValid);
    };

    const checkDefaultBranchValidity = () => {
        const isBranchValid = defaultBranchField.current.value.length;
        setDefaultBranchValid(isBranchValid);
        setFormValid(isBranchValid && storageNamespaceValid && repoValid);
    };

    useEffect(() => {
        getConfig()
    }, [getConfig]);

    const blockstoreType = config.payload ? config.payload['blockstore.type'] : DEFAULT_BLOCKSTORE_TYPE;
    const storageNamespaceValidityRegex = new RegExp(`^(${blockstoreType}):/.*$`);
    const storageNamespaceExample = `e.g. ${blockstoreType}://example-bucket/`;
    return (
        <Form className={"mt-5"} onSubmit={(e) => {
            e.preventDefault();
            if (!formValid) {
                return;
            }
            onSubmit({
                name: repoNameField.current.value,
                storage_namespace: storageNamespaceField.current.value,
                default_branch: defaultBranchField.current.value
            });
        }}>
            <Form.Group as={Row} controlId="id">
                <Form.Label column sm={fieldNameOffset}>Repository ID</Form.Label>
                <Col sm={sm}>
                    <Form.Control type="text" autoFocus ref={repoNameField} onChange={checkRepoValidity}/>
                    {!repoValid &&
                        <Form.Text className="text-danger">
                            Min 2 characters. Only lowercase alphanumeric characters and '-' allowed.
                        </Form.Text>
                    }
                </Col>
            </Form.Group>

            <Form.Group as={Row}>
                <Form.Label column sm={fieldNameOffset}>Storage Namespace</Form.Label>
                    <Col sm={sm}>
                        <Form.Control type="text" ref={storageNamespaceField} placeholder={storageNamespaceExample} onChange={checkStorageNamespaceValidity}/>
                        {!storageNamespaceValid &&
                            <Form.Text className="text-danger">
                                Invalid Storage Namespace.
                            </Form.Text>
                        }
                    </Col>
            </Form.Group>
            <Form.Group as={Row} controlId="defaultBranch">
                <Form.Label column sm={fieldNameOffset}>Default Branch</Form.Label>
                <Col sm={sm}>
                    <Form.Control type="text" ref={defaultBranchField} placeholder="defaultBranch" defaultValue={"master"} onChange={checkDefaultBranchValidity}/>
                    {!defaultBranchValid &&
                        <Form.Text className="text-danger">
                            Invalid Branch.
                        </Form.Text>
                    }
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