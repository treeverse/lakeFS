import React, {useEffect, useRef, useState} from 'react';

import Form from "react-bootstrap/Form";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import {Warnings} from "../../lib/components/controls";
import {InfoIcon} from "@primer/octicons-react";
import Tooltip from "react-bootstrap/Tooltip";
import {OverlayTrigger} from "react-bootstrap";

const DEFAULT_BLOCKSTORE_EXAMPLE = "e.g. s3://example-bucket/";
const DEFAULT_BLOCKSTORE_VALIDITY_REGEX = new RegExp(`^s3://`);

export const RepositoryCreateForm = ({ config, onSubmit, onCancel, error = null, inProgress = false, sm = 6 }) => {
    const fieldNameOffset = 3;
    const repoValidityRegex = /^[a-z0-9][a-z0-9-]{2,62}$/;

    const [formValid, setFormValid] = useState(false);
    const [repoValid, setRepoValid] = useState(null);
    const defaultNamespacePrefix = config.default_namespace_prefix

    const [storageNamespaceValid, setStorageNamespaceValid] = useState(defaultNamespacePrefix ? true : null);
    const [defaultBranchValid, setDefaultBranchValid] = useState(true);

    const storageNamespaceField = useRef(null);
    const defaultBranchField = useRef(null);
    const repoNameField = useRef(null);

    const onRepoNameChange = () => {
        const isRepoValid = repoValidityRegex.test(repoNameField.current.value);
        setRepoValid(isRepoValid);
        setFormValid(isRepoValid && storageNamespaceValid && defaultBranchValid);
        if (isRepoValid && defaultNamespacePrefix) {
            storageNamespaceField.current.value = defaultNamespacePrefix + "/" + repoNameField.current.value
            checkStorageNamespaceValidity()
        }
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

    const storageType = config.blockstore_type
    const storageNamespaceValidityRegexStr = config ? config.blockstore_namespace_ValidityRegex : DEFAULT_BLOCKSTORE_VALIDITY_REGEX;
    const storageNamespaceValidityRegex = RegExp(storageNamespaceValidityRegexStr);
    const storageNamespaceExample = config ? config.blockstore_namespace_example : DEFAULT_BLOCKSTORE_EXAMPLE;

    useEffect(() => {
        if (repoNameField.current) {
            repoNameField.current.focus();
        }
    }, []);

    return (
        <Form onSubmit={(e) => {
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
        {config?.warnings && <Warnings warnings={config.warnings}/>}

            <Form.Group as={Row} controlId="id" className="mb-3">
                <Form.Label column sm={fieldNameOffset}>Repository ID</Form.Label>
                <Col sm={sm}>
                    <Form.Control type="text" ref={repoNameField} onChange={onRepoNameChange}/>
                    {repoValid === false &&
                    <Form.Text className="text-danger">
                        Min 3 characters. Only lowercase alphanumeric characters and {'\'-\''} allowed.
                    </Form.Text>
                    }
                </Col>
            </Form.Group>
            <Form.Group as={Row} className="mb-3">
                <Form.Label column sm={fieldNameOffset}>
                    <span>Storage Namespace&nbsp;
                    <OverlayTrigger placement="bottom" overlay={<Tooltip style={{"font-size": "s"}}>What should I type here?</Tooltip>}>
                        <a href="https://docs.lakefs.io/setup/create-repo.html#create-the-repository" target={"_blank"} tabIndex="-1" rel="noopener noreferrer">
                            <InfoIcon />
                        </a>
                    </OverlayTrigger></span>
                </Form.Label>
                <Col sm={sm}>
                    <Form.Control type="text" ref={storageNamespaceField} placeholder={storageNamespaceExample} onChange={checkStorageNamespaceValidity} />
                    {storageNamespaceValid === false &&
                    <Form.Text className="text-danger">
                        {"Can only create repository with storage type: " + storageType}
                    </Form.Text>
                    }
                </Col>
            </Form.Group>
            <Form.Group as={Row} controlId="defaultBranch" className="mb-3">
                <Form.Label column sm={fieldNameOffset}>Default Branch</Form.Label>
                <Col sm={sm}>
                    <Form.Control type="text" ref={defaultBranchField} placeholder="defaultBranch" defaultValue={"main"} onChange={checkDefaultBranchValidity}/>
                    {defaultBranchValid === false &&
                    <Form.Text className="text-danger">
                        Invalid Branch.
                    </Form.Text>
                    }
                </Col>
            </Form.Group>

            {error &&
            <Row className="mb-3">
                <Col md={{span: sm, offset: fieldNameOffset}} >
                    <Alert variant={"danger"}>{error.message}</Alert>
                </Col>
            </Row>}

            <Row className="mb-3">
                <Col md={{span: sm, offset: fieldNameOffset}} >
                    <Button variant="success" type="submit" className="me-2" disabled={!formValid || inProgress}>
                        { inProgress ? 'Creating...' : 'Create Repository' }
                    </Button>
                    <Button variant="secondary" onClick={(e) => {
                        e.preventDefault();
                        onCancel();
                    }}>Cancel</Button>
                </Col>
            </Row>
        </Form>
    );
}

export default RepositoryCreateForm;
