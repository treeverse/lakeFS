import React, {useEffect, useRef, useState} from 'react';
import Form from "react-bootstrap/Form";
import Alert from "react-bootstrap/Alert";
import {Warnings} from "./controls";
import {FloatingLabel} from "react-bootstrap";
import Accordion from "react-bootstrap/Accordion";

const DEFAULT_BLOCKSTORE_EXAMPLE = "e.g. s3://example-bucket/";
const DEFAULT_BLOCKSTORE_VALIDITY_REGEX = new RegExp(`^s3://`);

export const RepositoryCreateForm = ({ id, config, onSubmit, formValid, setFormValid, error = null, sampleRepoChecked = false }) => {
    const repoValidityRegex = /^[a-z0-9][a-z0-9-]{2,62}$/;

    const [repoValid, setRepoValid] = useState(null);
    const defaultNamespacePrefix = config.default_namespace_prefix

    const [storageNamespaceValid, setStorageNamespaceValid] = useState(defaultNamespacePrefix ? true : null);
    const [defaultBranchValid, setDefaultBranchValid] = useState(true);
    
    const storageNamespaceField = useRef(null);
    const defaultBranchField = useRef(null);
    const repoNameField = useRef(null);
    const sampleDataCheckbox = useRef(null);

    useEffect(() => {
        if (sampleDataCheckbox.current) {
            sampleDataCheckbox.current.checked = sampleRepoChecked;
        }
    }, [sampleRepoChecked, sampleDataCheckbox.current]);


    const onRepoNameChange = () => {
        const isRepoValid = repoValidityRegex.test(repoNameField.current.value);
        setRepoValid(isRepoValid);
        setFormValid(isRepoValid && storageNamespaceValid && defaultBranchValid);
        if (defaultNamespacePrefix) {
            storageNamespaceField.current.value = defaultNamespacePrefix + repoNameField.current.value
            checkStorageNamespaceValidity()
        }
    };

    const checkStorageNamespaceValidity = () => {
        const isStorageNamespaceValid = storageNamespaceValidityRegex.test(storageNamespaceField.current.value);
        setStorageNamespaceValid(isStorageNamespaceValid);
        setFormValid(isStorageNamespaceValid && defaultBranchValid && repoValidityRegex.test(repoNameField.current.value));
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

    const sampleCheckbox = (
      <Form.Group controlId="sampleData" className="mt-3">
          <Form.Check ref={sampleDataCheckbox} type="checkbox" label="Add sample data, hooks, and configuration" />
      </Form.Group>
    );

    const flattenForm = !defaultNamespacePrefix;

    const basicSettings = (
      <>
          <Form.Group className="mb-3">
              <Form.Text>
                  A repository contains all of your objects, including the revision history. <a href="https://docs.lakefs.io/understand/model.html#repository" target="_blank" rel="noopener noreferrer">Learn more.</a>
              </Form.Text>
          </Form.Group>
          <Form.Group controlId="id">
              <FloatingLabel label="Repository ID" controlId="repositryIdControl">
                  <Form.Control type="text" ref={repoNameField} onChange={onRepoNameChange} placeholder="my-data-lake"/>
              </FloatingLabel>
              {repoValid === false &&
                <Form.Text className="text-danger">
                    Min 3 characters. Only lowercase alphanumeric characters and {'\'-\''} allowed.
                </Form.Text>
              }
          </Form.Group>

          {!flattenForm && sampleCheckbox}
      </>
    )

    const advancedSettings = (
      <>
          <Form.Group controlId="defaultBranch" className="mt-3">
              <FloatingLabel label="Default Branch Name" controlId="defaultBranchNameCtrl">
                  <Form.Control type="text" ref={defaultBranchField} defaultValue={"main"}  placeholder="main" onChange={checkDefaultBranchValidity}/>
              </FloatingLabel>
              {defaultBranchValid === false &&
                <Form.Text className="text-danger">
                    Invalid Branch.
                </Form.Text>
              }
          </Form.Group>
          <Form.Group className="mt-3">
              <FloatingLabel label="Storage Namespace" controlId="storageNamespaceCtrl">
                  <Form.Control type="text" ref={storageNamespaceField} placeholder={storageNamespaceExample} onChange={checkStorageNamespaceValidity} aria-describedby="namespaceHelpText"/>
              </FloatingLabel>
              <Form.Text muted id="namespaceHelpText">
                  Where should data be stored? (e.g. <code>{storageNamespaceExample}</code>)
              </Form.Text>
              {storageNamespaceValid === false &&
                <p>
                    <Form.Text className="text-danger">
                        {"Can only create repository with storage type: " + storageType}
                    </Form.Text>
                </p>
              }
          </Form.Group>
          {flattenForm && sampleCheckbox}
      </>
    );

    const creationForm = (defaultNamespacePrefix) ? (
      <>
        <section className="repository-creation-expanded">
          {basicSettings}
        </section>
        <Accordion className="repository-creation-accordion">
            <Accordion.Item eventKey={"1"}>
                <Accordion.Header>Advanced Settings</Accordion.Header>
                <Accordion.Body>
                    {advancedSettings}
                </Accordion.Body>
            </Accordion.Item>
        </Accordion>
      </>
    ) : (
      <section className="repository-creation-expanded">
          {basicSettings}
          {advancedSettings}
      </section>
    )

    return (
        <Form id={id} onSubmit={(e) => {
            e.preventDefault();
            if (!formValid) {
                return;
            }
            onSubmit({
                name: repoNameField.current.value,
                storage_namespace: storageNamespaceField.current.value,
                default_branch: defaultBranchField.current.value,
                sample_data: sampleDataCheckbox.current.checked,
            });
        }}>
            <h4 className="mb-3">Create A New Repository</h4>

            {creationForm}

            {config?.warnings &&
              <Warnings warnings={config.warnings}/>
            }

            {error &&
                <div className="mb-3">
                    <Alert variant={"danger"}>{error.message}</Alert>
                </div>
            }
        </Form>
    );
}
