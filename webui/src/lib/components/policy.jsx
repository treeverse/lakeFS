import React, {useEffect, useRef, useState} from "react";

import Table from "react-bootstrap/Table";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import {FormControl} from "react-bootstrap";
import Button from "react-bootstrap/Button";
import {Error, FormattedDate} from "./controls";

export const PolicyEditor = ({ show, onHide, onSubmit, policy = null, noID = false, isCreate = false, validationFunction = null, externalError = null }) => {
    const [error, setError] = useState(null);
    const idField = useRef(null);
    const bodyField = useRef(null);

    useEffect(() => {
        if (policy === null && !!idField.current && idField.current.value === "")
            idField.current.focus();
    });

    const [body, setBody] = useState('')
    useEffect(() => {
        if (policy !== null) {
        const newBody = JSON.stringify(policy, null, 4);
            setBody(newBody);
        setSavedBody(newBody);
        }
    }, [policy]);

    const [savedBody, setSavedBody] = useState(null);

    const submit = () => {
        if (validationFunction) {
            const validationResult = validationFunction(idField.current.value);
            if (!validationResult.isValid) {
                setError(validationResult.errorMessage);
                return;
            }
        }
        const statement = bodyField.current.value;
        try {
            JSON.parse(statement);
        } catch (error) {
            setError(error);
        return false;
        }
        const promise = (policy === null) ? onSubmit(idField.current.value, statement) : onSubmit(statement)
        return promise
        .then((res) => {
        setSavedBody(statement);
        setError(null);
        return res;
        })
        .catch((err) => {
        setError(err);
        return null;
        });
    };

    const hide = () => {
        setError(null);
    if (savedBody !== null) {
        setBody(savedBody);
    }
        onHide();
    };
    const actionName = policy === null || isCreate ? 'Create' : 'Edit'
    return (
        <Modal show={show} onHide={hide}>
            <Modal.Header closeButton>
                <Modal.Title>{actionName} Policy</Modal.Title>
            </Modal.Header>

            <Modal.Body>
                <Form onSubmit={e => {
                    e.preventDefault();
                    submit();
                }}>
                    {(policy === null) && !noID && (
                        <Form.Group className="mb-3">
                            <FormControl ref={idField} autoFocus placeholder="Policy ID (e.g. 'MyRepoReadWrite')" type="text"/>
                        </Form.Group>
                    )}
                    <Form.Group className="mb-3">
                        <FormControl
                            className="policy-document"
                            ref={bodyField}
                            placeholder="Policy JSON Document"
                            rows={15}
                            as="textarea"
                            type="text"
                            onChange={e => setBody(e.target.value)}
                            value={body}/>
                    </Form.Group>
                </Form>

                {(!!error) && <Error className="mt-3" error={error}/>}
                {(!!externalError) && <Error className="mt-3" error={externalError}/>}

            </Modal.Body>

            <Modal.Footer>
                <Button onClick={submit} variant="success">Save</Button>
                <Button onClick={hide} variant="secondary">Cancel</Button>
            </Modal.Footer>
        </Modal>
    );
};

export const PolicyDisplay = ({ policy, asJSON }) => {
    let childComponent;
    if (asJSON) {
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
                <FormattedDate dateValue={policy.creation_date}/>
            </p>
            {childComponent}
        </div>
    );
};
