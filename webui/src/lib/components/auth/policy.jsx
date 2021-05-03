import React, {useEffect, useRef, useState} from "react";

import Table from "react-bootstrap/Table";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import {FormControl} from "react-bootstrap";
import Button from "react-bootstrap/Button";

import {Error, FormattedDate} from "../controls";


export const PolicyEditor = ({ show, onHide, onSubmit, policy = null }) => {
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
            setBody(JSON.stringify({statement: policy.statement}, null, 4));
        }
    }, [policy]);

    const submit = () => {
        const statement = bodyField.current.value;
        try {
            JSON.parse(statement);
        } catch (error) {
            setError(error)
            return false
        }
        const promise = (policy === null) ? onSubmit(idField.current.value, statement) : onSubmit(statement)
        return promise.catch(err => setError(err));
    };

    const hide = () => {
        setError(null);
        onHide();
    };

    return (
        <Modal show={show} onHide={hide}>
            <Modal.Header closeButton>
                <Modal.Title>{(policy === null) ? 'Create' : 'Edit'} Policy</Modal.Title>
            </Modal.Header>

            <Modal.Body>
                <Form onSubmit={e => {
                    e.preventDefault();
                    submit();
                }}>
                    {(policy === null) && (
                        <Form.Group>
                            <FormControl ref={idField} autoFocus placeholder="Policy ID (e.g. 'MyRepoReadWrite')" type="text"/>
                        </Form.Group>
                    )}
                    <Form.Group>
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

            </Modal.Body>

            <Modal.Footer>
                <Button onClick={submit} variant="success">{(policy === null) ? 'Create' : 'Edit'}</Button>
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
