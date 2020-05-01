import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import Alert from "react-bootstrap/Alert";
import React, {useRef} from "react";

import {connect} from "react-redux";
import Col from "react-bootstrap/Col";
import Row from "react-bootstrap/Row";
import Card from "react-bootstrap/Card";

import {doSetupLakeFS, resetSetupLakeFS} from "../actions/setup";

const SetupForm = ({ doSetupLakeFS, resetSetupLakeFS, setupState }) => {
    const emailRef = useRef(null);
    const fullNameRef = useRef(null);

    const onSubmit = (event) => {
        if (setupState.inProgress) return;
        doSetupLakeFS(emailRef.current.value, fullNameRef.current.value);
        event.preventDefault();
    };

    return (
        <Row>
            <Col md={{offset: 2, span: 8}}>
                <Card className="setup-widget">
                    <Card.Header>Initial System Setup</Card.Header>
                    <Card.Body>
                        <Form onSubmit={onSubmit}>

                            <Form.Group controlId="email">
                                <Form.Control type="text" placeholder="Email" ref={emailRef} autoFocus/>
                                <Form.Text className="text-muted">
                                    <em>Admin user that will be used to access lakeFS for the first time</em>
                                </Form.Text>
                            </Form.Group>

                            <Form.Group controlId="fullName">
                                <Form.Control type="text" placeholder="Full Name" ref={fullNameRef}/>
                            </Form.Group>

                            {setupState.error && <Alert variant={"danger"}>{setupState.error}</Alert>}
                            {setupState.payload ?
                                <React.Fragment>
                                <Form.Group as={Row} controlId="formAccessKeyID">
                                    <Form.Label column sm="2">Key ID</Form.Label>
                                    <Col sm="10">
                                        <Form.Control plaintext readOnly defaultValue={setupState.payload.access_key_id}/>
                                    </Col>
                                </Form.Group>

                                <Form.Group as={Row} controlId="formAccessSecretKey">
                                    <Form.Label column sm="2">Secret Key</Form.Label>
                                    <Col sm="10">
                                        <Form.Control plaintext readOnly defaultValue={setupState.payload.access_secret_key}/>
                                    </Col>
                                </Form.Group>
                                </React.Fragment>
                                    :
                                <Button variant="primary" type="submit">Setup</Button>
                            }
                        </Form>
                    </Card.Body>
                </Card>
            </Col>
        </Row>
    );
};

export default connect(
    ({ setup }) => ({ setupState: setup.setupLakeFS }),
    ({ doSetupLakeFS, resetSetupLakeFS })
)(SetupForm);
