import React, {forwardRef, useCallback} from "react";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import Col from "react-bootstrap/Col";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import {Error} from "../../lib/components/controls";

interface AdminUserSetupProps {
    onSubmit: () => Promise<void>;
    setupError: Error;
    disabled: boolean;
}

export const AdminUserSetup = forwardRef<HTMLInputElement, AdminUserSetupProps>(({
    onSubmit,
    setupError,
    disabled,
}, inputRef) => {
    const submitHandler = useCallback((e) => {
        onSubmit();
        e.preventDefault();
    }, [onSubmit]);
    return (
        <Row>
            <Col md={{offset: 2, span: 8}}>
                <Card className="setup-widget">
                    <Card.Header>Initial Setup</Card.Header>
                    <Card.Body>
                        <Card.Text>
                            This process will initialize the database schema and a first admin user to access the system.<br/>
                            <a href="https://docs.lakefs.io/quickstart/repository.html#create-the-first-user" target="_blank" rel="noopener noreferrer">Learn more.</a>
                        </Card.Text>
                        <Form onSubmit={submitHandler}>
                            <Form.Group controlId="user-name">
                                <Form.Control type="text" placeholder="Admin Username" ref={inputRef} autoFocus/>
                            </Form.Group>

                            {!!setupError && <Error error={setupError}/>}
                            <Button variant="primary" disabled={disabled} type="submit">Setup</Button>
                        </Form>
                    </Card.Body>
                </Card>
            </Col>
        </Row>
    );
});