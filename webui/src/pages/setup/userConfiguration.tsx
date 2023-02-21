import React, {ChangeEvent, FC, FormEvent, useCallback, useState} from "react";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import Col from "react-bootstrap/Col";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import {Error} from "../../lib/components/controls";

interface UserConfigurationProps {
    onSubmit: (email: string, admin: string, updatesCheck: boolean, securityCheck: boolean) => Promise<void>;
    setupError: Error;
    disabled: boolean;
}

export const UserConfiguration: FC<UserConfigurationProps> = ({
    onSubmit,
    setupError,
    disabled,
}) => {
    const [userEmail, setUserEmail] = useState<string>("");
    const [adminUser, setAdminUser] = useState<string>("admin");
    const [updatesCheck, setUpdatesCheck] = useState<boolean>(false);
    const [securityCheck, setSecurityCheck] = useState<boolean>(true);

    const submitHandler = useCallback((e: FormEvent) => {
        onSubmit(adminUser, userEmail, updatesCheck, securityCheck);
        e.preventDefault();
    }, [onSubmit, adminUser, userEmail, updatesCheck, securityCheck]);

    const handleEmailChange = useCallback((e: ChangeEvent<HTMLInputElement>) => {
        setUserEmail(e.target.value);
    }, [setUserEmail]);

    const handleAdminUserChange = useCallback((e: ChangeEvent<HTMLInputElement>) => {
        setAdminUser(e.target.value);
    }, [setAdminUser]);

    const handleUpdatesChange = useCallback((e: ChangeEvent<HTMLInputElement>) => {
        setUpdatesCheck(e.target.checked);
    }, [setUpdatesCheck]);

    const handleSecurityChange = useCallback((e: ChangeEvent<HTMLInputElement>) => {
        setSecurityCheck(e.target.checked);
    }, [setSecurityCheck]);

    return (
        <Row>
            <Col md={{offset: 2, span: 8}}>
                <Card className="setup-widget">
                    <Card.Header>User Configuration</Card.Header>
                    <Card.Body>
                        <Card.Text>
                            Please specify the name of the first admin account to create, or leave it as the default.
                        </Card.Text>

                        <Form onSubmit={submitHandler}>

                        <Form.Group controlId="user-name" className="mb-3">
                            <Form.Control type="text" value={adminUser}  onChange={handleAdminUserChange} placeholder="Admin Username" autoFocus/>
                        </Form.Group>

                            <Card.Text>
                            Please provide your email address, and indicate what optional communications you&apos;d like to receive:
                            </Card.Text>
                            <Form.Group controlId="user-email" className="mt-4">
                                <Form.Label>Email <span className="required-field-label">*</span></Form.Label>
                                <Form.Control type="email" placeholder="name@company.com" value={userEmail}  onChange={handleEmailChange} />
                            </Form.Group>

                            <Form.Group controlId="updates-check" className="mt-4">
                                <Form.Check type="checkbox" checked={updatesCheck} onChange={handleUpdatesChange} label="I'd like to receive news and feature updates" />
                            </Form.Group>

                            <Form.Group controlId="security-check" className="mt-4 mb-3">
                                <Form.Check type="checkbox" checked={securityCheck} onChange={handleSecurityChange} label="I'd like to receive security updates (recommended)" />
                            </Form.Group>

                            {!!setupError && <Error error={setupError}/>}
                            <Button variant="primary" disabled={disabled} type="submit">Setup</Button>
                        </Form>
                    </Card.Body>
                </Card>
            </Col>
        </Row>
    );
};