import React, {ChangeEvent, FC, FormEvent, useCallback, useState} from "react";
import Button from "react-bootstrap/Button";
import Spinner from 'react-bootstrap/Spinner';
import Card from "react-bootstrap/Card";
import Col from "react-bootstrap/Col";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import {AlertError} from "../../lib/components/controls";

interface UserConfigurationProps {
    onSubmit: (email: string, admin: string, checks: boolean) => Promise<void>;
    setupError: Error;
    disabled: boolean;
    requireAdmin: boolean;
    requireCommPrefs: boolean;
}

export const UserConfiguration: FC<UserConfigurationProps> = ({
    onSubmit,
    setupError,
    disabled,
    requireAdmin,
    requireCommPrefs,
}) => {
    const [userEmail, setUserEmail] = useState<string>("");
    const [adminUser, setAdminUser] = useState<string>("admin");
    const [checks, setChecks] = useState<boolean>(false);

    const submitHandler = useCallback((e: FormEvent) => {
        onSubmit(adminUser, userEmail, checks);
        e.preventDefault();
    }, [onSubmit, adminUser, userEmail, checks]);

    const handleEmailChange = useCallback((e: ChangeEvent<HTMLInputElement>) => {
        setUserEmail(e.target.value);
    }, [setUserEmail]);

    const handleAdminUserChange = useCallback((e: ChangeEvent<HTMLInputElement>) => {
        setAdminUser(e.target.value);
    }, [setAdminUser]);

    const handleChecksChange = useCallback((e: ChangeEvent<HTMLInputElement>) => {
        setChecks(e.target.checked);
    }, [setChecks]);

    return (
        <Row>
            <Col md={{offset: 2, span: 8}}>
                <Card className="setup-widget">
                    <Card.Header>User Configuration</Card.Header>
                    <Card.Body>
                        {requireAdmin &&
                        <Card.Text>
                            Please specify the name of the first admin account, or leave it as the default &apos;admin&apos;.
                        </Card.Text>}

                        <Form onSubmit={submitHandler}>
                            {requireAdmin &&
                            <Form.Group controlId="user-name" className="mb-3">
                                <Form.Control type="text" value={adminUser}  onChange={handleAdminUserChange} placeholder="Admin Username" autoFocus/>
                            </Form.Group>}

                            {requireCommPrefs &&
                            <Form.Group controlId="user-email" className="mt-4">
                                <Form.Label>Email <span className="required-field-label">*</span></Form.Label>
                                <Form.Control type="email" placeholder="name@company.com" value={userEmail}  onChange={handleEmailChange} />
                            </Form.Group>}

                            {requireCommPrefs &&
                            <Form.Group controlId="security-check" className="mt-4 mb-3">
                                <Form.Check type="checkbox"  checked={checks} onChange={handleChecksChange} label="I'd like to receive security, product and feature updates" />
                            </Form.Group>}

                            {!!setupError && <AlertError error={setupError}/>}
                            <Button variant="primary" disabled={disabled} type="submit">
                                {disabled ? <Spinner as="span"
                                                     animation="border"
                                                     size="sm"
                                                     role="status"
                                                     aria-hidden="true"
                                                     /> : 'Setup'}
                            </Button>
                        </Form>
                    </Card.Body>
                </Card>
            </Col>
        </Row>
    );
};
