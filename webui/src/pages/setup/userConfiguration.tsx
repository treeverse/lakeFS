import React, { ChangeEvent, FC, FormEvent, useCallback, useState } from 'react';
import Button from 'react-bootstrap/Button';
import Spinner from 'react-bootstrap/Spinner';
import Card from 'react-bootstrap/Card';
import Col from 'react-bootstrap/Col';
import Form from 'react-bootstrap/Form';
import Row from 'react-bootstrap/Row';
import { AlertError } from '../../lib/components/controls';

interface UserConfigurationProps {
    onSubmit: (
        admin: string,
        firstName: string,
        lastName: string,
        email: string,
        companyName: string,
        checks: boolean,
    ) => Promise<void>;
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
    const [userEmail, setUserEmail] = useState<string>('');
    const [adminUser, setAdminUser] = useState<string>('admin');
    const [firstName, setFirstName] = useState<string>('');
    const [lastName, setLastName] = useState<string>('');
    const [companyName, setCompanyName] = useState<string>('');
    const [checks, setChecks] = useState<boolean>(false);

    const submitHandler = useCallback(
        (e: FormEvent) => {
            onSubmit(adminUser, firstName, lastName, userEmail, companyName, checks);
            e.preventDefault();
        },
        [onSubmit, adminUser, firstName, lastName, userEmail, companyName, checks],
    );

    const handleEmailChange = useCallback(
        (e: ChangeEvent<HTMLInputElement>) => {
            setUserEmail(e.target.value);
        },
        [setUserEmail],
    );

    const handleAdminUserChange = useCallback(
        (e: ChangeEvent<HTMLInputElement>) => {
            setAdminUser(e.target.value);
        },
        [setAdminUser],
    );

    const handleChecksChange = useCallback(
        (e: ChangeEvent<HTMLInputElement>) => {
            setChecks(e.target.checked);
        },
        [setChecks],
    );

    const handleFirstNameChange = useCallback(
        (e: ChangeEvent<HTMLInputElement>) => {
            setFirstName(e.target.value);
        },
        [setFirstName],
    );

    const handleLastNameChange = useCallback(
        (e: ChangeEvent<HTMLInputElement>) => {
            setLastName(e.target.value);
        },
        [setLastName],
    );

    const handleCompanyNameChange = useCallback(
        (e: ChangeEvent<HTMLInputElement>) => {
            setCompanyName(e.target.value);
        },
        [setCompanyName],
    );

    return (
        <Row>
            <Col md={{ offset: 2, span: 8 }}>
                <Card className="setup-widget">
                    <Card.Header>User Configuration</Card.Header>
                    <Card.Body>
                        {requireAdmin && (
                            <Card.Text>
                                Please specify the name of the first admin account, or leave it as the default
                                &apos;admin&apos;.
                            </Card.Text>
                        )}

                        <Form onSubmit={submitHandler}>
                            {requireAdmin && (
                                <Form.Group controlId="user-name" className="mb-3">
                                    <Form.Control
                                        type="text"
                                        value={adminUser}
                                        onChange={handleAdminUserChange}
                                        placeholder="Admin Username"
                                        autoFocus
                                    />
                                </Form.Group>
                            )}

                            {requireCommPrefs && (
                                <>
                                    <Row>
                                        <Col md={6}>
                                            <Form.Group controlId="user-first-name" className="mt-4">
                                                <Form.Label>First name</Form.Label>
                                                <Form.Control
                                                    type="text"
                                                    name="firstName"
                                                    placeholder="Jane"
                                                    value={firstName}
                                                    onChange={handleFirstNameChange}
                                                    autoComplete="given-name"
                                                />
                                            </Form.Group>
                                        </Col>
                                        <Col md={6}>
                                            <Form.Group controlId="user-last-name" className="mt-4">
                                                <Form.Label>Last name</Form.Label>
                                                <Form.Control
                                                    type="text"
                                                    name="lastName"
                                                    placeholder="Doe"
                                                    value={lastName}
                                                    onChange={handleLastNameChange}
                                                    autoComplete="family-name"
                                                />
                                            </Form.Group>
                                        </Col>
                                    </Row>

                                    <Form.Group controlId="user-email" className="mt-4">
                                        <Form.Label>
                                            Email <span className="required-field-label">*</span>
                                        </Form.Label>
                                        <Form.Control
                                            type="email"
                                            name="email"
                                            placeholder="name@company.com"
                                            value={userEmail}
                                            onChange={handleEmailChange}
                                            autoComplete="email"
                                        />
                                    </Form.Group>

                                    <Form.Group controlId="company-name" className="mt-4">
                                        <Form.Label>Company name</Form.Label>
                                        <Form.Control
                                            type="text"
                                            name="companyName"
                                            placeholder="Your Company"
                                            value={companyName}
                                            onChange={handleCompanyNameChange}
                                            autoComplete="organization"
                                        />
                                    </Form.Group>

                                    <Form.Group controlId="security-check" className="mt-4 mb-3">
                                        <Form.Check
                                            type="checkbox"
                                            checked={checks}
                                            onChange={handleChecksChange}
                                            label="I'd like to receive security, product and feature updates"
                                        />
                                    </Form.Group>
                                </>
                            )}

                            {!!setupError && <AlertError error={setupError} />}
                            <Button variant="primary" disabled={disabled} type="submit">
                                {disabled ? (
                                    <Spinner as="span" animation="border" size="sm" role="status" aria-hidden="true" />
                                ) : (
                                    'Setup'
                                )}
                            </Button>
                        </Form>
                    </Card.Body>
                </Card>
            </Col>
        </Row>
    );
};
