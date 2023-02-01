import React, {ChangeEvent, FC, FormEvent, useCallback, useState} from "react";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import Col from "react-bootstrap/Col";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import {Error} from "../../lib/components/controls";

interface CommunicationPreferencesSetupProps {
    onSubmit: (email: string, updatesCheck: boolean, securityCheck: boolean) => Promise<void>;
    setupError: Error;
    disabled: boolean;
}

export const CommunicationPreferencesSetup: FC<CommunicationPreferencesSetupProps> = ({
    onSubmit,
    setupError,
    disabled,
}) => {
    const [userEmail, setUserEmail] = useState<string>("");
    const [updatesCheck, setUpdatesCheck] = useState<boolean>(false);
    const [securityCheck, setSecurityCheck] = useState<boolean>(false);

    const submitHandler = useCallback((e: FormEvent) => {
        onSubmit(userEmail, updatesCheck, securityCheck);
        e.preventDefault();
    }, [onSubmit, userEmail, updatesCheck, securityCheck]);

    const handleEmailChange = useCallback((e: ChangeEvent<HTMLInputElement>) => {
        setUserEmail(e.target.value);
    }, [setUserEmail]);

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
                    <Card.Header>Communication Preferences</Card.Header>
                    <Card.Body>
                        <Card.Text>
                            Please provide your email below. <br />
                            We recommend at least signing up for security updates. <br />
                            You may unsubscribe from either or both at any time.
                        </Card.Text>
                        <Form onSubmit={submitHandler}>
                            <Form.Group controlId="user-email" className="mt-4">
                                <Form.Label>Email <span className="required-field-label">*</span></Form.Label>
                                <Form.Control type="email" placeholder="name@company.com" value={userEmail}  onChange={handleEmailChange} autoFocus/>
                            </Form.Group>

                            <Form.Group controlId="updates-check" className="mt-4">
                                <Form.Label>News & feature updates</Form.Label>
                                <Form.Check type="checkbox" checked={updatesCheck} onChange={handleUpdatesChange} label="I'd like to receive news and feature updates" />
                            </Form.Group>

                            <Form.Group controlId="security-check" className="mt-4 mb-3">
                                <Form.Label>Security updates</Form.Label>
                                <Form.Check type="checkbox" checked={securityCheck} onChange={handleSecurityChange} label="I'd like to receive security updates" />
                            </Form.Group>

                            {!!setupError && <Error error={setupError}/>}
                            <Button variant="primary" disabled={disabled} type="submit">Continue</Button>
                        </Form>
                    </Card.Body>
                </Card>
            </Col>
        </Row>
    );
};