import React, {useRef, useState} from "react";
import Layout from "../../lib/components/layout";
import Row from "react-bootstrap/Row";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import {
    AlertIcon,
    MailIcon
} from "@primer/octicons-react";
import {Error} from "../../lib/components/controls";
import {auth} from "../../lib/api";

const TOKEN = "token";

const RequestResetPasswordForm = () => {
    const [reqResetPwdError, setReqResetPwdError] = React.useState(null);
    const [riskAccepted, setRiskAccepted] = React.useState(false);
    const [resetReqSent, setResetReqSent] = React.useState(false);

    if (resetReqSent) {
        return (
            <Row>
                <Col md={{offset: 4, span: 4}}>
                    <ResetEmailSent/>
                </Col>
            </Row>
        );
    } else {
        return (
            <Row>
                <Col md={{offset: 4, span: 4}}>
                    <div className="reset-pwd-risks">
                        <div className="risk-icon">
                            <AlertIcon size={20}/>
                        </div>
                        <div className="warn-message">
                            We don&#39;t store plain text passwords, therefore, once reset, they&#39;ll be lost.<br/>
                            Please make sure you have access to your mailbox.<br/>
                            Your reset link will be available for 6 hours.
                        </div>
                    </div>
                    <Card className="request-reset-pwd-widget">
                        <Card.Header>Reset Password</Card.Header>
                        <Card.Body>
                            <Form onSubmit={async (e) => {
                                e.preventDefault()
                                try {
                                    // TODO: call activate reset password process endpoint
                                    // await auth.login(e.target.username.value, e.target.password.value)
                                    console.log("submit request to start reset pwd process")
                                    setReqResetPwdError(null);
                                    setResetReqSent(true)
                                } catch (err) {
                                    setReqResetPwdError(err);
                                }
                            }}>
                                <Form.Group controlId="email">
                                    <Form.Control type="text" placeholder="Email" autoFocus/>
                                </Form.Group>

                                {(!!reqResetPwdError) && <Error error={reqResetPwdError}/>}

                                <div>
                                    <input type="checkbox" checked={riskAccepted} onChange={() => {
                                        setRiskAccepted(!riskAccepted);
                                    }}/>
                                    <label className="accept-risk-label">I understand the risks listed above</label>
                                </div>

                                <Button variant="primary" type="submit" className="reset-pwd">Reset Password</Button>
                            </Form>
                        </Card.Body>
                    </Card>
                </Col>
            </Row>
        );
    }
}

const ResetEmailSent = () => {
    return (
        <div className="reset-req-sent">
            <div className="envelope-icon">
                <MailIcon size={45}/>
            </div>
            <div className="warn-message">
                Password reset request started.<br/> Please check your email and follow the instructions.
            </div>
        </div>
    )
}

const ResetPasswordForm = () => {

    const onConfirmPasswordChange = () => {
        setPwdConfirmValid(true)
        if (newPwdField.current != null) {
            console.log("here")
            const isPasswordMatch = newPwdField.current.value === confirmPasswordField.current.value;
            setPwdConfirmValid(isPasswordMatch);
            setFormValid(isPasswordMatch)
        }
    };

    const [formValid, setFormValid] = useState(false);
    const [pwdConfirmValid, setPwdConfirmValid] = React.useState(null);

    const [resetPwdError, setResetPwdError] = React.useState(null);
    const newPwdField = useRef(null);
    const confirmPasswordField = useRef(null);

    return (
        <Row>
            <Col md={{offset: 4, span: 4}}>
                <Card className="reset-pwd-widget">
                    <Card.Header>Reset Password</Card.Header>
                    <Card.Body>
                        <Form onSubmit={async (e) => {
                            e.preventDefault()
                            try {
                                // TODO: call reset password endpoint
                                // await auth.login(e.target.username.value, e.target.password.value)
                                setResetPwdError(null);
                            } catch (err) {
                                setResetPwdError(err);
                            }
                        }}>
                            <Form.Group controlId="new-password">
                                <Form.Control type="password" placeholder="New Password" ref={newPwdField}/>
                            </Form.Group>

                            <Form.Group controlId="confirm-password">
                                <Form.Control type="password" placeholder="Confirm Password" ref={confirmPasswordField} onChange={onConfirmPasswordChange}/>
                                {pwdConfirmValid === false &&
                                <Form.Text className="text-danger">
                                    Your password and confirmation password do not match.
                                </Form.Text>
                                }
                            </Form.Group>

                            {(!!resetPwdError) && <Error error={resetPwdError}/>}

                            <Button variant="primary" type="submit" className="reset-pwd" disabled={!formValid}>Reset</Button>
                        </Form>
                    </Card.Body>
                </Card>
            </Col>
        </Row>
    );
}

const ResetPasswordPage = () => {
    let queryString = window.location.search;
    let params = new URLSearchParams(queryString);
    const token = params.get(TOKEN);

    return (
        <Layout>
            {
                !!token ? <ResetPasswordForm/> : <RequestResetPasswordForm/>
            }
        </Layout>
    );
};

export default ResetPasswordPage;
