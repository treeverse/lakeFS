import React, {useRef, useState} from "react";
import Layout from "../../lib/components/layout";
import Row from "react-bootstrap/Row";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import {MailIcon} from "@primer/octicons-react";
import {AlertError} from "../../lib/components/controls";
import {auth} from "../../lib/api";
import validator from "validator/es";
import {useRouter} from "../../lib/hooks/router";

const TOKEN_PARAM_NAME = "token";

const RequestResetPasswordForm = () => {
    const onEmailChange = () => {
        const isValid = validator.isEmail(email.current.value);
        setFormValid(isValid);
        setEmailValid(isValid);
    };

    const [reqResetPwdError, setReqResetPwdError] = useState(null);
    const [resetReqSent, setResetReqSent] = useState(false);
    const [formValid, setFormValid] = useState(false);
    const [emailValid, setEmailValid] = useState(null);
    const email = useRef(null);

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
                    <Card className="request-reset-pwd-widget">
                        <Card.Header>Reset Password</Card.Header>
                        <Card.Body>
                            <Form onSubmit={async (e) => {
                                e.preventDefault();
                                try {
                                    await auth.passwordForgot(e.target.email.value);
                                    setReqResetPwdError(null);
                                    setResetReqSent(true);
                                } catch (err) {
                                    setReqResetPwdError(err);
                                }
                            }}>
                                <Form.Group controlId="email">
                                    <Form.Control type="text" placeholder="Email" autoFocus ref={email} onChange={onEmailChange}/>
                                    {emailValid === false &&
                                    <Form.Text className="text-danger">
                                        Invalid email address
                                    </Form.Text>
                                    }
                                </Form.Group>

                                {(!!reqResetPwdError) && <AlertError error={reqResetPwdError}/>}

                                <Button variant="primary" type="submit" className="reset-pwd" disabled={!formValid}>Reset Password</Button>
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

const ResetPasswordForm = ({token}) => {
    const router = useRouter();

    const onConfirmPasswordChange = () => {
        setPwdConfirmValid(true)
        if (newPwdField.current !== null) {
            const isPasswordMatch = newPwdField.current.value === confirmPasswordField.current.value;
            setPwdConfirmValid(isPasswordMatch);
            setFormValid(isPasswordMatch)
        }
    };

    const [formValid, setFormValid] = useState(false);
    const [pwdConfirmValid, setPwdConfirmValid] = useState(null);

    const [resetPwdError, setResetPwdError] = useState(null);
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
                                setResetPwdError(null);
                                await auth.updatePasswordByToken(token, e.target.newPassword.value)
                                router.push('/auth/login');
                            } catch (err) {
                                setResetPwdError(err);
                            }
                        }}>
                            <Form.Group controlId="newPassword">
                                <Form.Control type="password" placeholder="New Password" ref={newPwdField}/>
                            </Form.Group>

                            <Form.Group controlId="confirmPassword">
                                <Form.Control type="password" placeholder="Confirm Password" ref={confirmPasswordField} onChange={onConfirmPasswordChange}/>
                                {pwdConfirmValid === false &&
                                <Form.Text className="text-danger">
                                    Your password and confirmation password do not match.
                                </Form.Text>
                                }
                            </Form.Group>

                            {(!!resetPwdError) && <AlertError error={resetPwdError}/>}

                            <Button variant="primary" type="submit" className="reset-pwd" disabled={!formValid}>Reset</Button>
                        </Form>
                    </Card.Body>
                </Card>
            </Col>
        </Row>
    );
}

const ResetPasswordPage = () => {
    const queryString = window.location.search;
    const params = new URLSearchParams(queryString);
    const token = params.get(TOKEN_PARAM_NAME);
    return (
        <Layout logged={false}>
            {token
                ? <ResetPasswordForm token={token}/>
                : <RequestResetPasswordForm/>
            }
        </Layout>
    );
};

export default ResetPasswordPage;
