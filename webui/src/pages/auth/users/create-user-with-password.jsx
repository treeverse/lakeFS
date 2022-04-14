import Layout from "../../../lib/components/layout";
import React, {useRef, useState} from "react";
import {Redirect, Route} from "react-router-dom";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import {auth} from "../../../lib/api";
import {ActionGroup, ActionsBar, Error} from "../../../lib/components/controls";
import Button from "react-bootstrap/Button";
import {useRouter} from "../../../lib/hooks/router";
import jwt_decode from "jwt-decode";

const TOKEN_PARAM_NAME = "token";

const CreateUserWithPasswordForm = ({token}) => {

    const router = useRouter();
    const decoded = jwt_decode(token);
    const invitedUserEmail = decoded.sub;

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
    const [reqActivateUserError, setReqActivateUserError] = useState(null);
    const newPwdField = useRef(null);
    const confirmPasswordField = useRef(null);

    return (
        <Row>
            <Col md={{offset: 4, span: 4}}>
                <div className="invited-welcome-msg">
                    <div className="title">
                        Welcome to the lake!
                    </div>
                    <div className="body">
                        You were invited to use lakeFS Cloud!
                    </div>
                </div>
                <Card className="create-invited-user-widget">
                    <Card.Header>Activate User</Card.Header>
                    <Card.Body>
                        <Form id='activate-user' onSubmit={async (e) => {
                            e.preventDefault()
                            try {
                                await auth.passwordForgot(e.target.email.value)
                                setReqActivateUserError(null);
                                router.push("/auth/login");
                            } catch (err) {
                                setReqActivateUserError(err);
                            }
                        }}>
                            <Form.Group controlId="email">
                                <Form.Control type="text" placeholder={invitedUserEmail} disabled={true}/>
                            </Form.Group>

                            <Form.Group controlId="password">
                                <Form.Control type="password" placeholder="Password" ref={newPwdField}/>
                            </Form.Group>

                            <Form.Group controlId="confirmPassword">
                                <Form.Control type="password" placeholder="Confirm Password" ref={confirmPasswordField} onChange={onConfirmPasswordChange}/>
                                {pwdConfirmValid === false &&
                                <Form.Text className="text-danger">
                                    Your password and confirmation password do not match.
                                </Form.Text>
                                }
                            </Form.Group>

                            {(!!reqActivateUserError) && <Error error={reqActivateUserError}/>}
                        </Form>
                    </Card.Body>
                </Card>
                <ActionsBar>
                    <ActionGroup orientation="right">
                        <Button form='activate-user' type="submit" className="create-user" disabled={!formValid}>Create</Button>
                        <Button className="cancel-create-user" onClick={() => {router.push("/auth/login");}}>Cancel</Button>
                    </ActionGroup>
                </ActionsBar>
            </Col>
        </Row>
    );
}


export const CreateUserPage = () => {
    let queryString = window.location.search;
    let params = new URLSearchParams(queryString);
    const token = params.get(TOKEN_PARAM_NAME);

    return (
        <Layout>
            {
                !!token ?
                    <CreateUserWithPasswordForm token={token}/> :
                    <Route>
                        <Redirect to="/auth/login"/>
                    </Route>
            }
        </Layout>
    );
};

export default CreateUserPage;