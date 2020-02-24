import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import Alert from "react-bootstrap/Alert";
import React from "react";

import {connect} from "react-redux";
import {login} from "../actions";
import Col from "react-bootstrap/Col";
import Row from "react-bootstrap/Row";
import {useHistory, useLocation} from "react-router-dom";


const LoginForm = ({ loginError, login }) => {

    // change url
    let history = useHistory();
    let location = useLocation();
    let { from } = location.state || { from: { pathname: "/" } };

    return (
        <Row>
            <Col md={6}>
                <Form onSubmit={(e) => {
                    login(e.target.accessKeyId.value, e.target.secretAccessKey.value, () => {
                        history.replace(from);
                    });

                    e.preventDefault();
                }}>
                    <h3>Login</h3>
                    <Form.Group controlId="accessKeyId">
                        <Form.Control type="text" placeholder="Access Key ID"/>
                        <Form.Text className="text-muted">
                            Don't yet have a key pair? Run <code>lakefs init</code> to setup initial credentials
                        </Form.Text>
                    </Form.Group>

                    <Form.Group controlId="secretAccessKey">
                        <Form.Control type="password" placeholder="Secret Access Key"/>
                    </Form.Group>

                    {!!loginError ? <Alert variant={"danger"}>{loginError}</Alert> : <span/>}

                    <Button variant="primary" type="submit">Login</Button>
                </Form>
            </Col>
        </Row>
    );
}

export default connect(
    ({ auth }) => ({
        loginError: auth.loginError,
    }),
    {login}
)(LoginForm);