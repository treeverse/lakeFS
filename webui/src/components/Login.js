import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import Alert from "react-bootstrap/Alert";
import React from "react";

import {connect} from "react-redux";
import Col from "react-bootstrap/Col";
import Row from "react-bootstrap/Row";
import Card from "react-bootstrap/Card";
import {useLocation, useHistory} from "react-router-dom";

import {login} from "../actions/auth";


const LoginForm = ({ loginError, login, redirectTo, onRedirect }) => {

    // change url
    let location = useLocation();
    let { from } = location.state || { from: { pathname: "/" } };
    const redirectedUrl = from;

    const history = useHistory();
    if (!!redirectTo) {
        history.push(redirectTo);
        onRedirect();
    }

    return (
        <Row>
            <Col md={{offset: 2, span: 8}}>
                <Card className="login-widget">
                    <Card.Header>Login</Card.Header>
                    <Card.Body>
                        <Form onSubmit={(e) => {
                            login(e.target.username.value, e.target.password.value, redirectedUrl);

                            e.preventDefault();
                        }}>

                            <Form.Group controlId="username">
                                <Form.Control type="text" placeholder="Access Key ID" autoFocus/>
                                <Form.Text className="text-muted">
                                    <em>Running lakeFS for the first time? setup initial credentials by running <code>lakefs init</code></em>
                                </Form.Text>
                            </Form.Group>

                            <Form.Group controlId="password">
                                <Form.Control type="password" placeholder="Secret Access Key"/>
                            </Form.Group>

                            {!!loginError ? <Alert variant={"danger"}>{loginError}</Alert> : <span/>}

                            <Button variant="primary" type="submit">Login</Button>
                        </Form>
                    </Card.Body>
                </Card>
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