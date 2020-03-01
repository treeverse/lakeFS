import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import Alert from "react-bootstrap/Alert";
import React from "react";

import {connect} from "react-redux";
import Col from "react-bootstrap/Col";
import Row from "react-bootstrap/Row";
import Card from "react-bootstrap/Card";
import {useHistory, useLocation} from "react-router-dom";

import {login} from "../actions/auth";


const LoginForm = ({ loginError, login }) => {

    // change url
    let history = useHistory();
    let location = useLocation();
    let { from } = location.state || { from: { pathname: "/" } };

    return (
        <Row>
            <Col md={{offset: 2, span: 8}}>
                <Card>
                    <Card.Header>Login</Card.Header>
                    <Card.Body>
                        <Form onSubmit={(e) => {
                            login(e.target.accessKeyId.value, e.target.secretAccessKey.value, () => {
                                history.replace(from);
                            });

                            e.preventDefault();
                        }}>

                            <Form.Group controlId="accessKeyId">
                                <Form.Control type="text" placeholder="Access Key ID" autoFocus/>
                                <Form.Text className="text-muted">
                                    <em>Running lakeFS for the first time? setup initial credentials by running <code>lakefs init</code></em>
                                </Form.Text>
                            </Form.Group>

                            <Form.Group controlId="secretAccessKey">
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