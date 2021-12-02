import React, {useState} from "react";
import Layout from "../../lib/components/layout";
import Row from "react-bootstrap/Row";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import {auth, setup, SETUP_STATE_INITIALIZED} from "../../lib/api";
import {Error} from "../../lib/components/controls"
import {useRouter} from "../../lib/hooks/router";
import {useAPI} from "../../lib/hooks/api";

const LoginForm = () => {
    const router = useRouter();
    const [loginError, setLoginError] = useState(null);

    const { next } = router.query;

    return (
        <Row>
            <Col md={{offset: 4, span: 4}}>
                <Card className="login-widget">
                    <Card.Header>Login</Card.Header>
                    <Card.Body>
                        <Form onSubmit={async (e) => {
                            e.preventDefault()
                            try {
                                await auth.login(e.target.username.value, e.target.password.value)
                                setLoginError(null);
                                router.push(next ? next : '/');
                            } catch(err) {
                                setLoginError(err);
                            }
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

                            {(!!loginError) && <Error error={loginError}/>}

                            <Button variant="primary" type="submit">Login</Button>
                        </Form>
                    </Card.Body>
                </Card>
            </Col>
        </Row>
    )
}


const LoginPage = () => {
    const router = useRouter();
    const { response, error, loading } = useAPI(() => {
        return setup.getState()
    });
    if (loading) {
        return null;
    }
    if (!error && response && response.state !== SETUP_STATE_INITIALIZED) {
        router.push({pathname: '/setup', query: router.query})
    }
    return (
        <Layout>
            <LoginForm/>
        </Layout>
    );
};

export default LoginPage;
