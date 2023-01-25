import React, {useState} from "react";
import Layout from "../../lib/components/layout";
import Row from "react-bootstrap/Row";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import {auth, AuthenticationError, setup, SETUP_STATE_INITIALIZED} from "../../lib/api";
import {Error} from "../../lib/components/controls"
import {useRouter} from "../../lib/hooks/router";
import {useAPI} from "../../lib/hooks/api";

//const OIDC_LOGIN_URL = "/oidc/login?prompt=login";

interface LoginConfig {
    login_url: string;
    login_failed_message?: string;
    fallback_login_url?: string;
    fallback_login_label?: string;
    login_cookies: string[];
    logout_url: string;
}

const LoginForm = ({loginConfig}: {loginConfig: LoginConfig}) => {
    const router = useRouter();
    const [loginError, setLoginError] = useState(null);
    const { response, error, loading } = useAPI(() => auth.getAuthCapabilities());
    if (loading) {
        return null;
    }

    const showResetPwd = !error && response && response.forgot_password;
    const usernamePlaceholder = showResetPwd ? "Email / Access Key ID" : "Access Key ID"
    const passwordPlaceholder = showResetPwd ? "Password / Secret Access Key" : "Secret Access Key"
    const { next } = router.query;

    return (
        <Row>
            <Col md={{offset: 5, span: 2}}>
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
                                if (err instanceof AuthenticationError && err.status === 401) {
                                    const contents = {__html: `${loginConfig.login_failed_message}` ||
                                        "Credentials don't match."};
                                    setLoginError(<span dangerouslySetInnerHTML={contents}/>);
                                }
                            }
                        }}>
                            <Form.Group controlId="username" className="mb-3">
                                <Form.Control type="text" placeholder={usernamePlaceholder} autoFocus/>
                            </Form.Group>

                            <Form.Group controlId="password" className="mb-3">
                                <Form.Control type="password" placeholder={passwordPlaceholder}/>
                            </Form.Group>

                            {(!!loginError) && <Error error={loginError}/>}

                            <Button variant="primary" type="submit">Login</Button>
                        </Form>
                        <div className={"mt-2 mb-1"}>
                            { showResetPwd ?
                                <Button variant="link" className={"text-secondary mt-2"}  onClick={()=> {router.push("/auth/resetpassword")}}>Reset password</Button>
                                : ""
                            }
                            { loginConfig.fallback_login_url ?
                                <Button variant="link" className="text-secondary mt-2" onClick={async ()=> {
                                    loginConfig.login_cookies?.forEach(
                                        cookie => {
                                            document.cookie = `${cookie}=; Path=/; Expires=Thu, 01 Jan 1970 00:00:01 GMT;`;
                                        }
                                    );
                                    window.location = loginConfig.fallback_login_url;
                                }}>{loginConfig.fallback_login_label || 'Try another way to login'}</Button>
                                : ""
                            }
                        </div>
                    </Card.Body>
                </Card>
            </Col>
        </Row>
    )
}


const LoginPage = () => {
    const router = useRouter();
    const { response, error, loading } = useAPI(() => setup.getState());
    if (loading) {
        return null;
    }

    if (!error && response && response.state !== SETUP_STATE_INITIALIZED) {
        router.push({pathname: '/setup', query: router.query})
        return null;
    }
    if (router.query.redirected)  {
        if(!error && response?.loginConfig?.login_url) {
            window.location = response.loginConfig.login_url;
            return null;
        }
        delete router.query.redirected;
        router.push({pathname: '/auth/login', query: router.query})
    }
    return (
        <Layout logged={false}>
            <LoginForm loginConfig={response?.login_config}/>
        </Layout>
    );
};

export default LoginPage;
