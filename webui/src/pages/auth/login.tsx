import React, {useState} from "react";
import Row from "react-bootstrap/Row";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import {auth, AuthenticationError, setup, SETUP_STATE_INITIALIZED} from "../../lib/api";
import {AlertError} from "../../lib/components/controls"
import {useRouter} from "../../lib/hooks/router";
import {useAPI} from "../../lib/hooks/api";
import {useNavigate} from "react-router-dom";

interface LoginConfig {
    login_url: string;
    username_ui_placeholder: string;
    password_ui_placeholder: string;
    login_failed_message?: string;
    fallback_login_url?: string;
    fallback_login_label?: string;
    login_cookie_names: string[];
    logout_url: string;
}

const LoginForm = ({loginConfig}: {loginConfig: LoginConfig}) => {
    const router = useRouter();
    const navigate = useNavigate();
    const [loginError, setLoginError] = useState(null);
    const { next } = router.query;
    const usernamePlaceholder = loginConfig.username_ui_placeholder || "Access Key ID";
    const passwordPlaceholder = loginConfig.password_ui_placeholder || "Secret Access Key";
    return (
        <Row>
            <Col md={{offset: 4, span: 4}}>
                <Card className="login-widget shadow-lg border-0">
                    <Card.Header className="text">
                        <h4 className="mb-0">Login</h4>
                    </Card.Header>
                    <Card.Body className="p-4">
                        <Form onSubmit={async (e) => {
                            e.preventDefault()
                            try {
                                setLoginError(null);
                                await auth.login(e.target.username.value, e.target.password.value)
                                router.push(next || '/');
                                navigate(0);
                            } catch(err) {
                                if (err instanceof AuthenticationError && err.status === 401) {
                                    const contents = {__html: `${loginConfig.login_failed_message}` ||
                                        "Credentials don't match."};
                                    setLoginError(<span dangerouslySetInnerHTML={contents}/>);
                                }
                            }
                        }}>
                            <Form.Group controlId="username" className="mb-3">
                                <Form.Control 
                                    type="text" 
                                    placeholder={usernamePlaceholder} 
                                    autoFocus
                                    className="bg-light"
                                />
                            </Form.Group>

                            <Form.Group controlId="password" className="mb-3">
                                <Form.Control 
                                    type="password" 
                                    placeholder={passwordPlaceholder}
                                    className="bg-light"
                                />
                            </Form.Group>

                            {(!!loginError) && <AlertError error={loginError}/>}

                            <Button 
                                variant="primary" 
                                type="submit" 
                                className="w-100 mt-3 py-2"
                            >
                                Login
                            </Button>
                        </Form>
                        <div className={"mt-2 mb-1"}>
                            { loginConfig.fallback_login_url ?
                                <Button variant="link" className="text-secondary mt-2" onClick={async ()=> {
                                    loginConfig.login_cookie_names?.forEach(
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

    // if we are not initialized, or we are not done with comm prefs, redirect to 'setup' page
    if (!error && response && (response.state !== SETUP_STATE_INITIALIZED || response.comm_prefs_missing === true)) {
        router.push({pathname: '/setup', query: router.query})
        return null;
    }
    const loginConfig = response?.login_config;
    if (router.query.redirected)  {
        if(!error && loginConfig?.login_url) {
            window.location = loginConfig.login_url;
            return null;
        }
        delete router.query.redirected;

        router.push({pathname: '/auth/login', query: router.query})
    }
    return (
        <LoginForm loginConfig={loginConfig}/>
    );
};

export default LoginPage;
