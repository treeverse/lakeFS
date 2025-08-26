import React, {useState} from "react";
import Row from "react-bootstrap/Row";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import {auth, AuthenticationError, setup, SETUP_STATE_INITIALIZED} from "../../lib/api";
import {AlertError, Loading} from "../../lib/components/controls"
import {useRouter} from "../../lib/hooks/router";
import {useAPI} from "../../lib/hooks/api";
import {useNavigate} from "react-router-dom";
import {usePluginManager} from "../../extendable/plugins/pluginsContext";

interface SetupResponse {
    state: string;
    comm_prefs_missing?: boolean;
    login_config?: LoginConfig;
}

export interface LoginConfig {
    username_ui_placeholder?: string;
    password_ui_placeholder?: string;
    login_url: string;
    login_url_method?: 'none' | 'redirect' | 'select';
    login_failed_message?: string;
    fallback_login_url?: string;
    fallback_login_label?: string;
    login_cookie_names: string[];
    logout_url: string;
}

export const AUTH_LOGIN_PATH = '/auth/login';

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
    const pluginManager = usePluginManager();

    if (loading) {
        return <Loading message="Loading login configuration..." />;
    }

    // if we are not initialized, or we are not done with comm prefs, redirect to 'setup' page
    const setupResponse = response as SetupResponse | null;
    if (!error && setupResponse && (setupResponse.state !== SETUP_STATE_INITIALIZED || setupResponse.comm_prefs_missing)) {
        router.push({pathname: '/setup', params: {}, query: router.query as Record<string, string>})
        return null;
    }
    const loginConfig = setupResponse?.login_config;
    if (!loginConfig) {
        return null;
    }

    // SSO handling: when a user navigates directly to AUTH_LOGIN_PATH, they should see the lakeFS login form.
    // A login strategy is applied only if the user was redirected to AUTH_LOGIN_PATH (with the router.query.redirected flag).
    if (router.query.redirected)  {
        const result = pluginManager.loginStrategy.getLoginStrategy(loginConfig);
        // 'render' - show the login method selection UI.
        if (!error && result.type === 'render') {
            return result.element;
        }
        // 'redirected' - the login trategy plugin already redirected to the SSO URL so nothing to render.
        if (!error && result.type === 'redirected') {
            return null;
        }
        // 'none' - remove the router.query.redirected flag and route to AUTH_LOGIN_PATH to log in via lakeFS (LoginForm).
        delete router.query.redirected;
        router.push({pathname: AUTH_LOGIN_PATH, params: {}, query: router.query as Record<string, string>})
    }

    // Default: show the lakeFS login form when SSO isn’t configured, or when the user arrives directly at
    // AUTH_LOGIN_PATH (no router.query.redirected flag).
    return (
        <LoginForm loginConfig={loginConfig}/>
    );
};

export default LoginPage;
