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
import {usePluginManager} from "../../extendable/plugins/pluginsContext";

interface SetupResponse {
    state: string;
    comm_prefs_missing?: boolean;
    login_config?: LoginConfig;
}

export interface LoginConfig {
    login_url: string;
    login_url_method?: string;
    username_ui_placeholder: string;
    password_ui_placeholder: string;
    login_failed_message?: string;
    fallback_login_url?: string;
    fallback_login_label?: string;
    login_cookie_names: string[];
    logout_url: string;
}

export const AUTH_LOGIN_PATH = '/auth/login';
export const LOCAL_LOGIN_METHOD = 'local';
export const LOGIN_URL_METHOD_SELECT = 'select';

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

/**
 * Decides whether to use the login method selection flow - render the login selection component or fall back to the
 * local lakeFS login form or the legacy flow handled by the caller.
 *
 * Flow:
 * - Ask the plugin for a selection component.
 *   - If the plugin returns null → return null (caller continues with the legacy flow).
 *   - If the plugin returns a component:
 *       - The user is always redirected to /auth/login for all login methods.
 *         There is no auto-redirect to login_url; SSO redirection only occurs after the user selects SSO on the
 *         selection component.
 *       - If the user chose local lakeFS login (router.query.method === LOCAL_LOGIN_METHOD),
 *         render the LoginForm.
 *       - Otherwise, render the login selection component where the user can choose between SSO and lakeFS login.
 */
const handleLoginMethodSelection = (
    loginConfig: LoginConfig, 
    router: ReturnType<typeof useRouter<{method?: string}>>
): React.ReactElement | null => {
    const pluginManager = usePluginManager();
    const loginMethodSelectionComponent = pluginManager.loginMethodSelection.renderLoginMethodSelection(loginConfig);
    
    if (loginMethodSelectionComponent) {
        // If the user selected local lakeFS login, show the standard login form
        if (router.query.method === LOCAL_LOGIN_METHOD) {
            return <LoginForm loginConfig={loginConfig}/>;
        }
        
        // Otherwise, show the login method selection component
        return loginMethodSelectionComponent;
    }
    
    return null;
};

const LoginPage = () => {
    const router = useRouter();
    const { response, error, loading } = useAPI(() => setup.getState());

    if (loading) {
        return null;
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

    const loginMethodComponent = handleLoginMethodSelection(loginConfig, router);
    if (loginMethodComponent) {
        // Login method selection flow – triggered when login_url is set and login_url_method === 'select'.
        // In this case, skip the legacy flow and show either the login selection component or the local lakeFS login form.
        return loginMethodComponent;
    }

    // Legacy login flow – use local lakeFS login, or automatically redirect to login_url if SSO login_url is configured.
    if (router.query.redirected)  {
        if(!error && loginConfig.login_url) {
            window.location.href = loginConfig.login_url;
            return null;
        }
        delete router.query.redirected;

        router.push({pathname: '/auth/login', params: {}, query: router.query as Record<string, string>})
    }

    return (
        <LoginForm loginConfig={loginConfig}/>
    );
};

export default LoginPage;
