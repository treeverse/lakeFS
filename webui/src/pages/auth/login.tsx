import React, {useState} from "react";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import {auth, AuthenticationError, setup, SETUP_STATE_INITIALIZED} from "../../lib/api";
import {AlertError, Loading} from "../../lib/components/controls"
import {useRouter} from "../../lib/hooks/router";
import {useAPI} from "../../lib/hooks/api";
import {usePluginManager} from "../../extendable/plugins/pluginsContext";
import {AUTH_STATUS, useAuth} from "../../lib/auth/authContext";
import {Navigate, useLocation} from "react-router-dom";

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

const LoginForm = ({loginConfig}: {loginConfig: LoginConfig}) => {
    const router = useRouter();
    const location = useLocation();
    const { setAuthStatus } = useAuth();
    const [loginError, setLoginError] = useState<React.ReactNode>(null);
    const state = location.state as { next?: string; redirected?: boolean } | null;
    const next = (state?.next ?? (router.query as { next?: string })?.next) || "/";
    const usernamePlaceholder = loginConfig.username_ui_placeholder || "Access Key ID";
    const passwordPlaceholder = loginConfig.password_ui_placeholder || "Secret Access Key";

    return (
        <div className="d-flex align-items-center justify-content-center">
            <Card className="shadow-lg border-0 login-card">
                <Card.Header className="text-center">
                    <div className="mt-3 mb-3">
                        <img src="/logo.svg" alt="lakeFS" className="login-logo" />
                    </div>
                </Card.Header>
                <Card.Body className="p-4">
                    <Form onSubmit={async (e) => {
                        e.preventDefault()
                        const form = e.target as HTMLFormElement;
                        const formData = new FormData(form);
                        try {
                            setLoginError(null);
                            const username = formData.get('username');
                            const password = formData.get('password');
                            await auth.login(username, password);
                            setAuthStatus(AUTH_STATUS.AUTHENTICATED);
                            router.navigate(next || "/", { replace: true });
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
                                name="username"
                                type="text"
                                placeholder={usernamePlaceholder}
                                autoFocus
                                className="bg-light"
                            />
                        </Form.Group>

                        <Form.Group controlId="password" className="mb-3">
                            <Form.Control
                                name="password"
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
                                if (loginConfig.fallback_login_url) {
                                    window.location.href = loginConfig.fallback_login_url;
                                }
                            }}>{loginConfig.fallback_login_label || 'Try another way to login'}</Button>
                            : ""
                        }
                    </div>
                </Card.Body>
            </Card>
        </div>
    )
}

const LoginPage = () => {
    const router = useRouter();
    const location = useLocation();
    const pluginManager = usePluginManager();
    const { status } = useAuth();
    const { response, error, loading } = useAPI(() => setup.getState());

    if (loading) {
        return <Loading />;
    }

    if (error) {
        return <AlertError error={error} className={"mt-1 w-50 m-auto"} onDismiss={() => window.location.reload()} />;
    }

    // if we are not initialized, or we are not done with comm prefs, redirect to 'setup' page
    const setupResponse = response as SetupResponse | null;
    if (setupResponse && (setupResponse.state !== SETUP_STATE_INITIALIZED || setupResponse.comm_prefs_missing)) {
        router.push({pathname: '/setup', params: {}, query: router.query as Record<string, string>})
        return null;
    }
    const loginConfig = setupResponse?.login_config;
    if (!loginConfig) {
        return null;
    }

    if (status === AUTH_STATUS.AUTHENTICATED) {
        const next = (location.state && (location.state).next) || (router.query && (router.query).next) || "/repositories";
        return <Navigate to={next} replace />;
    }

    // SSO handling: A login strategy (e.g., auto-redirect to SSO or showing a login selection page) is applied only
    // when the user is redirected to '/auth/login' (router.query.redirected is true). If the user navigates directly
    // to '/auth/login', they should always see the lakeFS login form. When the login strategy is to show a
    // method-selection page, the '/auth/login' endpoint uses the redirected flag to distinguish between showing the
    // selection page or the default lakeFS login form, since both share the same endpoint.
    const redirected = (location.state)?.redirected || (router.query)?.redirected;

    if (redirected)  {
        const loginStrategy = pluginManager.loginStrategy.getLoginStrategy(loginConfig, router);
        // Return the element (component or null)
        if (loginStrategy.element !== undefined) {
            return loginStrategy.element;
        }
    }

    // Default: show the lakeFS login form when the user navigates directly at '/auth/login'
    // (no router.query.redirected flag) or when loginStrategyPlugin.element is undefined.
    return (
        <LoginForm loginConfig={loginConfig}/>
    );
};

export default LoginPage;