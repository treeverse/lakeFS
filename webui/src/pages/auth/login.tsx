import React, {useEffect, useState} from "react";
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
import {buildUrl, isTrue, normalizeNext, queryOf, stripParam} from "../../lib/utils";

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

type NavigateState = { redirected?: boolean; next?: string };

const withNext = (url: string, next: string) => {
    const safeNext = next?.startsWith("/") ? next : "/";
    try {
        const u = new URL(url, window.location.origin);
        u.searchParams.set("next", safeNext);
        return u.toString();
    } catch {
        return `${url}${url.includes("?") ? "&" : "?"}next=${encodeURIComponent(safeNext)}`;
    }
};

export const getLoginIntent = (location: ReturnType<typeof useLocation>) => {
    const st = location.state ?? {};
    const qp = queryOf(location);
    const redirectedFromQuery = isTrue(qp.get("redirected"));
    const redirected = Boolean(st.redirected) || redirectedFromQuery;
    const next = normalizeNext(st.next ?? qp.get("next"));
    const cleanUrl = buildUrl(location, stripParam(qp, "redirected"));

    return { redirected, redirectedFromQuery, next, cleanUrl };
};

const DoNavigate: React.FC<{ to: string; replace?: boolean; state?: NavigateState }> = ({ to, replace = true, state }) => {
    const router = useRouter();
    useEffect(() => { router.navigate(to, { replace, state }); }, [router, to, replace, state]);
    return <Loading />;
};

const ExternalRedirect: React.FC<{ to: string }> = ({ to }) => {
    useEffect(() => { window.location.replace(to); }, [to]);
    return <Loading />;
};

const LoginForm = ({loginConfig}: {loginConfig: LoginConfig}) => {
    const location = useLocation();
    const { setStatus } = useAuth();
    const [loginError, setLoginError] = useState<React.ReactNode>(null);
    const loginState = (location.state as { next?: string; redirected?: boolean } | null) ?? null;
    const search = new URLSearchParams(location.search);
    const next = loginState?.next ?? search.get("next") ?? "/";
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
                            window.sessionStorage.setItem("lakefs_post_login_next", next);
                            setStatus(AUTH_STATUS.AUTHENTICATED);
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
                                window.sessionStorage.setItem("lakefs_post_login_next", next);
                                loginConfig.login_cookie_names?.forEach(
                                    cookie => {
                                        document.cookie = `${cookie}=; Path=/; Expires=Thu, 01 Jan 1970 00:00:01 GMT;`;
                                    }
                                );
                                if (loginConfig.fallback_login_url) {
                                    window.location.href = withNext(loginConfig.fallback_login_url, next);
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
    const { response, error, loading } = useAPI(() => setup.getState());
    const { status } = useAuth();
    const setupResponse = response as SetupResponse | null;
    const { redirected, redirectedFromQuery, next, cleanUrl } = getLoginIntent(location);

    useEffect(() => {if (next) window.sessionStorage.setItem("lakefs_post_login_next", next);}, [next]);

    if (loading) return <Loading />;

    if (setupResponse && (setupResponse.state !== SETUP_STATE_INITIALIZED || setupResponse.comm_prefs_missing)) {
        const qs = new URLSearchParams(location.search);
        qs.set("redirected", "true");
        if (!qs.get("next")) qs.set("next", next);
        return <Navigate to={`/setup?${qs.toString()}${location.hash ?? ""}`} replace />;
    }

    if (error) return <AlertError error={error} className="mt-1 w-50 m-auto" onDismiss={() => window.location.reload()} />;
    if (redirectedFromQuery) return <DoNavigate to={cleanUrl} replace state={{ redirected: true, next }} />;

    if (status === AUTH_STATUS.AUTHENTICATED) {
        const stored = window.sessionStorage.getItem("lakefs_post_login_next");
        if (stored?.startsWith("/")) return <DoNavigate to={stored} replace />;
    }

    const loginConfig = setupResponse?.login_config;

    if (redirected) {
        const loginStrategy = pluginManager.loginStrategy.getLoginStrategy(loginConfig, router);
        if (loginStrategy.element !== undefined) return loginStrategy.element;
        if (loginConfig?.login_url && loginConfig?.login_url_method !== "none") {
            return <ExternalRedirect to={withNext(loginConfig.login_url, next)} />;
        }
    }

    return <LoginForm loginConfig={loginConfig} />;
};

export default LoginPage;