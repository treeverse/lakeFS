import React, { useState } from 'react';
import { Navigate, useLocation } from 'react-router-dom';
import Card from 'react-bootstrap/Card';
import Form from 'react-bootstrap/Form';
import Button from 'react-bootstrap/Button';
import { auth, AuthenticationError, ClientError, ServerError, setup, SETUP_STATE_INITIALIZED } from '../../lib/api';
import { AlertError, Loading } from '../../lib/components/controls';
import { useAPI } from '../../lib/hooks/api';
import { LAKEFS_POST_LOGIN_NEXT, useAuth } from '../../lib/auth/authContext';
import { normalizeNext, ROUTES } from '../../lib/utils';

type NavigateState = { redirected?: boolean; next?: string };

interface SetupResponse {
    state: string;
    comm_prefs_missing?: boolean;
    login_config?: LoginConfig;
}

export interface LoginConfig {
    login_failed_message?: string;
}

export const getLoginIntent = (location: ReturnType<typeof useLocation>) => {
    const st = location.state ?? {};
    const qp = new URLSearchParams(location.search);

    const redirectedFromQuery = qp.get('redirected') === 'true';
    const redirected = Boolean(st.redirected) || redirectedFromQuery;
    const next = normalizeNext(st.next ?? qp.get('next'));

    // If `redirected=true` appears in the URL, we drop it from the query and carry
    // `redirected: true` in history state for exactly one render. On the next render
    // the URL is clean, while `redirected` from state triggers the login strategy.
    // Any subsequent navigation replaces the history entry, so the state does not persist.
    qp.delete('redirected');

    const qs = qp.toString();
    const cleanUrl = `${location.pathname}${qs ? `?${qs}` : ''}${location.hash ?? ''}`;

    return { redirected, redirectedFromQuery, next, cleanUrl };
};

const LoginForm = ({ loginConfig }: { loginConfig?: LoginConfig }) => {
    const location = useLocation();
    const { refreshUser } = useAuth();
    const [loginError, setLoginError] = useState<React.ReactNode>(null);

    // Resolve "next" for post-login navigation
    const state = (location.state as NavigateState | null) ?? null;
    const qp = new URLSearchParams(location.search);
    const next = normalizeNext(state?.next ?? qp.get('next'));

    return (
        <div className="d-flex align-items-center justify-content-center">
            <Card className="shadow-lg border-0 login-card">
                <Card.Header className="text-center">
                    <div className="mt-3 mb-3">
                        <img src="/logo.svg" alt="lakeFS" className="login-logo" />
                    </div>
                </Card.Header>
                <Card.Body className="p-4">
                    <Form
                        onSubmit={async (e) => {
                            e.preventDefault();
                            const form = e.target as HTMLFormElement;
                            const formData = new FormData(form);
                            try {
                                setLoginError(null);
                                const username = formData.get('username');
                                const password = formData.get('password');
                                await auth.login(username, password);
                                window.sessionStorage.setItem(LAKEFS_POST_LOGIN_NEXT, next);
                                await refreshUser({ useCache: false });
                            } catch (err) {
                                if (err instanceof AuthenticationError) {
                                    // Invalid credentials (401)
                                    const message = loginConfig?.login_failed_message || "The credentials don't match.";
                                    setLoginError(message);
                                } else if (err instanceof ServerError) {
                                    // Server errors (5xx)
                                    setLoginError('A server error occurred. Please try again in a few moments.');
                                } else if (err instanceof ClientError) {
                                    // Other client errors (4xx) - bad request, rate limiting, etc.
                                    setLoginError('Unable to process login request. Please try again.');
                                } else {
                                    // Network errors, refreshUser errors, or other unexpected errors
                                    const message =
                                        err instanceof Error
                                            ? err.message
                                            : 'Unable to complete login. Please try again.';
                                    setLoginError(message);
                                }
                            }
                        }}
                    >
                        <Form.Group controlId="username" className="mb-3">
                            <Form.Control
                                name="username"
                                type="text"
                                placeholder="Access Key ID"
                                autoFocus
                                className="bg-light"
                            />
                        </Form.Group>

                        <Form.Group controlId="password" className="mb-3">
                            <Form.Control
                                name="password"
                                type="password"
                                placeholder="Secret Access Key"
                                className="bg-light"
                            />
                        </Form.Group>

                        {!!loginError && <AlertError error={loginError} />}

                        <Button variant="primary" type="submit" className="w-100 mt-3 py-2">
                            Login
                        </Button>
                    </Form>
                </Card.Body>
            </Card>
        </div>
    );
};

const LoginPage = () => {
    const location = useLocation();
    const { response, error, loading } = useAPI(() => setup.getState());
    const setupResponse = response as SetupResponse | null;
    const { redirectedFromQuery, next, cleanUrl } = getLoginIntent(location);

    // Persist next for post-login redirect
    if (next && next.startsWith('/')) window.sessionStorage.setItem(LAKEFS_POST_LOGIN_NEXT, next);

    if (loading) return <Loading />;
    if (error)
        return <AlertError error={error} className="mt-1 w-50 m-auto" onDismiss={() => window.location.reload()} />;

    // if we are not initialized, or we are not done with comm prefs, redirect to 'setup' page
    if (setupResponse && (setupResponse.state !== SETUP_STATE_INITIALIZED || setupResponse.comm_prefs_missing)) {
        return <Navigate to={{ pathname: ROUTES.SETUP, search: location.search }} replace />;
    }

    if (redirectedFromQuery) return <Navigate to={cleanUrl} replace state={{ next }} />;

    const loginConfig = setupResponse?.login_config;

    // Default: lakeFS login form
    return <LoginForm loginConfig={loginConfig} />;
};

export default LoginPage;
