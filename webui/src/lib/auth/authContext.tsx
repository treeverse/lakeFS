import React, {createContext, useContext, useMemo, useState, ReactNode, useCallback, useEffect} from "react";
import { useNavigate } from "react-router-dom";
import { auth } from "../api";
import {getCurrentRelativeUrl, isPublicAuthRoute, ROUTES} from "../utils";

export const LAKEFS_POST_LOGIN_NEXT = "lakefs_post_login_next";
export const AUTH_STATUS = {
    AUTHENTICATED: "authenticated",
    UNAUTHENTICATED: "unauthenticated",
    PENDING: "pending",
} as const;

export type AuthStatus = typeof AUTH_STATUS[keyof typeof AUTH_STATUS];

type AuthContextType = {
    status: AuthStatus;
    setStatus: (s: AuthStatus) => void;
    onUnauthorized: () => void;
};

const AuthContext = createContext<AuthContextType | null>(null);

export const AuthProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
    const [status, setStatus] = useState<AuthStatus>(AUTH_STATUS.PENDING);
    const navigate = useNavigate();

    const onUnauthorized = useCallback(() => {
        auth.clearCurrentUser();
        setStatus(AUTH_STATUS.UNAUTHENTICATED);

        if (isPublicAuthRoute(window.location.pathname)) return;

        navigate(ROUTES.LOGIN, {
            replace: true,
            state: { redirected: true, next: getCurrentRelativeUrl() },
        });
    }, [navigate]);

    useEffect(() => {
        if (status === AUTH_STATUS.AUTHENTICATED) {
            const postLoginNext = window.sessionStorage.getItem(LAKEFS_POST_LOGIN_NEXT);
            if (postLoginNext && postLoginNext.startsWith("/")) {
                window.sessionStorage.removeItem(LAKEFS_POST_LOGIN_NEXT);
                const next = getCurrentRelativeUrl();
                if (next !== postLoginNext) {
                    navigate(postLoginNext, { replace: true });
                }
            }
        }
    }, [status, navigate]);

    const value = useMemo<AuthContextType>(
        () => ({
            status,
            setStatus,
            onUnauthorized,
        }),
        [status, onUnauthorized]
    );

    return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};

export const useAuth = (): AuthContextType => {
    const ctx = useContext(AuthContext);
    if (!ctx) throw new Error("useAuth must be used within <AuthProvider>");
    return ctx;
};
