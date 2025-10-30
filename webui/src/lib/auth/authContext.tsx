import React, {createContext, useContext, useMemo, useState, ReactNode, useCallback, useEffect} from "react";
import { useNavigate } from "react-router-dom";
import { auth } from "../api";

export const AUTH_STATUS = {
    AUTHENTICATED: "authenticated",
    UNAUTHENTICATED: "unauthenticated",
    UNKNOWN: "unknown",
} as const;

export type AuthStatus = typeof AUTH_STATUS[keyof typeof AUTH_STATUS];

type AuthContextType = {
    status: AuthStatus;
    setAuthStatus: (s: AuthStatus) => void;
    onUnauthorized: () => void;
};

const AuthContext = createContext<AuthContextType | null>(null);

const isPublicAuthRoute = (path: string) => {
    if (path === "/auth/login") return true;
    return path.startsWith("/auth/oidc");
};

export const AuthProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
    const [status, setStatus] = useState<AuthStatus>(AUTH_STATUS.UNKNOWN);
    const navigate = useNavigate();

    const onUnauthorized = useCallback(() => {
        auth.clearCurrentUser();
        setStatus(AUTH_STATUS.UNAUTHENTICATED);
        const path = window.location.pathname;
        const next = path + (window.location.search || "") + (window.location.hash || "");
        if (!isPublicAuthRoute(path)) {
            navigate("/auth/login", {
                replace: true,
                state: { redirected: true, next },
            });
        }
    }, [navigate]);

    useEffect(() => {
        if (status === AUTH_STATUS.AUTHENTICATED) {
            const stored = window.sessionStorage.getItem("post_login_next");
            if (stored && stored.startsWith("/")) {
                window.sessionStorage.removeItem("post_login_next");
                const here = window.location.pathname + (window.location.search || "") + (window.location.hash || "");
                if (here !== stored) {
                    navigate(stored, { replace: true });
                }
            }
        }
    }, [status, navigate]);

    const value = useMemo<AuthContextType>(
        () => ({
            status,
            setAuthStatus: setStatus,
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
