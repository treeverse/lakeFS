import React, {createContext, useContext, useMemo, useState, ReactNode, useCallback, useEffect} from "react";
import { useNavigate } from "react-router-dom";
import { auth } from "../api";
import { getCurrentRelativeUrl, isPublicAuthRoute, ROUTES } from "../utils";

export const LAKEFS_POST_LOGIN_NEXT = "lakefs_post_login_next";
export const AUTH_STATUS = {
    AUTHENTICATED: "authenticated",
    UNAUTHENTICATED: "unauthenticated",
    PENDING: "pending",
} as const;

export type AuthStatus = typeof AUTH_STATUS[keyof typeof AUTH_STATUS];

type User = { id?: string } | null;

type AuthContextType = {
    status: AuthStatus;
    user: User;
    refreshUser: (opts?: { useCache?: boolean }) => Promise<void>;
    setStatus: (s: AuthStatus) => void;
    onUnauthenticated: () => void;
};

const AuthContext = createContext<AuthContextType | null>(null);

export const AuthProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
    const [status, setStatus] = useState<AuthStatus>(AUTH_STATUS.PENDING);
    const [user, setUser] = useState<User>(null);
    const navigate = useNavigate();

    const refreshUser = useCallback(
        async ({ useCache = true }: { useCache?: boolean } = {}) => {
            if (!useCache && status !== AUTH_STATUS.AUTHENTICATED) setStatus(AUTH_STATUS.PENDING);
            try {
                const u = useCache
                    ? await auth.getCurrentUserWithCache()
                    : await auth.getCurrentUser();
                const ok = Boolean(u?.id);
                setUser(ok ? u : null);
                setStatus(ok ? AUTH_STATUS.AUTHENTICATED : AUTH_STATUS.UNAUTHENTICATED);
            } catch {
                setUser(null);
                setStatus(AUTH_STATUS.UNAUTHENTICATED);
            }
        },
        [status]
    );

    const onUnauthenticated = useCallback(() => {
        auth.clearCurrentUser();
        setUser(null);
        setStatus(AUTH_STATUS.UNAUTHENTICATED);

        if (isPublicAuthRoute(window.location.pathname)) return;

        navigate(ROUTES.LOGIN, {
            replace: true,
            state: { redirected: true, next: getCurrentRelativeUrl() },
        });
    }, [navigate]);

    useEffect(() => { void refreshUser({ useCache: true }); }, [refreshUser]);

    useEffect(() => {
        const onPageShow = (e: PageTransitionEvent) => {
            if ((e).persisted) {
                void refreshUser({ useCache: false });
            }
        };
        window.addEventListener('pageshow', onPageShow);
        return () => window.removeEventListener('pageshow', onPageShow);
    }, [refreshUser]);

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
        () => ({ status, user, refreshUser, setStatus, onUnauthenticated }),
        [status, user, refreshUser, onUnauthenticated]
    );

    return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};

export const useAuth = (): AuthContextType => {
    const ctx = useContext(AuthContext);
    if (!ctx) throw new Error("useAuth must be used within <AuthProvider>");
    return ctx;
};
