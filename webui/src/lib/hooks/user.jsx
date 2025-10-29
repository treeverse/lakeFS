import { useAPI } from "./api";
import { auth } from "../api";
import { AUTH_STATUS, useAuth } from "../auth/authContext";
import { useCallback, useEffect, useMemo } from "react";
import { useLocation } from "react-router-dom";

const hasAuthSessionCookie = () => {
    const c = typeof document === "undefined" ? "" : document.cookie || "";
    return c.includes("internal_auth_session=") || c.includes("oidc_auth_session=");
};

const useUser = () => {
    const { setAuthStatus } = useAuth();
    const location = useLocation();

    const hardValidate = useMemo(() => location.pathname.startsWith("/auth/") && location.pathname !== "/auth/login", [location.pathname]);

    const hasSession = hasAuthSessionCookie();
    useEffect(() => {
        if (!hasSession) setAuthStatus(AUTH_STATUS.UNAUTHENTICATED);
    }, [hasSession, setAuthStatus]);

    if (!hasSession) return { user: null, loading: false, error: null, checked: true };

    const fetcher = useCallback(async () => {
        if (hardValidate) return auth.getCurrentUser();
        const cached = await auth.getCurrentUserWithCache();
        if (cached?.id) return cached;
        return auth.getCurrentUser();
    }, [hardValidate]);

    const { response, loading, error } = useAPI(fetcher, [hardValidate ? `${location.pathname}${location.search}` : undefined]);

    useEffect(() => {
        if (loading) return;
        const next = response?.id ? AUTH_STATUS.AUTHENTICATED : AUTH_STATUS.UNAUTHENTICATED;
        setAuthStatus(prev => (prev === next ? prev : next));
    }, [loading, response, setAuthStatus]);

    const user = response?.id ? response : null;
    return { user, loading, error, checked: !loading };
};

export default useUser;