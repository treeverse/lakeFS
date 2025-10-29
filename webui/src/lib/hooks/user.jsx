import { useAPI } from "./api";
import { auth } from "../api";
import { AUTH_STATUS, useAuth } from "../auth/authContext";
import { useCallback, useEffect, useMemo } from "react";
import { useLocation } from "react-router-dom";

const hasAuthSessionCookie = () =>
    typeof document !== "undefined" &&
    (document.cookie.includes("internal_auth_session=") ||
        document.cookie.includes("oidc_auth_session="));

const useUser = () => {
    const { setAuthStatus } = useAuth();
    const location = useLocation();

    const shouldHardValidate = useMemo(
        () => location.pathname.startsWith("/auth/") && location.pathname !== "/auth/login",
        [location.pathname]
    );
    const revalidateKey = shouldHardValidate ? `${location.pathname}${location.search}` : undefined;

    const fetcher = useCallback(async () => {
        if (!hasAuthSessionCookie()) return null;          // ⟵ קצר ולעניין
        if (shouldHardValidate) return auth.getCurrentUser();
        const cached = await auth.getCurrentUserWithCache();
        return cached?.id ? cached : auth.getCurrentUser();
    }, [shouldHardValidate]);

    const { response, loading, error } = useAPI(fetcher, [revalidateKey]);

    useEffect(() => {
        if (loading) return;
        const next = response?.id ? AUTH_STATUS.AUTHENTICATED : AUTH_STATUS.UNAUTHENTICATED;
        setAuthStatus(prev => (prev === next ? prev : next));
    }, [loading, response, setAuthStatus]);

    return { user: response?.id ? response : null, loading, error, checked: !loading };
};

export default useUser;