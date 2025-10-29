import { useAPI } from "./api";
import { auth } from "../api";
import { AUTH_STATUS, useAuth } from "../auth/authContext";
import { useCallback, useEffect, useMemo } from "react";
import { useLocation } from "react-router-dom";

const useUser = () => {
    const { status, setAuthStatus } = useAuth();
    const location = useLocation();

    const shouldHardValidate = useMemo(
        () => location.pathname.startsWith("/auth/") && location.pathname !== "/auth/login",
        [location.pathname]
    );
    const revalidateKey = shouldHardValidate ? `${location.pathname}${location.search}` : undefined;

    const fetcher = useCallback(async () => {
        if (status === AUTH_STATUS.UNAUTHENTICATED) return null;
        if (shouldHardValidate) return auth.getCurrentUser();
        const cached = await auth.getCurrentUserWithCache();
        return cached?.id ? cached : auth.getCurrentUser();
    }, [status, shouldHardValidate]);

    const { response, loading, error } = useAPI(fetcher, [status, revalidateKey]);

    useEffect(() => {
        const onPageShow = (e) => {
            if (e.persisted) {
                auth.getCurrentUser().then((u) => {
                    setAuthStatus(u?.id ? AUTH_STATUS.AUTHENTICATED : AUTH_STATUS.UNAUTHENTICATED);
                });
            }
        };
        window.addEventListener("pageshow", onPageShow);
        return () => window.removeEventListener("pageshow", onPageShow);
    }, [setAuthStatus]);

    useEffect(() => {
        if (loading) return;
        setAuthStatus(response?.id ? AUTH_STATUS.AUTHENTICATED : AUTH_STATUS.UNAUTHENTICATED);
    }, [loading, response, setAuthStatus]);

    const user = response?.id ? response : null;
    return { user, loading, error };
};

export default useUser;