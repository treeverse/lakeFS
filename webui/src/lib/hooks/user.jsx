import { useAPI } from "./api";
import { auth } from "../api";
import { AUTH_STATUS, useAuth } from "../auth/authContext";
import { useCallback, useEffect, useMemo } from "react";
import { useLocation } from "react-router-dom";

const useUser = () => {
    const { setAuthStatus } = useAuth();
    const location = useLocation();

    const shouldHardValidate = useMemo(() => location.pathname.startsWith("/auth/") && location.pathname !== "/auth/login", [location.pathname]);
    const revalidateKey = shouldHardValidate ? `${location.pathname}${location.search}` : undefined;

    const fetcher = useCallback(async () => {
        if (shouldHardValidate) return auth.getCurrentUser();
        const cached = await auth.getCurrentUserWithCache();
        if (cached?.id) return cached;
        return auth.getCurrentUser();
    }, [shouldHardValidate]);

    const { response, loading, error } = useAPI(fetcher, [revalidateKey]);

    useEffect(() => {
        if (loading) return;
        const next = response?.id ? AUTH_STATUS.AUTHENTICATED : AUTH_STATUS.UNAUTHENTICATED;
        setAuthStatus(prev => (prev === next ? prev : next));
    }, [loading, response, setAuthStatus]);

    const user = response?.id ? response : null;
    return { user, loading, error, checked: !loading };
};

export default useUser;