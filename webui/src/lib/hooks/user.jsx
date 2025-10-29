import { useAPI } from "./api";
import { auth } from "../api";
import { AUTH_STATUS, useAuth } from "../auth/authContext";
import { useCallback, useEffect, useMemo } from "react";
import { useLocation } from "react-router-dom";

const useUser = () => {
    const { setAuthStatus } = useAuth();
    const location = useLocation();
    const hardValidate = useMemo(
        () => location.pathname.startsWith("/auth/") && location.pathname !== "/auth/login",
        [location.pathname]
    );

    const fetcher = useCallback(async () => {
        if (hardValidate) return auth.getCurrentUser();
        const cached = await auth.getCurrentUserWithCache();
        return cached?.id ? cached : auth.getCurrentUser();
    }, [hardValidate]);

    const { response, loading, error } = useAPI(fetcher, [
        hardValidate ? `${location.pathname}${location.search}` : undefined,
    ]);

    useEffect(() => {
        if (!loading) setAuthStatus(response?.id ? AUTH_STATUS.AUTHENTICATED : AUTH_STATUS.UNAUTHENTICATED);
    }, [loading, response, setAuthStatus]);

    return { user: response?.id ? response : null, loading, error, checked: !loading };
};

export default useUser;