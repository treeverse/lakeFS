import { useAPI } from "./api";
import { auth } from "../api";
import { AUTH_STATUS, useAuth } from "../auth/authContext";
import { useCallback, useEffect, useMemo, useRef } from "react";
import { useLocation } from "react-router-dom";

const useUser = () => {
    const { status, setAuthStatus } = useAuth();
    const location = useLocation();

    const shouldHardValidate = useMemo(() => location.pathname.startsWith("/auth/") && location.pathname !== "/auth/login", [location.pathname]);
    const revalidateKey = shouldHardValidate ? `${location.pathname}${location.search}` : undefined;

    const fetcher = useCallback(async () => {
        if (status === AUTH_STATUS.UNAUTHENTICATED) return null;
        if (shouldHardValidate) return auth.getCurrentUser();
        const cached = await auth.getCurrentUserWithCache();
        if (cached?.id) return cached;
        return auth.getCurrentUser();
    }, [status, shouldHardValidate]);

    const { response, loading, error } = useAPI(fetcher, [status, revalidateKey]);

    const checkedRef = useRef(false);
    useEffect(() => {
        if (!loading) checkedRef.current = true;
    }, [loading]);

    useEffect(() => {
        if (loading) return;
        const next = response?.id ? AUTH_STATUS.AUTHENTICATED : AUTH_STATUS.UNAUTHENTICATED;
        setAuthStatus(next);
    }, [loading, response, setAuthStatus]);

    const user = response?.id ? response : null;
    return { user, loading, error, checked: checkedRef.current };
};

export default useUser;