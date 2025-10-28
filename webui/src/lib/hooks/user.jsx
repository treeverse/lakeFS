import {useAPI} from "./api";
import {auth} from "../api";
import {AUTH_STATUS, useAuth} from "../auth/authContext";
import {useEffect, useCallback, useMemo} from "react";
import {useLocation} from "react-router-dom";

const useUser = () => {
    const { status, setAuthStatus } = useAuth();
    const location = useLocation();

    const shouldRevalidateOnThisRoute = useMemo(() => location.pathname.startsWith("/auth/") && location.pathname !== "/auth/login", [location.pathname]);
    const revalidateKey = shouldRevalidateOnThisRoute ? `${location.pathname}${location.search}` : undefined;

    const fetcher = useCallback(() => {
        if (status === AUTH_STATUS.UNAUTHENTICATED) return Promise.resolve(null);
        return shouldRevalidateOnThisRoute ? auth.getCurrentUser() : auth.getCurrentUserWithCache();
    }, [status, shouldRevalidateOnThisRoute]);

    const { response, loading, error } = useAPI(fetcher, [status, revalidateKey]);

    useEffect(() => {
        if (loading) return;
        const hasUser = !!response?.id;
        const nextStatus = hasUser ? AUTH_STATUS.AUTHENTICATED : AUTH_STATUS.UNAUTHENTICATED;
        if (status !== nextStatus) setAuthStatus(nextStatus);
    }, [loading, response, status, setAuthStatus]);

    const user = status === AUTH_STATUS.AUTHENTICATED ? response : null;
    return { user, loading, error };
};

export default useUser;