import {useAPI} from "./api";
import {auth} from "../api";
import {AUTH_STATUS, useAuth} from "../auth/authContext";
import {useEffect, useCallback} from "react";

const useUser = () => {
    const { status, setAuthStatus } = useAuth();

    const fetcher = useCallback(() => {
        if (status === AUTH_STATUS.UNAUTHENTICATED) return Promise.resolve(null);
        return auth.getCurrentUserWithCache();
    }, [status]);

    const { response, loading, error } = useAPI(fetcher, [status]);

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
