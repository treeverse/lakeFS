import {useAPI} from "./api";
import {auth} from "../api";
import {AUTH_STATUS, useAuth} from "../auth/authContext";
import {useEffect, useCallback} from "react";

const useUser = () => {
    const { status, setAuthStatus } = useAuth();

    const fetcher = useCallback(() => auth.getCurrentUserWithCache(), []);
    const { response, loading, error } = useAPI(fetcher, [status]);

    useEffect(() => {
        if (loading) return;
        const hasUser = !!response?.id;
        setAuthStatus(hasUser ? AUTH_STATUS.AUTHENTICATED : AUTH_STATUS.UNAUTHENTICATED);
    }, [loading, response, setAuthStatus]);

    const user = status === AUTH_STATUS.AUTHENTICATED ? response : null;
    return { user, loading, error };
};

export default useUser;
