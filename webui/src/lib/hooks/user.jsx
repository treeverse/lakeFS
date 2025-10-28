import {useAPI} from "./api";
import {auth} from "../api";
import {AUTH_STATUS, useAuth} from "../auth/authContext";
import {useEffect, useCallback} from "react";

const useUser = () => {
    const { status, setAuthStatus } = useAuth();

    const fetcher = useCallback(() => {
        if (status === AUTH_STATUS.UNAUTHENTICATED) return Promise.resolve(null);
        return auth.getCurrentUser();
    }, [status]);

    const { response, loading, error } = useAPI(fetcher, [status]);

    useEffect(() => {
        if (loading) return;
        const hasValidUser = !!(response && response.id);

        if (hasValidUser) {
            if (status !== AUTH_STATUS.AUTHENTICATED) setAuthStatus(AUTH_STATUS.AUTHENTICATED);
        } else {
            if (status === AUTH_STATUS.UNKNOWN) setAuthStatus(AUTH_STATUS.UNAUTHENTICATED);
        }
    }, [loading, response, status, setAuthStatus]);

    const user = status === AUTH_STATUS.AUTHENTICATED ? response : null;
    return { user, loading, error };
};

export default useUser;
