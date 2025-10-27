import {useAPI} from "./api";
import {auth} from "../api";
import {AUTH_STATUS, useAuth} from "../auth/authContext";
import {useEffect, useCallback} from "react";

const useUser = () => {
    const { status, setAuthStatus } = useAuth();

    const fetcher = useCallback(() => auth.getCurrentUser(), []);
    const { response, loading, error } = useAPI(fetcher, [status]);

    useEffect(() => {
        if (loading) return;
        const user = response;
        const hasValidUser = !!(user && user.id);

        if (hasValidUser) {
            if (status !== AUTH_STATUS.AUTHENTICATED) setAuthStatus(AUTH_STATUS.AUTHENTICATED);
            return;
        }

        if (error?.name === 'AuthenticationError' || (response && !hasValidUser)) {
            if (status !== AUTH_STATUS.UNAUTHENTICATED) setAuthStatus(AUTH_STATUS.UNAUTHENTICATED);
        }
    }, [loading, response, error, status, setAuthStatus]);

    const user = status === AUTH_STATUS.AUTHENTICATED ? response : null;
    return { user, loading, error };
};

export default useUser;
