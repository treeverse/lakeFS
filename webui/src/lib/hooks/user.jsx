import { useAPI } from "./api";
import { auth } from "../api";
import { AUTH_STATUS, useAuth } from "../auth/authContext";
import { useCallback, useEffect } from "react";
import { useLocation } from "react-router-dom";

const useUser = () => {
    const { setAuthStatus } = useAuth();
    const location = useLocation();

    const fetcher = useCallback(() => auth.getCurrentUser(), []);

    const { response, loading, error } = useAPI(fetcher, [
        `${location.pathname}${location.search}${location.hash}`,
    ]);

    useEffect(() => {
        if (!loading) {
            setAuthStatus(response?.id ? AUTH_STATUS.AUTHENTICATED : AUTH_STATUS.UNAUTHENTICATED);
        }
    }, [loading, response, setAuthStatus]);

    return { user: response?.id ? response : null, loading, error, checked: !loading };
};

export default useUser;