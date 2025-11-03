import { useAPI } from "./api";
import { auth } from "../api";
import { AUTH_STATUS, useAuth } from "../auth/authContext";
import { useCallback, useEffect } from "react";

const useUser = () => {
    const { setStatus } = useAuth();
    const fetcher = useCallback(() => auth.getCurrentUser(), []);
    const { response, loading, error } = useAPI(fetcher, []);

    useEffect(() => {
        if (!loading) {
            setStatus(response?.id ? AUTH_STATUS.AUTHENTICATED : AUTH_STATUS.UNAUTHENTICATED);
        }
    }, [loading, response, setStatus]);

    return { user: response?.id ? response : null, loading, error, checked: !loading };
};

export default useUser;