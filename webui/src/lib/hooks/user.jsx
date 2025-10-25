import {useAPI} from "./api";
import {auth} from "../api";
import {AUTH_STATUS, useAuth} from "../auth/authContext";
import {useCallback} from "react";

const useUser = () => {
    const { status } = useAuth();

    const fetcher = useCallback(() => {
        return status === AUTH_STATUS.AUTHENTICATED
            ? auth.getCurrentUser()
            : Promise.resolve(null);
    }, [status]);

     const { response, loading, error } = useAPI(fetcher, [status]);
     const user = status === AUTH_STATUS.AUTHENTICATED ? response : null;
     return { user, loading, error };
    };

export default useUser;
