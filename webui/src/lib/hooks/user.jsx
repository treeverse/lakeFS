import {useAPI} from "./api";
import {auth} from "../api";
import {useAuth} from "../auth/authContext";
import {AUTH_STATUS} from "../auth/status";

const useUser = () => {
     const { status } = useAuth();
     const fetcher = status === AUTH_STATUS.AUTHENTICATED
         ? () => auth.getCurrentUserWithCache()
         : async () => null;

     const { response, loading, error } = useAPI(fetcher, [status]);
     const user = status === AUTH_STATUS.AUTHENTICATED ? response : null;
     return { user, loading, error };
    };

export default useUser;
