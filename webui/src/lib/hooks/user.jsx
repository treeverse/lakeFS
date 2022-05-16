import {useAPI} from "./api";
import {auth} from "../api";


const useUser = () => {
    const { response, loading, error } = useAPI(() => auth.getCurrentUserWithCache(), []);
    return { user: response, loading, error };
}

export default useUser;
