import {useAPI} from "../../rest/hooks";
import {auth} from "../../rest/api";


const useUser = () => {
    const { response, loading, error } = useAPI(() => auth.getCurrentUser(), [])
    return { user: response, loading, error }
}

export default useUser