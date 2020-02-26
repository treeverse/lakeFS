import {
    REPOSITORY_CREATE_SUCCESS,
    REPOSITORY_CREATE_ERROR,
    REPOSITORIES_LIST_START,
    REPOSITORIES_LIST_ERROR,
    REPOSITORIES_LIST_SUCCESS,
    REPOSITORY_DELETE_ERROR,
    REPOSITORY_DELETE_SUCCESS
} from '../actions/repositories';


const initialState = {
    loading: true,
    list: [],
    error: null,
    createError: null,
    deleteError: null,
};

export default  (state = initialState, action) => {
    switch (action.type) {
        case REPOSITORIES_LIST_START:
            return initialState;
        case REPOSITORIES_LIST_SUCCESS:
            return {
                ...state,
                list: action.list,
                loading: false,
                error: null,
            };
        case REPOSITORIES_LIST_ERROR:
            return {
                ...initialState,
                error: action.error,
            };
        case REPOSITORY_CREATE_ERROR:
            return {
                ...state,
                createError: action.error,
            };
        case REPOSITORY_CREATE_SUCCESS:
            return {
                ...state,
                createError: null,
            };
        case REPOSITORY_DELETE_ERROR:
            return {
                ...state,
                deleteError: action.error,
            };
        case REPOSITORY_DELETE_SUCCESS:
            return {
                ...state,
                deleteError: null,
            };
        default:
            return state;
    }
};