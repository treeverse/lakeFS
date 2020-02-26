import {
    OBJECTS_NAVIGATE,
    OBJECTS_NAVIGATE_SUCCESS,
    OBJECTS_NAVIGATE_ERROR,
    OBJECTS_NAVIGATE_SET_BRANCH,
} from '../actions/objects';


const initialState = {
    loading: false,
    error: null,
    entries: [],
    path: "",
    repoId: "",
    branchId: "",
};

export default (state = initialState, action) => {
    switch (action.type) {
        case OBJECTS_NAVIGATE:
            return {
                ...initialState,
                loading: true,
                path: action.path,
            };
        case OBJECTS_NAVIGATE_SUCCESS:
            return {
                ...state,
                loading: false,
                entries: action.entries,
            };
        case OBJECTS_NAVIGATE_ERROR:
            return {
                ...initialState,
                loading: false,
                error: action.error,
            };
        case OBJECTS_NAVIGATE_SET_BRANCH:
            if (action.repoId !== state.repoId || action.branchId !== state.brachId) {
                return {
                    ...initialState,
                };
            } else {
                return state;
            }
        default:
            return state;
    }
};