
import {
    BRANCH_CREATE_ERROR,
    BRANCHES_LIST_START,
    BRANCHES_LIST_ERROR,
    BRANCHES_LIST_SUCCESS,
    BRANCH_PREFIX_LOADED,
    BRANCH_SOURCE_SELECT
} from '../actions/branches';

const initialState = {
    loading: true,
    list: [],
    error: null,
    createError: null,
    prefixList: [],
    sourceBranchSelection: null,
};

export default  (state = initialState, action) => {
    switch (action.type) {
        case BRANCHES_LIST_START:
            return initialState;
        case BRANCHES_LIST_SUCCESS:
            return {
                ...state,
                list: action.list,
                loading: false,
                error: null,
            };
        case BRANCHES_LIST_ERROR:
            return {
                ...initialState,
                error: action.error,
            };
        case BRANCH_PREFIX_LOADED:
            return {
                ...state,
                prefixList: action.list,
            };
        case BRANCH_SOURCE_SELECT:
            return {
                ...state,
                sourceBranchSelection: action.branch,
            };
        case BRANCH_CREATE_ERROR:
            return {
                ...state,
                createError: action.error,
            };
        default:
            return state;
    }
};