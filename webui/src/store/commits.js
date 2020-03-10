import * as async from "./async";
import {COMMITS_LIST, COMMITS_COMMIT, COMMITS_COMMIT_RESET} from "../actions/commits";


const initialState = {
    log: async.initialState,
    commit: {inProgress: false, error: null, done: false},
};

export default  (state = initialState, action) => {
    state = {
        ...state,
        log: async.reduce(COMMITS_LIST, state.log, action),
    };

    switch (action.type) {
        case COMMITS_COMMIT.start:
            return {
                ...state,
                commit: {inProgress: true, error: null, done: false},
            };
        case COMMITS_COMMIT.success:
            return {
                ...state,
                commit: {inProgress: false, error: null, done: true},
            };
        case COMMITS_COMMIT.error:
            return {
                ...state,
                commit: {inProgress: false, error: action.error, done: false},
            };
        case COMMITS_COMMIT_RESET:
            return {
                ...state,
                commit: {...initialState.commit},
            };
        default:
            return state;
    }
};