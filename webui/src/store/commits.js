import * as async from "./async";
import {COMMITS_LIST, COMMITS_COMMIT} from "../actions/commits";


const initialState = {
    log: async.initialState,
    commit: async.actionInitialState,
};

export default  (state = initialState, action) => {
    state = {
        ...state,
        log: async.reduce(COMMITS_LIST, state.log, action),
        commit: async.actionReduce(COMMITS_COMMIT, state.commit, action),
    };

    switch (action.type) {
        default:
            return state;
    }
};