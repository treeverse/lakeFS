import * as async from "./async";
import {COMMITS_LIST} from "../actions/commits";


const initialState = {
    log: async.initialState,
};

export default  (state = initialState, action) => {
    state = {
        ...state,
        log: async.reduce(COMMITS_LIST, state.log, action),
    };

    switch (action.type) {
        default:
            return state;
    }
};