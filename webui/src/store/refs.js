import * as async from "./async";
import {MERGE_REFS, DIFF_REFS} from "../actions/refs";

const initialState = {
    diff: async.initialState,
    merge: async.initialState,
};

export default (state = initialState, action) => {
    state = {
        ...state,
        diff: async.reduce(DIFF_REFS, state.diff, action),
        merge: async.reduce(MERGE_REFS, state.merge, action),
    };

    switch (action.type) {
        default:
            return state;
    }
};
