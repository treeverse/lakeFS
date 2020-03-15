
import * as async from "./async";
import {DIFF_REFS} from "../actions/refs";

const initialState = {
    diff: async.initialState,
};

export default (state = initialState, action) => {
    state = {
        ...state,
        diff: async.reduce(DIFF_REFS, state.diff, action),
    };

    switch (action.type) {
        default:
            return state;
    }
};