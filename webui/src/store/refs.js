
import * as async from "./async";
import {DIFF_REFS, RESET_DIFF} from "../actions/refs";

const initialState = {
    diff: async.initialState,
};

export default (state = initialState, action) => {
    state = {
        ...state,
        diff: async.reduce(DIFF_REFS, state.diff, action),
    };

    switch (action.type) {
        case RESET_DIFF:
            return initialState;
        default:
            return state;
    }
};