import * as async from "./async";
import {MERGE_REFS, DIFF_REFS, DIFF_REFS_PAGINATE} from "../actions/refs";

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

    state.diff  = async.reducePaginate(DIFF_REFS_PAGINATE, state.diff, action);

    switch (action.type) {
        default:
            return state;
    }
};
