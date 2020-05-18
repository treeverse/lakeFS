import * as async from "./async";
import { SETUP_LAKEFS } from "../actions/setup";

const initialState = {
    setupLakeFS: async.actionInitialState,
};

export default (state = initialState, action) => {
    return {
        ...state,
        setupLakeFS: async.actionReduce(SETUP_LAKEFS, state.setupLakeFS, action),
    };
};
