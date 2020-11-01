import * as async from "./async";
import { SETUP_LAKEFS } from "../actions/setup";

const initialState = {
    setupLakeFS: async.actionInitialState,
};

const store = (state = initialState, action) => {
    return {
        ...state,
        setupLakeFS: async.reduce(SETUP_LAKEFS, state.setupLakeFS, action),
    };
};

export default store; 
