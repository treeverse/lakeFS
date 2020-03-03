import * as async from "./async";
import {OBJECTS_LIST_TREE} from "../actions/objects";

const initialState = {
    list: async.initialState,
    branches: async.initialState,
};

export default (state = initialState, action) => {
    state = {
        ...state,
        list: async.reduce(OBJECTS_LIST_TREE, state.list, action),
    };

    switch (action.type) {
        default:
            return state;
    }
};