import * as async from "./async";
import {OBJECTS_LIST_TREE, OBJECTS_UPLOAD} from "../actions/objects";

const initialState = {
    list: async.initialState,
    upload: async.actionInitialState,
};

export default (state = initialState, action) => {
    state = {
        ...state,
        list: async.reduce(OBJECTS_LIST_TREE, state.list, action),
        upload: async.actionReduce(OBJECTS_UPLOAD, state.upload, action),
    };

    switch (action.type) {
        default:
            return state;
    }
};