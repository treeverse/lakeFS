import * as async from "./async";
import {OBJECTS_LIST_TREE, OBJECTS_UPLOAD, OBJECTS_UPLOAD_RESET} from "../actions/objects";

const initialState = {
    list: async.initialState,
    branches: async.initialState,
    upload: {inProgress: false, error: null, done: false},
};

export default (state = initialState, action) => {
    state = {
        ...state,
        list: async.reduce(OBJECTS_LIST_TREE, state.list, action),
    };

    switch (action.type) {
        case OBJECTS_UPLOAD.start:
            return {
                ...state,
                upload: {inProgress: true, error: null, done: false},
            };
        case OBJECTS_UPLOAD.success:
            return {
                ...state,
                upload: {inProgress: false, error: null, done: true},
            };
        case OBJECTS_UPLOAD.error:
            return {
                ...state,
                upload: {inProgress: false, error: action.error, done: false},
            };
        case OBJECTS_UPLOAD_RESET:
            return {
                ...state,
                upload: {...initialState.upload},
            };
        default:
            return state;
    }
};