import {
    REPOSITORY_CREATE,
    REPOSITORY_LIST,
    REPOSITORY_GET, REPOSITORY_LIST_PAGINATE,
} from '../actions/repositories';

import * as async from "./async";

const initialState = {
    createIndex: 0,
    list: async.initialState,
    create: async.initialState,
    repo: async.initialState,
};

export default  (state = initialState, action) => {
    // register async reducers
    state = {
        ...state,
        list: async.reduce(REPOSITORY_LIST, state.list, action),
        create: async.actionReduce(REPOSITORY_CREATE, state.create, action),
        repo: async.reduce(REPOSITORY_GET, state.repo, action),
    };

    state.list = async.reducePaginate(REPOSITORY_LIST_PAGINATE, state.list, action);

    // handle other reducers
    switch (action.type) {
        case REPOSITORY_CREATE.success:
            return {
                ...state,
                createIndex: state.createIndex + 1,
            };
        default:
            return state;
    }
};