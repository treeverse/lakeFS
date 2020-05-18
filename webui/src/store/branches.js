import * as async from './async';

import {
    BRANCHES_CREATE,
    BRANCHES_REVERT,
    BRANCHES_LIST,
    BRANCHES_LIST_PAGINATE,
} from '../actions/branches';


const initialState = {
    list: async.initialState,
    create: async.actionInitialState,
    revert: async.actionInitialState,
};

export default  (state = initialState, action) => {
    state = {
        ...state,
        list: async.reduce(BRANCHES_LIST, state.list, action),
        create: async.actionReduce(BRANCHES_CREATE, state.create, action),
        revert: async.actionReduce(BRANCHES_REVERT, state.revert, action),
    };

    state.list  = async.reducePaginate(BRANCHES_LIST_PAGINATE, state.list, action);

    switch (action.type) {
        default:
            return state;
    }
};
