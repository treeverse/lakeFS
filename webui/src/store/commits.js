import * as async from "./async";
import {COMMITS_LIST,COMMITS_LIST_PAGINATE, COMMITS_COMMIT} from "../actions/commits";


const initialState = {
    log: async.initialState,
    commit: async.actionInitialState,
};

const store = (state = initialState, action) => {
    state = {
        ...state,
        log: async.reduce(COMMITS_LIST, state.log, action),
        commit: async.actionReduce(COMMITS_COMMIT, state.commit, action),
    };
    state.log = async.reducePaginate(COMMITS_LIST_PAGINATE, state.log, action);
    return state;
};

export default store;
