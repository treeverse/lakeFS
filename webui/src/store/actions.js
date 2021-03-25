import * as async from './async';

import {
    ACTIONS_RUNS,
    ACTIONS_RUN,
    ACTIONS_RUN_HOOKS,
    ACTIONS_RUN_HOOK_OUTPUT,
} from '../actions/actions';


const initialState = {
    runs: async.initialState,
    run: async.initialState,
    runHooks: async.initialState,
    runHookOutput: async.actionInitialState,
};

const store = (state = initialState, action) => {
    state = {
        ...state,
        runs: async.reduce(ACTIONS_RUNS, state.runs, action),
        run: async.reduce(ACTIONS_RUN, state.run, action),
        runHooks: async.reduce(ACTIONS_RUN_HOOKS, state.runHooks, action),
        runHookOutput: async.actionReduce(ACTIONS_RUN_HOOK_OUTPUT, state.runHookOutput, action),
    };
    return state;
};

export default store;
