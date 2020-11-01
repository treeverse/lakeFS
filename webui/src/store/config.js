import { CONFIG_GET } from "../actions/config";

import * as async from "./async";

const initialState = {
  config: async.initialState,
};

const store = (state = initialState, action) => {
  // register async reducers
  state = {
    ...state,
    config: async.reduce(CONFIG_GET, state.config, action),
  };

  // handle other reducers
  switch (action.type) {
    case CONFIG_GET.success:
    default:
      return state;
  }
};

export default store;
