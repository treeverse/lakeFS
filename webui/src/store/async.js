
export const initialState = {
    loading: true,
    payload: null,
    error: null,
};

export const actionInitialState = {
    inProgress: false,
    error: null,
    done: false,
    payload: {},
};

export const actionReduce = (actionType, state, action) => {
    if (actionType._id !== action._id) {
        return state;
    }

    switch (action.type) {
        case actionType.start:
            return {
                ...actionInitialState,
                inProgress: true,
            };
        case actionType.success:
            return {
                ...actionInitialState,
                done: true,
                payload: action.payload,
            };
        case actionType.error:
            return {
                ...actionInitialState,
                error: action.error,
            };
        case actionType.reset:
            return  {
                ...actionInitialState,
                payload: state.payload,
            };
        default:
            return state;
    }
};

export const reduce = (actionType, state, action, startFn = null, successFn = null, errorFn = null) => {
    if (actionType._id !== action._id) {
        return state;
    }
    switch (action.type) {
        case actionType.start:
            return (startFn !== null) ? startFn(state, action) : {
                ...initialState,
                loading: true,
            };
        case actionType.success:
            return (successFn !== null) ? successFn(state, action) : {
                ...initialState,
                loading: false,
                payload: action.payload,
            };
        case actionType.error:
            return (errorFn !== null) ? errorFn(state, action) : {
                ...initialState,
                loading: false,
                error: action.error,
            };
        case actionType.reset:
            return {
                ...initialState,
            };
        default:
            return state;
    }
};

export const reducePaginate =  (actionType, state, action) => {
    return reduce(actionType, state, action,
        () => {
            return {
                ...state,
                payload: state.payload, // retain original payload
            };
        },
        (newState, action) => {
            return {
                loading: false,
                error: null,
                payload: {
                    // add results to current results
                    results: [...state.payload.results, ...action.payload.results],
                    pagination: action.payload.pagination,
                },
            };
        }
    );
};
