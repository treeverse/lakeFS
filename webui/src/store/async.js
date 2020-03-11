
export const initialState = {
    loading: true,
    payload: null,
    error: null,
};

export const actionInitialState = {
    inProgress: false,
    error: null,
    done: false
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
                inProgress: false,
                error: null,
                done: true,
            };
        case actionType.error:
            return {
                inProgress: false,
                error: action.error,
                done: true,
            };
        case actionType.reset:
            return  {
                ...actionInitialState,
            };
        default:
            return state;
    }
};

export const reduce = (actionType, state, action) => {
    if (actionType._id !== action._id) {
        return state;
    }
    switch (action.type) {
        case actionType.start:
            return {
                ...initialState,
                loading: true,
            };
        case actionType.success:
            return {
                ...initialState,
                loading: false,
                payload: action.payload,
            };
        case actionType.error:
            return {
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