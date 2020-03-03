

export const initialState = {
    loading: true,
    payload: null,
    error: null,
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
        default:
            return state;
    }
};