
const initialState = {
    loading: false,
    inFlight: 0,
};

export default  (state = initialState, action) => {
    let inFlight = state.inFlight;
    if (!!action.asyncStart) {
        inFlight = state.inFlight + 1;
    } else if (!!action.asyncEnd) {
        inFlight = state.inFlight -1;
    }
    return {
        loading: inFlight > 0,
        inFlight,
    };
};