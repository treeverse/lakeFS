
import {
    AUTH_LOGIN,
    AUTH_LOGOUT,
    AUTH_REDIRECTED,
} from '../actions/auth';

const hydrateUser = () => {
    if (window.localStorage['user'] !== undefined) {
        return JSON.parse(window.localStorage['user']);
    }
    return null;
};

const initialState = {
    user: hydrateUser(),
    loginError: null,
    redirectTo: null,
};

export default (state = initialState, action) => {
    switch (action.type) {
        case AUTH_LOGIN.error:
            return {
                ...state,
                user: null,
                loginError: action.error,
            };
        case AUTH_LOGIN.success:
            // also save to localstorage
            window.localStorage['user'] = JSON.stringify(action.payload.user, null, "");
            return {
                ...state,
                user: action.payload.user,
                redirectTo: action.payload.redirectTo,
                loginError: null,
            };
        case AUTH_LOGOUT.success:
            window.localStorage.removeItem('user');
            return {
                ...initialState,
                user: null,
            };
        case AUTH_REDIRECTED:
            return {
                ...state,
                redirectTo: null,
            };
        default:
            return state;
    }
};
