

const hydrateUser = () => {
    if (window.localStorage['user'] !== undefined) {
        let user = JSON.parse(window.localStorage['user']);
        return user;
    }
    return null;
}

const initialState = {
    isLoggedIn: false,
    user: hydrateUser(),
    loginError: null,
};

export default (state = initialState, action) => {
    console.log('got type: ', action);
    switch (action.type) {
        case 'LOGIN_ERROR':
            return {
                ...state,
                user: null,
                loginError: action.error,
            };
        case 'LOGIN_SUCCESS':
            // also save to localstorage
            window.localStorage['user'] = JSON.stringify(action.user, null, "");
            return {
                ...state,
                user: action.user,
                loginError: null,
            };
        case 'LOGIN':
            return {
                ...state,
                loginError: "could not login",
            };
        case 'LOGOUT':
            window.localStorage.removeItem('user');
            return {
                ...initialState,
                user: null,
            };
    }
    return state;
};