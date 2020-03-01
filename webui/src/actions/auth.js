import apiRequest from './api';

export const LOGIN_ERROR = 'LOGIN_ERROR';
export const LOGIN_SUCCESS = 'LOGIN_SUCCESS';
export const LOGOUT = 'LOGOUT';

export const loginError = (err) => ({
    type: LOGIN_ERROR,
    error: err,
});

export const loginSuccess = (user) => ({
    type: LOGIN_SUCCESS,
    user: user,
});

export const logout = () => ({
    type: LOGOUT,
});

export const login = ( accessKeyId, secretAccessKey, redirectFn) => {
    return async function(dispatch)  {
        try {
            let response = await apiRequest(  '/authentication',
                undefined,
                undefined,
                {accessKeyId, secretAccessKey});
            if (response.status === 401) {
                dispatch(loginError('invalid credentials, try again'))
                return
            }
            if (response.status !== 200) {
                dispatch(loginError('unknown authentication error'));
                return
            }

            let responseJSON = await response.json();
            dispatch(loginSuccess({
                accessKeyId,
                secretAccessKey,
                ...responseJSON.user,
            }));
            redirectFn();

        } catch (error) {
            dispatch(loginError(`error connecting to API server: ${error}`));
        }
    };
};