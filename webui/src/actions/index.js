import base64 from 'base-64';

const api = '/api/v1';


const basicAuth = (accessKeyId, secretAccessKey) => ({
    headers: new Headers({
        "Authorization": `Basic ${base64.encode(`${accessKeyId}:${secretAccessKey}`)}`,
    }),
});

const cachedBasicAuth = () => {
    let userdata = window.localStorage.getItem("user");
    userdata = JSON.stringify(userdata);
    return {
        headers: new Headers({
            "Authorization": `Basic ${base64.encode(`${userdata.accessKeyId}:${userdata.secretAccessKey}`)}`,
        }),
    };
};

export const loginError = (err) => ({
    type: 'LOGIN_ERROR',
    error: err,
});


export const loginSuccess = (user) => ({
    type: 'LOGIN_SUCCESS',
    user: user,
});

export const logout = () => ({
    type: 'LOGOUT',
});


export const login = ( accessKeyId, secretAccessKey, redirectFn) => {
    return async function(dispatch)  {
        try {
            let response = await fetch(api + '/authentication', {
                ...basicAuth(accessKeyId, secretAccessKey),
            });
            if (response.status === 401) {
                dispatch(loginError('invalid credentials, try again'))
                return
            }
            if (response.status === 200) {
                let responseJSON = await response.json();
                dispatch(loginSuccess({
                    accessKeyId,
                    secretAccessKey,
                    ...responseJSON.user,
                }));
                redirectFn();
                return
            }
            dispatch(loginError('unknown authentication error'));
            return

        } catch (error) {
            dispatch(loginError(`error connecting to API server: ${error}`));
        }
    };
};