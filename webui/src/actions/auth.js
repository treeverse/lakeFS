import * as api from "./api";
import {AsyncActionType} from "./request";

export const AUTH_LOGIN = new AsyncActionType('AUTH_LOGIN');
export const AUTH_REDIRECTED = 'AUTH_REDIRECTED';
export const AUTH_LOGOUT = 'AUTH_LOGOUT';

export const logout = () => ({
    type: AUTH_LOGOUT,
});

export const redirected = () => ({
    type: AUTH_REDIRECTED,
});

export const login = (accessKeyId, secretAccessKey, redirectedUrl) => {
    return AUTH_LOGIN.execute(async () => {
        const response =  await api.auth.login(accessKeyId, secretAccessKey);
        return {
            user: {
                accessKeyId,
                secretAccessKey,
                ...response,
            },
            redirectTo: redirectedUrl,
        };
    })
};