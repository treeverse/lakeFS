import * as api from './api';
import {AsyncActionType} from "./request";

export const
    AUTH_LOGIN = new AsyncActionType('AUTH_LOGIN'),
    AUTH_LOGOUT = new AsyncActionType('AUTH_LOGOUT'),
    AUTH_REDIRECTED = 'AUTH_REDIRECTED';


export const logout = () => {
    return AUTH_LOGOUT.execute(async () => {
        return {};
    });
};


export const redirected = () => ({
    type: AUTH_REDIRECTED,
});


export const login = (accessKeyId, secretAccessKey, redirectedUrl) => {
    return AUTH_LOGIN.execute(async () => {
        const response =  await api.auth.login(accessKeyId, secretAccessKey);
        return {
            user: response,
            redirectTo: redirectedUrl,
        };
    })
};