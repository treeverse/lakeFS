import * as api from './api';
import {AsyncActionType} from "./request";

export const
    REPOSITORY_LOAD_LIST = new AsyncActionType('REPOSITORY_LOAD_LIST'),
    REPOSITORY_GET = new AsyncActionType('REPOSITORY_GET'),
    REPOSITORY_CREATE = new AsyncActionType('REPOSITORY_CREATE'),
    REPOSITORY_DELETE = new AsyncActionType('REPOSITORY_DELETE');


export const createRepository = (repo) => {
    return REPOSITORY_CREATE.execute(async () => {
        return await api.repositories.create(repo);
    });
};

export const deleteRepository = (repoId) => {
    return REPOSITORY_DELETE.execute(async () => {
        return await api.repositories.delete(repoId);
    })
};

export const listRepositories = (after = "", amount = 1000) => {
    return REPOSITORY_LOAD_LIST.execute(async () => {
        return await api.repositories.list(after, amount);
    });
};

export const filterRepositories = (from = "", amount = 1000) => {
    return REPOSITORY_LOAD_LIST.execute(async () => {
        return await api.repositories.filter(from, amount);
    });
};

export const getRepository = (repoId) => {
    return REPOSITORY_GET.execute(async () => {
        return await api.repositories.get(repoId);
    });
};