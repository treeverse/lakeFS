import * as api from "./api";
import {AsyncActionType} from "./request";

export const
    PAGINATION_AMOUNT = 300,
    REPOSITORY_LIST = new AsyncActionType('REPOSITORY_LIST'),
    REPOSITORY_LIST_PAGINATE = new AsyncActionType('REPOSITORY_LIST_PAGINATE'),
    REPOSITORY_GET = new AsyncActionType('REPOSITORY_GET'),
    REPOSITORY_CREATE = new AsyncActionType('REPOSITORY_CREATE'),
    REPOSITORY_DELETE = new AsyncActionType('REPOSITORY_DELETE');


export const createRepository = (repo) => {
    return REPOSITORY_CREATE.execute(async () => {
        return await api.repositories.create(repo);
    });
};

export const createRepositoryDone = () => REPOSITORY_CREATE.resetAction();

export const deleteRepository = (repoId) => {
    return REPOSITORY_DELETE.execute(async () => {
        return await api.repositories.delete(repoId);
    })
};

export const deleteRepositoryDone = () => REPOSITORY_DELETE.resetAction();

export const listRepositories = (after = "", amount = PAGINATION_AMOUNT) => {
    return REPOSITORY_LIST.execute(async () => {
        return await api.repositories.list(after, amount);
    });
};

export const listRepositoriesPaginate = (after = "", amount = PAGINATION_AMOUNT) => {
    return REPOSITORY_LIST_PAGINATE.execute(async () => {
        return await api.repositories.list(after, amount);
    });
};

export const filterRepositories = (from = "", amount = 300) => {
    return REPOSITORY_LIST.execute(async () => {
        return await api.repositories.filter(from, amount);
    });
};

export const getRepository = (repoId) => {
    return REPOSITORY_GET.execute(async () => {
        return await api.repositories.get(repoId);
    });
};
