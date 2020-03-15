
import * as api from './api';
import {AsyncActionType} from "./request";

export const
    PAGINATION_AMOUNT = 1000,
    BRANCHES_LIST = new AsyncActionType('BRANCHES_LIST'),
    BRANCHES_LIST_PAGINATE = new AsyncActionType('BRANCHES_LIST_PAGINATE'),
    BRANCHES_CREATE = new AsyncActionType('BRANCHES_CREATE');


export const listBranches = (repoId, from = "", amount = PAGINATION_AMOUNT) => {
    return BRANCHES_LIST.execute(async () => {
        return await api.branches.list(repoId, from, amount);
    })
};

export const listBranchesPaginate = (repoId, from = "", amount = PAGINATION_AMOUNT) => {
    return BRANCHES_LIST_PAGINATE.execute(async () => {
        return await api.branches.list(repoId, from, amount);
    })
};

export const createBranch = (repoId, branchName, sourceRefId) => {
    return BRANCHES_CREATE.execute(async () => {
        return await api.branches.create(repoId, branchName, sourceRefId)
    })
};

export const resetBranch = () => ({
    ...BRANCHES_CREATE.resetAction(),
});