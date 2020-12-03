import * as api from './api';
import {AsyncActionType} from "./request";

export const PAGINATION_AMOUNT = 300;
export const BRANCHES_LIST = new AsyncActionType('BRANCHES_LIST');
export const BRANCHES_LIST_PAGINATE = new AsyncActionType('BRANCHES_LIST_PAGINATE');
export const BRANCHES_CREATE = new AsyncActionType('BRANCHES_CREATE');
export const BRANCHES_DELETE = new AsyncActionType('BRANCHES_DELETE');
export const BRANCHES_REVERT = new AsyncActionType('BRANCHES_REVERT');

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

export const deleteBranch = (repoId, branchName) => {
    return BRANCHES_DELETE.execute(async () => {
        return await api.branches.delete(repoId, branchName)
    })
};

export const revertBranch = (repoId, branchName, options) =>
    BRANCHES_REVERT.execute(() => api.branches.revert(repoId, branchName, options));

export const resetRevertBranch = () =>
    BRANCHES_REVERT.resetAction();

