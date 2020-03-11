
import * as api from './api';
import {AsyncActionType} from "./request";

export const
    BRANCHES_LIST = new AsyncActionType('BRANCHES_LIST'),
    BRANCHES_CREATE = new AsyncActionType('BRANCHES_CREATE');


export const listBranches = (repoId, from = "", amount) => {
    return BRANCHES_LIST.execute(async () => {
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