
import * as api from './api';
import {AsyncActionType} from "./request";

export const
    BRANCHES_LIST = new AsyncActionType('BRANCHES_LIST');


export const listBranches = (repoId, from = "", amount = 100) => {
    return BRANCHES_LIST.execute(async () => {
        return await api.branches.filter(repoId, from, amount);
    })
};
