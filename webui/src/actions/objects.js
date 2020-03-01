import * as api from './api';
import {AsyncActionType} from "./request";

export const
    OBJECTS_LIST_TREE = new AsyncActionType('OBJECTS_GET_TREE'),
    OBJECTS_LIST_BRANCHES = new AsyncActionType('OBJECTS_LIST_BRANCHES');


export const listTree = (repoId, branchId, tree, after = "", amount = 1000) => {
    return OBJECTS_LIST_TREE.execute(async () => {
        return await api.objects.list(repoId, branchId, tree, after, amount);
    });
};

export const listBranches = (repoId, from = "", amount = 100) => {
    return OBJECTS_LIST_BRANCHES.execute(async () => {
        return await api.branches.filter(repoId, from, amount);
    })
};
