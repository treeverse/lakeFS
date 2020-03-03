import * as api from './api';
import {AsyncActionType} from "./request";

export const
    OBJECTS_LIST_TREE = new AsyncActionType('OBJECTS_GET_TREE');

export const listTree = (repoId, branchId, tree, after = "", amount = 1000) => {
    return OBJECTS_LIST_TREE.execute(async () => {
        return await api.objects.list(repoId, branchId, tree, after, amount);
    });
};

