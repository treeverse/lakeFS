import * as api from './api';
import {AsyncActionType} from "./request";

export const
    OBJECTS_LIST_TREE = new AsyncActionType('OBJECTS_GET_TREE'),
    OBJECTS_UPLOAD = new AsyncActionType('OBJECTS_UPLOAD'),
    OBJECTS_UPLOAD_RESET = 'OBJECTS_UPLOAD_RESET';

export const listTree = (repoId, branchId, tree, after = "", amount = 1000) => {
    return OBJECTS_LIST_TREE.execute(async () => {
        return await api.objects.list(repoId, branchId, tree, after, amount);
    });
};

export const upload = (repoId, branchId, path, fileObject) => {
    return OBJECTS_UPLOAD.execute(async () => {
        return await api.objects.upload(repoId, branchId, path, fileObject);
    });
};

export const uploadDone = () => ({
    type: OBJECTS_UPLOAD_RESET,
});