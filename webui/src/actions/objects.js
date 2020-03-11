import * as api from './api';
import {AsyncActionType} from "./request";

export const
    OBJECTS_LIST_TREE = new AsyncActionType('OBJECTS_GET_TREE'),
    OBJECTS_UPLOAD = new AsyncActionType('OBJECTS_UPLOAD'),
    OBJECTS_DELETE = new AsyncActionType('OBJECTS_DELETE');


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

export const uploadDone = () => {
    return OBJECTS_UPLOAD.resetAction();
};


export const deleteObject = (repoId, branchId, path) => {
    return OBJECTS_DELETE.execute(async () => {
        return await api.objects.delete(repoId, branchId, path);
    });
};

export const deleteObjectDone = () => {
    return OBJECTS_DELETE.resetAction();
};