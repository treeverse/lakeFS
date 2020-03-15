

import * as api from './api';
import {AsyncActionType} from "./request";

export const
    COMMITS_LIST = new AsyncActionType('COMMITS_LIST'),
    COMMITS_COMMIT = new AsyncActionType('COMMITS_COMMIT');


export const logCommits = (repoId, branchId) => {
    return COMMITS_LIST.execute(async () => {
        return await api.commits.log(repoId, branchId);
    })
};

export const doCommit = (repoId, branchId, message, metadata={}) => {
    return COMMITS_COMMIT.execute( async () => {
        return await api.commits.commit(repoId, branchId, message, metadata);
    });
};

export const resetCommit = () => {
  return COMMITS_COMMIT.resetAction();
};

