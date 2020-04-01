

import * as api from './api';
import {AsyncActionType} from "./request";

export const
    PAGINATION_AMOUNT = 1000,
    COMMITS_LIST = new AsyncActionType('COMMITS_LIST'),
    COMMITS_LIST_PAGINATE = new AsyncActionType('COMMITS_LIST_PAGINATE'),
    COMMITS_COMMIT = new AsyncActionType('COMMITS_COMMIT');


export const logCommits = (repoId, branchId,from = "", amount = PAGINATION_AMOUNT) => {
    return COMMITS_LIST.execute(async () => {
        return await api.commits.log(repoId, branchId, from, amount);
    })
};

export const logCommitsPaginate = (repoId,branchId, from = "", amount = PAGINATION_AMOUNT) => {
    return COMMITS_LIST_PAGINATE.execute(async () => {
        return await api.commits.log(repoId,branchId, from, amount);
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

