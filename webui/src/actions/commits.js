

import * as api from './api';
import {AsyncActionType} from "./request";

export const
    COMMITS_LIST = new AsyncActionType('COMMITS_LIST');


export const logCommits = (repoId, branchId) => {
    return COMMITS_LIST.execute(async () => {
        return await api.commits.log(repoId, branchId);
    })
};
