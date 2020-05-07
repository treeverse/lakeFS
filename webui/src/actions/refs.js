import * as api from "./api";
import {AsyncActionType} from "./request";

export const
    DIFF_REFS = new AsyncActionType('DIFF_REFS'),
    DIFF_BRANCH_REFS = new AsyncActionType('DIFF_BRANCH_REFS'),
    MERGE_REFS = new AsyncActionType('MERGE_REFS');

export const diff = (repoId, leftRef, rightRef) => {
    return DIFF_REFS.execute(async () => {
        return await api.refs.diff(repoId, leftRef, rightRef);
    });
};

export const resetDiff = () => ({
    ...DIFF_REFS.resetAction(),
});

export const diffBranch = (repoId, branch) => DIFF_BRANCH_REFS.execute(async () => api.refs.diff(repoId, branch, branch));

export const resetDiffBranch = () => DIFF_BRANCH_REFS.resetAction();

export const merge = (repoId, sourceBranchId, destinationBranchId) => {
    return MERGE_REFS.execute(async () => {
        return await api.refs.merge(repoId, sourceBranchId, destinationBranchId);
    });
};

export const resetMerge = () => MERGE_REFS.resetAction();
