import * as api from "./api";
import {AsyncActionType} from "./request";

export const PAGINATION_AMOUNT = 300;
export const DIFF_REFS = new AsyncActionType('DIFF_REFS');
export const DIFF_REFS_PAGINATE = new AsyncActionType('DIFF_REFS_PAGINATE');
export const MERGE_REFS = new AsyncActionType('MERGE_REFS');

export const diff = (repoId, leftRef, rightRef, amount=PAGINATION_AMOUNT) => {
    return DIFF_REFS.execute(async () => {
        return await api.refs.diff(repoId, leftRef, rightRef, '', amount);
    });
};

export const diffPaginate = (repoId, leftRef, rightRef, after, amount=PAGINATION_AMOUNT) => {
    return DIFF_REFS_PAGINATE.execute(async () => {
        return await api.refs.diff(repoId, leftRef, rightRef, after, amount);
    });
};

export const resetDiff = () => ({
    ...DIFF_REFS.resetAction(),
});

export const merge = (repoId, sourceBranchId, destinationBranchId) => {
    return MERGE_REFS.execute(async () => {
        return await api.refs.merge(repoId, sourceBranchId, destinationBranchId);
    });
};

export const resetMerge = () => MERGE_REFS.resetAction();
