import * as api from './api';
import {AsyncActionType} from "./request";

export const
    DIFF_REFS = new AsyncActionType('DIFF_REFS'),
    RESET_DIFF = 'RESET_DIFF';

export const diff = (repoId, leftRef, rightRef) => {
    return DIFF_REFS.execute(async () => {
        return await api.refs.diff(repoId, leftRef, rightRef);
    });
};

export const resetDiff = () => ({
    type: RESET_DIFF,
});
