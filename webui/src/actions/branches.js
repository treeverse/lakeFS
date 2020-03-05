
import apiRequest, {json} from './api';

export const BRANCHES_LIST_START = 'BRANCHES_LIST_START';
export const BRANCHES_LIST_SUCCESS = 'BRANCHES_LIST_SUCCESS';
export const BRANCHES_LIST_ERROR = 'BRANCHES_LIST_ERROR';
export const BRANCH_CREATE_SUCCESS = 'BRANCH_CREATE_SUCCESS';
export const BRANCH_CREATE_ERROR = 'BRANCH_CREATE_ERROR';
export const BRANCH_PREFIX_LOADED = 'BRANCH_PREFIX_LOADED';
export const BRANCH_SOURCE_SELECT = 'BRANCH_SOURCE_SELECT';


export const branchesLoadingSuccess = (results) => {
    return {
        type: BRANCHES_LIST_SUCCESS,
        list: results,
    };
};

export const branchesLoadingError = (error) => {
    return {
        type: BRANCHES_LIST_ERROR,
        error,
    };
};

export const branchesLoadingStart = () => ({
    type: BRANCHES_LIST_START,
});

export const listBranches = (repoId, prefix, setLoading = true) => {
    return async function(dispatch) {
        if (setLoading)
            dispatch(branchesLoadingStart());
        try {
            const response = await getBranchesAfter(repoId, (!!prefix) ? prefix : "");
            const prefixed = (!!prefix && prefix.length > 0) ?
                response.filter(repo => repo.id.indexOf(prefix) === 0) : response;
            dispatch(branchesLoadingSuccess(prefixed));
        } catch (error) {
            branchesLoadingError(error);
        }
    }
};


const branchCreateSuccess = () => ({
    type: BRANCH_CREATE_SUCCESS,
});

const branchCreateError = (error) => ({
    type: BRANCH_CREATE_ERROR,
    error,
});

async function getBranch(repoId, branchId) {
    let response = await apiRequest(`/repositories/${repoId}/branches/${branchId}`);
    if (response.status !== 200) {
        throw Error("branch not found");
    }
    const data = response.json();
    return data;
}

async function getBranchesAfter(repoId, prefix) {
    const qs = new URLSearchParams([['after', prefix], ['amount', '10']]).toString();
    let response = await apiRequest(`/repositories/${repoId}/branches?${qs}`);
    if (response.status !== 200) {
        return [];
    }
    const data = await response.json();
    let branches = data.results;

    // check if we also have an exact match
    if (!!prefix && prefix.length > 0) {
        try {
            const exactBranch = await getBranch(repoId, prefix);
            branches = [exactBranch, ...branches];
        } catch (error) { /* no branch whose named with this exact prefix */}
    }
    return branches;
}

export const branchPrefixLoaded = (list) => ({
    type: BRANCH_PREFIX_LOADED,
    list,
});


export const searchBranch = (repoId, prefix) => {
    return async function(dispatch) {
        const results = await getBranchesAfter(repoId, prefix);
        dispatch(branchPrefixLoaded(results.map(branch => branch.id)));
    }
};

export const createBranch = (repoId, branchId, sourceBranchId, successFn) => {
    return async function(dispatch) {
        let sourceBranch;
        try {
            sourceBranch = await getBranch(repoId, sourceBranchId);
        } catch (error) {
            dispatch(branchCreateError(`Source branch: ${error}`));
            return
        }

        try {
            let response = await apiRequest(`/repositories/${repoId}/branches`, {
                method: "POST",
                body: json({id: branchId, commit_id: sourceBranch.commit_id}),
            });

            if (response.status === 201) {
                dispatch(branchCreateSuccess());
                successFn();
            } else {
                let errorData = await response.json();
                dispatch(branchCreateError(errorData.message));
            }
        } catch (error) {
            dispatch(branchCreateError(error));
        }
    }
};

export const selectSourceBranch = (branchId) => {
    return {
        type: BRANCH_SOURCE_SELECT,
        branch: branchId,
    };
};
