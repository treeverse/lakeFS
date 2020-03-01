
import * as api from './api';

export const
    BRANCHES_LOAD_LIST = 'BRANCHES_LOAD_LIST',
    BRANCHES_LOAD_LIST_SUCCESS = 'BRANCHES_LOAD_LIST_SUCCESS',
    BRANCHES_LOAD_LIST_ERROR = 'BRANCHES_LOAD_LIST_ERROR',
    BRANCHES_CREATE = 'BRANCHES_CREATE',
    BRANCHES_CREATE_SUCCESS = 'BRANCHES_CREATE_SUCCESS',
    BRANCHES_CREATE_ERROR = 'BRANCHES_CREATE_ERROR';


const branchesLoadList = () => ({
    type: BRANCHES_LOAD_LIST,
});

const branchesLoadListSuccess = (branches) => ({
    type: BRANCHES_LOAD_LIST_SUCCESS,
    branches,

});

const branchesLoadListError = (error) => ({
    type: BRANCHES_LOAD_LIST_ERROR,
    error,
});

const branchesCreate = () => ({
    type: BRANCHES_CREATE,
});

const branchesCreateSuccess = (branch) => ({
    type: BRANCHES_CREATE_SUCCESS,
    branch,
});

const branchesCreateError = (error) => ({
    type: BRANCHES_CREATE_ERROR,
    error,
});

export const listBranches = (repoId, after = "", amount = 1000) => {
    return async (dispatch) => {
        dispatch(branchesLoadList());
        try {
            const response = await api.branches.list(repoId, prefix, amount);
            dispatch(branchesLoadListSuccess(response));
        } catch (error) {
            dispatch(branchesLoadListError(error));
        }
    };
};

export const createBranch = (repoId, branch) => {
    return async (dispatch) => {
        dispatch(branchesCreate());
        try {
            const response = api.branches.create(repoId, branch);
            dispatch(branchesCreateSuccess(await response.json()));
        } catch (error) {
            dispatch(branchesCreateError(error));
        }
    };
};