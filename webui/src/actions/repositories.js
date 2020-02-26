import apiRequest, {json, extractError} from './api';



export const REPOSITORIES_LIST_SUCCESS = 'REPOSITORIES_LIST_SUCCESS';
export const REPOSITORIES_LIST_ERROR = 'REPOSITORIES_LIST_ERROR';
export const REPOSITORIES_LIST_START = 'REPOSITORIES_LIST_START';
export const REPOSITORY_DELETE_SUCCESS = 'REPOSITORY_DELETE_SUCCESS';
export const REPOSITORY_DELETE_ERROR = 'REPOSITORY_DELETE_ERROR';
export const REPOSITORY_CREATE_SUCCESS = 'REPOSITORY_CREATE_SUCCESS';
export const REPOSITORY_CREATE_ERROR = 'REPOSITORY_CREATE_ERROR';


export const reposLoadingSuccess = ({ pagination, results }) => {
    return {
        type: REPOSITORIES_LIST_SUCCESS,
        list: results,
        pagination,
    };
};

export const reposLoadingError = (error) => {
    return {
        type: REPOSITORIES_LIST_ERROR,
        error,
    };
};

export const reposLoadingStart = () => ({
    type: REPOSITORIES_LIST_START,
});

export const listRepositories = () => {
    return async function(dispatch) {
        dispatch(reposLoadingStart());
        try {
            let response = await apiRequest('/repositories');

            if (response.status === 200) {
                let responseJSON = await response.json();
                dispatch(reposLoadingSuccess(responseJSON));
            } else {
                reposLoadingError(await response.text());
            }
        } catch (error) {
            reposLoadingError(error);
        }
    }
};

export const deleteRepository = (repoId, successFn) => {
    return async function(dispatch) {
        try {
            const response = await apiRequest(`/repositories/${repoId}`, {method: 'DELETE'});
            if (response.status === 204) {
                dispatch(deleteRepositorySuccess(repoId));
                successFn();
            }else {
                const error = await extractError(response);
                dispatch(deleteRepositoryError(repoId, `error deleting repository: ${error}`));
            }
        } catch (error) {
            dispatch(deleteRepositoryError(repoId, `error deleting repository: ${error.toString()}`));
        }
    }
};

const deleteRepositoryError = (repoId, error) => ({
    type: REPOSITORY_DELETE_ERROR,
    id: repoId,
    error,
});

const deleteRepositorySuccess = (repoId) => ({
    type: REPOSITORY_DELETE_SUCCESS,
    id: repoId,
});

const repoCreateSuccess = () => ({
    type: REPOSITORY_CREATE_SUCCESS,
});

const repoCreateError = (error) => ({
    type: REPOSITORY_CREATE_ERROR,
    error,
});

export const createRepository = (id, bucket_name, default_branch, successFn) => {
    return async function(dispatch) {
        try {
            let response = await apiRequest('/repositories', {
                method: 'POST',
                body: json({id, bucket_name, default_branch}),
            });
            if (response.status === 201) {
                dispatch(repoCreateSuccess());
                successFn();
            } else {
                let errorData = await response.json();
                dispatch(repoCreateError(errorData.message));
            }
        } catch (error) {
            dispatch(repoCreateError(`${error}`));
        }
    }
};
