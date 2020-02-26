import apiRequest, {qs, extractError} from './api';

export const OBJECTS_NAVIGATE_SET_BRANCH = 'OBJECTS_NAVIGATE_SET_BRANCH';
export const OBJECTS_NAVIGATE = 'OBJECTS_NAVIGATE';
export const OBJECTS_NAVIGATE_SUCCESS = 'OBJECTS_NAVIGATE_SUCCESS';
export const OBJECTS_NAVIGATE_ERROR = 'OBJECTS_NAVIGATE_ERROR';

const objectsNavigateStart = (path) => ({
    type: OBJECTS_NAVIGATE,
    path,
});

const objectsNavigateSuccess = (entries) => ({
    type: OBJECTS_NAVIGATE_SUCCESS,
    entries,
});

const objectsNavigateError = (error) => ({
    type: OBJECTS_NAVIGATE_ERROR,
    error
});

export const objectsSetBranch = (repoId, branchId) => ({
    type: OBJECTS_NAVIGATE_SET_BRANCH,
    repoId,
    branchId,
});


export const navigate = (repoId, branchId, tree) => {
    return async function(dispatch) {
        dispatch(objectsNavigateStart(tree));
        const query = qs({ tree});
        try {
            const response = await apiRequest(`/repositories/${repoId}/branches/${branchId}/objects/ls?${query}`);
            if (response.status === 200) {
                const content = await response.json();
                dispatch(objectsNavigateSuccess(content.results));
            } else {
                const error = await extractError(response);
                dispatch(objectsNavigateError(error));
            }
        } catch (error) {
            dispatch(objectsNavigateError(error.toString()));
        }
    }
};