import { useEffect, useState, useCallback } from 'react';
import { AuthenticationError } from '../api';
import { useAuth } from '../auth/authContext';

const initialPaginationState = {
    loading: true,
    error: null,
    nextPage: null,
    results: [],
};

export const useAPIWithPagination = (promise, deps = []) => {
    const [pagination, setPagination] = useState(initialPaginationState);

    // do the actual API request
    // we do this if pagination changed, or if we reset to an initial state
    const { response, error, loading } = useAPI(() => {
        setPagination({ ...initialPaginationState });
        return promise();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [...deps, initialPaginationState]);

    useEffect(() => {
        if (loading) {
            setPagination({ results: [], loading: true });
            return;
        }

        if (!!error || !response) {
            setPagination({ error, loading: false });
            return;
        }

        // calculate current state on API response
        setPagination({
            error: null,
            nextPage: !!response.pagination && response.pagination.has_more ? response.pagination.next_offset : null,
            loading: false,
            results: response.results,
        });
    }, [response, loading, error]);

    return pagination;
};

const initialAPIState = {
    loading: true,
    error: null,
    response: null,
    responseHeaders: null,
};

export const useAPI = (promise, deps = []) => {
    const [request, setRequest] = useState(initialAPIState);
    const { onUnauthenticated } = useAuth();
    const [refreshKey, setRefreshKey] = useState(0);

    useEffect(() => {
        let isMounted = true;
        setRequest(initialAPIState);
        const execute = async () => {
            try {
                const response = await promise();
                if (!isMounted) return;
                setRequest({ loading: false, error: null, response });
            } catch (error) {
                if (!isMounted) return;
                // On 401 we delegate to onUnauthenticated(), which redirects to /auth/login
                // with { redirected: true, next } so the login page can apply SSO and return.
                if (error instanceof AuthenticationError && error.status === 401) {
                    onUnauthenticated();
                }
                setRequest({
                    loading: false,
                    error,
                    response: null,
                });
            }
        };
        execute();
        return () => (isMounted = false);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [...deps, refreshKey]);

    const refetch = useCallback(() => {
        setRefreshKey((prev) => prev + 1);
    }, []);

    return { ...request, refetch };
};
