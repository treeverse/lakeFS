import {useEffect, useState} from 'react';
import {AuthenticationError} from "../api";
import {useRouter} from "./router";

const initialPaginationState = {
    loading: true,
    error: null,
    nextPage: null,
    results: []
};

export const useAPIWithPagination = (promise, deps = []) => {
    const [pagination, setPagination] = useState(initialPaginationState);

    // do the actual API request
    // we do this if pagination changed, or if we reset to an initial state
    const {response, error, loading} = useAPI(() => {
        setPagination({...initialPaginationState});
        return promise();
    }, [...deps, initialPaginationState]);

    useEffect(() => {
        if (loading) {
            setPagination({results: [], loading: true});
            return;
        }

        if (!!error || !response) {
            setPagination({error, loading: false});
            return;
        }

        // calculate current state on API response
        setPagination({
            error: null,
            nextPage: (!!response.pagination && response.pagination.has_more) ? response.pagination.next_offset : null,
            loading: false,
            results: response.results
        });
    }, [response, loading, error]);

    return pagination;
}

const initialAPIState = {
    loading: true,
    error: null,
    response: null,
    responseHeaders: null,
};

export const useAPI = (promise, deps = []) => {
    const router = useRouter();
    const [request, setRequest] = useState(initialAPIState);
    const [login, setLogin] = useState(false);

    useEffect(() => {
        if (login) {
            const loginPathname = '/auth/login';
            if (router.route === loginPathname) {
                return;
            }
            router.push({
                pathname: loginPathname,
                query: {next: router.route, redirected: true},
            });
            setLogin(false);
        }
    }, [login, router])

    useEffect(() => {
        let isMounted = true;
        setRequest(initialAPIState);
        const execute = async () => {
            try {
                const response = await promise();
                setRequest({
                    loading: false,
                    error: null,
                    response,
                });
            } catch (error) {
                if (error instanceof AuthenticationError) {
                    if (isMounted) {
                        setLogin(true);
                    }
                    return;
                }
                setRequest({
                    loading: false,
                    error,
                    response: null,
                });
            }
        };
        execute();
        return () => isMounted = false;
    }, deps);
    return {...request};
}
