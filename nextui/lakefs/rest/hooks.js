import {useEffect, useState} from 'react';


export const useAPIWithPagination = (promise, deps = []) => {
    const initialState = {
        loading: true,
        hasMore: false,
        currentAfter: {},
        nextPage: "",
        results: [],
    }
    const [pagination, setPagination] = useState(initialState)

    // deps changed, reset pagination
    useEffect(() => setPagination(initialState) , deps)

    // do the actual API request
    // we do this if pagination changed, or if we reset to an initial state
    const {response, error, loading} = useAPI(() => {
        return promise((!pagination.currentAfter.nextPage) ? "" : pagination.currentAfter.nextPage)
    }, [pagination.currentAfter])

    // will trigger useAPI above, as it depends on pagination.currentAfter
    const paginate = () => {
        setPagination({
            ...pagination,
            currentAfter: {nextPage: pagination.nextPage}
        })
    }

    useEffect(() => {
        if (loading) {
            setPagination({...pagination, loading: true})
            return
        }

        if(!!error || !response) {
            setPagination({...pagination, loading: false})
            return
        }

        // calculate current state on API response
        setPagination({
            ...pagination,
            hasMore: (!!response.pagination && response.pagination.has_more),
            nextPage: (!!response.pagination) ? response.pagination.next_offset : "",
            loading: false,
            results: [...pagination.results, ...response.results]
        })
    }, [response, loading, error])

    return {
        response,
        error,
        loading: pagination.loading,
        hasMore: pagination.hasMore,
        results: pagination.results,
        paginate
    }
}



export const useAPI = (promise, deps = []) => {
    const initialState = {
        loading: true,
        error: null,
        response: null,
    }
    const [request, setRequest] = useState(initialState)

    const execute = async () => {
        setRequest(initialState)
        try {
            const response = await promise()
            // setResponse(response)
            // setLoading(false)
            setRequest({
                loading: false,
                error: null,
                response
            })
        } catch (error) {
            setRequest({
                loading: false,
                error,
                response: null
            })
        }
    }

    useEffect(execute, deps)

    return {...request, refresh: execute}
}