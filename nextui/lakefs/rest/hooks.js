import {useEffect, useState} from 'react';


export const useAPIWithPagination = (promise, deps = []) => {
    const initialState = {
        loading: true,
        error: null,
        nextPage: null,
        results: []
    }

    const [pagination, setPagination] = useState(initialState)

    // do the actual API request
    // we do this if pagination changed, or if we reset to an initial state
    const {response, error, loading} = useAPI(() => {
        setPagination({...initialState})
        return promise()
    }, deps)

    useEffect(() => {
        if (loading) {
            setPagination({...pagination, results: [], loading: true})
            return
        }

        if (!!error || !response) {
            setPagination({...pagination, error, loading: false})
            return
        }

        // calculate current state on API response
        setPagination({
            ...initialState,
            nextPage: (!!response.pagination && response.pagination.has_more) ? response.pagination.next_offset : null,
            loading: false,
            results: response.results
        })
    }, [response, loading, error])

    return pagination
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