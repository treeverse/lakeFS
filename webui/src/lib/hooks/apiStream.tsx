import { useMemo } from 'react';

// Parameters for calling a paginated API to receive a page.
type PaginationParams = {
    prefix?: string;
    amount?: number;
    after?: string;
};

// Result of calling a paginated API.
type Paginator<T> = {
    // If true, call again to receive more data.
    has_more: boolean;
    max_per_page?: number;
    // If has_more is true, set this value in PaginationParams.after to receive the next page.
    next_offset?: string;
    // Array of results.
    results: [T];
};

// Return an async iterator to successive calls to a lakeFS paginated operation.  This async
// generator function returns an AsyncIterator.  During iteration it repeatedly calls
// api(params), advancing params.next to the returned next_offset.  It then returns the results
// one by one.
export const iterateAPI = async function*<T>(
    initParams: readonly PaginationParams,
    api: (pagination: readonly PaginationParams) => Promise<Paginator<T>>,
): AsyncIterator<T, void, void> {
    const params: PaginationParams = {...initParams};

    while (true) {
        const {results, pagination}  = await api(params);
        for (const result of results) {
            yield result;
        }
        if (!pagination.has_more) {
            break;
        } else {
            params.after = pagination.next_offset + '\0';
        }
    }
}

// Hook to paginate through calls to api.  A new AsyncIterator will be created whenever deps
// change.
export const useIteratedAPI = <T,>(
    params: readonly PaginationParams,
    api: (pagination: readonly PaginationParams) => Promise<Paginator<T>>,
    deps: [object] = [],
): AsyncIterator<T, void, void> => {
    return useMemo(() => iterateAPI(params, api), deps);
};

// Filter values on an AsyncIterator by pred.  There is no AsyncIterator.filter, so provide it
// as a function.
export const filter = async function*<T>(it: AsyncIterator<T, void, void>, pred: ((t: T) => boolean)) {
    for await (const t of it) {
        if (pred(t)) {
            yield t;
        }
    }
}
