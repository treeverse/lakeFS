import { useMemo } from 'react';

type PaginationParams = {
    prefix?: string;
    amount?: number;
    after?: string;
};

type Paginator<T> = {
    has_more: boolean;
    max_per_page?: number;
    next_offset?: string;
    results: [T];
};

// Return an async iterator to successive calls to a list* operation.
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

// Hook to paginate through calls to api.
export const useIteratedAPI = <T,>(
    params: readonly PaginationParams,
    api: (pagination: readonly PaginationParams) => Promise<Paginator<T>>,
    deps: [object] = [],
): AsyncIterator<T, void, void> => {
    return useMemo(() => iterateAPI(params, api), deps);
};

// No AsyncIterator.filter; provide it as a function.
export const filter = async function*<T>(it: AsyncIterator<T, void, void>, pred: ((t: T) => boolean)) {
    for await (const t of it) {
        if (pred(t)) {
            yield t;
        }
    }
}
