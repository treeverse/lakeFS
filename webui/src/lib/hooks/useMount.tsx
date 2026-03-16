import { useEffect } from 'react';

/**
 * Runs the given effect once when the component mounts.
 * Equivalent to useEffect(fn, []).
 */
export function useMount(fn: () => void | (() => void)) {
    // eslint-disable-next-line react-hooks/exhaustive-deps
    useEffect(fn, []);
}
