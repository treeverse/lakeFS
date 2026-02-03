import { useCallback, useMemo, useSyncExternalStore } from 'react';

export type RefType = 'branch' | 'tag';

export type RecentRef = {
    id: string;
    type: RefType;
    accessedAt: number;
};

const MAX_RECENT_REFS = 10;
const STORAGE_KEY_PREFIX = 'lakefs:recent-refs:';

const getStorageKey = (repoId: string): string => `${STORAGE_KEY_PREFIX}${repoId}`;

// Subscription management for cross-component sync
const listeners = new Map<string, Set<() => void>>();

const subscribe = (repoId: string) => (callback: () => void) => {
    if (!listeners.has(repoId)) {
        listeners.set(repoId, new Set());
    }
    listeners.get(repoId)!.add(callback);
    return () => listeners.get(repoId)?.delete(callback);
};

const notifyListeners = (repoId: string) => {
    listeners.get(repoId)?.forEach((callback) => callback());
};

const loadFromStorage = (repoId: string): RecentRef[] => {
    try {
        const stored = window.localStorage.getItem(getStorageKey(repoId));
        if (!stored) return [];
        const parsed = JSON.parse(stored);
        if (!Array.isArray(parsed)) return [];
        return parsed;
    } catch {
        return [];
    }
};

const saveToStorage = (repoId: string, refs: RecentRef[]): void => {
    try {
        window.localStorage.setItem(getStorageKey(repoId), JSON.stringify(refs));
    } catch {
        // Storage full or unavailable - graceful degradation
    }
};

// Cache to maintain referential equality
const recentRefsCache = new Map<string, RecentRef[]>();

const getRecentRefs = (repoId: string): RecentRef[] => {
    if (!recentRefsCache.has(repoId)) {
        recentRefsCache.set(repoId, loadFromStorage(repoId));
    }
    return recentRefsCache.get(repoId)!;
};

// Update storage, cache, and notify all subscribers
const setRecentRefs = (repoId: string, refs: RecentRef[]): void => {
    saveToStorage(repoId, refs);
    recentRefsCache.set(repoId, refs);
    notifyListeners(repoId);
};

/**
 * Hook for tracking recently accessed refs (branches/tags) per repository.
 * Persists to localStorage and syncs across all components using the hook.
 *
 * @param repoId - Repository identifier used as storage key
 * @returns recentRefs - Array of recently accessed refs, most recent first
 * @returns trackRef - Add or update a ref's access time
 * @returns clearRecentRefs - Clear all recent refs for this repo
 * @returns removeRef - Remove a specific ref from history
 */
export const useRecentRefs = (repoId: string) => {
    // Memoize subscribe function to maintain stable reference
    const subscribeToRepo = useMemo(() => subscribe(repoId), [repoId]);
    const getRepoRecentRefs = useCallback(() => getRecentRefs(repoId), [repoId]);

    const recentRefs = useSyncExternalStore(subscribeToRepo, getRepoRecentRefs);

    const trackRef = useCallback(
        (refId: string, type: RefType) => {
            const current = getRecentRefs(repoId);
            const filtered = current.filter((ref) => ref.id !== refId);
            const newEntry: RecentRef = { id: refId, type, accessedAt: Date.now() };
            const updated = [newEntry, ...filtered].slice(0, MAX_RECENT_REFS);
            setRecentRefs(repoId, updated);
        },
        [repoId],
    );

    const clearRecentRefs = useCallback(() => {
        setRecentRefs(repoId, []);
    }, [repoId]);

    const removeRef = useCallback(
        (refId: string) => {
            const current = getRecentRefs(repoId);
            const updated = current.filter((ref) => ref.id !== refId);
            setRecentRefs(repoId, updated);
        },
        [repoId],
    );

    return {
        recentRefs,
        trackRef,
        clearRecentRefs,
        removeRef,
    };
};
