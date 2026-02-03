import { useCallback, useEffect, useState } from 'react';

export type RefType = 'branch' | 'tag';

export type RecentRef = {
    id: string;
    type: RefType;
    accessedAt: number;
};

const MAX_RECENT_REFS = 10;
const STORAGE_KEY_PREFIX = 'lakefs:recent-refs:';

const getStorageKey = (repoId: string): string => `${STORAGE_KEY_PREFIX}${repoId}`;

const loadRecentRefs = (repoId: string): RecentRef[] => {
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

const saveRecentRefs = (repoId: string, refs: RecentRef[]): void => {
    try {
        window.localStorage.setItem(getStorageKey(repoId), JSON.stringify(refs));
    } catch {
        // Storage full or unavailable - graceful degradation
    }
};

export const useRecentRefs = (repoId: string) => {
    const [recentRefs, setRecentRefs] = useState<RecentRef[]>(() => loadRecentRefs(repoId));

    // Reload when repoId changes
    useEffect(() => {
        setRecentRefs(loadRecentRefs(repoId));
    }, [repoId]);

    const trackRef = useCallback(
        (refId: string, type: RefType) => {
            setRecentRefs((current) => {
                // Remove existing entry if present
                const filtered = current.filter((ref) => ref.id !== refId);

                // Add new entry at the front
                const newEntry: RecentRef = {
                    id: refId,
                    type,
                    accessedAt: Date.now(),
                };

                // Keep only MAX_RECENT_REFS entries
                const updated = [newEntry, ...filtered].slice(0, MAX_RECENT_REFS);

                // Persist to localStorage
                saveRecentRefs(repoId, updated);

                return updated;
            });
        },
        [repoId],
    );

    const clearRecentRefs = useCallback(() => {
        setRecentRefs([]);
        try {
            window.localStorage.removeItem(getStorageKey(repoId));
        } catch {
            // Ignore storage errors
        }
    }, [repoId]);

    const removeRef = useCallback(
        (refId: string) => {
            setRecentRefs((current) => {
                const updated = current.filter((ref) => ref.id !== refId);
                saveRecentRefs(repoId, updated);
                return updated;
            });
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
