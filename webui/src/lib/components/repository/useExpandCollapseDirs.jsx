/**
 * Custom hook to manage folder expansion state across a directory tree.
 *
 * Features:
 * - Tracks which folders are currently expanded.
 * - Allows toggling "Expand All" / "Collapse All" from the top-level.
 * - Ensures new folders added during expansion also respect global expand mode.
 * - Syncs global expand state (expandMode) with individual folders via version counter.
 *
 * Uses:
 * - `useRef` to persist sets of folders.
 * - `tick` to force re-renders when non-reactive data changes.
 */

import { useRef, useState, useCallback } from 'react';

export const useExpandCollapseDirs = () => {
    const opened = useRef(new Set());
    const allDirs = useRef(new Set());
    const [expandMode, setExpandMode] = useState({ value: null, version: 0 });

    // Dummy state to force re-render when refs change (refs are not reactive by default)
    const [tick, setTick] = useState(0);

    const forceRerender = () => setTick(t => t + 1);

    const registerDir = useCallback(path => {
        if (!allDirs.current.has(path)) {
            allDirs.current.add(path);
            forceRerender();
        }
    }, []);

    const updateOpenedDir = useCallback((path, isOpen) => {
        const set = opened.current;
        if (isOpen) {
            set.add(path);
        } else {
            // Remove this path and all its descendants from the opened set
            [...set].filter(p => p === path || p.startsWith(path)).forEach(p => set.delete(p));
        }
        forceRerender();
    }, []);

    const isAllExpanded = useCallback(
        () => [...allDirs.current].every(p => opened.current.has(p)),
        []
    );

    const toggleAllDirs = useCallback(() => {
        const shouldExpand = !isAllExpanded(); // Flip global expansion state
        const paths = opened.current;

        // Add/remove all folders from the opened set
        shouldExpand
            ? allDirs.current.forEach(p => paths.add(p))
            : paths.clear();

        // This triggers expansion via version update
        setExpandMode(prev => ({
            value: shouldExpand,
            version: prev.version + 1,
        }));

        forceRerender();
    }, [isAllExpanded]);

    // Manually removes a path from the globally expanded state.
    // Used to prevent global expandMode from overriding user's choice
    const markDirAsManuallyToggled = useCallback(path => {
        opened.current.delete(path);
    }, []);

    return {
        allDirsExpanded: isAllExpanded(),
        expandMode,
        toggleAllDirs,
        updateOpenedDir,
        registerDir,
        markDirAsManuallyToggled,
        tick,
    };
};
