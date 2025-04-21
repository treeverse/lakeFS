import { useRef, useState, useCallback } from 'react';

export const useExpandCollapseDirs = () => {
    const opened = useRef(new Set());
    const allDirs = useRef(new Set());
    const [tick, setTick] = useState(0);
    const [expandMode, setExpandMode] = useState({ value: null, version: 0 });

    const forceRerender = () => setTick(t => t + 1);

    const registerDir = useCallback(path => {
        if (!allDirs.current.has(path)) {
            allDirs.current.add(path);
            forceRerender();
        }
    }, []);

    const updateOpenedDir = useCallback((path, isOpen) => {
        const set = opened.current;
        if (isOpen) set.add(path);
        else [...set].filter(p => p === path || p.startsWith(path)).forEach(p => set.delete(p));
        forceRerender();
    }, []);

    const isAllExpanded = useCallback(
        () => [...allDirs.current].every(p => opened.current.has(p)),
        []
    );

    const toggleAllDirs = useCallback(() => {
        const shouldExpand = !isAllExpanded();
        const paths = opened.current;

        shouldExpand
            ? allDirs.current.forEach(p => paths.add(p))
            : paths.clear();

        setExpandMode(prev => ({
            value: shouldExpand,
            version: prev.version + 1,
        }));

        forceRerender();
    }, [isAllExpanded]);

    const markDirAsManuallyToggled = useCallback(path => {
        opened.current.delete(path);
    }, []);

    return {
        expandAllDirs: isAllExpanded(),
        expandMode,
        toggleAllDirs,
        updateOpenedDir,
        registerDir,
        markDirAsManuallyToggled,
        tick,
    };
};
