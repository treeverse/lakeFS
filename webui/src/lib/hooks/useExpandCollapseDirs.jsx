import { useCallback, useState } from "react";

export const useExpandCollapseDirs = () => {
    const [isAllExpanded, setIsAllExpanded] = useState(null);
    const [manuallyToggledDirs, setManuallyToggledDirs] = useState(() => new Set());

    const markDirAsManuallyToggled = useCallback((path) => {
        setManuallyToggledDirs(prev => {
            const next = new Set(prev);
            next.add(path);
            return next;
        });
    }, []);

    const wasDirManuallyToggled = useCallback(path => manuallyToggledDirs.has(path), [manuallyToggledDirs]);

    const expandAll = useCallback(() => {
        setManuallyToggledDirs(new Set());
        setIsAllExpanded(true);
    }, []);

    const collapseAll = useCallback(() => {
        setManuallyToggledDirs(new Set());
        setIsAllExpanded(false);
    }, []);

    return {
        isAllExpanded,
        expandAll,
        collapseAll,
        markDirAsManuallyToggled,
        wasDirManuallyToggled,
    };
};
