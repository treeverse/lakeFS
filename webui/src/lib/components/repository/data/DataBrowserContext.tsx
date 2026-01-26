import React, { createContext, useContext, useState, useCallback, ReactNode, useEffect } from 'react';

export interface ObjectEntry {
    path: string;
    path_type: 'object' | 'common_prefix';
    physical_address?: string;
    size_bytes?: number;
    checksum?: string;
    mtime?: number;
    content_type?: string;
    metadata?: Record<string, string>;
    diff_type?: DiffType;
}

export type DiffType = 'added' | 'removed' | 'changed' | 'conflict' | 'prefix_changed' | undefined;

// API response uses 'type' field
export interface DiffEntry {
    path: string;
    path_type: 'object' | 'common_prefix';
    type: DiffType;
}

// Normalize prefix_changed to changed for display purposes
export const normalizeDiffType = (type: DiffType): DiffType => {
    if (type === 'prefix_changed') return 'changed';
    return type;
};

export type ActiveTab = 'preview' | 'info' | 'blame' | 'statistics';

// Diff mode configuration for comparing two refs (e.g., commit diff)
export interface DiffModeConfig {
    enabled: boolean;
    leftRef: string; // Base ref (e.g., parent commit)
    rightRef: string; // Target ref (e.g., current commit)
}

interface DataBrowserContextType {
    expandedPaths: Set<string>;
    selectedObject: ObjectEntry | null;
    activeTab: ActiveTab;
    refreshToken: number;
    diffResults: Map<string, DiffType>;
    hasUncommittedChanges: boolean;
    showOnlyChanges: boolean;
    diffMode: DiffModeConfig | null;
    toggleExpand: (path: string) => void;
    expandPath: (path: string) => void;
    selectObject: (object: ObjectEntry | null) => void;
    setActiveTab: (tab: ActiveTab) => void;
    isExpanded: (path: string) => boolean;
    refresh: () => void;
    getDiffType: (path: string) => DiffType;
    setDiffResults: (entries: DiffEntry[]) => void;
    setHasUncommittedChanges: (value: boolean) => void;
    setShowOnlyChanges: (value: boolean) => void;
    reportDiffStatus: (status: DiffStatus) => void;
}

const DataBrowserContext = createContext<DataBrowserContextType | undefined>(undefined);

export interface DiffStatus {
    isEmpty: boolean;
    loading: boolean;
    error: Error | null;
}

interface DataBrowserProviderProps {
    children: ReactNode;
    initialPath?: string;
    onPathChange?: (path: string) => void;
    externalRefreshToken?: boolean;
    showOnlyChanges?: boolean;
    onShowOnlyChangesChange?: (value: boolean) => void;
    onHasUncommittedChangesChange?: (value: boolean) => void;
    diffMode?: DiffModeConfig;
    onDiffStatusChange?: (status: DiffStatus) => void;
}

// Helper to get all parent paths for a given path
const getParentPaths = (path: string): string[] => {
    const paths: string[] = [];
    const parts = path.split('/').filter(Boolean);

    // Remove the last part if it's a file (no trailing slash in original path)
    // We want to expand all directories leading to the target
    let currentPath = '';
    for (let i = 0; i < parts.length - 1; i++) {
        currentPath = currentPath ? `${currentPath}/${parts[i]}` : parts[i];
        paths.push(currentPath + '/');
    }

    // If the path itself ends with /, it's a directory - include it
    if (path.endsWith('/')) {
        paths.push(path);
    }

    return paths;
};

export const DataBrowserProvider: React.FC<DataBrowserProviderProps> = ({
    children,
    initialPath,
    onPathChange,
    externalRefreshToken,
    showOnlyChanges: externalShowOnlyChanges,
    onShowOnlyChangesChange,
    onHasUncommittedChangesChange,
    diffMode = null,
    onDiffStatusChange,
}) => {
    const [expandedPaths, setExpandedPaths] = useState<Set<string>>(() => {
        const paths = new Set<string>();
        if (initialPath) {
            // Expand all parent paths
            const parentPaths = getParentPaths(initialPath);
            parentPaths.forEach((p) => paths.add(p));
        }
        return paths;
    });

    const [selectedObject, setSelectedObject] = useState<ObjectEntry | null>(() => {
        if (initialPath) {
            // Determine if it's a directory or file based on trailing slash
            const isDirectory = initialPath.endsWith('/');
            return {
                path: initialPath,
                path_type: isDirectory ? 'common_prefix' : 'object',
            };
        }
        return null;
    });

    const [activeTab, setActiveTab] = useState<ActiveTab>('preview');
    const [refreshToken, setRefreshToken] = useState<number>(0);
    const [diffResults, setDiffResultsState] = useState<Map<string, DiffType>>(new Map());
    const [hasUncommittedChangesState, setHasUncommittedChangesState] = useState<boolean>(false);
    const [showOnlyChangesState, setShowOnlyChangesState] = useState<boolean>(externalShowOnlyChanges ?? false);

    // Use external showOnlyChanges if provided, otherwise use internal state
    const showOnlyChanges = externalShowOnlyChanges ?? showOnlyChangesState;

    const setShowOnlyChanges = useCallback(
        (value: boolean) => {
            if (onShowOnlyChangesChange) {
                onShowOnlyChangesChange(value);
            } else {
                setShowOnlyChangesState(value);
            }
        },
        [onShowOnlyChangesChange],
    );

    const setHasUncommittedChanges = useCallback(
        (value: boolean) => {
            setHasUncommittedChangesState(value);
            if (onHasUncommittedChangesChange) {
                onHasUncommittedChangesChange(value);
            }
        },
        [onHasUncommittedChangesChange],
    );

    const toggleExpand = useCallback((path: string) => {
        setExpandedPaths((prev) => {
            const next = new Set(prev);
            const normalizedPath = path.endsWith('/') ? path : path + '/';
            if (next.has(normalizedPath)) {
                next.delete(normalizedPath);
            } else {
                next.add(normalizedPath);
            }
            return next;
        });
    }, []);

    const expandPath = useCallback((path: string) => {
        setExpandedPaths((prev) => {
            const next = new Set(prev);
            const parentPaths = getParentPaths(path);
            parentPaths.forEach((p) => next.add(p));
            return next;
        });
    }, []);

    const selectObject = useCallback(
        (object: ObjectEntry | null) => {
            setSelectedObject(object);
            if (object) {
                setActiveTab(object.path_type === 'common_prefix' ? 'info' : 'preview');
                // Notify parent of path change
                if (onPathChange) {
                    onPathChange(object.path);
                }
            }
        },
        [onPathChange],
    );

    const isExpanded = useCallback(
        (path: string) => {
            const normalizedPath = path.endsWith('/') ? path : path + '/';
            return expandedPaths.has(normalizedPath);
        },
        [expandedPaths],
    );

    const refresh = useCallback(() => {
        setRefreshToken((prev) => prev + 1);
        setDiffResultsState(new Map());
        setHasUncommittedChangesState(false);
        if (onHasUncommittedChangesChange) {
            onHasUncommittedChangesChange(false);
        }
    }, [onHasUncommittedChangesChange]);

    const getDiffType = useCallback(
        (path: string): DiffType => {
            return diffResults.get(path);
        },
        [diffResults],
    );

    const setDiffResults = useCallback((entries: DiffEntry[]) => {
        setDiffResultsState((prev) => {
            const next = new Map(prev);
            entries.forEach((entry) => {
                next.set(entry.path, normalizeDiffType(entry.type));
            });
            return next;
        });
    }, []);

    const reportDiffStatus = useCallback(
        (status: DiffStatus) => {
            if (onDiffStatusChange) {
                onDiffStatusChange(status);
            }
        },
        [onDiffStatusChange],
    );

    // Update expanded paths and selected object when initialPath changes
    useEffect(() => {
        if (initialPath) {
            const parentPaths = getParentPaths(initialPath);
            setExpandedPaths((prev) => {
                const next = new Set(prev);
                parentPaths.forEach((p) => next.add(p));
                return next;
            });
            // Update selected object to match new path
            const isDirectory = initialPath.endsWith('/');
            setSelectedObject({
                path: initialPath,
                path_type: isDirectory ? 'common_prefix' : 'object',
            });
        } else {
            // Clear selection when navigating to root
            setSelectedObject(null);
        }
    }, [initialPath]);

    // Respond to external refresh token changes
    useEffect(() => {
        if (externalRefreshToken !== undefined) {
            setRefreshToken((prev) => prev + 1);
        }
    }, [externalRefreshToken]);

    return (
        <DataBrowserContext.Provider
            value={{
                expandedPaths,
                selectedObject,
                activeTab,
                refreshToken,
                diffResults,
                hasUncommittedChanges: hasUncommittedChangesState,
                showOnlyChanges,
                diffMode,
                toggleExpand,
                expandPath,
                selectObject,
                setActiveTab,
                isExpanded,
                refresh,
                getDiffType,
                setDiffResults,
                setHasUncommittedChanges,
                setShowOnlyChanges,
                reportDiffStatus,
            }}
        >
            {children}
        </DataBrowserContext.Provider>
    );
};

export const useDataBrowser = (): DataBrowserContextType => {
    const context = useContext(DataBrowserContext);
    if (!context) {
        throw new Error('useDataBrowser must be used within a DataBrowserProvider');
    }
    return context;
};
