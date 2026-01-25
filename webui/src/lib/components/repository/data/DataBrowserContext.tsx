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
}

export type ActiveTab = 'preview' | 'info' | 'blame' | 'statistics';

interface DataBrowserContextType {
    expandedPaths: Set<string>;
    selectedObject: ObjectEntry | null;
    activeTab: ActiveTab;
    refreshToken: number;
    toggleExpand: (path: string) => void;
    expandPath: (path: string) => void;
    selectObject: (object: ObjectEntry | null) => void;
    setActiveTab: (tab: ActiveTab) => void;
    isExpanded: (path: string) => boolean;
    refresh: () => void;
}

const DataBrowserContext = createContext<DataBrowserContextType | undefined>(undefined);

interface DataBrowserProviderProps {
    children: ReactNode;
    initialPath?: string;
    onPathChange?: (path: string) => void;
    externalRefreshToken?: boolean;
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
    }, []);

    // Update expanded paths when initialPath changes
    useEffect(() => {
        if (initialPath) {
            const parentPaths = getParentPaths(initialPath);
            setExpandedPaths((prev) => {
                const next = new Set(prev);
                parentPaths.forEach((p) => next.add(p));
                return next;
            });
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
                toggleExpand,
                expandPath,
                selectObject,
                setActiveTab,
                isExpanded,
                refresh,
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
