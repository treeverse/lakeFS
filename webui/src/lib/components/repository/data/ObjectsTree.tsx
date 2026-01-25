import React, { useState, useEffect, useCallback } from 'react';
import {
    ChevronDownIcon,
    ChevronRightIcon,
    FileDirectoryIcon,
    FileIcon,
    KebabHorizontalIcon,
} from '@primer/octicons-react';
import Spinner from 'react-bootstrap/Spinner';
import Button from 'react-bootstrap/Button';

import { objects } from '../../../api';
import { useDataBrowser, ObjectEntry } from './DataBrowserContext';

interface ObjectsTreeProps {
    repo: { id: string };
    reference: { id: string; type: string };
    config: {
        pre_sign_support?: boolean;
        pre_sign_support_ui?: boolean;
    };
    onNavigate?: (path: string) => void;
    initialPath?: string;
}

interface TreeNodeProps {
    entry: ObjectEntry;
    repo: { id: string };
    reference: { id: string; type: string };
    config: {
        pre_sign_support?: boolean;
        pre_sign_support_ui?: boolean;
    };
    depth: number;
    onNavigate?: (path: string) => void;
    targetPath?: string;
}

interface PaginationState {
    hasMore: boolean;
    nextOffset: string;
}

// Get the `after` value to position just before the target
const getAfterParamForTarget = (targetName: string): string => {
    if (!targetName || targetName.length === 0) return '';
    // Return a string that sorts just before the target
    // by decrementing the last character
    const lastChar = targetName.charCodeAt(targetName.length - 1);
    if (lastChar > 0) {
        return targetName.slice(0, -1) + String.fromCharCode(lastChar - 1) + '\uffff';
    }
    return targetName.slice(0, -1);
};

const TreeNode: React.FC<TreeNodeProps> = ({ entry, repo, reference, config, depth, onNavigate, targetPath }) => {
    const { isExpanded, toggleExpand, selectObject, selectedObject, refreshToken } = useDataBrowser();
    const [children, setChildren] = useState<ObjectEntry[]>([]);
    const [loading, setLoading] = useState(false);
    const [loadingMore, setLoadingMore] = useState(false);
    const [loaded, setLoaded] = useState(false);
    const [lastRefreshToken, setLastRefreshToken] = useState(refreshToken);
    const [pagination, setPagination] = useState<PaginationState>({ hasMore: false, nextOffset: '' });
    const [truncatedBefore, setTruncatedBefore] = useState(false);
    const [jumpedToTarget, setJumpedToTarget] = useState(false);

    const isDirectory = entry.path_type === 'common_prefix';
    const expanded = isExpanded(entry.path);
    const isSelected = selectedObject?.path === entry.path;

    const displayName = entry.path.split('/').filter(Boolean).pop() || entry.path;

    // Check if target path is within this directory
    const targetInThisDir = targetPath && targetPath.startsWith(entry.path) && targetPath !== entry.path;
    const targetFileName = targetInThisDir ? targetPath.slice(entry.path.length).split('/')[0] : null;

    const loadChildren = useCallback(
        async (after: string = '', append: boolean = false, markTruncated: boolean = false) => {
            if (!isDirectory) return;

            if (append) {
                setLoadingMore(true);
            } else {
                setLoading(true);
            }

            try {
                const response = await objects.list(
                    repo.id,
                    reference.id,
                    entry.path,
                    after,
                    config.pre_sign_support_ui,
                );
                const entries: ObjectEntry[] = response.results.map((item: ObjectEntry) => ({
                    path: item.path,
                    path_type: item.path_type,
                    physical_address: item.physical_address,
                    size_bytes: item.size_bytes,
                    checksum: item.checksum,
                    mtime: item.mtime,
                    content_type: item.content_type,
                    metadata: item.metadata,
                }));

                if (append) {
                    setChildren((prev) => [...prev, ...entries]);
                } else {
                    setChildren(entries);
                    if (markTruncated) {
                        setTruncatedBefore(true);
                    }
                }

                setPagination({
                    hasMore: response.pagination?.has_more || false,
                    nextOffset: response.pagination?.next_offset || '',
                });
                setLoaded(true);

                return entries;
            } catch (error) {
                console.error('Failed to load children:', error);
                return [];
            } finally {
                setLoading(false);
                setLoadingMore(false);
            }
        },
        [repo.id, reference.id, entry.path, config.pre_sign_support_ui, isDirectory],
    );

    useEffect(() => {
        if (expanded && !loaded && isDirectory) {
            loadChildren();
        }
    }, [expanded, loaded, isDirectory, loadChildren]);

    // If target is not found in first page and we haven't jumped yet, use `after` to jump to it
    useEffect(() => {
        if (!loaded || !targetInThisDir || !targetFileName || jumpedToTarget || loading) return;

        const targetFullPath = targetPath?.endsWith('/')
            ? entry.path + targetFileName + '/'
            : entry.path + targetFileName;

        const foundTarget = children.some(
            (child) => child.path === targetFullPath || child.path === targetFullPath + '/',
        );

        if (!foundTarget && pagination.hasMore && !loadingMore) {
            // Target not found in first page - jump to it using `after`
            setJumpedToTarget(true);
            const afterParam = getAfterParamForTarget(entry.path + targetFileName);
            loadChildren(afterParam, false, true);
        }
    }, [
        loaded,
        targetInThisDir,
        targetFileName,
        targetPath,
        entry.path,
        children,
        pagination,
        loadingMore,
        loading,
        loadChildren,
        jumpedToTarget,
    ]);

    // Re-fetch children when refresh token changes
    useEffect(() => {
        if (refreshToken !== lastRefreshToken && expanded && isDirectory) {
            setLoaded(false);
            setChildren([]);
            setPagination({ hasMore: false, nextOffset: '' });
            setTruncatedBefore(false);
            setJumpedToTarget(false);
            setLastRefreshToken(refreshToken);
        }
    }, [refreshToken, lastRefreshToken, expanded, isDirectory]);

    const handleToggleExpand = async (e: React.MouseEvent) => {
        e.stopPropagation();
        if (isDirectory) {
            toggleExpand(entry.path);
            if (!loaded) {
                await loadChildren();
            }
        }
    };

    const handleSelect = async (e: React.MouseEvent) => {
        e.stopPropagation();

        if (isDirectory) {
            selectObject({
                path: entry.path,
                path_type: 'common_prefix',
            });
        } else {
            try {
                const stat = await objects.getStat(repo.id, reference.id, entry.path);
                selectObject({
                    path: entry.path,
                    path_type: 'object',
                    physical_address: stat.physical_address,
                    size_bytes: stat.size_bytes,
                    checksum: stat.checksum,
                    mtime: stat.mtime,
                    content_type: stat.content_type,
                    metadata: stat.metadata,
                });
            } catch (error) {
                console.error('Failed to get object stat:', error);
                selectObject(entry);
            }
        }
    };

    const handleLoadMore = async (e: React.MouseEvent) => {
        e.stopPropagation();
        if (pagination.hasMore && !loadingMore) {
            await loadChildren(pagination.nextOffset, true);
        }
    };

    const handleLoadFromStart = async (e: React.MouseEvent) => {
        e.stopPropagation();
        setTruncatedBefore(false);
        // Keep jumpedToTarget true to prevent auto-jump from triggering again
        setChildren([]);
        setLoaded(false);
        await loadChildren('', false, false);
    };

    const paddingLeft = 12 + depth * 16;

    return (
        <div className="tree-node">
            <div
                className={`tree-node-content ${isSelected ? 'selected' : ''}`}
                style={{ paddingLeft }}
                onClick={handleSelect}
                role="button"
                tabIndex={0}
                onKeyDown={(e) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                        handleSelect(e as unknown as React.MouseEvent);
                    }
                }}
            >
                <span
                    className="tree-node-icon"
                    onClick={handleToggleExpand}
                    role="button"
                    tabIndex={isDirectory ? 0 : -1}
                    onKeyDown={(e) => {
                        if (isDirectory && (e.key === 'Enter' || e.key === ' ')) {
                            handleToggleExpand(e as unknown as React.MouseEvent);
                        }
                    }}
                >
                    {isDirectory ? (
                        loading ? (
                            <Spinner animation="border" size="sm" className="tree-spinner" />
                        ) : expanded ? (
                            <ChevronDownIcon size={16} />
                        ) : (
                            <ChevronRightIcon size={16} />
                        )
                    ) : (
                        <span className="tree-node-spacer" />
                    )}
                </span>
                <span className="tree-node-type-icon">
                    {isDirectory ? <FileDirectoryIcon size={16} /> : <FileIcon size={16} />}
                </span>
                <span className="tree-node-name" title={entry.path}>
                    {displayName}
                    {isDirectory && '/'}
                </span>
            </div>

            {expanded && isDirectory && (
                <div className="tree-node-children">
                    {truncatedBefore && (
                        <div className="tree-node-truncated" style={{ paddingLeft: paddingLeft + 52 }}>
                            <Button variant="link" size="sm" onClick={handleLoadFromStart} className="p-0 text-muted">
                                <KebabHorizontalIcon size={12} className="me-1" />
                                Load from start...
                            </Button>
                        </div>
                    )}
                    {children.map((child) => (
                        <TreeNode
                            key={child.path}
                            entry={child}
                            repo={repo}
                            reference={reference}
                            config={config}
                            depth={depth + 1}
                            onNavigate={onNavigate}
                            targetPath={targetPath}
                        />
                    ))}
                    {children.length === 0 && loaded && !loading && !truncatedBefore && (
                        <div className="tree-node-empty" style={{ paddingLeft: paddingLeft + 52 }}>
                            Empty directory
                        </div>
                    )}
                    {pagination.hasMore && (
                        <div className="tree-node-load-more" style={{ paddingLeft: paddingLeft + 52 }}>
                            <Button
                                variant="link"
                                size="sm"
                                onClick={handleLoadMore}
                                disabled={loadingMore}
                                className="p-0"
                            >
                                {loadingMore ? (
                                    <>
                                        <Spinner
                                            animation="border"
                                            size="sm"
                                            className="me-1"
                                            style={{ width: 12, height: 12 }}
                                        />
                                        Loading...
                                    </>
                                ) : (
                                    'Load more...'
                                )}
                            </Button>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
};

export const ObjectsTree: React.FC<ObjectsTreeProps> = ({ repo, reference, config, onNavigate, initialPath }) => {
    const { refreshToken } = useDataBrowser();
    const [rootEntries, setRootEntries] = useState<ObjectEntry[]>([]);
    const [loading, setLoading] = useState(true);
    const [loadingMore, setLoadingMore] = useState(false);
    const [error, setError] = useState<Error | null>(null);
    const [pagination, setPagination] = useState<PaginationState>({ hasMore: false, nextOffset: '' });
    const [truncatedBefore, setTruncatedBefore] = useState(false);
    const [jumpedToTarget, setJumpedToTarget] = useState(false);

    // Get the first segment of the initial path for targeting
    const targetFirstSegment = initialPath ? initialPath.split('/')[0] : null;

    const loadRoot = useCallback(
        async (after: string = '', append: boolean = false, markTruncated: boolean = false) => {
            if (append) {
                setLoadingMore(true);
            } else {
                setLoading(true);
                setError(null);
            }

            try {
                const response = await objects.list(repo.id, reference.id, '', after, config.pre_sign_support_ui);
                const entries: ObjectEntry[] = response.results.map((item: ObjectEntry) => ({
                    path: item.path,
                    path_type: item.path_type,
                    physical_address: item.physical_address,
                    size_bytes: item.size_bytes,
                    checksum: item.checksum,
                    mtime: item.mtime,
                    content_type: item.content_type,
                    metadata: item.metadata,
                }));

                if (append) {
                    setRootEntries((prev) => [...prev, ...entries]);
                } else {
                    setRootEntries(entries);
                    if (markTruncated) {
                        setTruncatedBefore(true);
                    }
                }

                setPagination({
                    hasMore: response.pagination?.has_more || false,
                    nextOffset: response.pagination?.next_offset || '',
                });

                return entries;
            } catch (err) {
                setError(err instanceof Error ? err : new Error('Failed to load objects'));
                return [];
            } finally {
                setLoading(false);
                setLoadingMore(false);
            }
        },
        [repo.id, reference.id, config.pre_sign_support_ui],
    );

    useEffect(() => {
        setTruncatedBefore(false);
        setJumpedToTarget(false);
        loadRoot();
    }, [loadRoot, refreshToken]);

    // If target is not found in first page and we haven't jumped yet, use `after` to jump to it
    useEffect(() => {
        if (loading || !targetFirstSegment || jumpedToTarget) return;

        const targetFullPath = initialPath?.includes('/') ? targetFirstSegment + '/' : targetFirstSegment;

        const foundTarget = rootEntries.some(
            (entry) => entry.path === targetFullPath || entry.path === targetFirstSegment,
        );

        if (!foundTarget && pagination.hasMore && !loadingMore) {
            // Target not found in first page - jump to it using `after`
            setJumpedToTarget(true);
            const afterParam = getAfterParamForTarget(targetFirstSegment);
            loadRoot(afterParam, false, true);
        }
    }, [loading, targetFirstSegment, initialPath, rootEntries, pagination, loadingMore, loadRoot, jumpedToTarget]);

    const handleLoadMore = async () => {
        if (pagination.hasMore && !loadingMore) {
            await loadRoot(pagination.nextOffset, true);
        }
    };

    const handleLoadFromStart = async () => {
        setTruncatedBefore(false);
        // Keep jumpedToTarget true to prevent auto-jump from triggering again
        setRootEntries([]);
        await loadRoot('', false, false);
    };

    if (loading && rootEntries.length === 0) {
        return (
            <div className="objects-tree-loading">
                <Spinner animation="border" size="sm" className="me-2" />
                Loading...
            </div>
        );
    }

    if (error) {
        return <div className="objects-tree-error text-danger">Error: {error.message}</div>;
    }

    if (rootEntries.length === 0 && !truncatedBefore) {
        return <div className="objects-tree-empty text-muted">No objects found</div>;
    }

    return (
        <div className="objects-tree">
            {truncatedBefore && (
                <div className="tree-node-truncated" style={{ paddingLeft: 48 }}>
                    <Button variant="link" size="sm" onClick={handleLoadFromStart} className="p-0 text-muted">
                        <KebabHorizontalIcon size={12} className="me-1" />
                        Load from start...
                    </Button>
                </div>
            )}
            {rootEntries.map((entry) => (
                <TreeNode
                    key={entry.path}
                    entry={entry}
                    repo={repo}
                    reference={reference}
                    config={config}
                    depth={0}
                    onNavigate={onNavigate}
                    targetPath={initialPath}
                />
            ))}
            {pagination.hasMore && (
                <div className="tree-node-load-more" style={{ paddingLeft: 48 }}>
                    <Button variant="link" size="sm" onClick={handleLoadMore} disabled={loadingMore} className="p-0">
                        {loadingMore ? (
                            <>
                                <Spinner
                                    animation="border"
                                    size="sm"
                                    className="me-1"
                                    style={{ width: 12, height: 12 }}
                                />
                                Loading...
                            </>
                        ) : (
                            'Load more...'
                        )}
                    </Button>
                </div>
            )}
        </div>
    );
};
