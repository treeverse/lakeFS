import React, { useState, useEffect, useCallback, useMemo } from 'react';
import {
    ChevronDownIcon,
    ChevronRightIcon,
    FileDirectoryIcon,
    FileIcon,
    KebabHorizontalIcon,
    DiffAddedIcon,
    DiffRemovedIcon,
    DiffModifiedIcon,
} from '@primer/octicons-react';
import Spinner from 'react-bootstrap/Spinner';
import Button from 'react-bootstrap/Button';

import { objects, refs } from '../../../api';
import { useDataBrowser, ObjectEntry, DiffType, DiffEntry, normalizeDiffType } from './DataBrowserContext';
import { RefTypeBranch } from '../../../../constants';

interface ObjectEntryWithDiff extends ObjectEntry {
    diff_type?: DiffType;
}

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
    entry: ObjectEntryWithDiff;
    repo: { id: string };
    reference: { id: string; type: string };
    config: {
        pre_sign_support?: boolean;
        pre_sign_support_ui?: boolean;
    };
    depth: number;
    onNavigate?: (path: string) => void;
    targetPath?: string;
    diffMap?: Map<string, DiffType>;
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

const TreeNode: React.FC<TreeNodeProps> = ({
    entry,
    repo,
    reference,
    config,
    depth,
    onNavigate,
    targetPath,
    diffMap,
}) => {
    const { isExpanded, toggleExpand, selectObject, selectedObject, refreshToken, showOnlyChanges, setDiffResults } =
        useDataBrowser();
    const [children, setChildren] = useState<ObjectEntryWithDiff[]>([]);
    const [deletedChildren, setDeletedChildren] = useState<ObjectEntryWithDiff[]>([]);
    const [loading, setLoading] = useState(false);
    const [loadingMore, setLoadingMore] = useState(false);
    const [loaded, setLoaded] = useState(false);
    const [lastRefreshToken, setLastRefreshToken] = useState(refreshToken);
    const [pagination, setPagination] = useState<PaginationState>({ hasMore: false, nextOffset: '' });
    const [truncatedBefore, setTruncatedBefore] = useState(false);
    const [jumpedToTarget, setJumpedToTarget] = useState(false);
    const [localDiffMap, setLocalDiffMap] = useState<Map<string, DiffType>>(new Map());

    const isDirectory = entry.path_type === 'common_prefix';
    const expanded = isExpanded(entry.path);
    const isSelected = selectedObject?.path === entry.path;
    const diffType = entry.diff_type || diffMap?.get(entry.path);

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
                // Fetch listing and diff in parallel (diff only for branches)
                const isBranch = reference.type === RefTypeBranch;
                const [listResponse, diffResponse] = await Promise.all([
                    objects.list(repo.id, reference.id, entry.path, after, config.pre_sign_support_ui),
                    isBranch ? refs.changes(repo.id, reference.id, '', entry.path, '/') : Promise.resolve(null),
                ]);

                // Build diff map for this directory
                const newDiffMap = new Map<string, DiffType>();
                const newDeletedEntries: ObjectEntryWithDiff[] = [];

                if (diffResponse?.results) {
                    diffResponse.results.forEach((item: DiffEntry) => {
                        const normalizedType = normalizeDiffType(item.type);
                        newDiffMap.set(item.path, normalizedType);
                        // Track deleted entries separately
                        if (item.type === 'removed') {
                            newDeletedEntries.push({
                                path: item.path,
                                path_type: item.path_type,
                                diff_type: 'removed',
                            });
                        }
                    });
                    // Update global diff results
                    setDiffResults(diffResponse.results);
                }

                if (!append) {
                    setLocalDiffMap(newDiffMap);
                    setDeletedChildren(newDeletedEntries);
                } else {
                    setLocalDiffMap((prev) => {
                        const merged = new Map(prev);
                        newDiffMap.forEach((v, k) => merged.set(k, v));
                        return merged;
                    });
                    // Append new deleted entries (dedupe)
                    setDeletedChildren((prev) => {
                        const existingPaths = new Set(prev.map((e) => e.path));
                        const newOnes = newDeletedEntries.filter((e) => !existingPaths.has(e.path));
                        return [...prev, ...newOnes];
                    });
                }

                // Map entries with their diff type
                const entries: ObjectEntryWithDiff[] = listResponse.results.map((item: ObjectEntry) => ({
                    path: item.path,
                    path_type: item.path_type,
                    physical_address: item.physical_address,
                    size_bytes: item.size_bytes,
                    checksum: item.checksum,
                    mtime: item.mtime,
                    content_type: item.content_type,
                    metadata: item.metadata,
                    diff_type: newDiffMap.get(item.path),
                }));

                if (append) {
                    setChildren((prev) => [...prev, ...entries]);
                } else {
                    setChildren(entries);
                    if (markTruncated) {
                        setTruncatedBefore(true);
                    }
                }

                // If appending and no new entries were returned, there's nothing more to load
                const apiHasMore = listResponse.pagination?.has_more || false;
                const effectiveHasMore = append && entries.length === 0 ? false : apiHasMore;

                setPagination({
                    hasMore: effectiveHasMore,
                    nextOffset: listResponse.pagination?.next_offset || '',
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
        [repo.id, reference.id, reference.type, entry.path, config.pre_sign_support_ui, isDirectory, setDiffResults],
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
            setDeletedChildren([]);
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
                diff_type: diffType,
            });
        } else if (diffType === 'removed') {
            // For deleted files, fetch stat from committed version (branch@)
            try {
                const committedRef = reference.id + '@';
                const stat = await objects.getStat(repo.id, committedRef, entry.path);
                selectObject({
                    path: entry.path,
                    path_type: 'object',
                    physical_address: stat.physical_address,
                    size_bytes: stat.size_bytes,
                    checksum: stat.checksum,
                    mtime: stat.mtime,
                    content_type: stat.content_type,
                    metadata: stat.metadata,
                    diff_type: 'removed',
                });
            } catch (error) {
                console.error('Failed to get deleted object stat:', error);
                selectObject({
                    path: entry.path,
                    path_type: 'object',
                    diff_type: 'removed',
                });
            }
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
                    diff_type: diffType,
                });
            } catch (error) {
                console.error('Failed to get object stat:', error);
                selectObject({ ...entry, diff_type: diffType });
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

    // Build class name based on diff status
    let nodeClass = 'tree-node-content';
    if (isSelected) nodeClass += ' selected';
    if (diffType) nodeClass += ` diff-${diffType}`;

    // Get diff icon for the entry
    const getDiffIcon = () => {
        if (!diffType) return null;
        switch (diffType) {
            case 'added':
                return <DiffAddedIcon size={12} className="tree-node-diff-icon diff-added-icon" />;
            case 'removed':
                return <DiffRemovedIcon size={12} className="tree-node-diff-icon diff-removed-icon" />;
            case 'changed':
                return <DiffModifiedIcon size={12} className="tree-node-diff-icon diff-changed-icon" />;
            default:
                return null;
        }
    };

    // Compute displayed children: merge deleted entries appropriately
    const displayedChildren = useMemo(() => {
        if (deletedChildren.length === 0) {
            // No deleted entries to merge
            if (showOnlyChanges) {
                return children.filter((child) => child.diff_type || localDiffMap.get(child.path));
            }
            return children;
        }

        // Merge deleted entries that aren't already in children
        const childPaths = new Set(children.map((c) => c.path));

        // In normal mode, only show deleted entries that fall within the current pagination range
        // In showOnlyChanges mode, show all deleted entries
        let missingDeleted = deletedChildren.filter((d) => !childPaths.has(d.path));

        if (!showOnlyChanges && children.length > 0) {
            // Filter to only include deleted entries within the current range
            const firstPath = children[0].path;
            const lastPath = children[children.length - 1].path;
            missingDeleted = missingDeleted.filter(
                (d) => d.path.localeCompare(firstPath) >= 0 && d.path.localeCompare(lastPath) <= 0,
            );
        }

        const merged = [...children, ...missingDeleted].sort((a, b) => a.path.localeCompare(b.path));

        if (showOnlyChanges) {
            // Filter to only show changed entries
            return merged.filter((child) => child.diff_type || localDiffMap.get(child.path));
        }

        return merged;
    }, [children, deletedChildren, showOnlyChanges, localDiffMap]);

    // In showOnlyChanges mode, hide this node if it has no changes
    if (showOnlyChanges && !diffType && !isDirectory) {
        return null;
    }

    // For directories in showOnlyChanges mode, check if there are any changed children
    const hasChangedChildren =
        children.some((child) => child.diff_type || localDiffMap.get(child.path)) || deletedChildren.length > 0;
    if (showOnlyChanges && isDirectory && !diffType && !hasChangedChildren && loaded) {
        return null;
    }

    return (
        <div className="tree-node">
            <div
                className={nodeClass}
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
                {getDiffIcon()}
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
                    {displayedChildren.map((child) => (
                        <TreeNode
                            key={child.path}
                            entry={child}
                            repo={repo}
                            reference={reference}
                            config={config}
                            depth={depth + 1}
                            onNavigate={onNavigate}
                            targetPath={targetPath}
                            diffMap={localDiffMap}
                        />
                    ))}
                    {displayedChildren.length === 0 && loaded && !loading && !truncatedBefore && (
                        <div className="tree-node-empty" style={{ paddingLeft: paddingLeft + 52 }}>
                            {showOnlyChanges ? 'No changes' : 'Empty directory'}
                        </div>
                    )}
                    {pagination.hasMore && !showOnlyChanges && (
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
    const { refreshToken, showOnlyChanges, setHasUncommittedChanges, setDiffResults } = useDataBrowser();
    const [rootEntries, setRootEntries] = useState<ObjectEntryWithDiff[]>([]);
    const [deletedRootEntries, setDeletedRootEntries] = useState<ObjectEntryWithDiff[]>([]);
    const [loading, setLoading] = useState(true);
    const [loadingMore, setLoadingMore] = useState(false);
    const [error, setError] = useState<Error | null>(null);
    const [pagination, setPagination] = useState<PaginationState>({ hasMore: false, nextOffset: '' });
    const [truncatedBefore, setTruncatedBefore] = useState(false);
    const [jumpedToTarget, setJumpedToTarget] = useState(false);
    const [rootDiffMap, setRootDiffMap] = useState<Map<string, DiffType>>(new Map());

    const isBranch = reference.type === RefTypeBranch;

    // Get the first segment of the initial path for targeting
    const targetFirstSegment = initialPath ? initialPath.split('/')[0] : null;

    // Check for uncommitted changes on mount (for branches only)
    useEffect(() => {
        if (!isBranch) {
            setHasUncommittedChanges(false);
            return;
        }

        const checkUncommittedChanges = async () => {
            try {
                const response = await refs.changes(repo.id, reference.id, '', '', '/', 1);
                setHasUncommittedChanges(response.results && response.results.length > 0);
            } catch {
                setHasUncommittedChanges(false);
            }
        };

        checkUncommittedChanges();
    }, [repo.id, reference.id, isBranch, refreshToken, setHasUncommittedChanges]);

    const loadRoot = useCallback(
        async (after: string = '', append: boolean = false, markTruncated: boolean = false) => {
            if (append) {
                setLoadingMore(true);
            } else {
                setLoading(true);
                setError(null);
            }

            try {
                // Fetch listing and diff in parallel (diff only for branches)
                const [listResponse, diffResponse] = await Promise.all([
                    objects.list(repo.id, reference.id, '', after, config.pre_sign_support_ui),
                    isBranch ? refs.changes(repo.id, reference.id, '', '', '/') : Promise.resolve(null),
                ]);

                // Build diff map for root
                const newDiffMap = new Map<string, DiffType>();
                const newDeletedEntries: ObjectEntryWithDiff[] = [];

                if (diffResponse?.results) {
                    diffResponse.results.forEach((item: DiffEntry) => {
                        const normalizedType = normalizeDiffType(item.type);
                        newDiffMap.set(item.path, normalizedType);
                        // Track deleted entries separately
                        if (item.type === 'removed') {
                            newDeletedEntries.push({
                                path: item.path,
                                path_type: item.path_type,
                                diff_type: 'removed',
                            });
                        }
                    });
                    setDiffResults(diffResponse.results);
                }

                if (!append) {
                    setRootDiffMap(newDiffMap);
                    setDeletedRootEntries(newDeletedEntries);
                } else {
                    setRootDiffMap((prev) => {
                        const merged = new Map(prev);
                        newDiffMap.forEach((v, k) => merged.set(k, v));
                        return merged;
                    });
                    // Append new deleted entries (dedupe)
                    setDeletedRootEntries((prev) => {
                        const existingPaths = new Set(prev.map((e) => e.path));
                        const newOnes = newDeletedEntries.filter((e) => !existingPaths.has(e.path));
                        return [...prev, ...newOnes];
                    });
                }

                // Map entries with their diff type
                const entries: ObjectEntryWithDiff[] = listResponse.results.map((item: ObjectEntry) => ({
                    path: item.path,
                    path_type: item.path_type,
                    physical_address: item.physical_address,
                    size_bytes: item.size_bytes,
                    checksum: item.checksum,
                    mtime: item.mtime,
                    content_type: item.content_type,
                    metadata: item.metadata,
                    diff_type: newDiffMap.get(item.path),
                }));

                if (append) {
                    setRootEntries((prev) => [...prev, ...entries]);
                } else {
                    setRootEntries(entries);
                    if (markTruncated) {
                        setTruncatedBefore(true);
                    }
                }

                // If appending and no new entries were returned, there's nothing more to load
                const apiHasMore = listResponse.pagination?.has_more || false;
                const effectiveHasMore = append && entries.length === 0 ? false : apiHasMore;

                setPagination({
                    hasMore: effectiveHasMore,
                    nextOffset: listResponse.pagination?.next_offset || '',
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
        [repo.id, reference.id, config.pre_sign_support_ui, isBranch, setDiffResults],
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

    // Compute displayed entries: merge deleted entries appropriately
    const displayedRootEntries = useMemo(() => {
        if (deletedRootEntries.length === 0) {
            // No deleted entries to merge
            if (showOnlyChanges) {
                return rootEntries.filter((entry) => entry.diff_type || rootDiffMap.get(entry.path));
            }
            return rootEntries;
        }

        // Merge deleted entries that aren't already in rootEntries
        const entryPaths = new Set(rootEntries.map((e) => e.path));

        // In normal mode, only show deleted entries that fall within the current pagination range
        // In showOnlyChanges mode, show all deleted entries
        let missingDeleted = deletedRootEntries.filter((d) => !entryPaths.has(d.path));

        if (!showOnlyChanges && rootEntries.length > 0) {
            // Filter to only include deleted entries within the current range
            const firstPath = rootEntries[0].path;
            const lastPath = rootEntries[rootEntries.length - 1].path;
            missingDeleted = missingDeleted.filter(
                (d) => d.path.localeCompare(firstPath) >= 0 && d.path.localeCompare(lastPath) <= 0,
            );
        }

        const merged = [...rootEntries, ...missingDeleted].sort((a, b) => a.path.localeCompare(b.path));

        if (showOnlyChanges) {
            // Filter to only show changed entries
            return merged.filter((entry) => entry.diff_type || rootDiffMap.get(entry.path));
        }

        return merged;
    }, [rootEntries, deletedRootEntries, showOnlyChanges, rootDiffMap]);

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

    if (rootEntries.length === 0 && deletedRootEntries.length === 0 && !truncatedBefore) {
        return <div className="objects-tree-empty text-muted">No objects found</div>;
    }

    if (showOnlyChanges && displayedRootEntries.length === 0 && !loading) {
        return <div className="objects-tree-empty text-muted">No uncommitted changes</div>;
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
            {displayedRootEntries.map((entry) => (
                <TreeNode
                    key={entry.path}
                    entry={entry}
                    repo={repo}
                    reference={reference}
                    config={config}
                    depth={0}
                    onNavigate={onNavigate}
                    targetPath={initialPath}
                    diffMap={rootDiffMap}
                />
            ))}
            {pagination.hasMore && !showOnlyChanges && (
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
