import React, { useEffect, useState, useCallback, useRef } from 'react';
import Tab from 'react-bootstrap/Tab';
import Tabs from 'react-bootstrap/Tabs';
import Dropdown from 'react-bootstrap/Dropdown';
import Alert from 'react-bootstrap/Alert';
import Badge from 'react-bootstrap/Badge';
import Spinner from 'react-bootstrap/Spinner';
import Modal from 'react-bootstrap/Modal';
import Button from 'react-bootstrap/Button';
import Table from 'react-bootstrap/Table';
import {
    EyeIcon,
    InfoIcon,
    HistoryIcon,
    FileIcon,
    TrashIcon,
    LinkIcon,
    DownloadIcon,
    PasteIcon,
    KebabHorizontalIcon,
    DiffAddedIcon,
    DiffRemovedIcon,
    DiffModifiedIcon,
    ReplyIcon,
    GraphIcon,
} from '@primer/octicons-react';
import { Box } from '@mui/material';

import { useDataBrowser, ActiveTab, DiffType } from './DataBrowserContext';
import { ObjectInfoPanel } from './ObjectInfoPanel';
import { ObjectBlamePanel } from './ObjectBlamePanel';
import { DirectoryInfoPanel } from './DirectoryInfoPanel';
import { ObjectRenderer } from '../../../../pages/repositories/repository/fileRenderers';
import { linkToPath, objects, branches, refs } from '../../../api';
import { URINavigator } from '../tree';
import { ConfirmationModal } from '../../modals';
import { copyTextToClipboard } from '../../controls';
import { RefTypeBranch } from '../../../../constants';

// Human-readable file size
const humanSize = (bytes: number): string => {
    if (bytes === 0) return '0 B';
    const sign = bytes < 0 ? '-' : '';
    const absBytes = Math.abs(bytes);
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(absBytes) / Math.log(1024));
    const size = absBytes / Math.pow(1024, i);
    return sign + size.toFixed(i > 0 ? 1 : 0) + ' ' + units[i];
};

// Summary stats interface
interface ChangesSummary {
    added: number;
    addedBytes: number;
    removed: number;
    removedBytes: number;
    changed: number;
    changedBytes: number;
}

// Summarize Changes Modal Component
interface SummarizeChangesModalProps {
    show: boolean;
    onHide: () => void;
    repoId: string;
    refId: string;
    prefix: string;
    leftRef?: string; // For diff mode: base ref
    rightRef?: string; // For diff mode: target ref
}

const SummarizeChangesModal: React.FC<SummarizeChangesModalProps> = ({
    show,
    onHide,
    repoId,
    refId,
    prefix,
    leftRef,
    rightRef,
}) => {
    const isDiffMode = !!leftRef && !!rightRef;
    const [summary, setSummary] = useState<ChangesSummary>({
        added: 0,
        addedBytes: 0,
        removed: 0,
        removedBytes: 0,
        changed: 0,
        changedBytes: 0,
    });
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [totalProcessed, setTotalProcessed] = useState(0);

    // Ref to track if the operation should be cancelled
    const cancelledRef = useRef(false);

    // Reset all state to initial values
    const resetState = useCallback(() => {
        setSummary({ added: 0, addedBytes: 0, removed: 0, removedBytes: 0, changed: 0, changedBytes: 0 });
        setLoading(false);
        setError(null);
        setTotalProcessed(0);
    }, []);

    // Handle modal close - cancel operation and reset state
    const handleClose = useCallback(() => {
        cancelledRef.current = true;
        resetState();
        onHide();
    }, [onHide, resetState]);

    const fetchChanges = useCallback(async () => {
        if (!show) return;

        // Reset cancelled flag when starting a new operation
        cancelledRef.current = false;

        setLoading(true);
        setError(null);
        setSummary({ added: 0, addedBytes: 0, removed: 0, removedBytes: 0, changed: 0, changedBytes: 0 });
        setTotalProcessed(0);

        let after = '';
        let hasMore = true;
        const newSummary: ChangesSummary = {
            added: 0,
            addedBytes: 0,
            removed: 0,
            removedBytes: 0,
            changed: 0,
            changedBytes: 0,
        };
        let processed = 0;

        try {
            while (hasMore) {
                // Check if cancelled before making the next API call
                if (cancelledRef.current) {
                    return;
                }

                // Use refs.diff in diff mode, refs.changes otherwise
                const response = isDiffMode
                    ? await refs.diff(repoId, leftRef!, rightRef!, after, prefix, '', 1000)
                    : await refs.changes(repoId, refId, after, prefix, '', 1000);

                // Check again after the API call returns
                if (cancelledRef.current) {
                    return;
                }

                const results = response.results || [];

                for (const item of results) {
                    // Skip prefix_changed entries (directory markers)
                    if (item.type === 'prefix_changed') continue;

                    const sizeBytes = item.size_bytes || 0;

                    switch (item.type) {
                        case 'added':
                            newSummary.added++;
                            newSummary.addedBytes += sizeBytes;
                            break;
                        case 'removed':
                            newSummary.removed++;
                            newSummary.removedBytes += sizeBytes;
                            break;
                        case 'changed':
                            newSummary.changed++;
                            newSummary.changedBytes += sizeBytes;
                            break;
                    }
                    processed++;
                }

                // Update state after each page (only if not cancelled)
                if (!cancelledRef.current) {
                    setSummary({ ...newSummary });
                    setTotalProcessed(processed);
                }

                // Check for more pages
                hasMore = response.pagination?.has_more || false;
                after = response.pagination?.next_offset || '';
            }
        } catch (err) {
            // Only set error if not cancelled
            if (!cancelledRef.current) {
                setError(err instanceof Error ? err.message : 'Failed to fetch changes');
            }
        } finally {
            // Only update loading state if not cancelled
            if (!cancelledRef.current) {
                setLoading(false);
            }
        }
    }, [show, repoId, refId, prefix, isDiffMode, leftRef, rightRef]);

    useEffect(() => {
        if (show) {
            fetchChanges();
        }
    }, [show, fetchChanges]);

    // Cleanup on unmount
    useEffect(() => {
        return () => {
            cancelledRef.current = true;
        };
    }, []);

    const totalChanges = summary.added + summary.removed + summary.changed;
    const netDeltaBytes = summary.addedBytes - summary.removedBytes;
    const displayPrefix = prefix || '(root)';

    return (
        <Modal show={show} onHide={handleClose} centered>
            <Modal.Header closeButton>
                <Modal.Title>
                    <GraphIcon className="me-2" />
                    Changes Summary
                </Modal.Title>
            </Modal.Header>
            <Modal.Body>
                <p className="text-muted mb-3">
                    Summarizing changes in <code>{displayPrefix}</code>
                </p>

                {loading && (
                    <div className="mb-3 d-flex align-items-center">
                        <Spinner animation="border" size="sm" className="me-2" />
                        <span className="text-muted">
                            Processing... ({totalProcessed.toLocaleString()} objects found)
                        </span>
                    </div>
                )}

                {error && <Alert variant="danger">{error}</Alert>}

                <Table bordered size="sm">
                    <thead>
                        <tr>
                            <th>Change Type</th>
                            <th className="text-end">Count</th>
                            <th className="text-end">Size</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr className="table-success">
                            <td>
                                <DiffAddedIcon className="me-2" />
                                Added
                            </td>
                            <td className="text-end">{summary.added.toLocaleString()}</td>
                            <td className="text-end">{humanSize(summary.addedBytes)}</td>
                        </tr>
                        <tr className="table-danger">
                            <td>
                                <DiffRemovedIcon className="me-2" />
                                Removed
                            </td>
                            <td className="text-end">{summary.removed.toLocaleString()}</td>
                            <td className="text-end">{humanSize(summary.removedBytes)}</td>
                        </tr>
                        <tr className="table-warning">
                            <td>
                                <DiffModifiedIcon className="me-2" />
                                Modified
                            </td>
                            <td className="text-end">{summary.changed.toLocaleString()}</td>
                            <td className="text-end">{humanSize(summary.changedBytes)}</td>
                        </tr>
                    </tbody>
                    <tfoot>
                        <tr className="fw-bold">
                            <td>Total</td>
                            <td className="text-end">{totalChanges.toLocaleString()}</td>
                            <td className="text-end">
                                <span className={netDeltaBytes >= 0 ? 'text-success' : 'text-danger'}>
                                    {netDeltaBytes >= 0 ? '+' : ''}
                                    {humanSize(netDeltaBytes)}
                                </span>
                            </td>
                        </tr>
                    </tfoot>
                </Table>

                {!loading && totalChanges === 0 && (
                    <Alert variant="info" className="mb-0">
                        No changes found in this location.
                    </Alert>
                )}
            </Modal.Body>
            <Modal.Footer>
                <Button variant="secondary" onClick={handleClose}>
                    {loading ? 'Cancel' : 'Close'}
                </Button>
            </Modal.Footer>
        </Modal>
    );
};

// Helper to get diff badge
const DiffBadge: React.FC<{ diffType: DiffType }> = ({ diffType }) => {
    if (!diffType) return null;

    switch (diffType) {
        case 'added':
            return (
                <Badge bg="success" className="diff-badge">
                    <DiffAddedIcon size={12} className="me-1" />
                    Added
                </Badge>
            );
        case 'removed':
            return (
                <Badge bg="danger" className="diff-badge">
                    <DiffRemovedIcon size={12} className="me-1" />
                    Deleted
                </Badge>
            );
        case 'changed':
            return (
                <Badge bg="warning" text="dark" className="diff-badge">
                    <DiffModifiedIcon size={12} className="me-1" />
                    Modified
                </Badge>
            );
        default:
            return null;
    }
};

interface ObjectViewerPanelProps {
    repo: { id: string; read_only?: boolean };
    reference: { id: string; type: string };
    config: {
        pre_sign_support?: boolean;
        pre_sign_support_ui?: boolean;
    };
}

const getFileExtension = (path: string): string => {
    const parts = path.split('.');
    return parts.length > 1 ? parts[parts.length - 1] : '';
};

const README_FILE_NAME = 'README.md';

export const ObjectViewerPanel: React.FC<ObjectViewerPanelProps> = ({ repo, reference, config }) => {
    const { selectedObject, activeTab, setActiveTab, selectObject, refresh, hasUncommittedChanges, diffMode } =
        useDataBrowser();
    const [hasReadme, setHasReadme] = useState(false);
    const [readmePath, setReadmePath] = useState<string | null>(null);
    const [hasRootReadme, setHasRootReadme] = useState(false);
    const [showDeleteConfirmation, setShowDeleteConfirmation] = useState(false);
    const [deleteError, setDeleteError] = useState<Error | null>(null);
    const [showRevertConfirmation, setShowRevertConfirmation] = useState(false);
    const [revertError, setRevertError] = useState<Error | null>(null);
    const [isReverting, setIsReverting] = useState(false);
    const [showRootRevertConfirmation, setShowRootRevertConfirmation] = useState(false);
    const [showSummarizeModal, setShowSummarizeModal] = useState(false);
    const [summarizePrefix, setSummarizePrefix] = useState('');

    const isDirectory = selectedObject?.path_type === 'common_prefix';
    const isBranch = reference.type === RefTypeBranch;
    const isInDiffMode = diffMode?.enabled || false;

    // In diff mode, disable all write operations
    const canDelete = !isInDiffMode && !repo.read_only && isBranch && selectedObject && !isDirectory;
    const canCopyPresignedUrl = config.pre_sign_support && selectedObject && !isDirectory;
    // Can undo addition if it's an uncommitted added file (not in diff mode)
    const canUndoAddition =
        !isInDiffMode && !repo.read_only && isBranch && selectedObject && selectedObject.diff_type === 'added';
    // Can undo change if it's an uncommitted modified file (not in diff mode)
    const canUndoChange =
        !isInDiffMode && !repo.read_only && isBranch && selectedObject && selectedObject.diff_type === 'changed';
    // Can undo deletion if it's a deleted file (restore it) (not in diff mode)
    const canUndoDeletion =
        !isInDiffMode && !repo.read_only && isBranch && selectedObject && selectedObject.diff_type === 'removed';
    // Can revert changes in a directory (not in diff mode)
    const canRevertDir = !isInDiffMode && !repo.read_only && isBranch && isDirectory;
    // Can revert all at root level if there are uncommitted changes (not in diff mode)
    const canRevertAll = !isInDiffMode && !repo.read_only && isBranch && hasUncommittedChanges;
    // Can summarize changes - always available in diff mode for directories, or when there are uncommitted changes
    const canSummarizeChanges = isInDiffMode || hasUncommittedChanges;

    // Get the appropriate revert label based on diff type
    const getRevertLabel = () => {
        if (!selectedObject) return 'Revert Changes';
        if (isDirectory) return 'Revert Changes Here';
        switch (selectedObject.diff_type) {
            case 'added':
                return 'Undo Addition';
            case 'changed':
                return 'Undo Change';
            case 'removed':
                return 'Undo Deletion';
            default:
                return 'Revert Changes';
        }
    };

    const handleDelete = async () => {
        if (!selectedObject) return;
        try {
            await objects.delete(repo.id, reference.id, selectedObject.path);
            setShowDeleteConfirmation(false);
            selectObject(null);
            refresh();
        } catch (err) {
            setDeleteError(err instanceof Error ? err : new Error('Failed to delete object'));
            setShowDeleteConfirmation(false);
        }
    };

    const handleCopyUri = async (e: React.MouseEvent) => {
        e.preventDefault();
        const path = selectedObject?.path || '';
        const uri = `lakefs://${repo.id}/${reference.id}/${path}`;
        await copyTextToClipboard(uri);
    };

    const handleRevertAll = async () => {
        setShowRootRevertConfirmation(false);
        setIsReverting(true);
        try {
            await branches.reset(repo.id, reference.id, { type: 'reset' });
            refresh();
        } catch (err) {
            setRevertError(err instanceof Error ? err : new Error('Failed to revert all changes'));
        } finally {
            setIsReverting(false);
        }
    };

    const handleCopyPresignedUrl = async (e: React.MouseEvent) => {
        e.preventDefault();
        if (!selectedObject) return;
        try {
            const stat = await objects.getStat(repo.id, reference.id, selectedObject.path, true);
            await copyTextToClipboard(stat.physical_address);
        } catch (err) {
            alert(err instanceof Error ? err.message : 'Failed to copy presigned URL');
        }
    };

    const handleRevert = async () => {
        if (!selectedObject) return;
        setShowRevertConfirmation(false);
        setIsReverting(true);
        try {
            await branches.reset(repo.id, reference.id, {
                type: selectedObject.path_type,
                path: selectedObject.path,
            });
            selectObject(null);
            refresh();
        } catch (err) {
            setRevertError(err instanceof Error ? err : new Error('Failed to revert changes'));
        } finally {
            setIsReverting(false);
        }
    };

    // Handle hotlinking to files - fetch full metadata if missing
    // This handles the case where a deleted file is accessed via direct URL
    useEffect(() => {
        if (!selectedObject || isDirectory) return;
        // Skip if we already have full metadata (diff_type is set or we have size_bytes)
        if (selectedObject.diff_type !== undefined || selectedObject.size_bytes !== undefined) return;
        // Only check on branches where files can be deleted
        if (reference.type !== RefTypeBranch) return;

        const fetchObjectData = async () => {
            try {
                // Try to fetch the object stat from current branch
                const stat = await objects.getStat(repo.id, reference.id, selectedObject.path);
                // Object exists - update with full metadata
                selectObject({
                    path: selectedObject.path,
                    path_type: 'object',
                    physical_address: stat.physical_address,
                    size_bytes: stat.size_bytes,
                    checksum: stat.checksum,
                    mtime: stat.mtime,
                    content_type: stat.content_type,
                    metadata: stat.metadata,
                });
            } catch {
                // Object doesn't exist on current branch - try committed version
                // If it exists there, it's a deleted file
                try {
                    const committedRef = `${reference.id}@`;
                    const stat = await objects.getStat(repo.id, committedRef, selectedObject.path);
                    // File exists in committed version but not in working tree - it's deleted
                    selectObject({
                        path: selectedObject.path,
                        path_type: 'object',
                        physical_address: stat.physical_address,
                        size_bytes: stat.size_bytes,
                        checksum: stat.checksum,
                        mtime: stat.mtime,
                        content_type: stat.content_type,
                        metadata: stat.metadata,
                        diff_type: 'removed',
                    });
                } catch {
                    // File doesn't exist in either location - truly doesn't exist
                }
            }
        };

        fetchObjectData();
    }, [selectedObject, isDirectory, repo.id, reference.id, reference.type, selectObject]);

    // Check for README.md at root when nothing is selected (skip in diff mode)
    useEffect(() => {
        if (selectedObject || isInDiffMode) {
            setHasRootReadme(false);
            return;
        }

        const checkRootReadme = async () => {
            try {
                await objects.head(repo.id, reference.id, README_FILE_NAME);
                setHasRootReadme(true);
            } catch {
                setHasRootReadme(false);
            }
        };

        checkRootReadme();
    }, [selectedObject, repo.id, reference.id, isInDiffMode]);

    // Check for README.md in directory (skip in diff mode)
    useEffect(() => {
        if (!selectedObject || !isDirectory || isInDiffMode) {
            setHasReadme(false);
            setReadmePath(null);
            return;
        }

        const checkReadme = async () => {
            const path = selectedObject.path.endsWith('/')
                ? `${selectedObject.path}${README_FILE_NAME}`
                : `${selectedObject.path}/${README_FILE_NAME}`;

            try {
                await objects.head(repo.id, reference.id, path);
                setHasReadme(true);
                setReadmePath(path);
            } catch {
                setHasReadme(false);
                setReadmePath(null);
            }
        };

        checkReadme();
    }, [selectedObject, isDirectory, repo.id, reference.id, isInDiffMode]);

    // Set default tab based on selection type
    useEffect(() => {
        if (selectedObject) {
            if (isDirectory) {
                if (hasReadme && activeTab !== 'preview' && activeTab !== 'info' && activeTab !== 'blame') {
                    setActiveTab('preview');
                } else if (!hasReadme && activeTab === 'preview') {
                    setActiveTab('info');
                }
            } else if (activeTab === 'statistics') {
                setActiveTab('preview');
            }
        }
    }, [selectedObject, isDirectory, hasReadme, activeTab, setActiveTab]);

    const presign = config.pre_sign_support_ui || false;

    // Root directory context menu component
    const RootContextMenu = () => (
        <Dropdown align="end">
            <Dropdown.Toggle variant="outline-secondary" size="sm" className="object-actions-toggle">
                <KebabHorizontalIcon />
            </Dropdown.Toggle>
            <Dropdown.Menu>
                <Dropdown.Item onClick={handleCopyUri}>
                    <PasteIcon className="me-2" /> Copy URI
                </Dropdown.Item>
                {canSummarizeChanges && (
                    <>
                        <Dropdown.Divider />
                        <Dropdown.Item
                            onClick={(e) => {
                                e.preventDefault();
                                setSummarizePrefix('');
                                setShowSummarizeModal(true);
                            }}
                        >
                            <GraphIcon className="me-2" /> Summarize Changes
                        </Dropdown.Item>
                    </>
                )}
                {canRevertAll && (
                    <Dropdown.Item
                        onClick={(e) => {
                            e.preventDefault();
                            setShowRootRevertConfirmation(true);
                        }}
                        className="text-warning"
                    >
                        <HistoryIcon className="me-2" /> Revert All Changes
                    </Dropdown.Item>
                )}
            </Dropdown.Menu>
        </Dropdown>
    );

    if (!selectedObject) {
        // In diff mode, show a friendly empty state prompting user to select an item
        if (isInDiffMode) {
            return (
                <div className="object-viewer-panel">
                    <SummarizeChangesModal
                        show={showSummarizeModal}
                        onHide={() => setShowSummarizeModal(false)}
                        repoId={repo.id}
                        refId={reference.id}
                        prefix={summarizePrefix}
                        leftRef={diffMode?.leftRef}
                        rightRef={diffMode?.rightRef}
                    />
                    <div className="object-viewer-empty">
                        <FileIcon size={48} className="mb-3 opacity-25" />
                        <p className="text-muted">Select an object or folder from the tree to view details</p>
                    </div>
                </div>
            );
        }

        // Show root README.md if it exists
        if (hasRootReadme) {
            return (
                <div className="object-viewer-panel">
                    {isReverting && (
                        <div className="object-viewer-loading-overlay">
                            <Spinner animation="border" size="sm" className="me-2" />
                            Reverting all changes...
                        </div>
                    )}
                    {revertError && (
                        <Alert variant="danger" dismissible onClose={() => setRevertError(null)} className="m-2">
                            {revertError.message}
                        </Alert>
                    )}
                    <ConfirmationModal
                        show={showRootRevertConfirmation}
                        onHide={() => setShowRootRevertConfirmation(false)}
                        msg="Are you sure you want to revert all uncommitted changes?"
                        onConfirm={handleRevertAll}
                    />
                    <SummarizeChangesModal
                        show={showSummarizeModal}
                        onHide={() => setShowSummarizeModal(false)}
                        repoId={repo.id}
                        refId={reference.id}
                        prefix={summarizePrefix}
                        leftRef={diffMode?.leftRef}
                        rightRef={diffMode?.rightRef}
                    />
                    <div className="object-viewer-header">
                        <div className="object-viewer-header-nav">
                            <URINavigator path="" repo={repo} reference={reference} isPathToFile={false} />
                        </div>
                        <div className="object-viewer-header-actions">
                            <RootContextMenu />
                        </div>
                    </div>
                    <div className="object-viewer-content">
                        <Box sx={{ mx: 1 }}>
                            <ObjectRenderer
                                repoId={repo.id}
                                refId={reference.id}
                                path={README_FILE_NAME}
                                fileExtension="md"
                                contentType="text/markdown"
                                sizeBytes={0}
                                presign={presign}
                            />
                        </Box>
                    </div>
                </div>
            );
        }

        return (
            <div className="object-viewer-panel">
                {isReverting && (
                    <div className="object-viewer-loading-overlay">
                        <Spinner animation="border" size="sm" className="me-2" />
                        Reverting all changes...
                    </div>
                )}
                {revertError && (
                    <Alert variant="danger" dismissible onClose={() => setRevertError(null)} className="m-2">
                        {revertError.message}
                    </Alert>
                )}
                <ConfirmationModal
                    show={showRootRevertConfirmation}
                    onHide={() => setShowRootRevertConfirmation(false)}
                    msg="Are you sure you want to revert all uncommitted changes?"
                    onConfirm={handleRevertAll}
                />
                <SummarizeChangesModal
                    show={showSummarizeModal}
                    onHide={() => setShowSummarizeModal(false)}
                    repoId={repo.id}
                    refId={reference.id}
                    prefix={summarizePrefix}
                    leftRef={diffMode?.leftRef}
                    rightRef={diffMode?.rightRef}
                />
                <div className="object-viewer-header">
                    <div className="object-viewer-header-nav">
                        <URINavigator path="" repo={repo} reference={reference} isPathToFile={false} />
                    </div>
                    <div className="object-viewer-header-actions">
                        <RootContextMenu />
                    </div>
                </div>
                <div className="object-viewer-empty">
                    <FileIcon size={48} className="mb-3 opacity-25" />
                    <p>No objects in this directory</p>
                </div>
            </div>
        );
    }

    const fileExtension = getFileExtension(selectedObject.path);

    const handleTabSelect = (key: string | null) => {
        if (key) {
            setActiveTab(key as ActiveTab);
        }
    };

    // Render tabs for directories
    if (isDirectory) {
        const dirHeaderClass = selectedObject.diff_type
            ? `object-viewer-header diff-${selectedObject.diff_type}`
            : 'object-viewer-header';

        const revertDirMsg =
            selectedObject.diff_type === 'added'
                ? `Are you sure you want to revert all uncommitted changes in "${selectedObject.path}"? This will remove all new files in this directory.`
                : `Are you sure you want to revert all uncommitted changes in "${selectedObject.path}"?`;

        return (
            <div className="object-viewer-panel">
                {isReverting && (
                    <div className="object-viewer-loading-overlay">
                        <Spinner animation="border" size="sm" className="me-2" />
                        Reverting changes...
                    </div>
                )}
                {revertError && (
                    <Alert variant="danger" dismissible onClose={() => setRevertError(null)} className="m-2">
                        {revertError.message}
                    </Alert>
                )}
                <ConfirmationModal
                    show={showRevertConfirmation}
                    onHide={() => setShowRevertConfirmation(false)}
                    msg={revertDirMsg}
                    onConfirm={handleRevert}
                />
                <SummarizeChangesModal
                    show={showSummarizeModal}
                    onHide={() => setShowSummarizeModal(false)}
                    repoId={repo.id}
                    refId={reference.id}
                    prefix={summarizePrefix}
                    leftRef={diffMode?.leftRef}
                    rightRef={diffMode?.rightRef}
                />
                <div className={dirHeaderClass}>
                    <div className="object-viewer-header-nav">
                        <URINavigator
                            path={selectedObject.path}
                            repo={repo}
                            reference={reference}
                            isPathToFile={false}
                        />
                        <DiffBadge diffType={selectedObject.diff_type} />
                    </div>
                    <div className="object-viewer-header-actions">
                        <Dropdown align="end">
                            <Dropdown.Toggle variant="outline-secondary" size="sm" className="object-actions-toggle">
                                <KebabHorizontalIcon />
                            </Dropdown.Toggle>
                            <Dropdown.Menu>
                                <Dropdown.Item onClick={handleCopyUri}>
                                    <PasteIcon className="me-2" /> Copy URI
                                </Dropdown.Item>
                                {(isInDiffMode || canRevertDir) && (
                                    <>
                                        <Dropdown.Divider />
                                        <Dropdown.Item
                                            onClick={(e) => {
                                                e.preventDefault();
                                                setSummarizePrefix(selectedObject.path);
                                                setShowSummarizeModal(true);
                                            }}
                                        >
                                            <GraphIcon className="me-2" /> Summarize Changes
                                        </Dropdown.Item>
                                    </>
                                )}
                                {canRevertDir && (
                                    <Dropdown.Item
                                        onClick={(e) => {
                                            e.preventDefault();
                                            setShowRevertConfirmation(true);
                                        }}
                                        className="text-warning"
                                    >
                                        <ReplyIcon className="me-2" /> {getRevertLabel()}
                                    </Dropdown.Item>
                                )}
                            </Dropdown.Menu>
                        </Dropdown>
                    </div>
                </div>
                <Tabs activeKey={activeTab} onSelect={handleTabSelect} className="object-viewer-tabs">
                    {hasReadme && readmePath && (
                        <Tab
                            eventKey="preview"
                            title={
                                <>
                                    <EyeIcon className="me-1" /> Preview
                                </>
                            }
                        >
                            <div className="object-viewer-content">
                                <Box sx={{ mx: 1 }}>
                                    <ObjectRenderer
                                        repoId={repo.id}
                                        refId={reference.id}
                                        path={readmePath}
                                        fileExtension="md"
                                        contentType="text/markdown"
                                        sizeBytes={0}
                                        presign={presign}
                                    />
                                </Box>
                            </div>
                        </Tab>
                    )}
                    <Tab
                        eventKey="info"
                        title={
                            <>
                                <InfoIcon className="me-1" /> Info
                            </>
                        }
                    >
                        <div className="object-viewer-content">
                            <DirectoryInfoPanel
                                entry={selectedObject}
                                repo={repo}
                                reference={reference}
                                hideStats={isInDiffMode}
                            />
                        </div>
                    </Tab>
                    {!isInDiffMode && (
                        <Tab
                            eventKey="blame"
                            title={
                                <>
                                    <HistoryIcon className="me-1" /> Blame
                                </>
                            }
                        >
                            <div className="object-viewer-content">
                                <ObjectBlamePanel entry={selectedObject} repo={repo} reference={reference} />
                            </div>
                        </Tab>
                    )}
                </Tabs>
            </div>
        );
    }

    const deleteConfirmMsg = `Are you sure you wish to delete "${selectedObject.path}"?`;
    const fileName = selectedObject.path.split('/').pop() || selectedObject.path;
    const isDeleted = selectedObject.diff_type === 'removed';

    // Confirmation message varies based on diff type
    const getRevertConfirmMsg = () => {
        switch (selectedObject.diff_type) {
            case 'added':
                return `Are you sure you want to undo the addition of "${selectedObject.path}"? This will remove the file.`;
            case 'removed':
                return `Are you sure you want to undo the deletion of "${selectedObject.path}"? This will restore the file to its last committed state.`;
            case 'changed':
                return `Are you sure you want to undo changes to "${selectedObject.path}"? This will restore the file to its last committed state.`;
            default:
                return `Are you sure you want to revert "${selectedObject.path}" to its last committed state?`;
        }
    };
    const revertConfirmMsg = getRevertConfirmMsg();

    // Determine the effective reference for fetching content
    // In diff mode: use leftRef for deleted files, rightRef for added/changed files
    // In normal mode: use branch@ for deleted files, reference.id for others
    const effectiveRefId = isDeleted
        ? diffMode?.enabled
            ? diffMode.leftRef
            : `${reference.id}@`
        : diffMode?.enabled
          ? diffMode.rightRef
          : reference.id;
    const effectiveObjectUrl = linkToPath(repo.id, effectiveRefId, selectedObject.path, presign);

    // Get header class based on diff type
    const headerClass = selectedObject.diff_type
        ? `object-viewer-header diff-${selectedObject.diff_type}`
        : 'object-viewer-header';

    // Render tabs for files
    return (
        <div className="object-viewer-panel">
            {isReverting && (
                <div className="object-viewer-loading-overlay">
                    <Spinner animation="border" size="sm" className="me-2" />
                    {isDeleted ? 'Restoring object...' : 'Reverting changes...'}
                </div>
            )}
            {deleteError && (
                <Alert variant="danger" dismissible onClose={() => setDeleteError(null)} className="m-2">
                    {deleteError.message}
                </Alert>
            )}
            {revertError && (
                <Alert variant="danger" dismissible onClose={() => setRevertError(null)} className="m-2">
                    {revertError.message}
                </Alert>
            )}
            <div className={headerClass}>
                <div className="object-viewer-header-nav">
                    <URINavigator path={selectedObject.path} repo={repo} reference={reference} isPathToFile={true} />
                    <DiffBadge diffType={selectedObject.diff_type} />
                </div>
                <div className="object-viewer-header-actions">
                    <Dropdown align="end">
                        <Dropdown.Toggle variant="outline-secondary" size="sm" className="object-actions-toggle">
                            <KebabHorizontalIcon />
                        </Dropdown.Toggle>
                        <Dropdown.Menu>
                            <Dropdown.Item onClick={handleCopyUri}>
                                <PasteIcon className="me-2" /> Copy URI
                            </Dropdown.Item>
                            {canCopyPresignedUrl && !isDeleted && (
                                <Dropdown.Item onClick={handleCopyPresignedUrl}>
                                    <LinkIcon className="me-2" /> Copy Presigned URL
                                </Dropdown.Item>
                            )}
                            <Dropdown.Item as="a" href={effectiveObjectUrl} download={fileName}>
                                <DownloadIcon className="me-2" /> Download
                            </Dropdown.Item>
                            {canDelete && !isDeleted && (
                                <>
                                    <Dropdown.Divider />
                                    <Dropdown.Item
                                        onClick={(e) => {
                                            e.preventDefault();
                                            setShowDeleteConfirmation(true);
                                        }}
                                        className="text-danger"
                                    >
                                        <TrashIcon className="me-2" /> Delete
                                    </Dropdown.Item>
                                </>
                            )}
                            {canUndoAddition && (
                                <>
                                    <Dropdown.Divider />
                                    <Dropdown.Item
                                        onClick={(e) => {
                                            e.preventDefault();
                                            setShowRevertConfirmation(true);
                                        }}
                                        className="text-warning"
                                    >
                                        <ReplyIcon className="me-2" /> {getRevertLabel()}
                                    </Dropdown.Item>
                                </>
                            )}
                            {canUndoChange && (
                                <>
                                    <Dropdown.Divider />
                                    <Dropdown.Item
                                        onClick={(e) => {
                                            e.preventDefault();
                                            setShowRevertConfirmation(true);
                                        }}
                                        className="text-warning"
                                    >
                                        <ReplyIcon className="me-2" /> {getRevertLabel()}
                                    </Dropdown.Item>
                                </>
                            )}
                            {canUndoDeletion && (
                                <>
                                    <Dropdown.Divider />
                                    <Dropdown.Item
                                        onClick={(e) => {
                                            e.preventDefault();
                                            setShowRevertConfirmation(true);
                                        }}
                                        className="text-success"
                                    >
                                        <ReplyIcon className="me-2" /> {getRevertLabel()}
                                    </Dropdown.Item>
                                </>
                            )}
                        </Dropdown.Menu>
                    </Dropdown>
                </div>
            </div>
            <ConfirmationModal
                show={showDeleteConfirmation}
                onHide={() => setShowDeleteConfirmation(false)}
                msg={deleteConfirmMsg}
                onConfirm={handleDelete}
            />
            <ConfirmationModal
                show={showRevertConfirmation}
                onHide={() => setShowRevertConfirmation(false)}
                msg={revertConfirmMsg}
                onConfirm={handleRevert}
            />
            <Tabs activeKey={activeTab} onSelect={handleTabSelect} className="object-viewer-tabs">
                <Tab
                    eventKey="preview"
                    title={
                        <>
                            <EyeIcon className="me-1" /> Preview
                        </>
                    }
                >
                    <div className="object-viewer-content">
                        <Box sx={{ mx: 1 }}>
                            <ObjectRenderer
                                repoId={repo.id}
                                refId={effectiveRefId}
                                path={selectedObject.path}
                                fileExtension={fileExtension}
                                contentType={selectedObject.content_type}
                                sizeBytes={selectedObject.size_bytes || 0}
                                presign={presign}
                            />
                        </Box>
                    </div>
                </Tab>
                <Tab
                    eventKey="info"
                    title={
                        <>
                            <InfoIcon className="me-1" /> Info
                        </>
                    }
                >
                    <div className="object-viewer-content">
                        <ObjectInfoPanel entry={selectedObject} />
                    </div>
                </Tab>
                {!isInDiffMode && (
                    <Tab
                        eventKey="blame"
                        title={
                            <>
                                <HistoryIcon className="me-1" /> Blame
                            </>
                        }
                    >
                        <div className="object-viewer-content">
                            <ObjectBlamePanel entry={selectedObject} repo={repo} reference={reference} />
                        </div>
                    </Tab>
                )}
            </Tabs>
        </div>
    );
};
