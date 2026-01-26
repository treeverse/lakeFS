import React, { useEffect, useState } from 'react';
import Tab from 'react-bootstrap/Tab';
import Tabs from 'react-bootstrap/Tabs';
import Dropdown from 'react-bootstrap/Dropdown';
import Alert from 'react-bootstrap/Alert';
import Badge from 'react-bootstrap/Badge';
import Spinner from 'react-bootstrap/Spinner';
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
} from '@primer/octicons-react';
import { Box } from '@mui/material';

import { useDataBrowser, ActiveTab, DiffType } from './DataBrowserContext';
import { ObjectInfoPanel } from './ObjectInfoPanel';
import { ObjectBlamePanel } from './ObjectBlamePanel';
import { DirectoryInfoPanel } from './DirectoryInfoPanel';
import { ObjectRenderer } from '../../../../pages/repositories/repository/fileRenderers';
import { linkToPath, objects, branches } from '../../../api';
import { URINavigator } from '../tree';
import { ConfirmationModal } from '../../modals';
import { copyTextToClipboard } from '../../controls';
import { RefTypeBranch } from '../../../../constants';

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
    const { selectedObject, activeTab, setActiveTab, selectObject, refresh } = useDataBrowser();
    const [hasReadme, setHasReadme] = useState(false);
    const [readmePath, setReadmePath] = useState<string | null>(null);
    const [hasRootReadme, setHasRootReadme] = useState(false);
    const [showDeleteConfirmation, setShowDeleteConfirmation] = useState(false);
    const [deleteError, setDeleteError] = useState<Error | null>(null);
    const [showRevertConfirmation, setShowRevertConfirmation] = useState(false);
    const [revertError, setRevertError] = useState<Error | null>(null);
    const [isReverting, setIsReverting] = useState(false);

    const isDirectory = selectedObject?.path_type === 'common_prefix';
    const canDelete = !repo.read_only && reference.type === RefTypeBranch && selectedObject && !isDirectory;
    const canCopyPresignedUrl = config.pre_sign_support && selectedObject && !isDirectory;
    // Can revert if it's a branch and the object has uncommitted changes (added or changed, not removed)
    const canRevert =
        !repo.read_only &&
        reference.type === RefTypeBranch &&
        selectedObject &&
        (selectedObject.diff_type === 'added' || selectedObject.diff_type === 'changed');
    // Can restore if it's a branch and the object is deleted (diff_type === 'removed')
    const canRestore =
        !repo.read_only && reference.type === RefTypeBranch && selectedObject && selectedObject.diff_type === 'removed';

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
        if (!selectedObject) return;
        const uri = `lakefs://${repo.id}/${reference.id}/${selectedObject.path}`;
        await copyTextToClipboard(uri);
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

    // Check for README.md at root when nothing is selected
    useEffect(() => {
        if (selectedObject) {
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
    }, [selectedObject, repo.id, reference.id]);

    // Check for README.md in directory
    useEffect(() => {
        if (!selectedObject || !isDirectory) {
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
    }, [selectedObject, isDirectory, repo.id, reference.id]);

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

    if (!selectedObject) {
        // Show root README.md if it exists
        if (hasRootReadme) {
            return (
                <div className="object-viewer-panel">
                    <div className="object-viewer-header">
                        <URINavigator
                            path=""
                            repo={repo}
                            reference={reference}
                            isPathToFile={false}
                            hasCopyButton={true}
                        />
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
                <div className="object-viewer-empty">
                    <FileIcon size={48} className="mb-3 opacity-25" />
                    <p>Select an object or directory to view its details</p>
                </div>
            </div>
        );
    }

    const fileExtension = getFileExtension(selectedObject.path);
    const objectUrl = linkToPath(repo.id, reference.id, selectedObject.path, presign);

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
                                {canRevert && (
                                    <>
                                        <Dropdown.Divider />
                                        <Dropdown.Item
                                            onClick={(e) => {
                                                e.preventDefault();
                                                setShowRevertConfirmation(true);
                                            }}
                                            className="text-warning"
                                        >
                                            <ReplyIcon className="me-2" /> Revert Changes
                                        </Dropdown.Item>
                                    </>
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
                            <DirectoryInfoPanel entry={selectedObject} repo={repo} reference={reference} />
                        </div>
                    </Tab>
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
                return `Are you sure you want to revert "${selectedObject.path}"? This will remove the file.`;
            case 'removed':
                return `Are you sure you want to restore "${selectedObject.path}"? This will restore the file to its last committed state.`;
            default:
                return `Are you sure you want to revert "${selectedObject.path}" to its last committed state?`;
        }
    };
    const revertConfirmMsg = getRevertConfirmMsg();

    // For deleted files, use the committed reference (branch@) to fetch content
    const effectiveRefId = isDeleted ? `${reference.id}@` : reference.id;
    const effectiveObjectUrl = isDeleted
        ? linkToPath(repo.id, effectiveRefId, selectedObject.path, presign)
        : objectUrl;

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
                            {canRevert && (
                                <>
                                    <Dropdown.Divider />
                                    <Dropdown.Item
                                        onClick={(e) => {
                                            e.preventDefault();
                                            setShowRevertConfirmation(true);
                                        }}
                                        className="text-warning"
                                    >
                                        <ReplyIcon className="me-2" /> Revert Changes
                                    </Dropdown.Item>
                                </>
                            )}
                            {canRestore && (
                                <>
                                    <Dropdown.Divider />
                                    <Dropdown.Item
                                        onClick={(e) => {
                                            e.preventDefault();
                                            setShowRevertConfirmation(true);
                                        }}
                                        className="text-success"
                                    >
                                        <ReplyIcon className="me-2" /> Restore Object
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
            </Tabs>
        </div>
    );
};
