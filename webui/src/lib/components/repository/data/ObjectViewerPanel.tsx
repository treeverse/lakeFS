import React, { useEffect, useState } from 'react';
import Tab from 'react-bootstrap/Tab';
import Tabs from 'react-bootstrap/Tabs';
import { EyeIcon, InfoIcon, HistoryIcon, FileIcon } from '@primer/octicons-react';
import { Box } from '@mui/material';

import { useDataBrowser, ActiveTab } from './DataBrowserContext';
import { ObjectInfoPanel } from './ObjectInfoPanel';
import { ObjectBlamePanel } from './ObjectBlamePanel';
import { DirectoryInfoPanel } from './DirectoryInfoPanel';
import { ObjectRenderer } from '../../../../pages/repositories/repository/fileRenderers';
import { linkToPath, objects } from '../../../api';
import { URINavigator } from '../tree';

interface ObjectViewerPanelProps {
    repo: { id: string };
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
    const { selectedObject, activeTab, setActiveTab } = useDataBrowser();
    const [hasReadme, setHasReadme] = useState(false);
    const [readmePath, setReadmePath] = useState<string | null>(null);
    const [hasRootReadme, setHasRootReadme] = useState(false);

    const isDirectory = selectedObject?.path_type === 'common_prefix';

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
        return (
            <div className="object-viewer-panel">
                <div className="object-viewer-header">
                    <URINavigator
                        path={selectedObject.path}
                        repo={repo}
                        reference={reference}
                        isPathToFile={false}
                        hasCopyButton={true}
                    />
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

    // Render tabs for files
    return (
        <div className="object-viewer-panel">
            <div className="object-viewer-header">
                <URINavigator
                    path={selectedObject.path}
                    repo={repo}
                    reference={reference}
                    isPathToFile={true}
                    downloadUrl={objectUrl}
                    hasCopyButton={true}
                />
            </div>
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
                                refId={reference.id}
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
