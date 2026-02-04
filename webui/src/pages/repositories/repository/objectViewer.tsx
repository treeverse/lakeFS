import React, { FC, useCallback, useState } from 'react';
import { useParams } from 'react-router-dom';
import { Box } from '@mui/material';
import Alert from 'react-bootstrap/Alert';
import Card from 'react-bootstrap/Card';

import { useAPI } from '../../../lib/hooks/api';
import { useQuery } from '../../../lib/hooks/router';
import { objects } from '../../../lib/api';
import { ObjectRenderer } from './fileRenderers';
import { AlertError } from '../../../lib/components/controls';
import { URINavigator } from '../../../lib/components/repository/tree';
import { RefTypeBranch } from '../../../constants';
import { RefContextProvider, useRefs } from '../../../lib/hooks/repo';
import { useConfigContext } from '../../../lib/hooks/configProvider';
import { linkToPath } from '../../../lib/api';
import { getRepoStorageConfig } from './utils';
import { usePluginManager } from '../../../extendable/plugins/pluginsContext';

import '../../../styles/quickstart.css';

type ObjectViewerPathParams = {
    objectName: string;
    repoId: string;
};

interface ObjectViewerQueryString {
    ref: string;
    path: string;
}

interface FileContentsProps {
    repoId: string;
    reference: { id: string; type: string };
    path: string;
    loading: boolean;
    error: Error | null;
    contentType?: string;
    fileExtension: string;
    sizeBytes: number;
    showFullNavigator?: boolean;
    presign?: boolean;
    checksum?: string;
    onRefresh?: () => void;
    storageConfig?: {
        pre_sign_support_ui?: boolean;
        blockstore_type?: string;
    };
}

export const Loading: FC = () => {
    return <Alert variant={'info'}>Loading...</Alert>;
};

export const getFileExtension = (objectName: string): string => {
    const objectNameParts = objectName.split('.');
    return objectNameParts[objectNameParts.length - 1];
};

export const getContentType = (headers: Headers): string | undefined => {
    if (!headers) return undefined;

    return headers.get('Content-Type') ?? undefined;
};

const FileObjectsViewerPage = () => {
    const { repo, loading: repoLoading, error: repoError } = useRefs();
    const { config, error: configsError, loading: configLoading } = useConfigContext();
    const { storageConfig, error: storageConfigError } = getRepoStorageConfig(config?.storages, repo);

    const { repoId } = useParams<ObjectViewerPathParams>();
    const queryString = useQuery<ObjectViewerQueryString>();
    const refId = queryString['ref'] ?? '';
    const path = queryString['path'] ?? '';
    const [refreshToken, setRefreshToken] = useState(0);

    const {
        response,
        error: apiError,
        loading: apiLoading,
    } = useAPI(() => {
        return objects.head(repoId, refId, path);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [repoId, refId, path, refreshToken]);

    const handleRefresh = useCallback(() => {
        setRefreshToken((prev) => prev + 1);
    }, []);

    const loading = apiLoading || repoLoading || configLoading;
    const error = loading ? null : apiError || repoError || configsError || storageConfigError;

    let content;
    if (loading) {
        content = <Loading />;
    } else if (error) {
        content = <AlertError error={error} />;
    } else {
        const fileExtension = getFileExtension(path);
        // We'll need to convert the API service to get rid of this any
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const contentType = getContentType((response as any)?.headers);
        const sizeBytes = parseInt(
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            (response as any)?.headers.get('Content-Length'),
        );
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const checksum = (response as any)?.headers.get('ETag')?.replace(/[" ]+/g, '');
        content = (
            <FileContents
                repoId={repoId || ''}
                // ref type is unknown since we lost that context while reaching here (and it's not worth a url param).
                // Effectively it means that if the ref is commit, we won't truncate it in the URI navigator,
                // which is a better behaviour than truncating it when it's a branch/tag.
                reference={{ id: refId, type: RefTypeBranch }}
                path={path}
                fileExtension={fileExtension}
                contentType={contentType}
                sizeBytes={sizeBytes}
                checksum={checksum}
                error={error}
                loading={loading}
                presign={storageConfig.pre_sign_support_ui}
                storageConfig={storageConfig}
                onRefresh={handleRefresh}
            />
        );
    }

    return <RefContextProvider>{content}</RefContextProvider>;
};

export const FileContents: FC<FileContentsProps> = ({
    repoId,
    reference,
    path,
    loading,
    error,
    contentType = undefined,
    fileExtension = '',
    sizeBytes = -1,
    showFullNavigator = true,
    presign = false,
    checksum,
    onRefresh,
    storageConfig,
}) => {
    const pluginManager = usePluginManager();
    const objectUrl = linkToPath(repoId, reference.id, path, presign);

    if (loading || error) {
        return <></>;
    }

    const repo = {
        id: repoId,
    };

    // Build action context for plugin toolbar actions
    const actionContext = {
        repo,
        reference,
        entry: {
            path,
            path_type: 'object' as const,
            checksum,
            size_bytes: sizeBytes,
            content_type: contentType,
        },
        config: {
            pre_sign_support_ui: storageConfig?.pre_sign_support_ui ?? presign,
            blockstore_type: storageConfig?.blockstore_type,
        },
        onRefresh,
    };

    // Get toolbar actions from plugin
    const toolbarActions = pluginManager.objectActions
        .getViewerToolbarActions()
        .filter((action) => action.isAvailable(actionContext));

    const titleComponent = showFullNavigator ? (
        <URINavigator
            path={path}
            repo={repo}
            reference={reference}
            isPathToFile={true}
            downloadUrl={objectUrl}
            hasCopyButton={true}
        />
    ) : (
        <span>{path}</span>
    );

    return (
        <Card className={'file-content-card'}>
            <Card.Header className={'file-content-heading'}>
                <div className="d-flex justify-content-between align-items-center">
                    <div className="flex-grow-1">{titleComponent}</div>
                    {toolbarActions.length > 0 && (
                        <div className="object-viewer-toolbar ms-2">
                            {toolbarActions.map((action) => {
                                const ActionComponent = action.render;
                                return <ActionComponent key={action.id} {...actionContext} />;
                            })}
                        </div>
                    )}
                </div>
            </Card.Header>
            <Card.Body className={'file-content-body'}>
                <Box sx={{ mx: 1 }}>
                    <ObjectRenderer
                        repoId={repoId}
                        refId={reference.id}
                        path={path}
                        fileExtension={fileExtension}
                        contentType={contentType}
                        sizeBytes={sizeBytes}
                        presign={presign}
                    />
                </Box>
            </Card.Body>
        </Card>
    );
};

export default FileObjectsViewerPage;
