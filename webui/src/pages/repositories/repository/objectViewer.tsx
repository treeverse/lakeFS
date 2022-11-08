import React, { ComponentType, FC } from "react";
import { useParams } from "react-router-dom";
import { Box } from "@mui/material";
import Alert from "react-bootstrap/Alert";
import Card from "react-bootstrap/Card"
import { FaDownload } from "react-icons/fa";

import { useAPI } from "../../../lib/hooks/api";
import { useQuery } from "../../../lib/hooks/router";
import { objects } from "../../../lib/api";
import { RendererComponent, supportedContentTypeRenderers, supportedFileExtensionRenderers } from "./fileRenderers";
import { ClipboardButton } from "../../../lib/components/controls";
import noop from "lodash/noop";
import { URINavigator } from "../../../lib/components/repository/tree";
import { RefTypeCommit } from "../../../constants";
import {RepositoryPageLayout} from "../../../lib/components/repository/layout";
import {RefContextProvider} from "../../../lib/hooks/repo";
import {linkToPath} from "../../../lib/api";

import "../../../styles/ipynb.css";

interface ObjectViewerPathParams {
    objectName: string;
    repoId: string;
}

interface ObjectViewerQueryString {
    ref: string;
    big: string;
}

interface FileContentsProps {
    repoId: string;
    refId: string;
    path: string;
    loading: boolean;
    error: Error | null;
    rawContent: string;
    blobContent: Blob; 
    contentType?: string | null;
    fileExtension: string;
    showFullNavigator?: boolean;
}

interface LargeFileContentsProps {
    repoId: string;
    refId: string;
    path: string;
    loading: boolean;
    error: Error | null;
    showFullNavigator?: boolean;
}

export const Loading: FC = () => {
    return (
        <Alert variant={"info"}>Loading...</Alert>
    );
};

// Resolve the correct renderer according to the priority:
// 1. If we have a content-type, use that
// 2. If we have a known file extension, use that
// 3. Fall back on download behavior
export const resolveRenderer = (contentType: string | null, extension: string | null): ComponentType<RendererComponent> | null => {
    
    if (contentType && contentType in supportedContentTypeRenderers) {
        return supportedContentTypeRenderers[contentType];
    }

    if (extension && extension in supportedFileExtensionRenderers) {
        return supportedFileExtensionRenderers[extension];
    }

    return null;
}

const FileTypeNotSupported: FC = () => (
    <Alert variant="info">
        {`This file type isn't supported for viewing. You can still download the file or copy its raw contents to the clipboard.`}
    </Alert>
);

const FileSizeTooLarge: FC = () => (
    <Alert variant="info">
        {`File size too large to preview. You can still download the file.`}
    </Alert>
);

export const getFileExtension = (objectName: string): string => {
    const objectNameParts = objectName.split(".");
    return objectNameParts[objectNameParts.length - 1];
}

export const getContentType = (headers: Headers): string | null => {
    if (!headers) return null;

    return headers.get("Content-Type") ?? null;
}

const FileObjectsViewerPage = () => {
    const { objectName, repoId } = useParams<ObjectViewerPathParams>();
    const decodedObjectName = decodeURIComponent(objectName);
    const queryString = (useQuery() as ObjectViewerQueryString);
    const refId = queryString["ref"];
    const isBigFile = queryString["big"] === "true";
    const {response, error, loading} = useAPI(async () => {
        if (!isBigFile) {
            return await objects.getWithHeaders(repoId, refId, decodedObjectName);
        }

        return null;
    }, [decodedObjectName]);

    const fileExtension = getFileExtension(decodedObjectName);
    // We'll need to convert the API service to get rid of this any
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const contentType = getContentType((response as any)?.headers);

    if (isBigFile) {
        return (
            <RefContextProvider>
            <RepositoryPageLayout activePage={'objects'}>
                    {loading && <Loading/>}
                    <LargeFileContents
                        repoId={repoId}
                        refId={refId}
                        path={decodedObjectName}
                        loading={loading}
                        error={error}
                    />
                </RepositoryPageLayout>
            </RefContextProvider>
        );
    }

    // We'll need to convert the API service to get rid of this any
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const responseText = (response as any)?.responseText;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const responseBlob = (response as any)?.responseBlob;
    return (
        <RefContextProvider>
            <RepositoryPageLayout activePage={'objects'}>
                {loading && <Loading/>}
                <FileContents 
                    repoId={repoId} 
                    refId={refId}
                    path={decodedObjectName}
                    fileExtension={fileExtension}
                    contentType={contentType}
                    rawContent={responseText}
                    blobContent={responseBlob}
                    error={error}
                    loading={loading}
                />
            </RepositoryPageLayout>
        </RefContextProvider>
    );
};

export const FileContents: FC<FileContentsProps> = ({repoId, refId, path, loading, error, rawContent, blobContent, contentType = null, fileExtension='', showFullNavigator = true}) => {

    const objectUrl = linkToPath(repoId, refId, path);

    if (loading || error) {
        return <></>;
    }

    let content;
    const renderer = resolveRenderer(contentType, fileExtension);
    if (!renderer) {
        // File type does not have a renderer
        // We cannot display it inline
        content = <FileTypeNotSupported />
    } else {
        content = React.createElement(renderer, { content: rawContent, contentBlob: blobContent, }, []);
    }

    const repo = {
        id: repoId,
    };

    const reference = {
        id: refId,
        type: RefTypeCommit,
    }

    const titleComponent = showFullNavigator ?
        (<URINavigator path={path} repo={repo} reference={reference} isPathToFile={true} />) :
        (<span>{path}</span>);

    return (
            <Card className={'readme-card'}>
                <Card.Header className={'readme-heading d-flex justify-content-between align-items-center'}>
                    {titleComponent}
                    <span className="object-viewer-buttons">
                        <a 
                            href={objectUrl}
                            download={true}
                            className="btn btn-primary btn-sm download-button">
                                <FaDownload />
                        </a>
                        <ClipboardButton text={rawContent} variant="outline-primary" size="sm" onSuccess={noop} onError={noop} />
                    </span>
                </Card.Header>
                <Card.Body>
                    <Box sx={{mx: 1}}>
                        {content}
                    </Box>
                </Card.Body>
            </Card>
    );
};

const LargeFileContents: FC<LargeFileContentsProps> = ({repoId, refId, path, loading, error, showFullNavigator = true}) => {
    if (loading || error) {
        return <></>;
    }
    
    const repo = {
        id: repoId,
    };

    const reference = {
        id: refId,
        type: RefTypeCommit,
    }
    
    const titleComponent = showFullNavigator ?
        (<URINavigator path={path} repo={repo} reference={reference} isPathToFile={true} />) :
        (<span>{path}</span>);

    const downloadPath = linkToPath(repoId, refId, path);

        return (
            <Card className={'readme-card'}>
                <Card.Header className={'readme-heading d-flex justify-content-between align-items-center'}>
                    {titleComponent}
                    <span className="object-viewer-buttons">
                        <a 
                            href={downloadPath}
                            download="true"
                            target="_blank"
                            rel="noreferrer"
                            className="btn btn-primary btn-sm download-button">
                                <FaDownload />
                        </a>
                    </span>
                </Card.Header>
                <Card.Body>
                    <Box sx={{mx: 1}}>
                        <FileSizeTooLarge />
                    </Box>
                </Card.Body>
            </Card>
    );
}

export default FileObjectsViewerPage;
