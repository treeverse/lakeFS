import React, { useEffect } from "react";
import { useParams } from "react-router-dom";
import { Box } from "@mui/material";
import Alert from "react-bootstrap/Alert";
import Card from "react-bootstrap/Card"
import { FaDownload } from "react-icons/fa";

import { useAPI } from "../../../lib/hooks/api";
import { useQuery } from "../../../lib/hooks/router";
import { objects } from "../../../lib/api";
import { supportedContentTypeRenderers, supportedFileExtensionRenderers } from "./fileRenderers";
import { ClipboardButton } from "../../../lib/components/controls";
import noop from "lodash/noop";
import { URINavigator } from "../../../lib/components/repository/tree";
import { RefTypeCommit } from "../../../constants";
import {RepositoryPageLayout} from "../../../lib/components/repository/layout";
import {RefContextProvider} from "../../../lib/hooks/repo";

import "../../../styles/ipynb.css";

export const Loading = () => {
    return (
        <Alert variant={"info"}>Loading...</Alert>
    );
};

// Resolve the correct renderer according to the priority:
// 1. If we have a content-type, use that
// 2. If we have a known file extension, use that
// 3. Fall back on download behavior
export const resolveRenderer = (contentType, extension) => {
    
    if (contentType && contentType in supportedContentTypeRenderers) {
        return supportedContentTypeRenderers[contentType];
    }

    if (extension && extension in supportedFileExtensionRenderers) {
        return supportedFileExtensionRenderers[extension];
    }

    return null;
}

const FileTypeNotSupported = () => (
    <Alert variant="info">
        {`This file type isn't supported for viewing. You can still download the file or copy its raw contents to the clipboard.`}
    </Alert>
);

export const getFileExtension = (objectName) => {
    const objectNameParts = objectName.split(".");
    return objectNameParts[objectNameParts.length - 1];
}

export const getContentType = (headers) => {
    if (!headers) return null;

    return headers.get("Content-Type") ?? null;
}

const FileObjectsViewerPage = () => {
    const { objectName, repoId } = useParams();
    const decodedObjectName = decodeURIComponent(objectName);
    const queryString = useQuery();
    const refId = queryString["ref"];
    const {response, error, loading} = useAPI(async () => {
        return await objects.getWithHeaders(repoId, refId, decodedObjectName);
    }, [decodedObjectName]);

    const fileExtension = getFileExtension(decodedObjectName);
    const contentType = getContentType(response?.headers);

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
                    rawContent={response?.responseText} 
                    error={error}
                    loading={loading}
                />
            </RepositoryPageLayout>
        </RefContextProvider>
    );
};

export const FileContents = ({repoId, refId, path, loading, error, rawContent, contentType = null, fileExtension='', showFullNavigator = true}) => {

    const blob = new Blob([rawContent], { type: contentType ?? "application/octet-stream" });
    const objectUrl = URL.createObjectURL(blob);
    useEffect(() => {
        return () => {
            URL.revokeObjectURL(objectUrl);
        }
    }, []);

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
        content = React.createElement(renderer, { content: rawContent }, []);
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
                    <span>
                        <a 
                            href={objectUrl}
                            download={path}
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

export default FileObjectsViewerPage;
