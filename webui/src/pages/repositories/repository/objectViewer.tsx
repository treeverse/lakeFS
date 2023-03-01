import React, {  FC } from "react";
import { useParams } from "react-router-dom";
import { Box } from "@mui/material";
import Alert from "react-bootstrap/Alert";
import Card from "react-bootstrap/Card"
import { FaDownload } from "react-icons/fa";

import { useAPI } from "../../../lib/hooks/api";
import { useQuery } from "../../../lib/hooks/router";
import { objects } from "../../../lib/api";
import { ObjectRenderer} from "./fileRenderers";
import {ClipboardButton, Error} from "../../../lib/components/controls";
import noop from "lodash/noop";
import { URINavigator } from "../../../lib/components/repository/tree";
import { RefTypeCommit } from "../../../constants";
import {RepositoryPageLayout} from "../../../lib/components/repository/layout";
import {RefContextProvider} from "../../../lib/hooks/repo";
import {linkToPath} from "../../../lib/api";

import "../../../styles/ipynb.css";


type ObjectViewerPathParams = {
    objectName: string;
    repoId: string;
}

interface ObjectViewerQueryString {
    ref: string;
    path: string;
}

interface FileContentsProps {
    repoId: string;
    refId: string;
    path: string;
    loading: boolean;
    error: Error | null;
    contentType?: string | null;
    fileExtension: string;
    sizeBytes: number;
    showFullNavigator?: boolean;
    presign?: boolean;
}


export const Loading: FC = () => {
    return (
        <Alert variant={"info"}>Loading...</Alert>
    );
};

export const getFileExtension = (objectName: string): string => {
    const objectNameParts = objectName.split(".");
    return objectNameParts[objectNameParts.length - 1];
}

export const getContentType = (headers: Headers): string | null => {
    if (!headers) return null;

    return headers.get("Content-Type") ?? null;
}

const FileObjectsViewerPage = () => {
    const { repoId } = useParams<ObjectViewerPathParams>();
    const queryString = useQuery<ObjectViewerQueryString>();
    const refId = queryString["ref"] ?? "";
    const path = queryString["path"] ?? "";
    const {response, error, loading} = useAPI( () => {
            return objects.head(repoId, refId, path)
        },
        [repoId, refId, path]);

    let content;
    if (loading) {
        content  = <Loading/>;
    } else if (error) {
        content = <Error error={error}/>
    } else {
        const fileExtension = getFileExtension(path);
        // We'll need to convert the API service to get rid of this any
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const contentType = getContentType((response as any)?.headers);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const sizeBytes = parseInt((response as any)?.headers.get('Content-Length'))
        content = <FileContents
            repoId={repoId || ''}
            refId={refId}
            path={path}
            fileExtension={fileExtension}
            contentType={contentType}
            sizeBytes={sizeBytes}
            error={error}
            loading={loading}
        />
    }

    return (
        <RefContextProvider>
            <RepositoryPageLayout activePage={'objects'}>
                {content}
            </RepositoryPageLayout>
        </RefContextProvider>
    );
};

export const FileContents: FC<FileContentsProps> = ({repoId, refId, path, loading, error, contentType = null, fileExtension='', sizeBytes = -1, showFullNavigator = true, presign= false}) => {

    const objectUrl = linkToPath(repoId, refId, path, presign);

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

    return (
            <Card className={'file-content-card'}>
                <Card.Header className={'file-content-heading d-flex justify-content-between align-items-center'}>
                    {titleComponent}
                    <span className="object-viewer-buttons">
                        <a 
                            href={objectUrl}
                            download={path.split('/').pop()}
                            className="btn btn-primary btn-sm download-button me-1">
                                <FaDownload />
                        </a>
                        <ClipboardButton
                            text={`lakefs://${repoId}/${refId}/${path}`}
                            variant="outline-primary"
                            size="sm"
                            onSuccess={noop}
                            onError={noop}
                            className={"me-1"}
                            tooltip={"copy URI to clipboard"} />
                    </span>
                </Card.Header>
                <Card.Body className={'file-content-body'}>
                    <Box sx={{mx: 1}}>
                        <ObjectRenderer
                            repoId={repoId}
                            refId={refId}
                            path={path}
                            fileExtension={fileExtension}
                            contentType={contentType}
                            sizeBytes={sizeBytes}
                            presign={presign}/>
                    </Box>
                </Card.Body>
            </Card>
    );
};

export default FileObjectsViewerPage;
