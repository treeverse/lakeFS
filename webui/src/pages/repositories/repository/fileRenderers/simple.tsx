import React, { FC, useContext } from 'react';
import Button from 'react-bootstrap/Button';
import { DownloadIcon, FileIcon } from '@primer/octicons-react';
import { humanSize } from '../../../../lib/components/repository/tree';
import { linkToPath } from '../../../../lib/api';
import { useAPI } from '../../../../lib/hooks/api';
import { objects, qs } from '../../../../lib/api';
import { AlertError, Loading } from '../../../../lib/components/controls';
import SyntaxHighlighter from 'react-syntax-highlighter';
import { IpynbRenderer as NbRenderer } from 'react-ipynb-renderer';
import { guessLanguage } from './index';
import { RendererComponent, RendererComponentWithText, RendererComponentWithTextCallback } from './types';

import 'react-ipynb-renderer/dist/styles/default.css';
import { useMarkdownProcessor } from './useMarkdownProcessor';
import { AppContext } from '../../../../lib/hooks/appContext';
import { GeoJSONPreview } from '../../../../lib/components/repository/GeoJSONPreview';
import { github as lightTheme, nightOwl as darkTheme } from 'react-syntax-highlighter/dist/esm/styles/hljs';

export const ObjectTooLarge: FC<RendererComponent> = ({ repoId, refId, path, sizeBytes, presign }) => {
    const downloadUrl = linkToPath(repoId, refId, path, presign || false);
    const fileName = path.split('/').pop() || path;

    return (
        <div className="cannot-render-file">
            <FileIcon size={48} className="cannot-render-icon" />
            <h5>Cannot preview this file</h5>
            <p className="text-muted">File size ({humanSize(sizeBytes)}) exceeds the maximum preview size.</p>
            <Button as="a" href={downloadUrl} download={fileName} variant="outline-primary">
                <DownloadIcon className="me-2" />
                Download File
            </Button>
        </div>
    );
};

export const UnsupportedFileType: FC<RendererComponent> = ({ repoId, refId, path, presign }) => {
    const downloadUrl = linkToPath(repoId, refId, path, presign || false);
    const fileName = path.split('/').pop() || path;

    return (
        <div className="cannot-render-file">
            <FileIcon size={48} className="cannot-render-icon" />
            <h5>Cannot preview this file</h5>
            <p className="text-muted">This file type is not supported for preview.</p>
            <Button as="a" href={downloadUrl} download={fileName} variant="outline-primary">
                <DownloadIcon className="me-2" />
                Download File
            </Button>
        </div>
    );
};

export const TextDownloader: FC<RendererComponentWithTextCallback> = ({ repoId, refId, path, presign, onReady }) => {
    const { response, error, loading } = useAPI(
        async () => await objects.get(repoId, refId, path, presign),
        // TODO: Review and remove this eslint-disable once dependencies are validated
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [repoId, refId, path],
    );
    if (loading) {
        return <Loading />;
    }
    if (error) {
        return <AlertError error={error} />;
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const component = onReady(response as any);
    return <>{component}</>;
};

export const MarkdownRenderer: FC<RendererComponentWithText> = ({ text, repoId, refId, path, presign = false }) => {
    return useMarkdownProcessor(text, repoId, refId, path, presign);
};

export const TextRenderer: FC<RendererComponentWithText> = ({ contentType, fileExtension, text }) => {
    const { state } = useContext(AppContext);
    const language = guessLanguage(fileExtension, contentType) ?? 'plaintext';
    const syntaxHighlightStyle = state.settings.darkMode ? darkTheme : lightTheme;

    return (
        <SyntaxHighlighter
            language={language}
            className="react-syntax-highlighter"
            style={syntaxHighlightStyle}
            customStyle={{
                margin: 0,
                padding: 'var(--spacing-md)',
                borderRadius: 'var(--radius-md)',
            }}
        >
            {text}
        </SyntaxHighlighter>
    );
};

export const IpynbRenderer: FC<RendererComponentWithText> = ({ text }) => {
    return <NbRenderer ipynb={JSON.parse(text)} syntaxTheme="ghcolors" language="python" bgTransparent={true} />;
};

export const ImageRenderer: FC<RendererComponent> = ({ repoId, refId, path, presign }) => {
    const query = qs({ path, presign });
    return (
        <p className="image-container">
            <img
                src={`/api/v1/repositories/${encodeURIComponent(
                    repoId,
                )}/refs/${encodeURIComponent(refId)}/objects?${query}`}
                alt={path}
            />
        </p>
    );
};

export const PDFRenderer: FC<RendererComponent> = ({ repoId, refId, path, presign }) => {
    const [blobUrl, setBlobUrl] = React.useState<string | null>(null);
    const [error, setError] = React.useState<string | null>(null);

    React.useEffect(() => {
        let objectUrl: string | null = null;

        const fetchPdf = async () => {
            try {
                const query = qs({ path, presign });
                const response = await fetch(
                    `/api/v1/repositories/${encodeURIComponent(repoId)}/refs/${encodeURIComponent(refId)}/objects?${query}`,
                );
                if (!response.ok) {
                    throw new Error(`Failed to load PDF: ${response.statusText}`);
                }
                const blob = await response.blob();
                objectUrl = URL.createObjectURL(blob);
                setBlobUrl(objectUrl);
            } catch (err) {
                setError(err instanceof Error ? err.message : 'Failed to load PDF');
            }
        };

        fetchPdf();

        return () => {
            if (objectUrl) {
                URL.revokeObjectURL(objectUrl);
            }
        };
    }, [repoId, refId, path, presign]);

    if (error) {
        return <AlertError error={error} />;
    }

    if (!blobUrl) {
        return <Loading />;
    }

    return (
        <div className="object-viewer-pdf">
            <iframe src={`${blobUrl}#toolbar=1&navpanes=0`} title={path} />
        </div>
    );
};

export const GeoJSONRenderer: FC<RendererComponentWithText> = ({ text }) => {
    return <GeoJSONPreview data={text} />;
};
