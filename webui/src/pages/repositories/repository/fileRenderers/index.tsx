import React, { FC, useEffect } from "react";
import remarkGfm from 'remark-gfm'
import remarkHtml from 'remark-html'
import ReactMarkdown from 'react-markdown';
import { IpynbRenderer as NbRenderer } from "react-ipynb-renderer";

type RequireAtLeastOne<T, Keys extends keyof T = keyof T> = Pick<T, Exclude<keyof T, Keys>> & {
    [K in Keys]-?: Required<Pick<T, K>> & Partial<Pick<T, Exclude<Keys, K>>>
}[Keys];

interface RendererComponentBase {
    content: string;
    contentBlob: Blob;
}

export type RendererComponent = RequireAtLeastOne<RendererComponentBase, 'content' | 'contentBlob'>;

export const MarkdownRenderer: FC<RendererComponent> = ({ content }) => {
    if (content) {
        return (
            <ReactMarkdown remarkPlugins={[remarkGfm, remarkHtml]} linkTarget={"_blank"}>
                {content}
            </ReactMarkdown>
        );
    }
    return null;
};

export const IpynbRenderer: FC<RendererComponent> = ({ content }) => {
    if (content) {
        return (
            <NbRenderer
                ipynb={JSON.parse(content)}
                syntaxTheme="ghcolors"
                language="python"
                bgTransparent={true}
            />
        );
    }
    return null;
};

export const ImageRenderer: FC<RendererComponent> = ({ contentBlob }) => {
    let blobUrl: string;
    useEffect(() => () => {
        if (blobUrl) {
            URL.revokeObjectURL(blobUrl);
        }
    }, []);

    if (contentBlob) {
        blobUrl = URL.createObjectURL(contentBlob)
        return (
            <img src={blobUrl} />
        );
    }
    return null;
}

export const supportedFileExtensionRenderers: Record<string, FC<RendererComponent>> = {
    md: MarkdownRenderer,
    ipynb: IpynbRenderer,
    png: ImageRenderer,
    jpg: ImageRenderer,
    jpeg: ImageRenderer,
};

export const supportedContentTypeRenderers: Record<string, FC<RendererComponent>> = {
    "application/x-ipynb+json": IpynbRenderer,
    "text/markdown":MarkdownRenderer,
    "image/png": ImageRenderer,
    "image/jpeg": ImageRenderer,
};