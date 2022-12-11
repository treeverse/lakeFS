import React, { FC, useEffect } from "react";
import remarkGfm from 'remark-gfm'
import remarkHtml from 'remark-html'
import ReactMarkdown from 'react-markdown';
import { IpynbRenderer as NbRenderer } from "react-ipynb-renderer";
import SyntaxHighlighter from "react-syntax-highlighter";
import {githubGist as syntaxHighlightStyle} from "react-syntax-highlighter/dist/esm/styles/hljs";

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

export interface GenericRendererProps {
    content: string,
    language: string,
}

export const GenericRenderer: FC<GenericRendererProps> = ({content, language}) => {
    return (
        <SyntaxHighlighter
            style={syntaxHighlightStyle}
            language={language}
            showInlineLineNumbers={true}
            showLineNumbers={true}>{content}</SyntaxHighlighter>
    );
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

export const guessLanguage =  (extension: string | null, contentType: string | null) => {
    if (extension  && SyntaxHighlighter.supportedLanguages.indexOf(extension) !== -1) {
        return extension;
    }
    if (contentType) {
        if (contentType.indexOf("application/x-") === 0) {
            let lang = contentType.substring(14);
            if (lang.endsWith('-script')) {
                lang = lang.substring(0, lang.length - 7);
            }
            if (SyntaxHighlighter.supportedLanguages.indexOf(lang) !== -1) {
                return lang;
            }
        }
        if (contentType.indexOf("application/") === 0) {
            const lang = contentType.substring(12);
            if (SyntaxHighlighter.supportedLanguages.indexOf(lang) !== -1) {
                return lang;
            }
        }
        if (contentType.indexOf("text/x-") === 0) {
            let lang = contentType.substring(7);
            if (lang.endsWith('-script')) {
                lang = lang.substring(0, lang.length - 7);
            }
            if (SyntaxHighlighter.supportedLanguages.indexOf(lang) !== -1) {
                return lang;
            }
        }
        if (contentType.indexOf("text/") === 0) {
            const lang = contentType.substring(5);
            if (SyntaxHighlighter.supportedLanguages.indexOf(lang) !== -1) {
                return lang;
            }
        }
    }
    return null;
}

