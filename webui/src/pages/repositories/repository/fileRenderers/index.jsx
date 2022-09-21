import React from "react";
import remarkGfm from 'remark-gfm'
import remarkHtml from 'remark-html'
import ReactMarkdown from 'react-markdown';
import { IpynbRenderer as NbRenderer } from "react-ipynb-renderer";

export const MarkdownRenderer = ({ content }) => (
    <ReactMarkdown remarkPlugins={[remarkGfm, remarkHtml]} linkTarget={"_blank"}>
        {content}
    </ReactMarkdown>
);

export const IpynbRenderer = ({ content }) => (
    <NbRenderer
        ipynb={JSON.parse(content)}
        syntaxTheme="ghcolors"
        language="python"
        bgTransparent={true}
    />
);

export const supportedFileExtensionRenderers = {
    md: MarkdownRenderer,
    ipynb: IpynbRenderer,
};

export const supportedContentTypeRenderers = {
    "application/x-ipynb+json": IpynbRenderer,
    "text/markdown": MarkdownRenderer,
};