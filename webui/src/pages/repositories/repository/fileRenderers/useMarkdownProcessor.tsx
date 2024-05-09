import {Fragment, createElement, useEffect, useState} from 'react'
import * as prod from 'react/jsx-runtime'
import remarkParse from 'remark-parse'
import remarkRehype from 'remark-rehype'
import remarkGfm from "remark-gfm";
import remarkHtml from "remark-html";
import rehypeRaw from "rehype-raw";
import rehypeReact from 'rehype-react';
import rehypeWrap from "rehype-wrap";
import {unified} from "unified";
import imageUriReplacer from "../../../../lib/remark-plugins/imageUriReplacer";
import {CustomMarkdownCodeComponent} from "./CustomMarkdownRenderer";

// @ts-expect-error: the react types are missing.
const options: Options = {Fragment: prod.Fragment, jsx: prod.jsx, jsxs: prod.jsxs, passNode: true};
options.components = {
    code: CustomMarkdownCodeComponent,
};

/**
 * @param {string} text
 * @returns {JSX.Element}
 */
export function useMarkdownProcessor(text: string, repoId: string, refId: string, path: string, presign: boolean): JSX.Element {
    const [content, setContent] = useState(createElement(Fragment));

    useEffect(() => {
        (async () => {
            const file = await unified()
                .use(remarkParse)
                .use(imageUriReplacer, {
                    repo: repoId,
                    ref: refId,
                    path,
                    presign,
                  })
                .use(remarkGfm)
                .use(remarkHtml)
                .use(remarkRehype, { allowDangerousHtml: true })
                .use(rehypeRaw)
                .use(rehypeReact, options)
                .use(rehypeWrap, {wrapper: "div.object-viewer-markdown"})
                .process(text);

            setContent(file.result);
        })();
    }, [text]);

    return content;
}
