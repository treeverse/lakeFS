import React from "react";
import SyntaxHighlighter from "react-syntax-highlighter";
import { github as syntaxHighlightStyle } from "react-syntax-highlighter/dist/esm/styles/hljs";
import { Components } from "react-markdown";

export const CustomMarkdownRenderer: Components = {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  code: ({ node, inline, className, style, children, ...props }) => {
    const hasLang = /language-(\w+)/.exec(className || "");

    return !inline && hasLang ? (
      <SyntaxHighlighter
        style={syntaxHighlightStyle}
        language={hasLang[1]}
        PreTag="div"
        className="codeStyle"
        showLineNumbers={false}
        useInlineStyles={true}
      >
        {String(children).replace(/\n$/, "")}
      </SyntaxHighlighter>
    ) : (
      <code className={className} {...props}>
        {children}
      </code>
    );
  },
};
