import React, {useContext} from "react";
import SyntaxHighlighter from "react-syntax-highlighter";
import {github as syntaxHighlightStyle} from "react-syntax-highlighter/dist/esm/styles/hljs";
import {dark} from 'react-syntax-highlighter/dist/esm/styles/prism';
import {AppContext} from "../../../../lib/hooks/appContext";

export const CustomMarkdownCodeComponent = ({inline, className, children, ...props}) => {
    const {state} = useContext(AppContext);
    const hasLang = /language-(\w+)/.exec(className || "");

    return !inline && hasLang ? (
        <SyntaxHighlighter
            style={state.settings.darkMode ? dark : syntaxHighlightStyle}
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
};
