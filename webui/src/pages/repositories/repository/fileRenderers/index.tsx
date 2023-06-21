import React, {FC} from "react";
import SyntaxHighlighter from "react-syntax-highlighter";
import {FileType, RendererComponent} from "./types";
import {DuckDBRenderer} from "./data";
import {
    ImageRenderer,
    IpynbRenderer,
    MarkdownRenderer,
    ObjectTooLarge,
    PDFRenderer,
    TextDownloader,
    TextRenderer,
    UnsupportedFileType
} from "./simple";


const MAX_FILE_SIZE = 20971520; // 20MiB


export const Renderers: {[fileType in FileType] : FC<RendererComponent> } = {
    [FileType.DATA]: props => (
        <DuckDBRenderer {...props}/>
    ),
    [FileType.MARKDOWN]: props => (
        <TextDownloader {...props} onReady={text =>
            <MarkdownRenderer {...props} text={text}/>
        } />
    ),
    [FileType.IPYNB]: props => (
        <TextDownloader {...props} onReady={text =>
            <IpynbRenderer {...props} text={text}/>}
        />
    ),
    [FileType.IMAGE]: props => (
        <ImageRenderer {...props}/>
    ),
    [FileType.PDF]: props => (
        <PDFRenderer {...props}/>
    ),
    [FileType.TEXT]: props => (
        <TextDownloader {...props} onReady={text =>
            <TextRenderer {...props} text={text}/>}
        />
    ),
    [FileType.UNSUPPORTED]: props => (
        <UnsupportedFileType {...props}/>
    ),
    [FileType.TOO_LARGE]: props => (
        <ObjectTooLarge {...props}/>
    ),
}

export const guessLanguage =  (extension: string | null, contentType: string | null) => {
    if (extension && SyntaxHighlighter.supportedLanguages.includes(extension)) {
        return extension;
    }
    if (contentType) {
        if (contentType.startsWith("application/x-")) {
            let lang = contentType.substring(14);
            if (lang.endsWith('-script')) {
                lang = lang.substring(0, lang.length - 7);
            }
            if (SyntaxHighlighter.supportedLanguages.includes(lang)) {
                return lang;
            }
        }
        if (contentType.startsWith("application/")) {
            const lang = contentType.substring(12);
            if (SyntaxHighlighter.supportedLanguages.includes(lang)) {
                return lang;
            }
        }
        if (contentType.startsWith("text/x-")) {
            let lang = contentType.substring(7);
            if (lang.endsWith('-script')) {
                lang = lang.substring(0, lang.length - 7);
            }
            if (SyntaxHighlighter.supportedLanguages.includes(lang)) {
                return lang;
            }
        }
        if (contentType.startsWith("text/")) {
            const lang = contentType.substring(5);
            if (SyntaxHighlighter.supportedLanguages.includes(lang)) {
                return lang;
            }
            return "plaintext"
        }
    }
    return null;
}


export function guessType(contentType: string | null, fileExtension: string | null): FileType {
    switch (contentType) {
        case 'application/x-yaml':
        case 'application/yaml':
        case 'application/x-yml':
        case 'application/yml':
        case 'application/x-json':
        case 'application/json':
        case 'application/x-toml':
        case 'application/toml':
            return FileType.TEXT
        case 'application/x-ipynb+json':
        case 'application/x-ipynb':
        case 'application/ipynb':
            return FileType.IPYNB
        case 'text/markdown':
            return FileType.MARKDOWN
        case 'image/jpeg':
        case 'image/jpg':
        case 'image/png':
        case 'image/gif':
        case 'image/webm':
            return FileType.IMAGE
        case 'application/pdf':
        case 'application/x-pdf':
            return FileType.PDF
    }
    switch (fileExtension) {
        case 'parquet':
        case 'csv':
        case 'tsv':
            return FileType.DATA
        case 'md':
            return FileType.MARKDOWN
        case 'ipynb':
        case 'pynb':
            return FileType.IPYNB
        case 'png':
        case 'jpeg':
        case 'jpg':
        case 'webm':
        case 'gif':
            return FileType.IMAGE
        case 'pdf':
            return FileType.PDF
        case 'txt':
        case 'text':
        case 'yaml':
        case 'yml':
        case 'json':
            return FileType.TEXT
    }
    if (guessLanguage(fileExtension, contentType))
        return FileType.TEXT
    return FileType.UNSUPPORTED
}

export const ObjectRenderer: FC<RendererComponent> = (props: RendererComponent) => {
   const fileType = guessType(props.contentType, props.fileExtension)
    if (fileType !== FileType.DATA && props.sizeBytes > MAX_FILE_SIZE)
        return Renderers[FileType.TOO_LARGE](props)
    return Renderers[fileType](props)
}
