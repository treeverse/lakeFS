import React, {FC, useEffect, useState} from "react";
import remarkGfm from 'remark-gfm'
import remarkHtml from 'remark-html'
import ReactMarkdown from 'react-markdown';
import { IpynbRenderer as NbRenderer } from "react-ipynb-renderer";
import SyntaxHighlighter from "react-syntax-highlighter";
import {githubGist as syntaxHighlightStyle} from "react-syntax-highlighter/dist/esm/styles/hljs";

import {withConnection} from './duckdb';
import {Table} from "react-bootstrap";
import moment from "moment";
import {useAPI} from "../../../../lib/hooks/api";
import {objects} from "../../../../lib/api";
import Alert from "react-bootstrap/Alert";
import {Error, Loading} from "../../../../lib/components/controls";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import {DatabaseIcon} from "@primer/octicons-react";
import {humanSize} from "../../../../lib/components/repository/tree";


const MAX_FILE_SIZE = 20971520; // 20MiB

interface RendererComponent {
    repoId: string;
    refId: string;
    path: string;
    fileExtension: string | null;
    contentType: string | null;
    sizeBytes: number;
}

interface RendererComponentWithText extends RendererComponent{
    text: string;
}


interface RendererComponentWithDataFormat extends RendererComponent{
    format: string;
}

interface RendererComponentWithTextCallback extends RendererComponent {
    onReady: (text: string) => JSX.Element;
}

export const DATA_FORMATS = ["parquet", "csv", "tsv"]

export const ObjectRenderer: FC<RendererComponent> = (props: RendererComponent) => {
    const {fileExtension, contentType, sizeBytes} = props;
    // is this a data file?
    const dataFormat = (fileExtension) ? DATA_FORMATS.indexOf(fileExtension) : -1
    if (dataFormat !== -1) {
        return <DuckDBRenderer {...props} format={DATA_FORMATS[dataFormat]}/>
    }

    // other types download the whole thing, which we do not want
    if (sizeBytes > MAX_FILE_SIZE) {
        return <ObjectTooLarge {...props}/>
    }
    // is this markdown?
    if (contentType === "text/markdown" || fileExtension === 'md') {
        return (
            <TextDownloader {...props} onReady={text =>
                <MarkdownRenderer {...props} text={text}/>
            } />
        );
    }
    // is this a notebook?
    if (contentType === 'application/x-ipynb+json' || fileExtension === 'ipynb') {
        return (
            <TextDownloader {...props} onReady={text =>
                <IpynbRenderer {...props} text={text}/>}
            />
        );
    }

    // is this an image?
    if ((contentType && ['image/jpeg', 'image/png'].indexOf(contentType) !== -1) || (fileExtension && ['png', 'jpg', 'jpeg'].indexOf(fileExtension) !== -1)) {
        return <ImageRenderer {...props}/>
    }

    // is this a PDF?
    if ((contentType && ['application/pdf', 'application/x-pdf'].indexOf(contentType) !== -1) || (fileExtension && fileExtension === 'pdf')) {
        return <PDFRenderer {...props}/>
    }

    // assume it's textual and try to render?
    const lang = guessLanguage(fileExtension, contentType);
    if (lang)
        return (
        <TextDownloader {...props} onReady={text =>
            <TextRenderer {...props} text={text}/>}
        />
    );

    return <UnsupportedFileType {...props}/>
}

export const ObjectTooLarge: FC<RendererComponent> = ({path, sizeBytes}) => {
    return (
        <Alert variant="warning" className="m-5">
            <div>Could not render: <code>{path}</code>:</div>
            <div>{`size ${sizeBytes}b (${humanSize(sizeBytes)}) is too big`}</div>
        </Alert>
    );
}

export const UnsupportedFileType: FC<RendererComponent> = ({path, fileExtension, contentType}) => {
    return (
        <Alert variant="warning" className="m-5">
            <div>Could not render: <code>{path}</code>: <br/></div>
            <div>{`lakeFS doesn't know how to render this file (extension = "${fileExtension}", content-type = "${contentType}")`}</div>
        </Alert>
    );
}

export const DataLoader: FC<RendererComponent> = () => {
    return <Loading/>
}

export const TextDownloader: FC<RendererComponentWithTextCallback> = (props) => {
    const { repoId, refId, path, onReady } = props;
    const {response, error, loading} = useAPI(
        async () => await objects.get(repoId, refId, path),
        [repoId, refId, path]
    );
    if (loading) {
        return <DataLoader {...props}/>
    }
    if (error) {
        return <Error error={error}/>
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const component = onReady((response as any));
    return (
        <>
            {component}
        </>
    )
}

export const MarkdownRenderer: FC<RendererComponentWithText> = ({text}) => {
    return (
            <ReactMarkdown remarkPlugins={[remarkGfm, remarkHtml]} linkTarget={"_blank"}>
                {text}
            </ReactMarkdown>
    );

};

export const TextRenderer: FC<RendererComponentWithText> = ({ contentType, fileExtension, text }) => {
    const language = guessLanguage(fileExtension, contentType) ?? "plaintext";
    return (
        <SyntaxHighlighter
            style={syntaxHighlightStyle}
            language={language}
            showInlineLineNumbers={true}
            showLineNumbers={true}>{text}</SyntaxHighlighter>
    );
};


export const DuckDBRenderer: FC<RendererComponentWithDataFormat> = (props) => {
    const {repoId, refId, path, format } = props;
    let initialQuery = `SELECT * 
FROM read_parquet(lakefs_object('${repoId}', '${refId}', '${path}')) 
LIMIT 20`;
   if (format === 'csv') {
        initialQuery = `SELECT * 
FROM read_csv(lakefs_object('${repoId}', '${refId}', '${path}'), AUTO_DETECT = TRUE) 
LIMIT 20`
    } else if (format === 'tsv') {
        initialQuery = `SELECT * 
FROM read_csv(lakefs_object('${repoId}', '${refId}', '${path}'), DELIM='\t', AUTO_DETECT=TRUE) 
LIMIT 20`
    }
    const [query, setQuery] = useState<string>(initialQuery)
    const [shouldSubmit, setShouldSubmit] = useState<boolean>(true)
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const [data, setData] = useState<any[] | null>(null);
    const [error, setError] = useState<string | null>(null)

    useEffect(() => {
        withConnection(async conn => {
            const results = await conn.query(query)
            const data = results.toArray()
            setData(data)
            setError(null)
        }).catch(e => {
            setError(e.toString())
        })
    }, [repoId, refId, path, format, shouldSubmit])

    let content;
    if (error) {
        content = <Error error={error}/>
    } else if (data === null) {
        content = <DataLoader {...props}/>
    } else {
        if (!data || data.length === 0) {
            content = (
                <p className={"text-md-center mt-5 mb-5"}>
                    No rows returned.
                </p>
            )
        } else {
            const totalRows = data.length
            let res = data;
            if (totalRows > 100) {
                res = data.slice(0, 100)
            }
            content = (
                <>
                    <Table striped bordered hover size={"sm"} responsive={"sm"}>
                        <thead>
                        <tr>
                            {Object.getOwnPropertyNames(res[0]).map(name =>
                                <th key={name}>{name}</th>
                            )}
                        </tr>
                        </thead>
                        <tbody>
                        {res.map((row, i) => (
                            <tr key={`row-${i}`}>
                                {Object.getOwnPropertyNames(res[0]).map(name => (
                                    <DataRow key={`row-${i}-${name}`} value={row[name]}/>
                                ))}
                            </tr>
                        ))}
                        </tbody>
                    </Table>
                    {(res.length < data.length) &&
                        <Alert>{`Showing only the first ${res.length} rows (out of ${data.length})`}</Alert>
                    }
                </>
            )
        }
    }


    return (
        <div>
            <Form onSubmit={(e) => {
                e.preventDefault()
                setShouldSubmit(!shouldSubmit)
            }}>
                <Form.Group className="mb-2 mt-2" controlId="objectQuery">
                    <Form.Control as="textarea" className="object-viewer-sql-input" rows={5} value={query} spellCheck={false} onChange={e => {
                        setQuery(e.target.value)
                    }} />
                    <Form.Text className="text-muted align-right">
                        Powered by <a href="https://duckdb.org/2021/10/29/duckdb-wasm.html" target="_blank" rel="noreferrer">DuckDB-WASM</a>.
                        For a full SQL reference, see the <a href="https://duckdb.org/docs/sql/statements/select" target="_blank" rel="noreferrer">DuckDB Documentation</a>
                    </Form.Text>
                </Form.Group>
                <Button type="submit" variant={"success"} >
                    <DatabaseIcon/>{' '}
                    Execute
                </Button>
            </Form>
            <div className={"mt-3 object-viewer-sql-results"}>
                {content}
            </div>
        </div>
    )
}

interface RowProps {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    value: any;
}

const DataRow: FC<RowProps> = ({ value }) => {
     let dataType = 'regular';
     if (typeof value === 'string') {
         dataType = 'string';
     } else if (value instanceof Date) {
         dataType = 'date'
     } else if (typeof value === 'number') {
         dataType = 'number'
     }

     if (dataType === 'string') {
         return <td>{value}</td>
     }

     if (dataType === 'date') {
         return <td>{moment(value).format(moment.defaultFormat)}</td>
     }

    if (dataType === 'number') {
        return <td>{value.toLocaleString("en-US")}</td>
    }

    return <td>{""  + value}</td>;
}

export const IpynbRenderer: FC<RendererComponentWithText> = ({ text }) => {
    return (
        <NbRenderer
            ipynb={JSON.parse(text)}
            syntaxTheme="ghcolors"
            language="python"
            bgTransparent={true}
        />
    );
};

const qs = (queryParts: {[key: string]: string}) => {
    const parts = Object.keys(queryParts).map(key => [key, queryParts[key]]);
    return new URLSearchParams(parts).toString();
};

export const ImageRenderer: FC<RendererComponent> = ({ repoId, refId,  path }) => {
    const query = qs({path});
    return (
        <p className="image-container">
            <img src={`/api/v1/repositories/${encodeURIComponent(repoId)}/refs/${encodeURIComponent(refId)}/objects?${query}`} alt={path}/>
        </p>
    );
}

export const PDFRenderer: FC<RendererComponent> = ({ repoId, refId, path }) => {
    const query = qs({path});
    return (
        <div className="m-3 object-viewer-pdf">
            <object
                data={`/api/v1/repositories/${encodeURIComponent(repoId)}/refs/${encodeURIComponent(refId)}/objects?${query}`}
                type="application/pdf">
            </object>
        </div>
    )
}



export const guessLanguage =  (extension: string | null, contentType: string | null) => {
    if (extension && SyntaxHighlighter.supportedLanguages.indexOf(extension) !== -1) {
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
            return "plaintext"
        }
    }
    return null;
}

