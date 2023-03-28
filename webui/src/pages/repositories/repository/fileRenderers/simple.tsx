import React, { FC } from "react";
import Alert from "react-bootstrap/Alert";
import { humanSize } from "../../../../lib/components/repository/tree";
import { useAPI } from "../../../../lib/hooks/api";
import { objects, qs } from "../../../../lib/api";
import { Error, Loading } from "../../../../lib/components/controls";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import remarkHtml from "remark-html";
import SyntaxHighlighter from "react-syntax-highlighter";
import { githubGist as syntaxHighlightStyle } from "react-syntax-highlighter/dist/esm/styles/hljs";
import { IpynbRenderer as NbRenderer } from "react-ipynb-renderer";
import { guessLanguage } from "./index";
import { CustomMarkdownRenderer } from "./CustomMarkdownRenderer";
import {
  RendererComponent,
  RendererComponentWithText,
  RendererComponentWithTextCallback,
} from "./types";
import imageUriReplacer from "../../../../lib/remark-plugins/imageUriReplacer";

export const ObjectTooLarge: FC<RendererComponent> = ({ path, sizeBytes }) => {
  return (
    <Alert variant="warning" className="m-5">
      <div>
        Could not render: <code>{path}</code>:
      </div>
      <div>{`size ${sizeBytes}b (${humanSize(sizeBytes)}) is too big`}</div>
    </Alert>
  );
};

export const UnsupportedFileType: FC<RendererComponent> = ({
  path,
  fileExtension,
  contentType,
}) => {
  return (
    <Alert variant="warning" className="m-5">
      <div>
        Could not render: <code>{path}</code>: <br />
      </div>
      <div>{`lakeFS doesn't know how to render this file (extension = "${fileExtension}", content-type = "${contentType}")`}</div>
    </Alert>
  );
};

export const TextDownloader: FC<RendererComponentWithTextCallback> = ({
  repoId,
  refId,
  path,
  presign,
  onReady,
}) => {
  const { response, error, loading } = useAPI(
    async () => await objects.get(repoId, refId, path, presign),
    [repoId, refId, path]
  );
  if (loading) {
    return <Loading />;
  }
  if (error) {
    return <Error error={error} />;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const component = onReady(response as any);
  return <>{component}</>;
};

export const MarkdownRenderer: FC<RendererComponentWithText> = ({
  text,
  repoId,
  refId,
}) => {
  return (
    <ReactMarkdown
      components={CustomMarkdownRenderer}
      remarkPlugins={[
        [
          imageUriReplacer,
          {
            repo: repoId,
            ref: refId,
          },
        ],
        remarkGfm,
        remarkHtml,
      ]}
      linkTarget={"_blank"}
    >
      {text}
    </ReactMarkdown>
  );
};

export const TextRenderer: FC<RendererComponentWithText> = ({
  contentType,
  fileExtension,
  text,
}) => {
  const language = guessLanguage(fileExtension, contentType) ?? "plaintext";
  return (
    <SyntaxHighlighter
      style={syntaxHighlightStyle}
      language={language}
      showInlineLineNumbers={true}
      showLineNumbers={true}
    >
      {text}
    </SyntaxHighlighter>
  );
};

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

export const ImageRenderer: FC<RendererComponent> = ({
  repoId,
  refId,
  path,
  presign,
}) => {
  const query = qs({ path, presign });
  return (
    <p className="image-container">
      <img
        src={`/api/v1/repositories/${encodeURIComponent(
          repoId
        )}/refs/${encodeURIComponent(refId)}/objects?${query}`}
        alt={path}
      />
    </p>
  );
};

export const PDFRenderer: FC<RendererComponent> = ({
  repoId,
  refId,
  path,
  presign,
}) => {
  const query = qs({ path, presign });
  return (
    <div className="m-3 object-viewer-pdf">
      <object
        data={`/api/v1/repositories/${encodeURIComponent(
          repoId
        )}/refs/${encodeURIComponent(refId)}/objects?${query}`}
        type="application/pdf"
      ></object>
    </div>
  );
};
