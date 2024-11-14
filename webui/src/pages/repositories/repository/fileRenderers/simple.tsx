import React, { FC, useContext } from "react";
import Alert from "react-bootstrap/Alert";
import { humanSize } from "../../../../lib/components/repository/tree";
import { useAPI } from "../../../../lib/hooks/api";
import { objects, qs } from "../../../../lib/api";
import { AlertError, Loading } from "../../../../lib/components/controls";
import SyntaxHighlighter from "react-syntax-highlighter";
import { githubGist as syntaxHighlightStyle } from "react-syntax-highlighter/dist/esm/styles/hljs";
import { IpynbRenderer as NbRenderer } from "react-ipynb-renderer";
import { guessLanguage } from "./index";
import {
  RendererComponent,
  RendererComponentWithText,
  RendererComponentWithTextCallback,
} from "./types";

import "react-ipynb-renderer/dist/styles/default.css";
import { useMarkdownProcessor } from "./useMarkdownProcessor";
import { AppContext } from "../../../../lib/hooks/appContext";
import { dark } from "react-syntax-highlighter/dist/esm/styles/prism";

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
    return <AlertError error={error} />;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const component = onReady(response as any);
  return <>{component}</>;
};

export const MarkdownRenderer: FC<RendererComponentWithText> = ({
  text,
  repoId,
  refId,
  path,
  presign = false,
}) => {
  return useMarkdownProcessor(text, repoId, refId, path, presign);
};

export const TextRenderer: FC<RendererComponentWithText> = ({
  contentType,
  fileExtension,
  text,
}) => {
  const {state} = useContext(AppContext);
  const language = guessLanguage(fileExtension, contentType) ?? "plaintext";

  return (
    <SyntaxHighlighter
      style={state.settings.darkMode ? dark : syntaxHighlightStyle}
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
