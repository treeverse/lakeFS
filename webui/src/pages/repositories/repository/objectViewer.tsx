import React, { FC } from "react";
import { useParams } from "react-router-dom";
import { Box } from "@mui/material";
import Alert from "react-bootstrap/Alert";
import Card from "react-bootstrap/Card";

import { useAPI } from "../../../lib/hooks/api";
import { useQuery } from "../../../lib/hooks/router";
import { objects } from "../../../lib/api";
import { ObjectRenderer } from "./fileRenderers";
import { AlertError } from "../../../lib/components/controls";
import { URINavigator } from "../../../lib/components/repository/tree";
import { RefTypeBranch } from "../../../constants";
import { RefContextProvider, useRefs } from "../../../lib/hooks/repo";
import { useConfigContext } from "../../../lib/hooks/configProvider";
import { linkToPath } from "../../../lib/api";
import { getRepoStorageConfig } from "./utils";

import "../../../styles/quickstart.css";

type ObjectViewerPathParams = {
  objectName: string;
  repoId: string;
};

interface ObjectViewerQueryString {
  ref: string;
  path: string;
}

interface FileContentsProps {
  repoId: string;
  reference:  { id: string; type: string };
  path: string;
  loading: boolean;
  error: Error | null;
  contentType?: string;
  fileExtension: string;
  sizeBytes: number;
  showFullNavigator?: boolean;
  presign?: boolean;
}

export const Loading: FC = () => {
  return <Alert variant={"info"}>Loading...</Alert>;
};

export const getFileExtension = (objectName: string): string => {
  const objectNameParts = objectName.split(".");
  return objectNameParts[objectNameParts.length - 1];
};

export const getContentType = (headers: Headers): string | undefined => {
  if (!headers) return undefined;

  return headers.get("Content-Type") ?? undefined;
};

const FileObjectsViewerPage = () => {
  const {repo, loading: repoLoading, error: repoError} = useRefs();
  const {config, error: configsError, loading: configLoading} = useConfigContext();
  const {storageConfig, error: storageConfigError} = getRepoStorageConfig(config?.storages, repo);

  const { repoId } = useParams<ObjectViewerPathParams>();
  const queryString = useQuery<ObjectViewerQueryString>();
  const refId = queryString["ref"] ?? "";
  const path = queryString["path"] ?? "";
  const { response, error: apiError, loading: apiLoading } = useAPI(() => {
    return objects.head(repoId, refId, path);
  }, [repoId, refId, path]);
  const loading = apiLoading || repoLoading || configLoading;
  const error = loading ? null : apiError || repoError || configsError || storageConfigError;

  let content;
  if (loading) {
    content = <Loading />;
  } else if (error) {
    content = <AlertError error={error} />;
  } else {
    const fileExtension = getFileExtension(path);
    // We'll need to convert the API service to get rid of this any
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const contentType = getContentType((response as any)?.headers);
    const sizeBytes = parseInt(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (response as any)?.headers.get("Content-Length")
    );
    content = (
      <FileContents
        repoId={repoId || ""}
        // ref type is unknown since we lost that context while reaching here (and it's not worth a url param).
        // Effectively it means that if the ref is commit, we won't truncate it in the URI navigator,
        // which is a better behaviour than truncating it when it's a branch/tag.
        reference={{ id: refId, type: RefTypeBranch}}
        path={path}
        fileExtension={fileExtension}
        contentType={contentType}
        sizeBytes={sizeBytes}
        error={error}
        loading={loading}
        presign={storageConfig.pre_sign_support_ui}
      />
    );
  }

  return (
    <RefContextProvider>
        {content}
    </RefContextProvider>
  );
};

export const FileContents: FC<FileContentsProps> = ({
  repoId,
  reference,
  path,
  loading,
  error,
  contentType = undefined,
  fileExtension = "",
  sizeBytes = -1,
  showFullNavigator = true,
  presign = false,
}) => {
  const objectUrl = linkToPath(repoId, reference.id, path, presign);

  if (loading || error) {
    return <></>;
  }

  const repo = {
    id: repoId,
  };

  const titleComponent = showFullNavigator ? (
    <URINavigator
      path={path}
      repo={repo}
      reference={reference}
      isPathToFile={true}
      downloadUrl={objectUrl}
      hasCopyButton={true}
    />
  ) : (
    <span>{path}</span>
  );

  return (
    <Card className={"file-content-card"}>
      <Card.Header className={"file-content-heading"}>
        {titleComponent}
      </Card.Header>
      <Card.Body className={"file-content-body"}>
        <Box sx={{ mx: 1 }}>
          <ObjectRenderer
            repoId={repoId}
            refId={reference.id}
            path={path}
            fileExtension={fileExtension}
            contentType={contentType}
            sizeBytes={sizeBytes}
            presign={presign}
          />
        </Box>
      </Card.Body>
    </Card>
  );
};

export default FileObjectsViewerPage;
