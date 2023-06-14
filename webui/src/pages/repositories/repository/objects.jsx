import React, { useCallback, useEffect, useRef, useState } from "react";
import dayjs from "dayjs";
import {CheckboxIcon, UploadIcon, XIcon} from "@primer/octicons-react";
import { RepositoryPageLayout } from "../../../lib/components/repository/layout";
import RefDropdown from "../../../lib/components/repository/refDropdown";
import {
    ActionGroup,
    ActionsBar,
    AlertError,
    Loading,
    PrefixSearchWidget,
    RefreshButton,
    Warnings
} from "../../../lib/components/controls";
import Button from "react-bootstrap/Button";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import Alert from "react-bootstrap/Alert";
import { BsCloudArrowUp } from "react-icons/bs";

import {humanSize, Tree} from "../../../lib/components/repository/tree";
import {objects, staging, retention, repositories, imports, NotFoundError, uploadWithProgress} from "../../../lib/api";
import {useAPI, useAPIWithPagination} from "../../../lib/hooks/api";
import {useRefs} from "../../../lib/hooks/repo";
import {useRouter} from "../../../lib/hooks/router";
import {RefTypeBranch} from "../../../constants";
import {
    ExecuteImportButton,
    ImportDone,
    ImportForm,
    ImportPhase,
    ImportProgress,
    startImport
} from "../services/import_data";
import { Box } from "@mui/material";
import { RepoError } from "./error";
import { getContentType, getFileExtension, FileContents } from "./objectViewer";
import {OverlayTrigger, ProgressBar} from "react-bootstrap";
import Tooltip from "react-bootstrap/Tooltip";
import { useSearchParams } from "react-router-dom";
import { useStorageConfig } from "../../../lib/hooks/storageConfig";
import {useDropzone} from "react-dropzone";
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import pMap from "p-map";

const README_FILE_NAME = "README.md";
const REPOSITORY_AGE_BEFORE_GC = 14;
const MAX_PARALLEL_UPLOADS = 5;

const ImportButton = ({ variant = "success", onClick, config }) => {
  const tip = config.import_support
    ? "Import data from a remote source"
    : config.blockstore_type === "local"
    ? "Import is not enabled for local blockstore"
    : "Unsupported for " + config.blockstore_type + " blockstore";

  return (
    <OverlayTrigger placement="bottom" overlay={<Tooltip>{tip}</Tooltip>}>
      <span>
        <Button
          variant={variant}
          disabled={!config.import_support}
          onClick={onClick}
        >
          <BsCloudArrowUp /> Import
        </Button>
      </span>
    </OverlayTrigger>
  );
};

export const useInterval = (callback, delay) => {
    const savedCallback = useRef();

    useEffect(() => {
        savedCallback.current = callback;
    }, [callback]);

    useEffect(() => {
        function tick() {
            savedCallback.current();
        }
        if (delay !== null) {
            const id = setInterval(tick, delay);
            return () => clearInterval(id);
        }
    }, [delay]);
}

const ImportModal = ({config, repoId, referenceId, referenceType, path = '', onDone, onHide, show = false}) => {
    const [importPhase, setImportPhase] = useState(ImportPhase.NotStarted);
    const [numberOfImportedObjects, setNumberOfImportedObjects] = useState(0);
    const [isImportEnabled, setIsImportEnabled] = useState(false);
    const [importError, setImportError] = useState(null);
    const [metadataFields, setMetadataFields] = useState([])
    const [importID, setImportID] = useState("")

    const sourceRef = useRef(null);
    const destRef = useRef(null);
    const commitMsgRef = useRef(null);

    useInterval(() => {
        if (importID !== "" && importPhase === ImportPhase.InProgress) {
            const getState = async () => {
                try {
                    const importState = await imports.get(repoId, referenceId, importID);
                    setNumberOfImportedObjects(importState.ingested_objects);
                    if (importState.error) {
                        throw importState.error;
                    }
                    if (importState.completed) {
                        setImportPhase(ImportPhase.Completed);
                        onDone();
                    }
                } catch (error) {
                    setImportPhase(ImportPhase.Failed);
                    setImportError(error);
                    setIsImportEnabled(false);
                }
            };
            getState()
        }
    }, 3000);
    
    if (!referenceId || referenceType !== RefTypeBranch) return <></>

    let branchId = referenceId;
    
    const resetState = () => {
        setImportError(null);
        setImportPhase(ImportPhase.NotStarted);
        setIsImportEnabled(false);
        setNumberOfImportedObjects(0);
        setMetadataFields([]);
        setImportID("");
    }

  const hide = () => {
    if (
      ImportPhase.InProgress === importPhase ||
      ImportPhase.Merging === importPhase
    )
      return;
    resetState();
    onHide();
  };

    const doImport = async () => {
        setImportPhase(ImportPhase.InProgress);
        try {
            const metadata = {};
            metadataFields.forEach(pair => metadata[pair.key] = pair.value)
            setImportPhase(ImportPhase.InProgress)
            await startImport(
                setImportID,
                destRef.current.value,
                commitMsgRef.current.value,
                sourceRef.current.value,
                repoId,
                branchId,
                metadata
            );
        } catch (error) {
            setImportPhase(ImportPhase.Failed);
            setImportError(error);
            setIsImportEnabled(false);
        }
    }
    const pathStyle = {'minWidth': '25%'};

    return (
        <>
            <Modal show={show} onHide={hide} size="lg">
                <Modal.Header closeButton>
                    <Modal.Title>Import data from {config.blockstore_type}</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    {
                        (importPhase === ImportPhase.NotStarted ||
                            importPhase === ImportPhase.Failed) &&
                        <ImportForm
                            config={config}
                            pathStyle={pathStyle}
                            sourceRef={sourceRef}
                            destRef={destRef}
                            updateSrcValidity={(isValid) => setIsImportEnabled(isValid)}
                            path={path}
                            commitMsgRef={commitMsgRef}
                            shouldAddPath={true}
                            metadataFields={metadataFields}
                            setMetadataFields={setMetadataFields}
                            err={importError}
                        />
                    }
                    {
                        importPhase === ImportPhase.InProgress &&
                        <ImportProgress numObjects={numberOfImportedObjects}/>
                    }
                    {
                        importPhase === ImportPhase.Completed &&
                        <ImportDone branch={branchId}
                                    numObjects={numberOfImportedObjects}/>
                    }
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" onClick={ async () => {
                        if (importPhase === ImportPhase.InProgress && importID.length > 0) {
                            await imports.delete(repoId, branchId, importID);
                        }
                        hide();
                    }} hidden={importPhase === ImportPhase.Completed}>
                        Cancel
                    </Button>

                    <ExecuteImportButton
                        importPhase={importPhase}
                        importFunc={doImport}
                        doneFunc={hide}
                        isEnabled={isImportEnabled}/>
        </Modal.Footer>
      </Modal>
    </>
  );
};


const uploadFile = async (config, repo, reference, path, file, onProgress) => {
  const fpath = destinationPath(path, file);
  if (config.pre_sign_support_ui) {
    const getResp = await staging.get(repo.id, reference.id, fpath, config.pre_sign_support_ui);
    const { status, etag } = await uploadWithProgress(getResp.presigned_url, file, 'PUT', onProgress)
    if (status >= 400) {
      throw new Error(`Error uploading file: HTTP ${status}`)
    }
    const hash = etag.replace(/"/g, "");
    await staging.link(repo.id, reference.id, fpath, getResp, hash, file.size, file.type);
  } else {
    await objects.upload(repo.id, reference.id, fpath, file, onProgress);
  }
};

const destinationPath = (path, file) => {
  return `${path ? path : ""}${file.path.replace(/\\/g, '/').replace(/^\//, '')}`;
};

const UploadCandidate = ({ repo, reference, path, file, state, onRemove = null }) => {
  const fpath = destinationPath(path, file)
  let uploadIndicator = null;
  if (state && state.status === "uploading") {
    uploadIndicator = <ProgressBar variant="success" now={state.percent}/>
  } else if (state && state.status === "done") {
    uploadIndicator = <strong><CheckboxIcon/></strong>
  } else if (!state && onRemove !== null) {
    uploadIndicator = (
      <a  href="#" onClick={ e => {
        e.preventDefault()
        onRemove()
      }}>
        <XIcon />
      </a>
    );
  }
  return (
    <Container>
      <Row className={`upload-item upload-item-${state ? state.status : "none"}`}>
        <Col>
          <span className="path">
            lakefs://{repo.id}/{reference.id}/{fpath}
          </span>
        </Col>
        <Col xs md="2">
          <span className="size">
            {humanSize(file.size)}
          </span>
        </Col>
        <Col xs md="1">
          <span className="upload-state">
            {uploadIndicator ? uploadIndicator : <></>}
          </span>
        </Col>
      </Row>
    </Container>
  )
};

const UploadButton = ({config, repo, reference, path, onDone, onClick, onHide, show = false}) => {
  const initialState = {
    inProgress: false,
    error: null,
    done: false,
  };
  const [currentPath, setCurrentPath] = useState(path);
  const [uploadState, setUploadState] = useState(initialState);
  const [files, setFiles] = useState([]);
  const [fileStates, setFileStates] = useState({});
  const [abortController, setAbortController] = useState(null)
  const onDrop = useCallback(acceptedFiles => {
    setFiles([...acceptedFiles])
  }, [files])

  const { getRootProps, getInputProps, isDragAccept } = useDropzone({onDrop})

  if (!reference || reference.type !== RefTypeBranch) return <></>;

  const hide = () => {
    if (uploadState.inProgress) {
      if (abortController !== null) {
          abortController.abort()
      } else {
        return
      }
    }
    setUploadState(initialState);
    setFileStates({});
    setFiles([]);
    setCurrentPath(path);
    setAbortController(null)
    onHide();
  };

  useEffect(() => {
    setCurrentPath(path)
  }, [path])

  const upload = async () => {
    if (files.length < 1) {
      return
    }

    const abortController = new AbortController()
    setAbortController(abortController)

    const mapper = async (file) => {
      try {
        setFileStates(next => ( {...next, [file.path]: {status: 'uploading', percent: 0}}))
        await uploadFile(config, repo, reference, currentPath, file, progress => {
          setFileStates(next => ( {...next, [file.path]: {status: 'uploading', percent: progress}}))
        })
      } catch (error) {
        setFileStates(next => ( {...next, [file.path]: {status: 'error'}}))
        setUploadState({ ...initialState, error });
        throw error;
      }
      setFileStates(next => ( {...next, [file.path]: {status: 'done'}}))
    }

    setUploadState({...initialState,  inProgress: true });
    try {
      await pMap(files, mapper, {
        concurrency: MAX_PARALLEL_UPLOADS,
        signal: abortController.signal
      });
      onDone();
      hide();
    } catch (error) {
      if (error instanceof DOMException) {
        // abort!
        onDone();
        hide();
      } else {
        setUploadState({ ...initialState, error });
      }
    }


  };

  const changeCurrentPath = useCallback(e => {
    setCurrentPath(e.target.value)
  }, [setCurrentPath])

  const onRemoveCandidate = useCallback(file => {
    return () => setFiles(current => current.filter(f => f !== file))
  }, [setFiles])

  return (
    <>
      <Modal size="xl" show={show} onHide={hide}>
        <Modal.Header closeButton>
          <Modal.Title>Upload Object</Modal.Title>
        </Modal.Header>
        <Modal.Body>
          <Form
            onSubmit={(e) => {
              if (uploadState.inProgress) return;
              e.preventDefault();
              upload();
            }}
          >
            {config?.warnings && (
              <Form.Group controlId="warnings" className="mb-3">
                <Warnings warnings={config.warnings} />
              </Form.Group>
            )}

            <Form.Group controlId="path" className="mb-3">
              <Form.Text>Path</Form.Text>
              <Form.Control defaultValue={currentPath} onChange={changeCurrentPath}/>
            </Form.Group>

            <Form.Group controlId="content" className="mb-3">
              <div {...getRootProps({className: 'dropzone'})}>
                  <input {...getInputProps()} />
                  <div className={isDragAccept ? "file-drop-zone file-drop-zone-focus" : "file-drop-zone"}>
                    Drag &apos;n&apos; drop files or folders here (or click to select)
                  </div>
              </div>
              <aside className="mt-3">
                {(files && files.length > 0) &&
                  <h5>Files to upload</h5>
                }
                {files && files.map(file =>
                    <UploadCandidate
                      key={file.path}
                      config={config}
                      repo={repo}
                      reference={reference}
                      file={file}
                      path={currentPath}
                      state={fileStates[file.path]}
                      onRemove={!uploadState.inProgress ? onRemoveCandidate(file) : null}
                    />
                )}
              </aside>
            </Form.Group>
          </Form>
        {(uploadState.error) ? (<AlertError error={uploadState.error}/>) : (<></>)}
      </Modal.Body>
    <Modal.Footer>
        <Button variant="secondary" onClick={hide}>
            Cancel
        </Button>
        <Button variant="success" disabled={uploadState.inProgress || files.length < 1} onClick={() => {
            if (uploadState.inProgress) return;
            upload()
        }}>
            {(uploadState.inProgress) ? 'Uploading...' : 'Upload'}
        </Button>
    </Modal.Footer>
  </Modal>

    <Button
      variant={!config.import_support ? "success" : "light"}
      onClick={onClick}
      >
      <UploadIcon /> Upload Object
    </Button>
  </>
  );
};

const TreeContainer = ({
  config,
  repo,
  reference,
  path,
  after,
  onPaginate,
  onRefresh,
  onUpload,
  onImport,
  refreshToken,
}) => {
  const { results, error, loading, nextPage } = useAPIWithPagination(() => {
    return objects.list(
      repo.id,
      reference.id,
      path,
      after,
      config.pre_sign_support_ui
    );
  }, [repo.id, reference.id, path, after, refreshToken]);
  const initialState = {
    inProgress: false,
    error: null,
    done: false,
  };
  const [deleteState, setDeleteState] = useState(initialState);

    if (loading) return <Loading/>;
    if (error) return <AlertError error={error}/>;

    return (
        <>
            {deleteState.error && <AlertError error={deleteState.error} onDismiss={() => setDeleteState(initialState)}/>}
            <Tree
                config={{config}}
                repo={repo}
                reference={reference}
                path={(path) ? path : ""}
                showActions={true}
                results={results}
                after={after}
                nextPage={nextPage}
                onPaginate={onPaginate}
                onUpload={onUpload}
                onImport={onImport}
                onDelete={entry => {
                    objects
                        .delete(repo.id, reference.id, entry.path)
                        .catch(error => {
                            setDeleteState({...initialState, error: error})
                            throw error
                        })
                        .then(onRefresh)
                }}
            /></>
    );
}

const ReadmeContainer = ({
  config,
  repo,
  reference,
  path = "",
  refreshDep = "",
}) => {
  let readmePath = "";

  if (path) {
    readmePath = path.endsWith("/")
      ? `${path}${README_FILE_NAME}`
      : `${path}/${README_FILE_NAME}`;
  } else {
    readmePath = README_FILE_NAME;
  }
  const { response, error, loading } = useAPI(
    () => objects.head(repo.id, reference.id, readmePath),
    [path, refreshDep]
  );

  if (loading || error) {
    return <></>; // no file found.
  }

  const fileExtension = getFileExtension(readmePath);
  const contentType = getContentType(response?.headers);

    return (
        <FileContents 
            repoId={repo.id} 
            reference={reference}
            path={readmePath}
            fileExtension={fileExtension}
            contentType={contentType}
            error={error}
            loading={loading}
            showFullNavigator={false}
            presign={config.pre_sign_support_ui}
        />
    );
}

const NoGCRulesWarning = ({ repoId }) => {
  const storageKey = `show_gc_warning_${repoId}`;
  const [show, setShow] = useState(
    window.localStorage.getItem(storageKey) !== "false"
  );
  const closeAndRemember = useCallback(() => {
    window.localStorage.setItem(storageKey, "false");
    setShow(false);
  }, [repoId]);

  const { response } = useAPI(async () => {
    const repo = await repositories.get(repoId);
    if (
      !repo.storage_namespace.startsWith("s3:") &&
      !repo.storage_namespace.startsWith("http")
    ) {
      return false;
    }
    const createdAgo = dayjs().diff(dayjs.unix(repo.creation_date), "days");
    if (createdAgo > REPOSITORY_AGE_BEFORE_GC) {
      try {
        await retention.getGCPolicy(repoId);
      } catch (e) {
        if (e instanceof NotFoundError) {
          return true;
        }
      }
    }
    return false;
  }, [repoId]);

  if (show && response) {
    return (
      <Alert variant="warning" onClose={closeAndRemember} dismissible>
        <strong>Warning</strong>: No garbage collection rules configured for
        this repository.{" "}
        <a
          href="https://docs.lakefs.io/howto/garbage-collection.html"
          target="_blank"
          rel="noreferrer"
        >
          Learn More
        </a>
        .
      </Alert>
    );
  }
  return <></>;
};

const ObjectsBrowser = ({ config, configError }) => {
  const router = useRouter();
  const { path, after, importDialog } = router.query;
  const [searchParams, setSearchParams] = useSearchParams();
  const { repo, reference, loading, error } = useRefs();
  const [showUpload, setShowUpload] = useState(false);
  const [showImport, setShowImport] = useState(false);
  const [refreshToken, setRefreshToken] = useState(false);

  const refresh = () => setRefreshToken(!refreshToken);
  const parts = (path && path.split("/")) || [];
  const searchSuffix = parts.pop();
  let searchPrefix = parts.join("/");
  searchPrefix = searchPrefix && searchPrefix + "/";

  useEffect(() => {
    if (importDialog) {
      setShowImport(true);
      searchParams.delete("importDialog");
      setSearchParams(searchParams);
    }
  }, [router.route, importDialog, searchParams, setSearchParams]);

  if (loading || !config) return <Loading />;
  if (error || configError) return <RepoError error={error || configError} />;

  return (
    <>
      <ActionsBar>
        <ActionGroup orientation="left">
          <RefDropdown
            emptyText={"Select Branch"}
            repo={repo}
            selected={reference}
            withCommits={true}
            withWorkspace={true}
            selectRef={(ref) =>
              router.push({
                pathname: `/repositories/:repoId/objects`,
                params: {
                  repoId: repo.id,
                  path: path === undefined ? "" : path,
                },
                query: { ref: ref.id, path: path === undefined ? "" : path },
              })
            }
          />
        </ActionGroup>

        <ActionGroup orientation="right">
          <PrefixSearchWidget
            text="Search by Prefix"
            key={path}
            defaultValue={searchSuffix}
            onFilter={(prefix) => {
              const query = { path: "" };
              if (searchPrefix !== undefined) query.path = searchPrefix;
              if (prefix) query.path += prefix;
              if (reference) query.ref = reference.id;
              const url = {
                pathname: `/repositories/:repoId/objects`,
                query,
                params: { repoId: repo.id },
              };
              router.push(url);
            }}
          />
          <RefreshButton onClick={refresh} />
          <UploadButton
            config={config}
            path={path}
            repo={repo}
            reference={reference}
            onDone={refresh}
            onClick={() => {
              setShowUpload(true);
            }}
            onHide={() => {
              setShowUpload(false);
            }}
            show={showUpload}
          />
          <ImportButton onClick={() => setShowImport(true)} config={config} />
          <ImportModal
            config={config}
            path={path}
            repoId={repo.id}
            referenceId={reference.id}
            referenceType={reference.type}
            onDone={refresh}
            onHide={() => {
              setShowImport(false);
            }}
            show={showImport}
          />
        </ActionGroup>
      </ActionsBar>

      <NoGCRulesWarning repoId={repo.id} />

      <Box
        sx={{
          display: "flex",
          flexDirection: "column",
          gap: "10px",
          mb: "30px",
        }}
      >
        <TreeContainer
          config={config}
          reference={reference}
          repo={repo}
          path={path ? path : ""}
          after={after ? after : ""}
          onPaginate={(after) => {
            const query = { after };
            if (path) query.path = path;
            if (reference) query.ref = reference.id;
            const url = {
              pathname: `/repositories/:repoId/objects`,
              query,
              params: { repoId: repo.id },
            };
            router.push(url);
          }}
          refreshToken={refreshToken}
          onUpload={() => {
            setShowUpload(true);
          }}
          onImport={() => {
            setShowImport(true);
          }}
          onRefresh={refresh}
        />

        <ReadmeContainer
          config={config}
          reference={reference}
          repo={repo}
          path={path}
          refreshDep={refreshToken}
        />
      </Box>
    </>
  );
};

const RepositoryObjectsPage = () => {
  const config = useStorageConfig();

  return (
    <RepositoryPageLayout activePage={"objects"}>
      {config.loading && <Loading />}
      <ObjectsBrowser config={config} configError={config.error} />
    </RepositoryPageLayout>
  );
};

export default RepositoryObjectsPage;
