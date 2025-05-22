import React, { useCallback, useEffect, useRef, useState } from "react";
import dayjs from "dayjs";
import { useOutletContext } from "react-router-dom";
import {CheckboxIcon, UploadIcon, XIcon, AlertIcon, PencilIcon} from "@primer/octicons-react";
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
import {objects, staging, retention, repositories, imports, NotFoundError, uploadWithProgress, parseRawHeaders} from "../../../lib/api";
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
import { useStorageConfigs } from "../../../lib/hooks/storageConfig";
import { getRepoStorageConfig } from "./utils";
import {useDropzone} from "react-dropzone";
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

                        <ImportForm
                            config={config}
                            repo={repoId}
                            branch={branchId}
                            pathStyle={pathStyle}
                            sourceRef={sourceRef}
                            destRef={destRef}
                            updateSrcValidity={(isValid) => setIsImportEnabled(isValid)}
                            path={path}
                            commitMsgRef={commitMsgRef}
                            metadataFields={metadataFields}
                            setMetadataFields={setMetadataFields}
                            err={importError}
                            className={importPhase === ImportPhase.NotStarted || importPhase === ImportPhase.Failed ? '' : 'd-none'}
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



function extractChecksumFromResponse(parsedHeaders) {
  if (parsedHeaders['content-md5']) {
    // drop any quote and space
    return parsedHeaders['content-md5'];
  }
  // fallback to ETag
  if (parsedHeaders['etag']) {
    // drop any quote and space
    return parsedHeaders['etag'].replace(/[" ]+/g, "");
  }
  return null;
}

const uploadFile = async (config, repo, reference, destinationPath, file, onProgress) => {  
  if (config.pre_sign_support_ui) {
    let additionalHeaders;
    if (config.blockstore_type === "azure") {
      additionalHeaders = { "x-ms-blob-type": "BlockBlob" }
    }
    const getResp = await staging.get(repo.id, reference.id, destinationPath, config.pre_sign_support_ui);
    try {
      const uploadResponse = await uploadWithProgress(getResp.presigned_url, file, 'PUT', onProgress, additionalHeaders);
      const parsedHeaders = parseRawHeaders(uploadResponse.rawHeaders);
      const checksum = extractChecksumFromResponse(parsedHeaders);
      await staging.link(repo.id, reference.id, destinationPath, getResp, checksum, file.size, file.type);
    } catch(error) {
       throw new Error(`Error uploading file- HTTP ${error.status}${error.response ? `: ${error.response}` : ''}`);
    }
  } else {
    await objects.upload(repo.id, reference.id, destinationPath, file, onProgress);
    }
};

const joinPath = (basePath, filePath) => {
    // 1. Normalize the file path first: remove leading slash
    const normalizedFilePath = filePath.replace(/^\//, '');

    // 2. Handle the base path
    // If basePath is empty or just '/', the result is just the normalized file path
    if (!basePath || basePath === '/') {
        return normalizedFilePath;
    }

    // 3. If basePath is non-empty, ensure it ends with '/'
    const normalizedBasePath = basePath.endsWith('/') ? basePath : basePath + '/';

    // 4. Combine
    return normalizedBasePath + normalizedFilePath;
}

const generateInitialDestination = (filePath, currentOverallPath) => {
    const relativePathFromDrop = filePath.replace(/\\/g, '/');
    return joinPath(currentOverallPath, relativePathFromDrop);
};

const UploadCandidate = ({ 
  file, 
  state, 
  destination, 
  onDestinationChange,
  onRemove,
  isUploading,
  isEditing,
  onEditToggle,
}) => {
  let statusIndicator = null;
  const inputRef = useRef(null);

  useEffect(() => {
    if (isEditing && inputRef.current) {
      inputRef.current.focus();
      inputRef.current.select();
    }
  }, [isEditing]);

  if (state && state.status === "uploading") {
    statusIndicator = <ProgressBar 
                          animated
                          variant="success" 
                          now={state.percent} 
                          className="upload-progress" 
                          title={`${state.percent}% uploaded`}
                          style={{ height: '8px' }}
                        />;
  } else if (state && state.status === "done") {
    statusIndicator = <span className="text-success" title="Completed"><CheckboxIcon/></span>;
  } else if (state && state.status === "error") {
    statusIndicator = <span className="text-danger" title="Error"><AlertIcon/></span>;
  } else if (onRemove !== null && !isUploading && state?.status !== 'uploading') {
    statusIndicator = (
      <button 
        className="remove-button" 
        onClick={e => { e.preventDefault(); onRemove(); }}
        title="Remove file"
        disabled={isUploading}
      >
        <XIcon />
      </button>
    );
  }
  
  const handleBlur = () => {
    onEditToggle(false);
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' || e.key === 'Escape') {
      onEditToggle(false);
      e.preventDefault(); // Prevent form submission on Enter
    }
  };
  
  return (
    <div className={`upload-item upload-item-${state ? state.status : "pending"}`}>
      <div className="file-destination-column">
        {isEditing ? (
          <Form.Control
            ref={inputRef}
            size="sm"
            type="text"
            value={destination}
            onChange={(e) => onDestinationChange(e.target.value)}
            onBlur={handleBlur}
            onKeyDown={handleKeyDown}
            disabled={isUploading} 
            placeholder="Destination path/name"
          />
        ) : (
          <>
            <span 
              className="file-destination-display" 
              title={destination} 
              onClick={() => !isUploading && onEditToggle(true)} 
            >
              {destination || <span>&nbsp;</span>} 
            </span>
            {!isUploading && state?.status !== 'uploading' && state?.status !== 'done' && state?.status !== 'error' && (
              <button 
                className="edit-destination-button" 
                onClick={(e) => {e.preventDefault(); onEditToggle(true);}}
                title="Edit destination path"
                disabled={isUploading}
              >
                <PencilIcon size={14}/>
              </button>
            )}
          </>
        )}
      </div>
      <div className="file-size" title={humanSize(file.size)}>
        {humanSize(file.size)}
      </div>
      <div className="upload-status">
        {statusIndicator}
      </div>
    </div>
  )
};

const UploadButton = ({config, repo, reference, path, onDone, onClick, onHide, show = false, disabled = false}) => {
  const initialState = {
    inProgress: false,
    error: null,
    done: false,
  };
  const [overallPath, setOverallPath] = useState(path || "");
  const [uploadState, setUploadState] = useState(initialState);
  const [files, setFiles] = useState([]);
  const [fileStates, setFileStates] = useState({});
  const [fileDestinations, setFileDestinations] = useState({});
  const [editingDestinations, setEditingDestinations] = useState({});
  const [manuallyEditedDestinations, setManuallyEditedDestinations] = useState({});
  const [abortController, setAbortController] = useState(null);

  const onDrop = useCallback(acceptedFiles => {
    if (uploadState.inProgress) return;
    
    const newFiles = acceptedFiles.filter(f => !files.some(existing => existing.path === f.path));
    if (newFiles.length === 0) return;

    const nextFiles = [...files, ...newFiles];
    const nextDestinations = { ...fileDestinations };
    const nextStates = { ...fileStates };
    const nextEditing = { ...editingDestinations };
    const nextManualEditFlags = { ...manuallyEditedDestinations };

    newFiles.forEach(file => {
      const initialDest = generateInitialDestination(file.path, overallPath);
      nextDestinations[file.path] = initialDest;
      nextStates[file.path] = { status: 'pending', percent: 0 };
      nextEditing[file.path] = false;
      nextManualEditFlags[file.path] = false;
    });

    setFiles(nextFiles);
    setFileDestinations(nextDestinations);
    setFileStates(nextStates);
    setEditingDestinations(nextEditing);
    setManuallyEditedDestinations(nextManualEditFlags);

  }, [files, fileDestinations, fileStates, editingDestinations, manuallyEditedDestinations, overallPath, uploadState.inProgress]);

  const { getRootProps, getInputProps, isDragAccept } = useDropzone({
    onDrop,
    disabled: uploadState.inProgress,
  })

  useEffect(() => {
    setFileDestinations(currentDestinations => {
      const nextDestinations = { ...currentDestinations };
      let changed = false;
      files.forEach(file => {
        if (!manuallyEditedDestinations[file.path]) {
          const newDest = generateInitialDestination(file.path, overallPath);
          if (nextDestinations[file.path] !== newDest) {
             nextDestinations[file.path] = newDest;
             changed = true;
          }
        }
      });
      return changed ? nextDestinations : currentDestinations;
    });
  }, [overallPath, files, manuallyEditedDestinations]);


  if (!reference || reference.type !== RefTypeBranch) return <></>;

  const hide = () => {
    if (uploadState.inProgress) {
      if (abortController !== null) {
          abortController.abort();
      } else {
        return;
      }
    }
    setUploadState(initialState);
    setFiles([]);
    setFileStates({});
    setFileDestinations({});
    setEditingDestinations({});
    setManuallyEditedDestinations({});
    setOverallPath(path || "");
    setAbortController(null);
    onHide();
  };

  useEffect(() => {
    setOverallPath(path || "")
  }, [path])

  const upload = async () => {
    if (files.length < 1 || uploadState.inProgress) {
      return
    }

    setEditingDestinations({});

    const controller = new AbortController();
    setAbortController(controller);
    setUploadState({ ...initialState, inProgress: true });

    const mapper = async (file) => {
      const currentDestination = fileDestinations[file.path];
      if (!currentDestination) {
        console.error(`No destination path found for file: ${file.path}`);
        setFileStates(next => ({ ...next, [file.path]: { status: 'error', percent: 0 } }));
        throw new Error(`Missing destination for ${file.path}`);
      }
      try {
        setFileStates(next => ({ ...next, [file.path]: { status: 'uploading', percent: 0 } }));
        
        const handleProgress = (progress) => {
          if (controller.signal.aborted) return;
          setFileStates(next => {
            if (next[file.path]?.status === 'uploading') {
                return { ...next, [file.path]: { status: 'uploading', percent: progress } };
            }
            return next;
          });
        };

        await uploadFile(config, repo, reference, currentDestination, file, handleProgress);
        
        if (controller.signal.aborted) return;
        setFileStates(next => ({ ...next, [file.path]: { status: 'done', percent: 100 } }));

      } catch (error) {
        if (controller.signal.aborted) return;
        console.error("Upload error for:", file.path, error);
        setFileStates(next => ({ ...next, [file.path]: { status: 'error', percent: 0 } }));
        if (!(error instanceof DOMException && error.name === 'AbortError') && !controller.signal.aborted) {
          setUploadState(prev => ({ ...prev, error: error }));
        }                                                                       
        throw error;
      }
    }

    try {
      await pMap(files, mapper, {
        concurrency: MAX_PARALLEL_UPLOADS,
        signal: controller.signal,
        stopOnError: true
      });
      if (!controller.signal.aborted) {
          setUploadState(prev => ({ ...prev, inProgress: false, done: true, error: null }));
          onDone();
          hide(); 
      }
    } catch (error) {
       if (!(error instanceof DOMException && error.name === 'AbortError') && !controller.signal.aborted) {
           console.error("pMap upload error:", error);
           setUploadState(prev => ({...prev, inProgress: false, error: prev.error || error })); 
       } else {
           console.log("Upload process aborted.");
           setUploadState(prev => ({...prev, inProgress: false}));
       }
    } finally {
       setUploadState(prev => ({...prev, inProgress: false}));
       setAbortController(null);
    }
  };

  const handleOverallPathChange = useCallback(e => {
    setOverallPath(e.target.value)
  }, [])

  const handleIndividualDestinationChange = useCallback((originalPath, newDestination) => {
    setFileDestinations(prev => ({ ...prev, [originalPath]: newDestination }));
    setManuallyEditedDestinations(prev => ({ ...prev, [originalPath]: true }));
  }, []);

  const handleRemoveFile = useCallback(originalPath => {
    setFiles(prev => prev.filter(f => f.path !== originalPath));
    setFileDestinations(prev => {
      const next = { ...prev };
      delete next[originalPath];
      return next;
    });
    setFileStates(prev => {
      const next = { ...prev };
      delete next[originalPath];
      return next;
    });
    setEditingDestinations(prev => {
      const next = { ...prev };
      delete next[originalPath];
      return next;
    });
    setManuallyEditedDestinations(prev => {
      const next = { ...prev };
      delete next[originalPath];
      return next;
    });
  }, []);

  const handleEditToggle = useCallback((originalPath, editMode) => {
    setEditingDestinations(prev => ({ ...prev, [originalPath]: editMode }));
  }, []);

  const totalSize = files.reduce((a, f) => a + f.size, 0);
  const canUpload = files.length > 0 && !uploadState.inProgress;

  const totalProgress = files.reduce((sum, file) => sum + (fileStates[file.path]?.percent || 0), 0);
  const averageProgress = files.length > 0 ? Math.round(totalProgress / files.length) : 0;
  const uploadingCount = files.filter(f => fileStates[f.path]?.status === 'uploading').length;

  return (
    <>
      <Modal size="xl" show={show} onHide={hide} backdrop="static" keyboard={!uploadState.inProgress}>
        <Modal.Header closeButton={!uploadState.inProgress}>
          <Modal.Title>
            <UploadIcon className="me-2" />
            Upload Objects to Branch &apos;{reference.id}&apos;
          </Modal.Title>
        </Modal.Header>
        <Modal.Body>
          <Form
            onSubmit={(e) => {
              e.preventDefault();
              if (canUpload) upload();
            }}
          >
            {config?.warnings && (
              <Form.Group controlId="warnings" className="mb-3">
                <Warnings warnings={config.warnings} />
              </Form.Group>
            )}

            <Form.Group controlId="dropzone" className="mb-3">
              <div {...getRootProps({className: 'dropzone', 'aria-disabled': uploadState.inProgress})}>
                <input {...getInputProps({'aria-disabled': uploadState.inProgress})} />
                <div className={isDragAccept ? "file-drop-zone file-drop-zone-focus" : "file-drop-zone"}>
                  <UploadIcon size={24} className="file-drop-zone-icon" />
                   <div className="file-drop-zone-text">
                     {isDragAccept ? "Drop files here" : "Drag & drop files or folders here"}
                   </div>
                   <div className="file-drop-zone-hint">or click to browse</div>
                 </div>
              </div>
            </Form.Group>

            <Form.Group controlId="overallPath" className="mb-3">
              <Form.Label>Common Destination Directory (Optional)</Form.Label>
              <Form.Control 
                type="text"
                disabled={uploadState.inProgress} 
                value={overallPath} 
                onChange={handleOverallPathChange}
                placeholder="e.g., data/images/ (leave empty for root)"
                title="Prefix for all uploaded files. Can be overridden individually below."
              />
            </Form.Group>

            {files.length > 0 && (
              <div className="upload-items-container">
                <div className="d-flex justify-content-between align-items-center mb-2">
                  <h5 className="mb-0">Files to Upload ({files.length})</h5>
                  <span className="text-muted">Total size: {humanSize(totalSize)}</span>
                </div>

                <div className="upload-items-header d-none d-md-grid">
                  <div>File (Original / Destination)</div>
                  <div>Size</div>
                  <div>Status</div>
                </div>

                <div className="upload-items-list">
                  {files.map(file =>
                    <UploadCandidate
                      key={file.path}
                      file={file}
                      destination={fileDestinations[file.path] || ''}
                      state={fileStates[file.path]}
                      onDestinationChange={(newDest) => handleIndividualDestinationChange(file.path, newDest)}
                      onRemove={() => handleRemoveFile(file.path)}
                      isUploading={uploadState.inProgress || fileStates[file.path]?.status === 'uploading'}
                      isEditing={editingDestinations[file.path] || false}
                      onEditToggle={(editMode) => handleEditToggle(file.path, editMode)}
                    />
                  )}
                </div>
              </div>
            )}
          </Form>
          {(uploadState.error && !(uploadState.error instanceof DOMException && uploadState.error.name === 'AbortError')) && 
             <AlertError error={uploadState.error} onDismiss={() => setUploadState(prev => ({...prev, error: null}))}/>
          } 
        </Modal.Body>
        <Modal.Footer>
          <Button variant="secondary" onClick={hide} disabled={uploadState.inProgress}>
            {uploadState.inProgress ? "Cancel Upload" : "Close"}
          </Button>
          <Button 
            variant="success" 
            disabled={!canUpload} 
            onClick={upload} 
          >
            {uploadState.inProgress ? (
              <>
                <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
                Uploading {uploadingCount > 0 ? `${uploadingCount} / ${files.length}` : averageProgress + '%'}...
              </>
            ) : (
              <>
                <UploadIcon className="me-2" /> Upload {files.length || ''} File{files.length !== 1 ? 's' : ''}
              </>
            )}
          </Button>
        </Modal.Footer>
      </Modal>

      <Button
        variant={!config.import_support ? "success" : "light"}
        disabled={disabled || !reference || reference.type !== RefTypeBranch}
        onClick={onClick}
        title={(!reference || reference.type !== RefTypeBranch) ? "Select a branch to upload" : "Upload objects"}
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
          href="https://docs.lakefs.io/howto/garbage-collection/"
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

const ObjectsBrowser = ({ config }) => {
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

  if (loading) return <Loading />;
  if (error) return <RepoError error={error} />;

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
            disabled={repo?.read_only}
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
    const {repo} = useRefs();
    const {configs: storageConfigs, loading: configsLoading, error: configsError} = useStorageConfigs();

    const [setActivePage] = useOutletContext();
    useEffect(() => setActivePage("objects"), [setActivePage]);

    if (configsLoading) return <Loading/>;
    if (configsError) return <RepoError error={configsError}/>;

    const {storageConfig, loading: configLoading, error: configError} = getRepoStorageConfig(storageConfigs, repo);
    if (configLoading) return <Loading/>;
    if (configError) return <RepoError error={configError}/>;

    return <ObjectsBrowser config={storageConfig}/>;
};

export default RepositoryObjectsPage;
