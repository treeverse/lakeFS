import React, { useCallback, useEffect, useRef, useState } from "react";
import dayjs from "dayjs";
import { useOutletContext } from "react-router-dom";
import {CheckboxIcon, UploadIcon, XIcon, AlertIcon, PencilIcon, GitCommitIcon, HistoryIcon, NorthStarIcon} from "@primer/octicons-react";
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
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import Alert from "react-bootstrap/Alert";
import Dropdown from "react-bootstrap/Dropdown";
import { BsCloudArrowUp } from "react-icons/bs";

import {humanSize, Tree, URINavigator} from "../../../lib/components/repository/tree";
import {objects, staging, retention, repositories, imports, NotFoundError, uploadWithProgress, parseRawHeaders, branches, commits, refs} from "../../../lib/api";
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
import {ProgressBar} from "react-bootstrap";
import { useSearchParams } from "react-router-dom";
import { useConfigContext } from "../../../lib/hooks/configProvider";
import { getRepoStorageConfig } from "./utils";
import {useDropzone} from "react-dropzone";
import pMap from "p-map";
import {formatAlertText} from "../../../lib/components/repository/errors";
import {ChangesTreeContainer, MetadataFields} from "../../../lib/components/repository/changes";
import {ConfirmationModal} from "../../../lib/components/modals";
import { Link } from "../../../lib/components/nav";
import Card from "react-bootstrap/Card";

const README_FILE_NAME = "README.md";
const REPOSITORY_AGE_BEFORE_GC = 14;
const MAX_PARALLEL_UPLOADS = 5;


export async function appendMoreResults(resultsState, prefix, lastSeenPath, setLastSeenPath, setResultsState, getMore) {
    let resultsFiltered = resultsState.results
    if (resultsState.prefix !== prefix) {
        // prefix changed, need to delete previous results
        setLastSeenPath("")
        resultsFiltered = []
    }

    if (resultsFiltered.length > 0 && resultsFiltered.at(-1).path > lastSeenPath) {
        // results already cached
        return {prefix: prefix, results: resultsFiltered, pagination: resultsState.pagination}
    }

    const {results, pagination} = await getMore()
    // Ensure concatenated results maintain lexicographic order
    const concatenatedResults = resultsFiltered.concat(results).sort((a, b) => a.path.localeCompare(b.path))
    setResultsState({prefix: prefix, results: concatenatedResults, pagination: pagination})
    return {results: resultsState.results, pagination: pagination}
}

const isAbortedError = (error, controller) => {
  return (error instanceof DOMException && error.name === 'AbortError') || controller?.signal?.aborted;
};

const CommitButton = ({repo, onCommit, enabled = false}) => {
    const textRef = useRef(null);

    const [committing, setCommitting] = useState(false)
    const [show, setShow] = useState(false)
    const [metadataFields, setMetadataFields] = useState([])
    
    const hide = () => {
        if (committing) return;
        setShow(false)
    }

    const onSubmit = () => {
        const message = textRef.current.value;
        const metadata = {};
        metadataFields.forEach(pair => metadata[pair.key] = pair.value)
        setCommitting(true)
        onCommit({message, metadata}, () => {
            setCommitting(false)
            setShow(false);
        })
    };

    const alertText = formatAlertText(repo.id, null);
    return (
        <>
            <Modal show={show} onHide={hide} size="lg">
                <Modal.Header closeButton>
                    <Modal.Title>Commit Changes</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    <Form className="mb-2" onSubmit={(e) => {
                        onSubmit();
                        e.preventDefault();
                    }}>
                        <Form.Group controlId="message" className="mb-3">
                            <Form.Control type="text" placeholder="Commit Message" ref={textRef}/>
                        </Form.Group>

                        <MetadataFields metadataFields={metadataFields} setMetadataFields={setMetadataFields}/>
                    </Form>
                    {alertText ? (<Alert variant="danger">{alertText}</Alert>) : (<span/>)}
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" disabled={committing} onClick={hide}>
                        Cancel
                    </Button>
                    <Button variant="success" disabled={committing} onClick={onSubmit}>
                        Commit Changes
                    </Button>
                </Modal.Footer>
            </Modal>
            <Button variant="success" disabled={!enabled} onClick={() => setShow(true)}>
                <GitCommitIcon/> Commit Changes
            </Button>
        </>
    );
}



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

const UploadButtonText = ({ inProgress, uploadingCount, filesLength, averageProgress }) => {
  if (inProgress) {
    return (
      <>
        <span className="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>
        Uploading {uploadingCount > 0 ? `${uploadingCount} / ${filesLength}` : averageProgress + '%'}...
      </>
    );
  }
  return (
    <>
      <UploadIcon className="me-2" /> Upload {filesLength || ''} File{filesLength !== 1 ? 's' : ''}
    </>
  );
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
        if (!isAbortedError(error, controller)) {
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
       if (!isAbortedError(error, controller)) {
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
                  {files.map(file => {
                    const fileState = fileStates[file.path];
                    const isUploading = uploadState.inProgress || fileState?.status === 'uploading';
                    return (
                      <UploadCandidate
                        key={file.path}
                        file={file}
                        destination={fileDestinations[file.path] || ''}
                        state={fileState}
                        onDestinationChange={(newDest) => handleIndividualDestinationChange(file.path, newDest)}
                        onRemove={() => handleRemoveFile(file.path)}
                        isUploading={isUploading}
                        isEditing={editingDestinations[file.path] || false}
                        onEditToggle={(editMode) => handleEditToggle(file.path, editMode)}
                      />
                    );
                  })}
                </div>
              </div>
            )}
          </Form>
          {(uploadState.error && !isAbortedError(uploadState.error, abortController)) && 
             <AlertError className="mt-3" error={uploadState.error} onDismiss={() => setUploadState(prev => ({...prev, error: null}))}/>
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
            <UploadButtonText 
              inProgress={uploadState.inProgress}
              uploadingCount={uploadingCount}
              filesLength={files.length}
              averageProgress={averageProgress}
            />
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

export const EmptyChangesState = ({ repo, reference, toggleShowChanges }) => {
  return (
      <div className="tree-container">
          <Card className="border-0 shadow-sm">
              <Card.Body className="text-center p-5">
                  <h3 className="mb-3">No Changes Here</h3>
                  <p className="text-muted mb-1">
                      No uncommitted changes on <code>{reference.id}</code>!
                  </p>
                  <p className="text-muted mb-4">
                    Upload or modify files to see them appear here.
                  </p>
                  <Link 
                      href={{
                          pathname: "/repositories/:repoId/objects",
                          params: { repoId: repo.id },
                          query: { ref: reference.id, upload: true }
                      }}
                      className="btn btn-primary me-2"
                  >
                      <UploadIcon className="me-1" /> Upload Files
                  </Link>
                  <Button variant="outline-secondary" onClick={() => toggleShowChanges(false)}>
                      <NorthStarIcon  className="me-1" /> See All Objects
                  </Button>
              </Card.Body>
          </Card>
      </div>
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
  showChangesOnly,
  toggleShowChangesOnly,
}) => {
  const [actionError, setActionError] = useState(null);
  const [internalRefresh, setInternalRefresh] = useState(true);
  const [lastSeenPath, setLastSeenPath] = useState("");
  const [resultsState, setResultsState] = useState({prefix: path, results:[], pagination:{}});

  const delimiter = '/';

  // Reset results when switching between modes or changing path
  React.useEffect(() => {
    setResultsState({prefix: path, results:[], pagination:{}});
    setLastSeenPath("");
  }, [showChangesOnly, path]);

  // Fetch changes to highlight them in regular view
  const { response: changesData } = useAPI(async () => {
    if (!showChangesOnly && reference && reference.type === RefTypeBranch) {
      try {
        return await refs.changes(repo.id, reference.id, "", path, delimiter);
      } catch (error) {
        return { results: [] };
      }
    }
    return { results: [] };
  }, [repo.id, reference.id, path, refreshToken, showChangesOnly]);

  // Use different API calls based on whether we're showing changes only or all objects
  const { results, error, loading, nextPage } = useAPIWithPagination(() => {
    if (showChangesOnly) {
      // Show only changes - use current path to filter changes
      return appendMoreResults(resultsState, path, lastSeenPath, setLastSeenPath, setResultsState,
        () => refs.changes(repo.id, reference.id, lastSeenPath, path, delimiter));
    } else {
      // Show all objects
      return objects.list(
        repo.id,
        reference.id,
        path,
        after,
        config.pre_sign_support_ui
      );
    }
  }, [repo.id, reference.id, path, after, refreshToken, showChangesOnly, internalRefresh, lastSeenPath, delimiter]);

  // Merge changes with objects for highlighting
  const mergedResults = React.useMemo(() => {
    if (showChangesOnly || !results || !changesData?.results) {
      // Ensure regular results are also sorted lexicographically
      return results ? results.sort((a, b) => a.path.localeCompare(b.path)) : results;
    }

    const changesMap = new Map();
    const directoryChanges = new Map(); // Store change type for directories
    
    // Map direct changes and identify affected directories
    changesData.results.forEach(change => {
      // Map change type to what EntryRow expects
      const mappedType = change.type === 'removed' ? 'removed' : 
                        change.type === 'added' ? 'added' : 'changed';
      
      changesMap.set(change.path, mappedType);
      
      // If this is a prefix entry from changes, mark it directly with its type
      if (change.path_type === "common_prefix") {
        directoryChanges.set(change.path, mappedType);
      } else {
        // For file changes, mark parent directories as changed
        const pathParts = change.path.split('/');
        for (let i = 1; i < pathParts.length; i++) {
          const dirPath = pathParts.slice(0, i).join('/') + '/';
          // Only mark as changed if not already marked with a more specific type
          if (!directoryChanges.has(dirPath)) {
            directoryChanges.set(dirPath, "changed");
          }
        }
      }
    });

    // Add missing items from changes that aren't in the regular results
    // This includes deleted files and deleted/changed prefixes
    const missingItems = changesData.results
      .filter(change => !results.find(result => result.path === change.path));

    // Merge regular results with change info
    const enhancedResults = results.map(entry => {
      const directChangeType = changesMap.get(entry.path);
      if (directChangeType) {
        return { ...entry, diff_type: directChangeType };
      }
      
      // Check if this directory contains changes (either directly or has changed children)
      const directoryChangeType = directoryChanges.get(entry.path);
      if (entry.path_type === "common_prefix" && directoryChangeType) {
        return { ...entry, diff_type: directoryChangeType };
      }
      
      return entry;
    });

    // Add missing items (deleted files, deleted/changed prefixes) to the results
    const allResults = [...enhancedResults, ...missingItems.map(item => {
      const mappedType = item.type === 'removed' ? 'removed' : 
                        item.type === 'added' ? 'added' : 'changed';
      return {
        ...item,
        diff_type: mappedType
      };
    })];

    // Sort to maintain proper order
    return allResults.sort((a, b) => a.path.localeCompare(b.path));
  }, [results, changesData, showChangesOnly]);

  const initialState = {
    inProgress: false,
    error: null,
    done: false,
  };
  const [deleteState, setDeleteState] = useState(initialState);

  const refresh = () => {
    setResultsState({prefix: path, results:[], pagination:{}})
    setInternalRefresh(!internalRefresh)
    onRefresh();
  }

  const getMoreUncommittedChanges = (lastSeenPath, changesPath, useDelimiter= true, amount = -1) => {
    return refs.changes(repo.id, reference.id, lastSeenPath, changesPath, useDelimiter ? delimiter : "", amount > 0 ? amount : undefined)
  }

  let onReset = async (entry) => {
    branches
        .reset(repo.id, reference.id, {type: entry.path_type, path: entry.path})
        .then(refresh)
        .catch(error => {
            setActionError(error)
        })
  }

  if (loading) return <Loading/>;
  if (error) return <AlertError error={error}/>;

  // If showing changes only, use ChangesTreeContainer
  if (showChangesOnly) {
    const rawChangesResults = resultsState.results.length > 0 ? resultsState.results : results || [];
    // Always sort changes lexicographically to maintain consistent ordering
    const changesResults = rawChangesResults.sort((a, b) => a.path.localeCompare(b.path));
    
    if (changesResults.length === 0) {
      return <EmptyChangesState repo={repo} reference={reference} toggleShowChanges={toggleShowChangesOnly} />;
    }

    const committedRef = reference.id + "@"
    const uncommittedRef = reference.id

    // Create URI navigator that preserves "show changes" mode
    const changesUriNavigator = (
      <URINavigator 
        path={path || ""} 
        repo={repo} 
        reference={reference} 
        hasCopyButton={true}
        pathURLBuilder={(params, query) => {
          return {
            pathname: '/repositories/:repoId/objects',
            params: params,
            query: { ...query, ref: reference.id, showChanges: 'true' }
          }
        }}
      />
    );

    return (
      <>
        {actionError && <AlertError error={actionError} onDismiss={() => setActionError(null)}/>}
        <ChangesTreeContainer 
          results={changesResults} 
          delimiter={delimiter}
          uriNavigator={changesUriNavigator}
          leftDiffRefID={committedRef} 
          rightDiffRefID={uncommittedRef}
          repo={repo} 
          reference={reference} 
          internalRefresh={internalRefresh} 
          prefix={path}
          getMore={getMoreUncommittedChanges}
          loading={loading} 
          nextPage={nextPage} 
          setLastSeenPath={setLastSeenPath}
          onNavigate={(entry) => {
            return {
              pathname: `/repositories/:repoId/objects`,
              params: {repoId: repo.id},
              query: {
                ref: reference.id,
                path: entry.path,
                showChanges: 'true'
              }
            }
          }} 
          onRevert={onReset}
          changesTreeMessage={<p>Showing {changesResults.length} change{changesResults.length !== 1 ? 's' : ''} for branch <strong>{reference.id}</strong></p>}
          noChangesText="No changes - you can modify this branch by uploading data using the UI or any of the supported SDKs"
          emptyStateComponent={<EmptyChangesState repo={repo} reference={reference} toggleShowChanges={toggleShowChangesOnly} />}
        />
      </>
    );
  }

  // Regular objects view
  return (
    <>
      {deleteState.error && <AlertError error={deleteState.error} onDismiss={() => setDeleteState(initialState)}/>}
      {actionError && <AlertError error={actionError} onDismiss={() => setActionError(null)}/>}
      <Tree
        config={{config}}
        repo={repo}
        reference={reference}
        path={(path) ? path : ""}
        showActions={true}
        results={mergedResults}
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
      />
    </>
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
  const { path, after, importDialog, upload, showChanges } = router.query;
  const [searchParams, setSearchParams] = useSearchParams();
  const { repo, reference, loading, error } = useRefs();
  const [showUpload, setShowUpload] = useState(false);
  const [showImport, setShowImport] = useState(false);
  const [refreshToken, setRefreshToken] = useState(false);
  const [showChangesOnly, setShowChangesOnly] = useState(showChanges === 'true');
  const [actionError, setActionError] = useState(null);
  const [hasChanges, setHasChanges] = useState(false);
  const [showRevertModal, setShowRevertModal] = useState(false);

  const refresh = () => {
    setRefreshToken(!refreshToken);
    // Also refresh changes status
    if (reference && reference.type === RefTypeBranch) {
      refs.changes(repo.id, reference.id, "", "", "/")
        .then(result => {
          setHasChanges(result.results && result.results.length > 0);
        })
        .catch(() => {
          setHasChanges(false);
        });
    }
  };
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

  useEffect(() => {
    if (upload) {
      setShowUpload(true);
      searchParams.delete("upload");
      setSearchParams(searchParams);
    }
  }, [router.route, upload, searchParams, setSearchParams]);

  // Check for changes when component loads or reference changes
  useEffect(() => {
    if (reference && reference.type === RefTypeBranch) {
      refs.changes(repo.id, reference.id, "", "", "/")
        .then(result => {
          setHasChanges(result.results && result.results.length > 0);
        })
        .catch(() => {
          setHasChanges(false);
        });
    } else {
      setHasChanges(false);
    }
  }, [repo?.id, reference?.id, refreshToken]);

  // Handle toggle changes view
  const handleToggleChanges = () => {
    const newShowChanges = !showChangesOnly;
    setShowChangesOnly(newShowChanges);
    
    const query = { path: path || "" };
    if (reference) query.ref = reference.id;
    if (newShowChanges) query.showChanges = 'true';
    
    router.push({
      pathname: `/repositories/:repoId/objects`,
      query,
      params: { repoId: repo.id },
    });
  };

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
          
          {/* Changes Management Button Group */}
          {reference && reference.type === RefTypeBranch && hasChanges && (
            <Dropdown as={ButtonGroup} className="me-2">
              {/* Toggle Switch */}
              <Button
                variant={showChangesOnly ? "secondary" : "outline-secondary"}
                size="sm"
                onClick={handleToggleChanges}
                className="d-flex align-items-center"
              >
                <NorthStarIcon className="me-1"/> 
                Uncommitted Changes
              </Button>
              
              {/* Actions Dropdown */}
              <Dropdown.Toggle 
                variant="outline-secondary" 
                size="sm"
                id="changes-dropdown"
              >

              </Dropdown.Toggle>
              
              <Dropdown.Menu className="changes-dropdown-menu">
                <div className="d-flex flex-column gap-2 p-2">
                  <Button
                    variant="success"
                    size="sm"
                    onClick={() => {
                      // Trigger the commit modal by finding the actual button and clicking it
                      const commitBtn = document.querySelector('[data-commit-btn] button');
                      if (commitBtn) {
                        commitBtn.click();
                      }
                    }}
                    disabled={repo?.read_only}
                    className="d-flex align-items-center justify-content-center changes-action-btn"
                  >
                    <GitCommitIcon className="me-1" />
                    Commit Changes
                  </Button>
                  
                  <Button
                    variant="outline-secondary"
                    size="sm"
                    onClick={() => setShowRevertModal(true)}
                    disabled={repo?.read_only}
                    className="d-flex align-items-center justify-content-center changes-action-btn"
                  >
                    <HistoryIcon className="me-1" />
                    Revert All Changes
                  </Button>
                </div>
              </Dropdown.Menu>
            </Dropdown>
          )}
        </ActionGroup>

        <ActionGroup orientation="right">
          {!showChangesOnly && (
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
          )}
          
          <RefreshButton onClick={refresh} />
          
          <Button
            variant="success"
            disabled={repo?.read_only}
            onClick={() => setShowUpload(true)}
          >
            <UploadIcon /> Upload
          </Button>
          
          <Button
            variant={!config.import_support ? "success" : "light"}
            disabled={!config.import_support}
            onClick={() => setShowImport(true)}
          >
            <BsCloudArrowUp /> Import
          </Button>
          
          {/* Hidden components for modals */}
          <div style={{ display: 'none' }}>
            <div data-commit-btn>
              <CommitButton 
                repo={repo} 
                enabled={hasChanges && !repo?.read_only} 
                onCommit={async (commitDetails, done) => {
                  try {
                    await commits.commit(repo.id, reference.id, commitDetails.message, commitDetails.metadata);
                    setActionError(null);
                    
                    // Reset to normal view after commit
                    setShowChangesOnly(false);
                    const query = { path: path || "" };
                    if (reference) query.ref = reference.id;
                    // Don't include showChanges parameter to go back to normal mode
                    router.push({
                      pathname: `/repositories/:repoId/objects`,
                      query,
                      params: { repoId: repo.id },
                    });
                    
                    refresh();
                  } catch (err) {
                    setActionError(err);
                  }
                  done();
                }}
              />
            </div>
            
            <UploadButton
              config={config}
              path={path}
              repo={repo}
              reference={reference}
              onDone={refresh}
              onClick={() => {}}
              onHide={() => {
                setShowUpload(false);
              }}
              show={showUpload}
              disabled={repo?.read_only}
            />
          </div>
          

          
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
          
          <ConfirmationModal
            show={showRevertModal}
            onHide={() => setShowRevertModal(false)}
            msg="Are you sure you want to revert all uncommitted changes?"
            onConfirm={() => {
              branches.reset(repo.id, reference.id, {type: 'reset'})
                .then(() => {
                  // Reset to normal view after revert
                  setShowChangesOnly(false);
                  const query = { path: path || "" };
                  if (reference) query.ref = reference.id;
                  // Don't include showChanges parameter to go back to normal mode
                  router.push({
                    pathname: `/repositories/:repoId/objects`,
                    query,
                    params: { repoId: repo.id },
                  });
                  
                  refresh();
                })
                .catch(error => setActionError(error));
              setShowRevertModal(false);
            }}
          />
        </ActionGroup>
      </ActionsBar>
      
      {actionError && <AlertError error={actionError} onDismiss={() => setActionError(null)}/>}

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
            if (showChangesOnly) query.showChanges = 'true';
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
          showChangesOnly={showChangesOnly}
          toggleShowChangesOnly={() => setShowChangesOnly(false)}
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
    const {config, loading: configsLoading, error: configsError} = useConfigContext();

    const [setActivePage] = useOutletContext();
    useEffect(() => setActivePage("objects"), [setActivePage]);

    if (configsLoading) return <Loading/>;
    if (configsError) return <RepoError error={configsError}/>;

    const {storageConfig, loading: configLoading, error: configError} = getRepoStorageConfig(config?.storages, repo);
    if (configLoading) return <Loading/>;
    if (configError) return <RepoError error={configError}/>;

    return <ObjectsBrowser config={storageConfig}/>;
};

export default RepositoryObjectsPage;
