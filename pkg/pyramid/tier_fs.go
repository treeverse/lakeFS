package pyramid

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/cache"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/pyramid/params"
)

// TierFS is a filesystem where written files are never edited.
// All files are stored in the block storage. Local paths are treated as a
// cache layer that will be evicted according to the eviction control.
type TierFS struct {
	logger         logging.Logger
	adapter        block.Adapter
	eviction       params.Eviction
	keyLock        cache.OnlyOne
	syncDir        *directory
	fileTracker    *fileTracker
	fsName         string
	fsLocalBaseDir string
	remotePrefix   string

	// dirDeleteCh is a channel for queuing directory deletion requests
	dirDeleteCh chan string
	// done is a channel to signal shutdown to the background worker
	done chan struct{}
	// wg is used to wait for the background worker to finish
	wg sync.WaitGroup
}

const (
	workspaceDir = "workspace"
	// dirDeleteChSize is the buffer size for the directory deletion channel
	dirDeleteChSize = 1000
)

// NewFS creates a new TierFS.
// It will traverse the existing local folders and will update
// the local disk cache to reflect existing files.
func NewFS(c *params.InstanceParams) (FS, error) {
	fsLocalBaseDir := filepath.Join(c.Local.BaseDir, c.FSName)
	if err := os.MkdirAll(fsLocalBaseDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("creating base dir: %s - %w", fsLocalBaseDir, err)
	}

	tfs := &TierFS{
		adapter:        c.Adapter,
		fsName:         c.FSName,
		logger:         c.Logger,
		fsLocalBaseDir: fsLocalBaseDir,
		syncDir:        &directory{ceilingDir: fsLocalBaseDir},
		keyLock:        cache.NewChanOnlyOne(),
		remotePrefix:   c.BlockStoragePrefix,
		dirDeleteCh:    make(chan string, dirDeleteChSize),
		done:           make(chan struct{}),
	}
	tfs.fileTracker = NewFileTracker(tfs.deleteLocalCacheFile)
	if c.Eviction == nil {
		var err error
		c.Eviction, err = newRistrettoEviction(c.AllocatedBytes(), tfs.removeFromLocal)
		if err != nil {
			return nil, fmt.Errorf("creating eviction control: %w", err)
		}
		c.Logger.WithFields(logging.Fields{
			"cache_name":        c.FSName,
			"cache_max_bytes":   c.AllocatedBytes(),
			"cache_local_dir":   fsLocalBaseDir,
			"disk_alloc_ratio":  c.DiskAllocProportion,
			"total_alloc_bytes": c.Local.TotalAllocatedBytes,
		}).Info("Initialized local cache for committed data")
	}

	tfs.eviction = c.Eviction
	if err := tfs.handleExistingFiles(); err != nil {
		return nil, fmt.Errorf("handling existing files: %w", err)
	}

	// Start background worker for directory deletions
	tfs.wg.Add(1)
	go tfs.dirDeleteWorker()

	return tfs, nil
}

// log returns a logger with added fields from ctx.
func (tfs *TierFS) log(ctx context.Context) logging.Logger {
	// TODO(ariels): Does this add the fields?  (Uses a different logging path...)
	return tfs.logger.WithContext(ctx)
}

// handleExistingFiles should only be called during init of the TierFS.
// It does 2 things:
//  1. Adds stored files to the eviction control
//  2. Remove workspace directories and all its content if it
//     exists under the namespace dir.
func (tfs *TierFS) handleExistingFiles() error {
	// setup cache dir location with separator as suffix. store are relative without prefix.
	dir := tfs.fsLocalBaseDir
	if !strings.HasSuffix(dir, string(filepath.Separator)) {
		dir += string(filepath.Separator)
	}
	// walk the local dir and add all files to the cache
	log := tfs.logger.WithField("dir", dir)
	if err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if errors.Is(err, os.ErrNotExist) {
			log.WithField("path", p).
				WithError(err).
				Warn("skipping not found file; another lakeFS might be sharing this cache directory - which is UNSUPPORTED")
			return nil
		}
		if err != nil {
			return err
		}
		if info.IsDir() {
			if info.Name() == workspaceDir {
				// skipping workspaces and saving them for later delete
				if err := os.RemoveAll(p); err != nil {
					return fmt.Errorf("removing dir: %w", err)
				}
				return filepath.SkipDir
			}
			return nil
		}

		rPath := strings.TrimPrefix(p, dir)
		tfs.storeLocalFile(params.RelativePath(rPath), info.Size())
		return nil
	}); err != nil {
		return fmt.Errorf("walking the fs dir: %w", err)
	}

	return nil
}

func (tfs *TierFS) removeFromLocal(rPath params.RelativePath, filesize int64) {
	// This will be called by the cache eviction mechanism during entry insert.
	// We don't want to wait while the file is being removed from the local disk.
	evictionHistograms.WithLabelValues(tfs.fsName).Observe(float64(filesize))
	// Notify tracker on delete
	tfs.fileTracker.Delete(rPath)
}

func (tfs *TierFS) deleteLocalCacheFile(rPath params.RelativePath) {
	fullPath := filepath.Join(tfs.fsLocalBaseDir, string(rPath))
	if tfs.logger.IsTracing() {
		tfs.logger.WithField("path", fullPath).Trace("Remove from local")
	}
	if err := os.Remove(fullPath); err != nil {
		tfs.logger.WithError(err).WithField("path", fullPath).Info("Removing file failed")
		errorsTotal.WithLabelValues(tfs.fsName, "FileRemoval").Inc()
		return
	}

	// Queue directory deletion (non-blocking) - use absolute path for deleteDirRecIfEmpty
	dirPath := filepath.Dir(fullPath)
	select {
	case tfs.dirDeleteCh <- dirPath:
	default:
		// Channel full, skip - directory will remain until next cleanup
		errorsTotal.WithLabelValues(tfs.fsName, "DirDeleteQueueFull").Inc()
	}
}

// dirDeleteWorker processes directory deletion requests from the channel.
// It runs as a background goroutine and exits when the done channel is closed.
func (tfs *TierFS) dirDeleteWorker() {
	defer tfs.wg.Done()
	for {
		select {
		case <-tfs.done:
			return
		case dirPath := <-tfs.dirDeleteCh:
			if err := tfs.syncDir.deleteDirRecIfEmpty(dirPath); err != nil {
				errorsTotal.WithLabelValues(tfs.fsName, "DirRemoval").Inc()
			}
		}
	}
}

// Close shuts down the TierFS, stopping the background worker and waiting for it to finish.
// The eviction cache is closed, but the cached files are preserved on disk for reuse after restart.
func (tfs *TierFS) Close() error {
	close(tfs.done)
	tfs.wg.Wait()
	return tfs.eviction.Close()
}

func (tfs *TierFS) store(ctx context.Context, storageID, namespace, originalPath, nsPath, filename string) error {
	if tfs.logger.IsTracing() {
		tfs.log(ctx).WithFields(logging.Fields{
			"storageID":     storageID,
			"namespace":     namespace,
			"original_path": originalPath,
			"ns_path":       nsPath,
			"filename":      filename,
		}).Trace("store")
	}

	f, err := os.Open(originalPath)
	if err != nil {
		return fmt.Errorf("open file %s: %w", originalPath, err)
	}

	stat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		if removeErr := os.Remove(originalPath); removeErr != nil {
			tfs.log(ctx).WithError(removeErr).WithField("original_path", originalPath).Warn("failed to remove temp file after stat failure")
		}
		return fmt.Errorf("file stat %s: %w", originalPath, err)
	}

	_, err = tfs.adapter.Put(ctx, tfs.objPointer(storageID, namespace, filename), stat.Size(), f, block.PutOpts{})
	if closeErr := f.Close(); closeErr != nil {
		tfs.log(ctx).WithError(closeErr).WithField("original_path", originalPath).Warn("failed to close file after put")
	}
	if err != nil {
		// Remove temp file on adapter.Put failure
		if removeErr := os.Remove(originalPath); removeErr != nil {
			tfs.log(ctx).WithError(removeErr).WithFields(logging.Fields{
				"storageID":     storageID,
				"namespace":     namespace,
				"original_path": originalPath,
			}).Warn("failed to remove temp file after put failure")
		}
		return fmt.Errorf("adapter put %s %s: %w", namespace, filename, err)
	}

	fileRef := tfs.newLocalFileRef(storageID, namespace, nsPath, filename)

	// Rename file to final path BEFORE calling Store() to ensure the file is at fullPath
	// when OnReject/OnEvict callbacks are invoked (they expect the file at fullPath).
	if err := tfs.syncDir.renameFile(originalPath, fileRef.fullPath); err != nil {
		return err
	}

	// Register with file tracker to protect against race with OnReject callback.
	// This ensures the file won't be deleted until we release the tracker.
	// Using defer guarantees closer() runs after any Delete() call, even if code is added later.
	closer := tfs.fileTracker.Open(fileRef.fsRelativePath)
	defer closer()

	if !tfs.eviction.Store(fileRef.fsRelativePath, stat.Size()) {
		// Rejected synchronously - mark for deletion.
		// The file will be deleted when closer() is called via defer.
		tfs.fileTracker.Delete(fileRef.fsRelativePath)
	}
	// Note: OnReject may also be called asynchronously for items that exceed MaxCost.
	// In that case, fileTracker.Delete() will be called again, which is safe because
	// the tracker handles duplicate deletes correctly.

	return nil
}

func (tfs *TierFS) GetRemoteURI(_ context.Context, filename string) (string, error) {
	return tfs.blockStoragePath(filename), nil
}

// Create creates a new file in TierFS.  File isn't stored in TierFS until a successful close
// operation.  Open(namespace, filename) calls will return an error before the close was
// called.  Create only performs local operations so it ignores the context.
func (tfs *TierFS) Create(_ context.Context, storageID, namespace string) (StoredFile, error) {
	nsPath, err := parseNamespacePath(storageID, namespace)
	if err != nil {
		return nil, err
	}
	if err := tfs.createNSWorkspaceDir(nsPath); err != nil {
		return nil, fmt.Errorf("create namespace dir: %w", err)
	}

	tempPath := tfs.workspaceTempFilePath(nsPath)
	fh, err := os.Create(tempPath)
	if err != nil {
		return nil, fmt.Errorf("creating file: %w", err)
	}

	return &WRFile{
		File:   fh,
		logger: tfs.logger,
		store: func(ctx context.Context, filename string) error {
			return tfs.store(ctx, storageID, namespace, tempPath, nsPath, filename)
		},
		abort: func(context.Context) error {
			return os.Remove(tempPath)
		},
	}, nil
}

// Open returns a file descriptor to the local file.
// If the file is missing from the local disk, it will try to fetch it from the block storage.
func (tfs *TierFS) Open(ctx context.Context, storageID, namespace, filename string) (File, error) {
	nsPath, err := parseNamespacePath(storageID, namespace)
	if err != nil {
		return nil, err
	}
	if err := validateFilename(filename); err != nil {
		return nil, err
	}

	// check if file is there - without taking the lock
	fileRef := tfs.newLocalFileRef(storageID, namespace, nsPath, filename)
	fh, err := os.Open(fileRef.fullPath)
	if err == nil {
		if tfs.logger.IsTracing() {
			tfs.log(ctx).WithFields(logging.Fields{
				"storageID": storageID,
				"namespace": namespace,
				"ns_path":   nsPath,
				"filename":  filename,
			}).Trace("opened locally")
		}
		cacheAccess.WithLabelValues(tfs.fsName, "Hit").Inc()
		return tfs.openFile(ctx, fileRef, fh)
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("open file: %w", err)
	}

	cacheAccess.WithLabelValues(tfs.fsName, "Miss").Inc()
	fh, err = tfs.openWithLock(ctx, fileRef)
	if err != nil {
		return nil, err
	}

	return tfs.openFile(ctx, fileRef, fh)
}

func (tfs *TierFS) Exists(ctx context.Context, storageID, namespace, filename string) (bool, error) {
	cacheAccess.WithLabelValues(tfs.fsName, "Exists").Inc()
	return tfs.adapter.Exists(ctx, tfs.objPointer(storageID, namespace, filename))
}

// openFile converts an os.File to pyramid.ROFile and updates the eviction control.
func (tfs *TierFS) openFile(ctx context.Context, fileRef localFileRef, fh *os.File) (*ROFile, error) {
	stat, err := fh.Stat()
	if err != nil {
		return nil, fmt.Errorf("file stat: %w", err)
	}

	// Register with file tracker BEFORE calling Store() to prevent race conditions.
	// The file tracker ensures the file won't be deleted while we have it open.
	// The closer will be called when ROFile.Close() is called.
	closer := tfs.fileTracker.Open(fileRef.fsRelativePath)

	if !tfs.eviction.Store(fileRef.fsRelativePath, stat.Size()) {
		// File rejected from cache - mark for deletion when all references are closed.
		// The file will be deleted when closer() is called (via ROFile.Close()).
		tfs.fileTracker.Delete(fileRef.fsRelativePath)
		tfs.log(ctx).WithFields(logging.Fields{
			"storageID": fileRef.storageID,
			"namespace": fileRef.namespace,
			"file":      fileRef.filename,
			"full_path": fileRef.fullPath,
		}).Info("stored file immediately rejected from cache (delete but continue)")
	}
	return &ROFile{
		File:     fh,
		rPath:    fileRef.fsRelativePath,
		eviction: tfs.eviction,
		closer:   closer,
	}, nil
}

// openWithLock reads the referenced file from the block storage
// and places it in the local FS for further reading.
// It returns a file handle to the local file.
// Note: File tracker registration is handled by openFile() which is called after this function.
func (tfs *TierFS) openWithLock(ctx context.Context, fileRef localFileRef) (*os.File, error) {
	log := tfs.log(ctx)
	if tfs.logger.IsTracing() {
		log.WithFields(logging.Fields{
			"storageID": fileRef.storageID,
			"namespace": fileRef.namespace,
			"file":      fileRef.filename,
			"fullpath":  fileRef.fullPath,
		}).Trace("try to lock for open")
	}

	fileFullPath, err := tfs.keyLock.Compute(fileRef.filename, func() (any, error) {
		// check again file existence, now that we have the lock
		_, err := os.Stat(fileRef.fullPath)
		if err == nil {
			if log.IsTracing() {
				log.WithFields(logging.Fields{
					"storageID": fileRef.storageID,
					"namespace": fileRef.namespace,
					"file":      fileRef.filename,
					"fullpath":  fileRef.fullPath,
				}).Trace("got lock; file exists after all")
			}
			cacheAccess.WithLabelValues(tfs.fsName, "Hit").Inc()

			return fileRef.fullPath, nil
		}
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("stat file: %w", err)
		}

		if log.IsTracing() {
			log.WithFields(logging.Fields{
				"storageID": fileRef.storageID,
				"namespace": fileRef.namespace,
				"file":      fileRef.filename,
				"fullpath":  fileRef.fullPath,
			}).Trace("get file from block storage")
		}
		// Use background context because Compute shares the result with multiple callers.
		// Using a caller's context could cause one caller's cancellation to fail the fetch for all waiting callers.
		reader, err := tfs.adapter.Get(context.Background(), tfs.objPointer(fileRef.storageID, fileRef.namespace, fileRef.filename))
		if err != nil {
			return nil, fmt.Errorf("read from block storage: %w", err)
		}
		defer func() { _ = reader.Close() }()

		// write to temp file - otherwise the file is available to other readers with partial data
		writer, err := tfs.syncDir.createTempFile(fileRef.fullPath)
		if err != nil {
			return nil, fmt.Errorf("creating file: %w", err)
		}
		tmpFullPath := writer.Name()
		written, err := io.Copy(writer, reader)
		if err != nil {
			_ = writer.Close()
			_ = os.Remove(tmpFullPath)
			return nil, fmt.Errorf("copying data to file: %w", err)
		}
		downloadHistograms.WithLabelValues(tfs.fsName).Observe(float64(written))

		if err = writer.Close(); err != nil {
			return nil, fmt.Errorf("writer close: %w", err)
		}

		// copy from temp path to actual path
		if log.IsTracing() {
			log.WithFields(logging.Fields{
				"storageID":    fileRef.storageID,
				"namespace":    fileRef.namespace,
				"file":         fileRef.filename,
				"tmp_fullpath": tmpFullPath,
				"fullpath":     fileRef.fullPath,
			}).Trace("rename downloaded file")
		}
		if err = tfs.syncDir.renameFile(tmpFullPath, fileRef.fullPath); err != nil {
			return nil, fmt.Errorf("rename temp file: %w", err)
		}

		return fileRef.fullPath, nil
	})
	if err != nil {
		return nil, err
	}

	fh, err := os.Open(fileFullPath.(string))
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	return fh, nil
}

func validateFilename(filename string) error {
	if strings.HasPrefix(filename, workspaceDir+string(os.PathSeparator)) {
		return errPathInWorkspace
	}
	if strings.Contains(filename, strings.Repeat(string(os.PathSeparator), 2)) { //nolint: mnd
		return errEmptyDirInPath
	}
	return nil
}

// localFileRef consists of all possible local file references
type localFileRef struct {
	// storageID is the block storage identifier
	storageID string
	// namespace is the storage namespace URL (e.g., "s3://bucket")
	namespace string
	// filename is the name of the file within the namespace
	filename string
	// fullPath is the absolute path to the file on the local filesystem
	fullPath string
	// fsRelativePath is the path relative to the TierFS base directory, used for cache eviction
	fsRelativePath params.RelativePath
}

func (tfs *TierFS) storeLocalFile(rPath params.RelativePath, size int64) {
	// Register with file tracker to coordinate with async OnReject callback.
	closer := tfs.fileTracker.Open(rPath)
	defer closer()

	if !tfs.eviction.Store(rPath, size) {
		// Rejected synchronously - mark for deletion.
		tfs.fileTracker.Delete(rPath)
		tfs.logger.WithFields(logging.Fields{
			"path": rPath,
			"size": size,
		}).Warn("existing file immediately rejected from cache on startup (safe if cache size changed)")
	}
	// Note: OnReject may also be called asynchronously for items that exceed MaxCost.
	// In that case, fileTracker.Delete() will be called, and the file will be deleted
	// when closer() is called (via defer).
}

func (tfs *TierFS) newLocalFileRef(storageID, namespace, nsPath, filename string) localFileRef {
	rPath := filepath.Join(nsPath, filename)
	return localFileRef{
		storageID:      storageID,
		namespace:      namespace,
		filename:       filename,
		fsRelativePath: params.RelativePath(rPath),
		fullPath:       filepath.Join(tfs.fsLocalBaseDir, rPath),
	}
}

func (tfs *TierFS) objPointer(storageID, namespace, filename string) block.ObjectPointer {
	return block.ObjectPointer{
		StorageID:        storageID,
		StorageNamespace: namespace,
		IdentifierType:   block.IdentifierTypeRelative,
		Identifier:       tfs.blockStoragePath(filepath.ToSlash(filename)),
	}
}

func (tfs *TierFS) blockStoragePath(filename string) string {
	return path.Join(tfs.remotePrefix, filename)
}

func (tfs *TierFS) createNSWorkspaceDir(namespace string) error {
	return os.MkdirAll(tfs.workspaceDirPath(namespace), os.ModePerm)
}

func (tfs *TierFS) workspaceDirPath(namespace string) string {
	return filepath.Join(tfs.fsLocalBaseDir, namespace, workspaceDir)
}

func (tfs *TierFS) workspaceTempFilePath(namespace string) string {
	return filepath.Join(tfs.workspaceDirPath(namespace), uuid.Must(uuid.NewRandom()).String())
}

// Convert the storageID and namespace to a filepath to be used for storage
func parseNamespacePath(storageID, namespace string) (string, error) {
	u, err := url.Parse(namespace)
	if err != nil {
		return "", fmt.Errorf("parse namespace: %w", err)
	}
	// extract host without port
	h := u.Host
	idx := strings.Index(h, ":")
	if idx != -1 {
		h = h[:idx]
	}
	// namespace path include host if found
	var nsPath string
	if h == "" {
		nsPath = u.Path
	} else {
		nsPath = h + "/" + u.Path
	}

	// If there is a non-empty storageID, we need to add another level to the path
	if storageID == config.SingleBlockstoreID {
		return nsPath, nil
	}
	return storageID + ":" + nsPath, nil
}
