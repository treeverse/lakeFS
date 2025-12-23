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
}

const workspaceDir = "workspace"

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
	}
	tfs.fileTracker = NewFileTracker(tfs.removeFromLocalInternal)
	if c.Eviction == nil {
		var err error
		c.Eviction, err = newRistrettoEviction(c.AllocatedBytes(), tfs.removeFromLocal)
		if err != nil {
			return nil, fmt.Errorf("creating eviction control: %w", err)
		}
	}

	tfs.eviction = c.Eviction
	if err := tfs.handleExistingFiles(); err != nil {
		return nil, fmt.Errorf("handling existing files: %w", err)
	}

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

func (tfs *TierFS) removeFromLocalInternal(rPath params.RelativePath) {
	p := path.Join(tfs.fsLocalBaseDir, string(rPath))
	if tfs.logger.IsTracing() {
		tfs.logger.WithField("path", p).Trace("remove from local")
	}
	if err := os.Remove(fullPath); err != nil {
		if os.IsNotExist(err) {
			// File already deleted - this is fine, can happen with async ristretto callbacks
			return
		}
		tfs.logger.WithError(err).WithField("path", fullPath).Info("Removing file failed")
		errorsTotal.WithLabelValues(tfs.fsName, "FileRemoval").Inc()
		return
	}

	// Delete Dir async
	go func() {
		if err := tfs.syncDir.deleteDirRecIfEmpty(path.Dir(string(rPath))); err != nil {
			tfs.logger.WithError(err).Error("Failed deleting empty dir")
			errorsTotal.WithLabelValues(tfs.fsName, "DirRemoval")
		}
	}()
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
	defer func() {
		if f != nil {
			// If there is an error, remove the temp file (in the happy path, it's removed at the end of the "store" func)
			if removeErr := os.Remove(originalPath); removeErr != nil {
				tfs.log(ctx).WithFields(logging.Fields{
					"storageID":     storageID,
					"namespace":     namespace,
					"original_path": originalPath,
				}).Error("failed to remove temp file")
			}
		}
	}()

	stat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("file stat %s: %w", originalPath, err)
	}

	if _, err = tfs.adapter.Put(ctx, tfs.objPointer(storageID, namespace, filename), stat.Size(), f, block.PutOpts{}); err != nil {
		return fmt.Errorf("adapter put %s %s: %w", namespace, filename, err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("closing file %s: %w", filename, err)
	}
	f = nil

	fileRef := tfs.newLocalFileRef(storageID, namespace, nsPath, filename)
	if tfs.eviction.Store(fileRef.fsRelativePath, stat.Size()) {
		// file was stored by the policy
		return tfs.syncDir.renameFile(originalPath, fileRef.fullPath)
	} else {
		return os.Remove(originalPath)
	}
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

	fileRef := tfs.newLocalFileRef(storageID, namespace, nsPath, filename)

	// Register with file tracker FIRST to prevent eviction during the open process.
	// This protects against a race where the file could be evicted between checking
	// its existence and opening it.
	closer := tfs.fileTracker.Open(fileRef.fsRelativePath)

	fh, err := tfs.openOrDownload(ctx, fileRef)
	if err != nil {
		closer()
		return nil, err
	}

	return tfs.wrapFile(ctx, fileRef, fh, closer)
}

func (tfs *TierFS) Exists(ctx context.Context, storageID, namespace, filename string) (bool, error) {
	cacheAccess.WithLabelValues(tfs.fsName, "Exists").Inc()
	return tfs.adapter.Exists(ctx, tfs.objPointer(storageID, namespace, filename))
}

// openOrDownload opens a local file, downloading it first if needed.
// The caller must already hold a file tracker reference to prevent eviction races.
func (tfs *TierFS) openOrDownload(ctx context.Context, fileRef localFileRef) (*os.File, error) {
	// Fast path: try to open directly
	fh, err := os.Open(fileRef.fullPath)
	if err == nil {
		if tfs.logger.IsTracing() {
			tfs.log(ctx).WithFields(logging.Fields{
				"storageID": fileRef.storageID,
				"namespace": fileRef.namespace,
				"file":      fileRef.filename,
				"fullpath":  fileRef.fullPath,
			}).Trace("opened locally")
		}
		cacheAccess.WithLabelValues(tfs.fsName, "Hit").Inc()
		return fh, nil
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("open file: %w", err)
	}

	// Slow path: ensure file exists (download if needed)
	cacheAccess.WithLabelValues(tfs.fsName, "Miss").Inc()
	if err := tfs.ensureFileExists(ctx, fileRef); err != nil {
		return nil, err
	}

	// Open after ensuring existence
	fh, err = os.Open(fileRef.fullPath)
	if err != nil {
		return nil, fmt.Errorf("open file after download: %w", err)
	}
	return fh, nil
}

// ensureFileExists ensures the file exists locally, downloading from block storage if needed.
// Uses Compute to deduplicate concurrent downloads of the same file.
func (tfs *TierFS) ensureFileExists(ctx context.Context, fileRef localFileRef) error {
	_, err := tfs.keyLock.Compute(fileRef.filename, func() (any, error) {
		// Check if file was downloaded by another goroutine while we waited
		if _, err := os.Stat(fileRef.fullPath); err == nil {
			if tfs.logger.IsTracing() {
				tfs.log(ctx).WithFields(logging.Fields{
					"storageID": fileRef.storageID,
					"namespace": fileRef.namespace,
					"file":      fileRef.filename,
					"fullpath":  fileRef.fullPath,
				}).Trace("got lock; file exists after all")
			}
			cacheAccess.WithLabelValues(tfs.fsName, "Hit").Inc()
			return nil, nil
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("stat file: %w", err)
		}

		return nil, tfs.downloadFromStorage(ctx, fileRef)
	})
	return err
}

// downloadFromStorage downloads a file from block storage to local disk.
func (tfs *TierFS) downloadFromStorage(ctx context.Context, fileRef localFileRef) error {
	if tfs.logger.IsTracing() {
		tfs.log(ctx).WithFields(logging.Fields{
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
		return fmt.Errorf("read from block storage: %w", err)
	}
	defer func() { _ = reader.Close() }()

	// Write to temp file first - otherwise the file is available to other readers with partial data
	writer, err := tfs.syncDir.createTempFile(fileRef.fullPath)
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpFullPath := writer.Name()

	written, err := io.Copy(writer, reader)
	if err != nil {
		_ = writer.Close()
		_ = os.Remove(tmpFullPath)
		return fmt.Errorf("copying data to file: %w", err)
	}
	downloadHistograms.WithLabelValues(tfs.fsName).Observe(float64(written))

	if err = writer.Close(); err != nil {
		_ = os.Remove(tmpFullPath)
		return fmt.Errorf("closing temp file: %w", err)
	}

	// Atomic rename to final path
	if tfs.logger.IsTracing() {
		tfs.log(ctx).WithFields(logging.Fields{
			"storageID":    fileRef.storageID,
			"namespace":    fileRef.namespace,
			"file":         fileRef.filename,
			"tmp_fullpath": tmpFullPath,
			"fullpath":     fileRef.fullPath,
		}).Trace("rename downloaded file")
	}
	if err = tfs.syncDir.renameFile(tmpFullPath, fileRef.fullPath); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}

	return nil
}

// wrapFile wraps an os.File in an ROFile with eviction tracking.
func (tfs *TierFS) wrapFile(ctx context.Context, fileRef localFileRef, fh *os.File, closer func()) (*ROFile, error) {
	stat, err := fh.Stat()
	if err != nil {
		closer()
		_ = fh.Close()
		return nil, fmt.Errorf("file stat: %w", err)
	}

	if !tfs.eviction.Store(fileRef.fsRelativePath, stat.Size()) {
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
	}, nil
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
	storageID      string
	namespace      string
	filename       string
	fullPath       string
	fsRelativePath params.RelativePath
}

func (tfs *TierFS) storeLocalFile(rPath params.RelativePath, size int64) {
	if !tfs.eviction.Store(rPath, size) {
		// Rejected from cache, so deleted.  This is safe, but can only happen when
		// the cache size was lowered -- so warn.
		tfs.logger.WithFields(logging.Fields{
			"path": rPath,
			"size": size,
		}).Warn("existing file immediately rejected from cache on startup (safe if cache size changed; continue)")

		// A rare occurrence, (currently) happens when Ristretto cache is not set up
		// to perform any caching.  So be less strict: prefer to serve the file and
		// delete it from the cache. It will be removed from disk when the last
		// surviving file descriptor -- returned from this function -- is closed.
		if err := os.Remove(string(rPath)); err != nil {
			tfs.logger.WithFields(logging.Fields{
				"path": rPath,
				"size": size,
			}).Error("failed to delete immediately-rejected existing file from cache on startup")
			return
		}
	}
}

func (tfs *TierFS) newLocalFileRef(storageID, namespace, nsPath, filename string) localFileRef {
	rPath := path.Join(nsPath, filename)
	return localFileRef{
		storageID:      storageID,
		namespace:      namespace,
		filename:       filename,
		fsRelativePath: params.RelativePath(rPath),
		fullPath:       path.Join(tfs.fsLocalBaseDir, rPath),
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
	return path.Join(tfs.fsLocalBaseDir, namespace, workspaceDir)
}

func (tfs *TierFS) workspaceTempFilePath(namespace string) string {
	return path.Join(tfs.workspaceDirPath(namespace), uuid.Must(uuid.NewRandom()).String())
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
	} else {
		return storageID + ":" + nsPath, nil
	}
}
