package pyramid

import (
	"context"
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
	if err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
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
	if err := os.Remove(p); err != nil {
		tfs.logger.WithError(err).WithField("path", p).Error("Removing file failed")
		errorsTotal.WithLabelValues(tfs.fsName, "FileRemoval")
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

func (tfs *TierFS) store(ctx context.Context, namespace, originalPath, nsPath, filename string) error {
	if tfs.logger.IsTracing() {
		tfs.log(ctx).WithFields(logging.Fields{
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
		return fmt.Errorf("file stat %s: %w", originalPath, err)
	}

	if err := tfs.adapter.Put(ctx, tfs.objPointer(namespace, filename), stat.Size(), f, block.PutOpts{}); err != nil {
		return fmt.Errorf("adapter put %s %s: %w", namespace, filename, err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("closing file %s: %w", filename, err)
	}

	fileRef := tfs.newLocalFileRef(namespace, nsPath, filename)
	if tfs.eviction.Store(fileRef.fsRelativePath, stat.Size()) {
		// file was stored by the policy
		return tfs.syncDir.renameFile(originalPath, fileRef.fullPath)
	} else {
		return os.Remove(originalPath)
	}
}

func (tfs *TierFS) GetRemoteURI(_ context.Context, _, filename string) (string, error) {
	return tfs.blockStoragePath(filename), nil
}

// Create creates a new file in TierFS.  File isn't stored in TierFS until a successful close
// operation.  Open(namespace, filename) calls will return an error before the close was
// called.  Create only performs local operations so it ignores the context.
func (tfs *TierFS) Create(_ context.Context, namespace string) (StoredFile, error) {
	nsPath, err := parseNamespacePath(namespace)
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
			return tfs.store(ctx, namespace, tempPath, nsPath, filename)
		},
		abort: func(context.Context) error {
			return os.Remove(tempPath)
		},
	}, nil
}

// Open returns a file descriptor to the local file.
// If the file is missing from the local disk, it will try to fetch it from the block storage.
func (tfs *TierFS) Open(ctx context.Context, namespace, filename string) (File, error) {
	nsPath, err := parseNamespacePath(namespace)
	if err != nil {
		return nil, err
	}
	if err := validateFilename(filename); err != nil {
		return nil, err
	}

	// check if file is there - without taking the lock
	fileRef := tfs.newLocalFileRef(namespace, nsPath, filename)
	if tfs.logger.IsTracing() {
		tfs.log(ctx).WithFields(logging.Fields{
			"file_full_path": fileRef.fullPath,
		}).Trace("Trying to open file")
	}
	fh, err := os.Open(fileRef.fullPath)
	if err == nil {
		if tfs.logger.IsTracing() {
			tfs.log(ctx).WithFields(logging.Fields{
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

func (tfs *TierFS) Exists(ctx context.Context, namespace, filename string) (bool, error) {
	cacheAccess.WithLabelValues(tfs.fsName, "Exists").Inc()
	return tfs.adapter.Exists(ctx, tfs.objPointer(namespace, filename))
}

// openFile converts an os.File to pyramid.ROFile and updates the eviction control.
func (tfs *TierFS) openFile(ctx context.Context, fileRef localFileRef, fh *os.File) (*ROFile, error) {
	stat, err := fh.Stat()
	if err != nil {
		return nil, fmt.Errorf("file stat: %w", err)
	}

	if !tfs.eviction.Store(fileRef.fsRelativePath, stat.Size()) {
		tfs.fileTracker.Delete(fileRef.fsRelativePath)
		tfs.log(ctx).WithFields(logging.Fields{
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

// openWithLock reads the referenced file from the block storage
// and places it in the local FS for further reading.
// It returns a file handle to the local file.
func (tfs *TierFS) openWithLock(ctx context.Context, fileRef localFileRef) (*os.File, error) {
	log := tfs.log(ctx)
	if tfs.logger.IsTracing() {
		log.WithFields(logging.Fields{
			"namespace": fileRef.namespace,
			"file":      fileRef.filename,
			"fullpath":  fileRef.fullPath,
		}).Trace("try to lock for open")
	}

	closer := tfs.fileTracker.Open(fileRef.fsRelativePath)
	defer closer()
	fileFullPath, err := tfs.keyLock.Compute(fileRef.filename, func() (interface{}, error) {
		// check again file existence, now that we have the lock
		_, err := os.Stat(fileRef.fullPath)
		if err == nil {
			if log.IsTracing() {
				log.WithFields(logging.Fields{
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
				"namespace": fileRef.namespace,
				"file":      fileRef.filename,
				"fullpath":  fileRef.fullPath,
			}).Trace("get file from block storage")
		}
		reader, err := tfs.adapter.Get(ctx, tfs.objPointer(fileRef.namespace, fileRef.filename), 0)
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
	if strings.Contains(filename, strings.Repeat(string(os.PathSeparator), 2)) { //nolint: gomnd
		return errEmptyDirInPath
	}
	return nil
}

// localFileRef consists of all possible local file references
type localFileRef struct {
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

func (tfs *TierFS) newLocalFileRef(namespace, nsPath, filename string) localFileRef {
	rPath := path.Join(nsPath, filename)
	return localFileRef{
		namespace:      namespace,
		filename:       filename,
		fsRelativePath: params.RelativePath(rPath),
		fullPath:       path.Join(tfs.fsLocalBaseDir, rPath),
	}
}

func (tfs *TierFS) objPointer(namespace, filename string) block.ObjectPointer {
	return block.ObjectPointer{
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

func parseNamespacePath(namespace string) (string, error) {
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
	return nsPath, nil
}
