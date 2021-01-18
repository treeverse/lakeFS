package pyramid

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/cache"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/pyramid/params"

	"github.com/google/uuid"
)

// TierFS is a filesystem where written files are never edited.
// All files are stored in the block storage. Local paths are treated as a
// cache layer that will be evicted according to the eviction control.
type TierFS struct {
	logger  logging.Logger
	adapter block.Adapter

	eviction params.Eviction
	keyLock  cache.OnlyOne
	syncDir  *directory

	fsName string

	fsLocalBaseDir string

	remotePrefix string
}

const workspaceDir = "workspace"

// NewFS creates a new TierFS.
// It will traverse the existing local folders and will update
// the local disk cache to reflect existing files.
func NewFS(c *params.InstanceParams) (FS, error) {
	fsLocalBaseDir := filepath.Clean(path.Join(c.Local.BaseDir, c.FSName))
	if err := os.MkdirAll(fsLocalBaseDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("creating base dir: %s - %w", fsLocalBaseDir, err)
	}

	tierFS := &TierFS{
		adapter:        c.Adapter,
		fsName:         c.FSName,
		logger:         c.Logger,
		fsLocalBaseDir: fsLocalBaseDir,
		syncDir:        &directory{ceilingDir: fsLocalBaseDir},
		keyLock:        cache.NewChanOnlyOne(),
		remotePrefix:   path.Join(c.BlockStoragePrefix, c.FSName),
	}
	if c.Eviction == nil {
		var err error
		c.Eviction, err = newRistrettoEviction(c.AllocatedBytes(), tierFS.removeFromLocal)
		if err != nil {
			return nil, fmt.Errorf("creating eviction control: %w", err)
		}
	}

	tierFS.eviction = c.Eviction
	if err := handleExistingFiles(tierFS.eviction, fsLocalBaseDir); err != nil {
		return nil, fmt.Errorf("handling existing files: %w", err)
	}

	return tierFS, nil
}

// handleExistingFiles should only be called during init of the TierFS.
// It does 2 things:
// 1. Adds stored files to the eviction control
// 2. Remove workspace directories and all its content if it
//	  exist under the namespace dir.
func handleExistingFiles(eviction params.Eviction, fsLocalBaseDir string) error {
	if err := filepath.Walk(fsLocalBaseDir, func(rPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if info.Name() == workspaceDir {
				// skipping workspaces and saving them for later delete
				if err := os.RemoveAll(rPath); err != nil {
					return fmt.Errorf("removing dir: %w", err)
				}
				return filepath.SkipDir
			}
			return nil
		}

		if err := storeLocalFile(rPath, info.Size(), eviction); err != nil {
			return err
		}
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
	go tfs.removeFromLocalInternal(rPath)
}

func (tfs *TierFS) removeFromLocalInternal(rPath params.RelativePath) {
	p := path.Join(tfs.fsLocalBaseDir, string(rPath))
	tfs.logger.WithField("path", p).Trace("remove from local")
	if err := os.Remove(p); err != nil {
		tfs.logger.WithError(err).WithField("path", p).Error("Removing file failed")
		errorsTotal.WithLabelValues(tfs.fsName, "FileRemoval")
		return
	}

	if err := tfs.syncDir.deleteDirRecIfEmpty(path.Dir(p)); err != nil {
		tfs.logger.WithError(err).Error("Failed deleting empty dir")
		errorsTotal.WithLabelValues(tfs.fsName, "DirRemoval")
	}
}

func (tfs *TierFS) store(ctx context.Context, namespace, originalPath, nsPath, filename string) error {
	tfs.logger.WithFields(logging.Fields{
		"namespace":     namespace,
		"original_path": originalPath,
		"ns_path":       nsPath,
		"filename":      filename,
	}).Trace("store")

	f, err := os.Open(originalPath)
	if err != nil {
		return fmt.Errorf("open file %s: %w", originalPath, err)
	}

	stat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("file stat %s: %w", originalPath, err)
	}

	if err := tfs.adapter.WithContext(ctx).Put(tfs.objPointer(namespace, filename), stat.Size(), f, block.PutOpts{}); err != nil {
		return fmt.Errorf("adapter put %s: %w", filename, err)
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
		File: fh,
		store: func(ctx context.Context, filename string) error {
			return tfs.store(ctx, namespace, tempPath, nsPath, filename)
		},
		abort: func(context.Context) error {
			return os.Remove(tempPath)
		},
	}, nil
}

// Open returns the a file descriptor to the local file.
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
	fh, err := os.Open(fileRef.fullPath)
	if err == nil {
		tfs.logger.WithFields(logging.Fields{
			"namespace": namespace,
			"ns_path":   nsPath,
			"filename":  filename,
		}).Trace("opened locally")
		cacheAccess.WithLabelValues(tfs.fsName, "Hit").Inc()
		return tfs.openFile(fileRef, fh)
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("open file: %w", err)
	}

	cacheAccess.WithLabelValues(tfs.fsName, "Miss").Inc()
	fh, err = tfs.openWithLock(ctx, fileRef)
	if err != nil {
		return nil, err
	}

	return tfs.openFile(fileRef, fh)
}

func (tfs *TierFS) Exists(ctx context.Context, namespace, filename string) (bool, error) {
	cacheAccess.WithLabelValues(tfs.fsName, "Exists").Inc()
	return tfs.adapter.WithContext(ctx).Exists(tfs.objPointer(namespace, filename))
}

// openFile converts an os.File to pyramid.ROFile and updates the eviction control.
func (tfs *TierFS) openFile(fileRef localFileRef, fh *os.File) (*ROFile, error) {
	stat, err := fh.Stat()
	if err != nil {
		return nil, fmt.Errorf("file stat: %w", err)
	}

	if !tfs.eviction.Store(fileRef.fsRelativePath, stat.Size()) {
		// This is where we get less strict.
		// Ideally, newly fetched file will never be rejected by the cache.
		// But if it did, we prefer to serve the file and delete it.
		// When the user will close the file, the file will be deleted from the disk too.
		if err := os.Remove(fileRef.fullPath); err != nil {
			return nil, err
		}
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
	tfs.logger.WithFields(logging.Fields{
		"namespace": fileRef.namespace,
		"file":      fileRef.filename,
		"fullpath":  fileRef.fullPath,
	}).Trace("try to lock for open")
	_, err := tfs.keyLock.Compute(fileRef.filename, func() (interface{}, error) {
		// check again file existence, now that we have the lock
		_, err := os.Stat(fileRef.fullPath)
		if err == nil {
			tfs.logger.WithFields(logging.Fields{
				"namespace": fileRef.namespace,
				"file":      fileRef.filename,
				"fullpath":  fileRef.fullPath,
			}).Trace("got lock; file exists after all")
			cacheAccess.WithLabelValues(tfs.fsName, "Hit").Inc()
			return nil, nil
		}
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("stat file: %w", err)
		}

		tfs.logger.WithFields(logging.Fields{
			"namespace": fileRef.namespace,
			"file":      fileRef.filename,
			"fullpath":  fileRef.fullPath,
		}).Trace("get file from block storage")
		reader, err := tfs.adapter.WithContext(ctx).Get(tfs.objPointer(fileRef.namespace, fileRef.filename), 0)
		if err != nil {
			return nil, fmt.Errorf("read from block storage: %w", err)
		}
		defer reader.Close()

		// write to temp file - otherwise the file is available to other readers with partial data
		tmpFullPath := fileRef.fullPath + ".tmp"
		writer, err := tfs.syncDir.createFile(tmpFullPath)
		if err != nil {
			return nil, fmt.Errorf("creating file: %w", err)
		}

		written, err := io.Copy(writer, reader)
		if err != nil {
			return nil, fmt.Errorf("copying data to file: %w", err)
		}
		downloadHistograms.WithLabelValues(tfs.fsName).Observe(float64(written))

		if err = writer.Close(); err != nil {
			return nil, fmt.Errorf("writer close: %w", err)
		}

		// copy from temp path to actual path
		tfs.logger.WithFields(logging.Fields{
			"namespace":    fileRef.namespace,
			"file":         fileRef.filename,
			"tmp_fullpath": tmpFullPath,
			"fullpath":     fileRef.fullPath,
		}).Trace("rename downloaded file")
		if err = tfs.syncDir.renameFile(tmpFullPath, fileRef.fullPath); err != nil {
			return nil, fmt.Errorf("rename temp file: %w", err)
		}

		return nil, nil
	})

	if err != nil {
		return nil, err
	}

	fh, err := os.Open(fileRef.fullPath)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	return fh, nil
}

func storeLocalFile(rPath string, size int64, eviction params.Eviction) error {
	relativePath := params.RelativePath(rPath)
	if !eviction.Store(relativePath, size) {
		err := os.Remove(rPath)
		if err != nil {
			return fmt.Errorf("removing file: %w", err)
		}
	}
	return nil
}

func validateFilename(filename string) error {
	if strings.HasPrefix(filename, workspaceDir+string(os.PathSeparator)) {
		return errPathInWorkspace
	}
	if strings.Contains(filename, strings.Repeat(string(os.PathSeparator), 2)) {
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

func (tfs *TierFS) newLocalFileRef(namespace, nsPath, filename string) localFileRef {
	relative := path.Join(nsPath, filename)
	return localFileRef{
		namespace:      namespace,
		filename:       filename,
		fsRelativePath: params.RelativePath(relative),
		fullPath:       path.Join(tfs.fsLocalBaseDir, relative),
	}
}

func (tfs *TierFS) objPointer(namespace, filename string) block.ObjectPointer {
	if runtime.GOOS == "windows" {
		filename = strings.ReplaceAll(filename, `\\`, "/")
	}

	return block.ObjectPointer{
		StorageNamespace: namespace,
		Identifier:       tfs.blockStoragePath(filename),
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
