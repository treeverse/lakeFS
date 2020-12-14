package pyramid

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/treeverse/lakefs/cache"
	"github.com/treeverse/lakefs/logging"

	"github.com/google/uuid"

	"github.com/treeverse/lakefs/block"
)

// TierFS is a filesystem where written files are never edited.
// All files are stored in the block storage. Local paths are treated as a
// cache layer that will be evicted according to the eviction control.
type TierFS struct {
	logger  logging.Logger
	adaptor block.Adapter

	eviction eviction
	keyLock  *cache.ChanLocker
	syncDir  *directory

	fsName string

	fsLocalBaseDir string

	remotePrefix string
}

type Config struct {
	// fsName is the unique filesystem name for this TierFS instance.
	// If two TierFS instances have the same name, behaviour is undefined.
	fsName string

	adaptor block.Adapter
	logger  logging.Logger

	// Prefix for all metadata file lakeFS stores in the block storage.
	fsBlockStoragePrefix string

	// The directory where TierFS files are kept locally.
	localBaseDir string

	// Maximum number of bytes an instance of TierFS can allocate to local files.
	// This is not a hard limit - there might be short period of times where TierFS
	// uses more disk due to ongoing writes and slow disk cleanups.
	allocatedDiskBytes int64
}

const workspaceDir = "workspace"

// NewFS creates a new TierFS.
// It will traverse the existing local folders and will update
// the local disk cache to reflect existing files.
func NewFS(c *Config) (FS, error) {
	fsLocalBaseDir := path.Join(c.localBaseDir, c.fsName)
	if err := os.MkdirAll(fsLocalBaseDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("creating base dir: %w", err)
	}

	tierFS := &TierFS{
		adaptor:        c.adaptor,
		fsName:         c.fsName,
		logger:         c.logger,
		fsLocalBaseDir: fsLocalBaseDir,
		syncDir:        &directory{ceilingDir: fsLocalBaseDir},
		keyLock:        cache.NewChanLocker(),
		remotePrefix:   path.Join(c.fsBlockStoragePrefix, c.fsName),
	}
	eviction, err := newLRUSizeEviction(c.allocatedDiskBytes, tierFS.removeFromLocal)
	if err != nil {
		return nil, fmt.Errorf("creating eviction control: %w", err)
	}

	if err := handleExistingFiles(eviction, fsLocalBaseDir); err != nil {
		return nil, fmt.Errorf("handling existing files: %w", err)
	}

	tierFS.eviction = eviction
	return tierFS, nil
}

// handleExistingFiles should only be called during init of the TierFS.
// It does 2 things:
// 1. Adds stored files to the eviction control
// 2. Remove workspace directories and all its content if it
//	  exist under the namespace dir.
func handleExistingFiles(eviction eviction, fsLocalBaseDir string) error {
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

		eviction.store(relativePath(rPath), info.Size())
		return nil
	}); err != nil {
		return fmt.Errorf("walking the fs dir: %w", err)
	}

	return nil
}

func (tfs *TierFS) removeFromLocal(rPath relativePath, filesize int64) {
	// This will be called by the cache eviction mechanism during entry insert.
	// We don't want to wait while the file is being removed from the local disk.
	evictionHistograms.WithLabelValues(tfs.fsName).Observe(float64(filesize))
	go tfs.removeFromLocalInternal(rPath)
}

func (tfs *TierFS) removeFromLocalInternal(rPath relativePath) {
	p := path.Join(tfs.fsLocalBaseDir, string(rPath))
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

func (tfs *TierFS) store(namespace, originalPath, filename string) error {
	f, err := os.Open(originalPath)
	if err != nil {
		return fmt.Errorf("open file %s: %w", originalPath, err)
	}

	stat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("file stat %s: %w", originalPath, err)
	}

	if err := tfs.adaptor.Put(tfs.objPointer(namespace, filename), stat.Size(), f, block.PutOpts{}); err != nil {
		return fmt.Errorf("adapter put %s: %w", filename, err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("closing file %s: %w", filename, err)
	}

	fileRef := tfs.newLocalFileRef(namespace, filename)

	tfs.eviction.store(fileRef.fsRelativePath, stat.Size())
	if err := tfs.syncDir.renameFile(originalPath, fileRef.fullPath); err != nil {
		return fmt.Errorf("rename file %s: %w", originalPath, err)
	}

	return nil
}

// Create creates a new file in TierFS.
// File isn't stored in TierFS until a successful close operation.
// Open(namespace, filename) calls will return an error before the close was called.
func (tfs *TierFS) Create(namespace string) (StoredFile, error) {
	if err := validateNamespace(namespace); err != nil {
		return nil, fmt.Errorf("invalid args: %w", err)
	}

	if err := tfs.createNSWorkspaceDir(namespace); err != nil {
		return nil, fmt.Errorf("create namespace dir: %w", err)
	}

	tempPath := tfs.workspaceTempFilePath(namespace)
	fh, err := os.Create(tempPath)
	if err != nil {
		return nil, fmt.Errorf("creating file: %w", err)
	}

	return &WRFile{
		File: fh,
		store: func(filename string) error {
			return tfs.store(namespace, tempPath, filename)
		},
	}, nil
}

// Open returns the a file descriptor to the local file.
// If the file is missing from the local disk, it will try to fetch it from the block storage.
func (tfs *TierFS) Open(namespace, filename string) (File, error) {
	if err := validateArgs(namespace, filename); err != nil {
		return nil, err
	}

	fileRef := tfs.newLocalFileRef(namespace, filename)
	fh, err := os.Open(fileRef.fullPath)
	if err == nil {
		cacheAccess.WithLabelValues(tfs.fsName, "Hit").Inc()
		return tfs.openFile(fileRef, fh)
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("open file: %w", err)
	}

	cacheAccess.WithLabelValues(tfs.fsName, "Miss").Inc()
	fh, err = tfs.readFromBlockStorage(fileRef)
	if err != nil {
		return nil, err
	}

	return tfs.openFile(fileRef, fh)
}

// openFile converts an os.File to pyramid.ROFile and updates the eviction control.
func (tfs *TierFS) openFile(fileRef localFileRef, fh *os.File) (*ROFile, error) {
	stat, err := fh.Stat()
	if err != nil {
		return nil, fmt.Errorf("file stat: %w", err)
	}

	// No need to check for the store output here,
	// we already have the file.
	tfs.eviction.store(fileRef.fsRelativePath, stat.Size())
	return &ROFile{
		File:     fh,
		rPath:    fileRef.fsRelativePath,
		eviction: tfs.eviction,
	}, nil
}

// readFromBlockStorage reads the referenced file from the block storage
// and places it in the local FS for further reading.
// It returns a file handle to the local file.
func (tfs *TierFS) readFromBlockStorage(fileRef localFileRef) (*os.File, error) {
	var e error
	tfs.keyLock.Lock(fileRef.filename, func() {
		reader, err := tfs.adaptor.Get(tfs.objPointer(fileRef.namespace, fileRef.filename), 0)
		if err != nil {
			e = fmt.Errorf("read from block storage: %w", err)
			return
		}
		defer reader.Close()

		writer, err := tfs.syncDir.createFile(fileRef.fullPath)
		if err != nil {
			e = fmt.Errorf("creating file: %w", err)
			return
		}

		written, err := io.Copy(writer, reader)
		if err != nil {
			e = fmt.Errorf("copying date to file: %w", err)
			return
		}

		if err := writer.Close(); err != nil {
			e = fmt.Errorf("writer close: %w", err)
		}
		downloadHistograms.WithLabelValues(tfs.fsName).Observe(float64(written))
	})

	if e != nil {
		return nil, e
	}

	fh, err := os.Open(fileRef.fullPath)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	return fh, nil
}

func validateArgs(namespace, filename string) error {
	if err := validateNamespace(namespace); err != nil {
		return err
	}
	return validateFilename(filename)
}

var (
	errSeparatorInFS   = errors.New("path contains separator")
	errPathInWorkspace = errors.New("file cannot be located in the workspace")
	errEmptyDirInPath  = errors.New("file path cannot contain an empty directory")
)

func validateFilename(filename string) error {
	if strings.HasPrefix(filename, workspaceDir+string(os.PathSeparator)) {
		return errPathInWorkspace
	}
	if strings.Contains(filename, strings.Repeat(string(os.PathSeparator), 2)) {
		return errEmptyDirInPath
	}
	return nil
}

func validateNamespace(ns string) error {
	if strings.ContainsRune(ns, os.PathSeparator) {
		return errSeparatorInFS
	}
	return nil
}

// relativePath is the path of the file under TierFS
type relativePath string

// localFileRef consists of all possible local file references
type localFileRef struct {
	namespace string
	filename  string

	fullPath       string
	fsRelativePath relativePath
}

func (tfs *TierFS) newLocalFileRef(namespace, filename string) localFileRef {
	relative := path.Join(namespace, filename)
	return localFileRef{
		namespace: namespace,
		filename:  filename,

		fsRelativePath: relativePath(relative),
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
