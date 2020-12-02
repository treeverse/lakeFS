package pyramid

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/treeverse/lakefs/block"
)

// TierFS is a filesystem where written files are never edited.
// All files are stored in the block storage. Local paths are treated as a
// cache layer that will be evicted according to the eviction control.
type TierFS struct {
	adaptor  block.Adapter
	eviction *lruSizeEviction

	fsName string

	fsLocalBaseDir string

	remotePrefix string
}

type Config struct {
	// fsName is the unique filesystem name for this TierFS instance.
	// If two TierFS instances have the same name, behaviour is undefined.
	fsName string

	adaptor block.Adapter

	// Prefix for all metadata file lakeFS stores in the block storage.
	fsBlockStoragePrefix string

	// The directory where TierFS files are kept locally.
	localBaseDir string

	// Maximum number of bytes an instance of TierFS can allocate to local files.
	// This is not a hard limit - there might be short period of times where TierFS
	// uses more disk due to ongoing writes and slow disk cleanups.
	allocatedDiskBytes int64

	// The average estimated file size in bytes.
	// Useful for minimizing memory consumption of the files in-mem cache.
	estimatedFileBytes int64
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
		fsLocalBaseDir: fsLocalBaseDir,
		remotePrefix:   path.Join(c.fsBlockStoragePrefix, c.fsName),
	}
	eviction, err := newLRUSizeEviction(c.allocatedDiskBytes, c.estimatedFileBytes, tierFS.removeFromLocal)
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
func handleExistingFiles(eviction *lruSizeEviction, fsLocalBaseDir string) error {
	var workspaceDirs []string
	if err := filepath.Walk(fsLocalBaseDir, func(rPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if info.Name() == workspaceDir {
				// skipping workspaces and saving them for later delete
				workspaceDirs = append(workspaceDirs, rPath)
				return filepath.SkipDir
			}
			return nil
		}

		if !eviction.store(relativePath(rPath), info.Size()) {
			// Cache rejected the file, can be removed from local storage.
			// Should be relatively rare, as cache sum cost on startup should accurately
			// represent previous cost limitations.
			removeFromLocal(fsLocalBaseDir, relativePath(rPath))
		}
		return nil
	}); err != nil {
		return fmt.Errorf("walking the fs dir: %w", err)
	}

	for _, dir := range workspaceDirs {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("removing dir: %w", err)
		}
	}

	return nil
}

func (tfs *TierFS) removeFromLocal(rPath relativePath) {
	removeFromLocal(tfs.fsLocalBaseDir, rPath)
}

func removeFromLocal(fsLocalBaseDir string, rPath relativePath) {
	_ = os.Remove(path.Join(fsLocalBaseDir, string(rPath)))
}

// Store adds the local file to the FS.
// on successful operation, file will no longer be available under the originalPath.
func (tfs *TierFS) Store(namespace, originalPath, filename string) error {
	if err := tfs.validateArgs(namespace, filename); err != nil {
		return fmt.Errorf("invalid args: %w", err)
	}
	return tfs.store(namespace, originalPath, filename)
}

func (tfs *TierFS) store(namespace, originalPath, filename string) error {
	f, err := os.Open(originalPath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("file stat: %w", err)
	}

	if err := tfs.adaptor.Put(tfs.objPointer(namespace, filename), stat.Size(), f, block.PutOpts{}); err != nil {
		return fmt.Errorf("adapter put: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("closing file: %w", err)
	}

	if err := tfs.createNSWorkspaceDir(namespace); err != nil {
		return fmt.Errorf("create namespace dir: %w", err)
	}

	fileRef := tfs.newLocalFileRef(namespace, filename)

	if tfs.eviction.store(fileRef.fsRelativePath, stat.Size()) {
		if err := os.Rename(originalPath, fileRef.fullPath); err != nil {
			return fmt.Errorf("rename file: %w", err)
		}
	} else {
		// cache rejected the insert, but that's ok -
		// it's already stored in the block storage.
		if err := os.Remove(originalPath); err != nil {
			return fmt.Errorf("remove file: %w", err)
		}
	}

	return nil
}

// Create creates a new file in TierFS.
// File isn't stored in TierFS until a successful close operation.
// Open(namespace, filename) calls will return an error before the close was called.
func (tfs *TierFS) Create(namespace, filename string) (*File, error) {
	if err := tfs.validateArgs(namespace, filename); err != nil {
		return nil, fmt.Errorf("invalid args: %w", err)
	}

	if err := tfs.createNSWorkspaceDir(namespace); err != nil {
		return nil, fmt.Errorf("create namespace dir: %w", err)
	}

	tempPath := tfs.workspaceFilePath(namespace, filename)
	fh, err := os.Create(tempPath)
	if err != nil {
		return nil, fmt.Errorf("creating file: %w", err)
	}

	return &File{
		fh:       fh,
		readOnly: false,
		eviction: tfs.eviction,
		store: func() error {
			return tfs.store(namespace, tempPath, filename)
		},
	}, nil
}

// Load returns the a file descriptor to the local file.
// If the file is missing from the local disk, it will try to fetch it from the block storage.
func (tfs *TierFS) Open(namespace, filename string) (*File, error) {
	fileRef := tfs.newLocalFileRef(namespace, filename)
	fh, err := os.Open(fileRef.fullPath)
	if err == nil {
		return tfs.openFile(fileRef, fh)
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("open file: %w", err)
	}

	fh, err = tfs.readFromBlockStorage(fileRef)
	if err != nil {
		return nil, err
	}

	return tfs.openFile(fileRef, fh)
}

// openFile converts an os.File to pyramid.File and updates the eviction control.
func (tfs *TierFS) openFile(fileRef localFileRef, fh *os.File) (*File, error) {
	stat, err := fh.Stat()
	if err != nil {
		return nil, fmt.Errorf("file stat: %w", err)
	}

	// No need to check for the store output here,
	// we already have the file.
	tfs.eviction.store(fileRef.fsRelativePath, stat.Size())
	return &File{
		fh:       fh,
		readOnly: true,
		rPath:    fileRef.fsRelativePath,
		eviction: tfs.eviction,
	}, nil
}

// readFromBlockStorage reads the referenced file from the block storage
// and places it in the local FS for further reading.
// It returns a file handle to the local file.
func (tfs *TierFS) readFromBlockStorage(fileRef localFileRef) (*os.File, error) {
	reader, err := tfs.adaptor.Get(tfs.objPointer(fileRef.namespace, fileRef.filename), 0)
	if err != nil {
		return nil, fmt.Errorf("read from block storage: %w", err)
	}
	defer reader.Close()

	writer, err := os.Create(fileRef.fullPath)
	if err != nil {
		return nil, fmt.Errorf("creating file: %w", err)
	}

	if _, err := io.Copy(writer, reader); err != nil {
		return nil, fmt.Errorf("copying date to file: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("writer close: %w", err)
	}

	fh, err := os.Open(fileRef.fullPath)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	return fh, nil
}

func (tfs *TierFS) validateArgs(namespace, filename string) error {
	if err := tfs.validateNamespace(namespace); err != nil {
		return err
	}

	return tfs.validateFilename(filename)
}

func (tfs *TierFS) validateFilename(filename string) error {
	return tfs.containsSeparator(filename)
}

func (tfs *TierFS) validateNamespace(ns string) error {
	return tfs.containsSeparator(ns)
}

var errSeparatorInFS = errors.New("path contains separator")

func (tfs *TierFS) containsSeparator(str string) error {
	if strings.ContainsRune(str, os.PathSeparator) {
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

func (tfs *TierFS) workspaceFilePath(namespace string, filename string) string {
	return path.Join(tfs.workspaceDirPath(namespace), filename)
}
