package pyramid

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/treeverse/lakefs/block"
)

// ImmutableTierFS is a filesystem where written files are never edited.
// All files are stored in the block storage. Local paths are treated as a
// cache layer that will be evicted according to the given eviction algorithm.
type ImmutableTierFS struct {
	adaptor  block.Adapter
	eviction *evictionControl

	fsName string

	fsLocalBaseDir string
	remotePrefix   string
}

type Config struct {
	fsName  string
	adaptor block.Adapter

	fsBlockStoragePrefix string
	localBaseDir         string

	allocatedDiskSize int64
	estimatedFilesize int64
}

// NewTierFS creates a new TierFS.
// It will traverse the existing local folders and will update
// the local disk cache to reflect existing files.
func NewTierFS(c *Config) (FS, error) {
	fsLocalBaseDir := path.Join(c.localBaseDir, c.fsName)
	if err := os.MkdirAll(fsLocalBaseDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("creating base dir: %w", err)
	}

	tierFS := &ImmutableTierFS{
		adaptor:        c.adaptor,
		fsName:         c.fsName,
		fsLocalBaseDir: fsLocalBaseDir,
		remotePrefix:   path.Join(c.fsBlockStoragePrefix, c.fsName),
	}
	eviction, err := newEvictionControl(c.allocatedDiskSize, c.estimatedFilesize, tierFS.removeFromLocal)
	if err != nil {
		return nil, fmt.Errorf("creating eviction control :%w", err)
	}

	if err := addExistingFiles(eviction, fsLocalBaseDir); err != nil {
		return nil, fmt.Errorf("adding existing files to eviction:%w", err)
	}

	tierFS.eviction = eviction
	return tierFS, nil
}

func addExistingFiles(eviction *evictionControl, dir string) error {
	return filepath.Walk(dir, func(rPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			// nothing to do with dirs
			return nil
		}

		eviction.store(relativePath(rPath), info.Size())
		return nil
	})
}

func (tfs *ImmutableTierFS) removeFromLocal(rPath relativePath) {
	_ = os.Remove(path.Join(tfs.fsLocalBaseDir, string(rPath)))
}

// Store adds the local file to the FS.
func (tfs *ImmutableTierFS) Store(namespace, originalPath, filename string) error {
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

	if err := tfs.createNSDir(namespace); err != nil {
		return fmt.Errorf("create namespace dir: %w", err)
	}

	fileRef := tfs.newLocalFileRef(namespace, filename)
	if err := os.Rename(originalPath, fileRef.fullPath); err != nil {
		return fmt.Errorf("rename file: %w", err)
	}

	tfs.eviction.store(fileRef.fsRelativePath, stat.Size())
	return nil
}

func (tfs *ImmutableTierFS) Create(namespace, filename string) (*File, error) {
	if err := tfs.createNSDir(namespace); err != nil {
		return nil, fmt.Errorf("create namespace dir: %w", err)
	}

	fileRef := tfs.newLocalFileRef(namespace, filename)
	fh, err := os.Create(fileRef.fullPath)
	if err != nil {
		return nil, fmt.Errorf("creating file: %w", err)
	}

	return &File{
		fh:     fh,
		rPath:  fileRef.fsRelativePath,
		access: tfs.eviction,
		close:  tfs.adapterStore(fileRef, fh),
	}, nil
}

func (tfs *ImmutableTierFS) adapterStore(fileRef localFileRef, reader io.Reader) func(size int64) error {
	return func(size int64) error {
		tfs.eviction.store(fileRef.fsRelativePath, size)
		return tfs.adaptor.Put(tfs.objPointer(fileRef.namespace, fileRef.filename), size, reader, block.PutOpts{})
	}
}

// Load returns the a file descriptor to the local file.
// If the file is missing from the local disk, it will try to fetch it from the block storage.
func (tfs *ImmutableTierFS) Open(namespace, filename string) (*File, error) {
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
func (tfs *ImmutableTierFS) openFile(fileRef localFileRef, fh *os.File) (*File, error) {
	stat, err := fh.Stat()
	if err != nil {
		return nil, fmt.Errorf("file stat: %w", err)
	}

	tfs.eviction.store(fileRef.fsRelativePath, stat.Size())
	return &File{
		fh:     fh,
		rPath:  fileRef.fsRelativePath,
		access: tfs.eviction,
	}, nil
}

// readFromBlockStorage reads the referenced file from the block storage
// and places it in the local FS for further reading.
// It returns a file handle to the local file.
func (tfs *ImmutableTierFS) readFromBlockStorage(fileRef localFileRef) (*os.File, error) {
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

// relativePath is the path of the file under TierFS
type relativePath string

// localFileRef consists of all possible local file references
type localFileRef struct {
	namespace string
	filename  string

	fullPath       string
	fsRelativePath relativePath
}

func (tfs *ImmutableTierFS) newLocalFileRef(namespace, filename string) localFileRef {
	relative := path.Join(namespace, filename)
	return localFileRef{
		namespace: namespace,
		filename:  filename,

		fsRelativePath: relativePath(relative),
		fullPath:       path.Join(tfs.fsLocalBaseDir, relative),
	}
}

func (tfs *ImmutableTierFS) blockStoragePath(filename string) string {
	return path.Join(tfs.remotePrefix, filename)
}

func (tfs *ImmutableTierFS) objPointer(namespace, filename string) block.ObjectPointer {
	return block.ObjectPointer{
		StorageNamespace: namespace,
		Identifier:       tfs.blockStoragePath(filename),
	}
}

func (tfs *ImmutableTierFS) createNSDir(namespace string) error {
	dir := path.Join(tfs.fsLocalBaseDir, namespace)
	return os.MkdirAll(dir, os.ModePerm)
}
