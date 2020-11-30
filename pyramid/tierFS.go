package pyramid

import (
	"fmt"
	"io"
	"os"
	"path"

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

const (
	fsBlockStoragePrefix = "_lakeFS"

	// TODO: to flags
	localBaseDir      = "/local/lakeFS"
	estimatedFilesize = 10 * 1024 * 1024
)

func NewTierFS(fsName string, adaptor block.Adapter, allocatedDiskSize int64) (FS, error) {
	fsLocalBaseDir := path.Join(localBaseDir, fsName)
	if err := os.MkdirAll(fsLocalBaseDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("creating base dir: %w", err)
	}

	// TODO(itai): handle files that exist in dir on start-up:
	// 1. discover namespaces
	// 2. fill eviction-control with the files

	tierFS := &ImmutableTierFS{
		adaptor:        adaptor,
		fsName:         fsName,
		fsLocalBaseDir: fsLocalBaseDir,
		remotePrefix:   path.Join(fsBlockStoragePrefix, fsName),
	}
	eviction, err := newEvictionControl(allocatedDiskSize, estimatedFilesize, tierFS.removeFromLocal)
	if err != nil {
		return nil, fmt.Errorf("creating eviction control :%w", err)
	}

	tierFS.eviction = eviction
	return tierFS, nil
}

func (tfs *ImmutableTierFS) removeFromLocal(localPath string) {
	_ = os.Remove(localPath)
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

	localpath := tfs.localpath(namespace, filename)
	if err := os.Rename(originalPath, localpath); err != nil {
		return fmt.Errorf("rename file: %w", err)
	}

	tfs.eviction.store(filename, stat.Size())
	return nil
}

func (tfs *ImmutableTierFS) Create(namespace, filename string) (*File, error) {
	if err := tfs.createNSDir(namespace); err != nil {
		return nil, fmt.Errorf("create namespace dir: %w", err)
	}

	localpath := tfs.localpath(namespace, filename)
	fh, err := os.Create(localpath)
	if err != nil {
		return nil, fmt.Errorf("creating file: %w", err)
	}

	return &File{
		fh:        fh,
		localpath: localpath,
		access:    tfs.eviction,
		close:     tfs.adapterStore(namespace, filename, localpath, fh),
	}, nil
}

func (tfs *ImmutableTierFS) adapterStore(namespace string, filename string, localpath string, fh *os.File) func(size int64) error {
	return func(size int64) error {
		tfs.eviction.store(localpath, size)
		return tfs.adaptor.Put(tfs.objPointer(namespace, filename), size, fh, block.PutOpts{})
	}
}

// Load returns the a file descriptor to the local file.
// If the file is missing from the local disk, it will try to fetch it from the block storage.
func (tfs *ImmutableTierFS) Open(namespace, filename string) (*File, error) {
	localPath := tfs.localpath(namespace, filename)
	fh, err := os.Open(localPath)
	if err == nil {
		return tfs.openFile(localPath, fh)
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("open file: %w", err)
	}

	fh, err = tfs.readFromBlockStorage(namespace, filename)
	if err != nil {
		return nil, err
	}

	tfs.eviction.touch(localPath)
	return tfs.openFile(localPath, fh)
}

func (tfs *ImmutableTierFS) openFile(localPath string, fh *os.File) (*File, error) {
	stat, err := fh.Stat()
	if err != nil {
		return nil, fmt.Errorf("file stat: %w", err)
	}

	tfs.eviction.store(localPath, stat.Size())

	return &File{
		fh:        fh,
		localpath: localPath,
		access:    tfs.eviction,
	}, nil
}

func (tfs *ImmutableTierFS) readFromBlockStorage(namespace, filename string) (*os.File, error) {
	reader, err := tfs.adaptor.Get(tfs.objPointer(namespace, filename), 0)
	if err != nil {
		return nil, fmt.Errorf("read from block storage: %w", err)
	}
	defer reader.Close()

	localPath := tfs.localpath(namespace, filename)
	writer, err := os.Create(localPath)
	if err != nil {
		return nil, fmt.Errorf("creating file: %w", err)
	}

	if _, err := io.Copy(writer, reader); err != nil {
		return nil, fmt.Errorf("copying date to file: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("writer close: %w", err)
	}

	fh, err := os.Open(localPath)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	return fh, nil
}

func (tfs *ImmutableTierFS) localpath(namespace, filename string) string {
	return path.Join(tfs.fsLocalBaseDir, namespace, filename)
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
