package pyramid

//go:generate mockgen -source=pyramid.go -destination=mock/pyramid.go -package=mock

import (
	"io"
	"os"
)

// FS is pyramid abstraction of filesystem where the persistent storage-layer is the block storage.
// Files on the local disk are transient and might be cleaned up by the eviction policy.
// File structure under a namespace and namespace itself are flat (no directories).
type FS interface {
	// Create creates a new file in the FS.
	// It will only be persistent after the returned file is stored.
	Create(namespace string) (StoredFile, error)

	// Open finds the referenced file and returns its read-only File.
	// If file isn't in the local disk, it is fetched from the block storage.
	Open(namespace, filename string) (File, error)
}

// File is pyramid abstraction for an os.File
type File interface {
	io.Reader
	io.Writer
	io.Closer
	io.ReaderAt
	Sync() error
	Stat() (os.FileInfo, error)
}

// StoredFile is pyramid abstraction for an os.File with
// a Store operation that makes the file persistent
type StoredFile interface {
	File

	// Successful Store operation guarantees that the file is persistent.
	// If the file wasn't closed, Store closes it.
	Store(filename string) error

	// Abort removes all traces of the file from the filesystem.
	// It's allowed to call Abort on the file at any stage, unless the file was already stored.
	Abort() error
}
