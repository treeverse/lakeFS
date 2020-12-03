package pyramid

// FS is pyramid abstraction of filesystem where the persistent storage-layer is the block storage.
// Files on the local disk are transient and might be cleaned up by the eviction policy.
// File structure under a namespace and namespace itself are flat (no directories).
type FS interface {
	// Create creates a new file in the FS.
	// It will only be persistent after the returned file is stored.
	Create(namespace string) (*File, error)

	// Open finds the referenced file and returns its read-only File.
	// If file isn't in the local disk, it is fetched from the block storage.
	Open(namespace, filename string) (*ROFile, error)
}
