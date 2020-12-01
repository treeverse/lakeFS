package pyramid

// Params is pyramid.FS params that are identical for all file-systems
// in a single lakeFS instance.
type Params struct {
	// AllocatedBytes is the disk size in bytes that lakeFS is allowed to use.
	AllocatedBytes int64

	// BaseDir is the local directory where lakeFS app is storing the files.
	BaseDir string

	// BlockStoragePrefix is the prefix prepended to lakeFS metadata files in
	// the blockstore.
	BlockStoragePrefix string
}
