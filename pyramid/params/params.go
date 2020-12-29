package params

import (
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/logging"
)

// RelativePath is the path of the file under TierFS, used in the Eviction interface.
type RelativePath string

// Eviction abstracts eviction control.
type Eviction interface {
	// Touch indicates the eviction that the file has been used now
	Touch(rPath RelativePath)

	// Store orders the eviction to Store the path.
	// returns true iff the eviction accepted the path.
	Store(rPath RelativePath, filesize int64) bool
}

// LocalDiskParams is pyramid.FS params that are identical for all file-systems
// in a single lakeFS instance.
type LocalDiskParams struct {
	// AllocatedDiskBytes is the maximum number of bytes an instance of TierFS can
	// allocate to local files.  It is not a hard limit - there may be short period of
	// times where TierFS uses more disk due to ongoing writes and slow disk cleanups.
	AllocatedBytes int64

	// BaseDir names a directory for TierFS to store local copies of files.
	BaseDir string
}

type Params struct {
	// FSName is the unique filesystem name for this TierFS instance.
	// If two TierFS instances have the same name, behaviour is undefined.
	FSName string

	// Logger receives all logs for this FS.
	Logger logging.Logger

	// Local holds all local configuration.
	Local LocalDiskParams

	// Adapter is used to write to underlying storage.
	Adapter block.Adapter

	// BlockStoragePrefix is the prefix prepended to lakeFS metadata files in
	// the blockstore.
	BlockStoragePrefix string

	// eviction is the cache to use to evict objects from local storage.  Only
	// configurable in testing.
	Eviction Eviction
}

func (p Params) WithLogger(logger logging.Logger) Params {
	p.Logger = logger
	return p
}
