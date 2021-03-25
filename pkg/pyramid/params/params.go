package params

import (
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/logging"
)

// RelativePath is the path of the file under TierFS, used in the Eviction interface.
type RelativePath string

// Eviction abstracts eviction control.
type Eviction interface {
	// Touch indicates the eviction that the file has been used now
	Touch(path RelativePath)

	// Store orders the eviction to Store the path.
	// returns true iff the eviction accepted the path.
	Store(path RelativePath, filesize int64) bool
}

// LocalDiskParams is pyramid.FS params that are identical for all file-systems
// in a single lakeFS instance.
type LocalDiskParams struct {
	// TotalAllocatedBytes is the maximum number of bytes an instance of TierFS can
	// allocate to local files.  It is not a hard limit - there may be short period of
	// times where TierFS uses more disk due to ongoing writes and slow disk cleanups.
	TotalAllocatedBytes int64

	// BaseDir names a directory for TierFS to store local copies of files.
	BaseDir string
}

type SharedParams struct {
	// Logger receives all logs for this FS.
	Logger logging.Logger

	// Local holds all local configuration.
	Local LocalDiskParams

	// Adapter is used to write to underlying storage.
	Adapter block.Adapter

	// BlockStoragePrefix is the prefix prepended to lakeFS metadata files in
	// the blockstore.
	BlockStoragePrefix string

	// Eviction is the cache to use to evict objects from local storage.  Only
	// configurable in testing.
	Eviction Eviction

	// PebbleSSTableCacheSizeBytes is the size (in bytes) of the cache used by the Pebble
	// SSTable library.  This cache is shared between all readers active on the system.
	PebbleSSTableCacheSizeBytes int64
}

type ExtParams struct {
	SharedParams

	// RangeAllocationProportion is the proportion allocated to range TierFS instance.
	// The rest of the allocation is to be used by the meta-range TierFS instance.
	// TODO(itai): make this configurable for more than 2 TierFS intances.
	RangeAllocationProportion     float64
	MetaRangeAllocationProportion float64
}

type InstanceParams struct {
	SharedParams

	// FSName is the unique filesystem name for this TierFS instance.
	// If two TierFS instances have the same name, behaviour is undefined.
	FSName string

	// DiskAllocProportion is the proportion of the SharedParams.LocalDiskParams.TotalAllocatedBytes the TierFS instance
	// is allowed to use. Each instance treats the multiplication of the two as its cap.
	DiskAllocProportion float64
}

// AllocatedBytes returns the maximum bytes an instance of TierFS is allowed to use.
func (ip InstanceParams) AllocatedBytes() int64 {
	return int64(ip.DiskAllocProportion * float64(ip.Local.TotalAllocatedBytes))
}

func (p ExtParams) WithLogger(logger logging.Logger) ExtParams {
	p.Logger = logger
	return p
}
