package params

import (
	"fmt"
	"math"

	"github.com/mitchellh/go-homedir"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
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
	// TODO(itai): make this configurable for more than 2 TierFS instances.
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

var ErrInvalidProportion = fmt.Errorf("%w: total proportion isn't 1.0", config.ErrBadConfiguration)

// AllocatedBytes returns the maximum bytes an instance of TierFS is allowed to use.
func (ip InstanceParams) AllocatedBytes() int64 {
	return int64(ip.DiskAllocProportion * float64(ip.Local.TotalAllocatedBytes))
}

func (p ExtParams) WithLogger(logger logging.Logger) ExtParams {
	p.Logger = logger
	return p
}

// NewCommittedTierFSParams returns parameters for building a tierFS.
// Caller must separately build and populate Adapter.
func NewCommittedTierFSParams(c *config.BaseConfig, adapter block.Adapter) (*ExtParams, error) {
	const floatSumTolerance = 1e-6
	rangePro := c.Committed.LocalCache.RangeProportion
	metaRangePro := c.Committed.LocalCache.MetaRangeProportion
	if math.Abs(rangePro+metaRangePro-1) > floatSumTolerance {
		return nil, fmt.Errorf("range_proportion(%f) and metarange_proportion(%f): %w", rangePro, metaRangePro, ErrInvalidProportion)
	}

	localCacheDir, err := homedir.Expand(c.Committed.LocalCache.Dir)
	if err != nil {
		return nil, fmt.Errorf("expand %s: %w", c.Committed.LocalCache.Dir, err)
	}

	logger := logging.ContextUnavailable().WithField("module", "pyramid")
	return &ExtParams{
		RangeAllocationProportion:     rangePro,
		MetaRangeAllocationProportion: metaRangePro,
		SharedParams: SharedParams{
			Logger:             logger,
			Adapter:            adapter,
			BlockStoragePrefix: c.Committed.BlockStoragePrefix,
			Local: LocalDiskParams{
				BaseDir:             localCacheDir,
				TotalAllocatedBytes: c.Committed.LocalCache.SizeBytes,
			},
			PebbleSSTableCacheSizeBytes: c.Committed.SSTable.Memory.CacheSizeBytes,
		},
	}, nil
}
