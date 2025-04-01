package block

import (
	"context"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/treeverse/lakefs/pkg/config"
)

const (
	MetadataBlockstoreTypeKey = "blockstore_type"
	MetadataBlockstoreCount   = "blockstore_count"
)

// MetadataProvider is a metadata provider that reports a single blockstore type.
type MetadataProvider struct {
	blockstoreType  string
	blockstoreCount int
}

// GetMetadata returns metadata with a blockstore type(s).
// in case there is more than one blockstore - the count is also reported.
func (p *MetadataProvider) GetMetadata(_ context.Context) (map[string]string, error) {
	m := map[string]string{
		MetadataBlockstoreTypeKey: p.blockstoreType,
	}
	if p.blockstoreCount > 1 {
		m[MetadataBlockstoreCount] = strconv.Itoa(p.blockstoreCount)
	}
	return m, nil
}

// NewMetadataProvider returns metadata provider to report blockstore type(s) based on storage config.
// It reports the blockstore type(s) and the count of blockstores.
// The blockstore type(s) are sorted and joined by comma.
func NewMetadataProvider(cfg config.StorageConfig) *MetadataProvider {
	ids := cfg.GetStorageIDs()

	var uniqueTypes []string
	for _, id := range ids {
		storage := cfg.GetStorageByID(id)
		blockstoreType := storage.BlockstoreType()
		if !slices.Contains(uniqueTypes, blockstoreType) {
			uniqueTypes = append(uniqueTypes, blockstoreType)
		}
	}
	sort.Strings(uniqueTypes)

	return &MetadataProvider{
		blockstoreType:  strings.Join(uniqueTypes, ","),
		blockstoreCount: len(ids),
	}
}
