package block

import (
	"context"

	"github.com/treeverse/lakefs/pkg/config"
)

const MetadataBlockstoreTypeKey = "blockstore_type"

// BlockstoreTypeMetadataProvider is a metadata provider that reports a single blockstore type.
type BlockstoreTypeMetadataProvider struct {
	blockstoreType string
}

// GetMetadata returns metadata with a single blockstore type.
func (p *BlockstoreTypeMetadataProvider) GetMetadata(ctx context.Context) (map[string]string, error) {
	return map[string]string{MetadataBlockstoreTypeKey: p.blockstoreType}, nil
}

// BuildMetadataProviders returns metadata providers for each unique blockstore type in the storage config.
func BuildMetadataProviders(cfg config.StorageConfig) []*BlockstoreTypeMetadataProvider {
	ids := cfg.GetStorageIDs()

	uniqueTypes := make(map[string]struct{})
	for _, id := range ids {
		storage := cfg.GetStorageByID(id)
		uniqueTypes[storage.BlockstoreType()] = struct{}{}
	}

	var providers []*BlockstoreTypeMetadataProvider
	for t := range uniqueTypes {
		providers = append(providers, &BlockstoreTypeMetadataProvider{blockstoreType: t})
	}
	return providers
}
