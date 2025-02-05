package stats

import (
	"context"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/cloud"
	"github.com/treeverse/lakefs/pkg/cloud/aws"
	"github.com/treeverse/lakefs/pkg/cloud/azure"
	"github.com/treeverse/lakefs/pkg/cloud/gcp"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

const BlockstoreTypeKey = "blockstore_type"

type MetadataEntry struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Metadata struct {
	InstallationID string          `json:"installation_id"`
	Entries        []MetadataEntry `json:"entries"`
}
type MetadataProvider interface {
	GetMetadata(context.Context) (map[string]string, error)
}

func NewMetadata(ctx context.Context, logger logging.Logger, blockstoreType string, metadataProvider MetadataProvider, cloudMetadataProvider cloud.MetadataProvider) *Metadata {
	res := &Metadata{}
	authMetadata, err := metadataProvider.GetMetadata(ctx)
	if err != nil {
		logger.WithError(err).Debug("failed to collect account metadata")
	}
	for k, v := range authMetadata {
		if k == "installation_id" {
			res.InstallationID = v
		}
		res.Entries = append(res.Entries, MetadataEntry{Name: k, Value: v})
	}
	if cloudMetadataProvider != nil {
		cloudMetadata := cloudMetadataProvider.GetMetadata()
		for k, v := range cloudMetadata {
			res.Entries = append(res.Entries, MetadataEntry{Name: k, Value: v})
		}
	}
	res.Entries = append(res.Entries, MetadataEntry{Name: BlockstoreTypeKey, Value: blockstoreType})
	return res
}

type noopMetadataProvider struct{}

func (n *noopMetadataProvider) GetMetadata() map[string]string {
	return nil
}

func BuildMetadataProvider(logger logging.Logger, c *config.BaseConfig) cloud.MetadataProvider {
	switch c.Blockstore.Type {
	case block.BlockstoreTypeGS:
		return gcp.NewMetadataProvider(logger)
	case block.BlockstoreTypeS3:
		s3Params, err := c.Blockstore.BlockstoreS3Params()
		if err != nil {
			logger.WithError(err).Warn("Failed to create S3 client for MetadataProvider")
			return &noopMetadataProvider{}
		}
		provider, err := aws.NewMetadataProvider(logger, s3Params)
		if err != nil {
			logger.WithError(err).Warn("Failed to create S3 client for MetadataProvider")
			return &noopMetadataProvider{}
		}
		return provider
	case block.BlockstoreTypeAzure:
		return azure.NewMetadataProvider(logger)
	}
	return &noopMetadataProvider{}
}
