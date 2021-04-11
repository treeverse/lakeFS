package stats

import (
	"context"

	"github.com/treeverse/lakefs/pkg/auth"
	azurestorage "github.com/treeverse/lakefs/pkg/block/azure"
	"github.com/treeverse/lakefs/pkg/block/gs"
	s3a "github.com/treeverse/lakefs/pkg/block/s3"
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

func NewMetadata(ctx context.Context, logger logging.Logger, blockstoreType string, authMetadataManager auth.MetadataManager, cloudMetadataProvider cloud.MetadataProvider) *Metadata {
	res := &Metadata{}
	authMetadata, err := authMetadataManager.Write(ctx)
	if err != nil {
		logger.WithError(err).Debug("failed to collect account metadata")
	}
	for k, v := range authMetadata {
		if k == auth.InstallationIDKeyName {
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

func BuildMetadataProvider(logger logging.Logger, c *config.Config) cloud.MetadataProvider {
	switch c.GetBlockstoreType() {
	case gs.BlockstoreType:
		return gcp.NewMetadataProvider(logger)
	case s3a.BlockstoreType:
		return aws.NewMetadataProvider(logger, c.GetAwsConfig())
	case azurestorage.BlockstoreType:
		return azure.NewMetadataProvider(logger)
	default:
		return nil
	}
}
