package stats

import (
	"context"

	"github.com/treeverse/lakefs/pkg/auth"
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

type MetadataManager interface {
	Write(context.Context) (map[string]string, error)
}

func NewMetadata(ctx context.Context, logger logging.Logger, blockstoreType string, statsMetadataManager MetadataManager, cloudMetadataProvider cloud.MetadataProvider) *Metadata {
	res := &Metadata{}
	authMetadata, err := statsMetadataManager.Write(ctx)
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
	switch c.BlockstoreType() {
	case block.BlockstoreTypeGS:
		return gcp.NewMetadataProvider(logger)
	case block.BlockstoreTypeS3:
		return aws.NewMetadataProvider(logger, c.GetAwsConfig())
	case block.BlockstoreTypeAzure:
		return azure.NewMetadataProvider(logger)
	default:
		return nil
	}
}
