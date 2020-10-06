package cloud

import (
	"github.com/treeverse/lakefs/block/gs"
	s3a "github.com/treeverse/lakefs/block/s3"
	"github.com/treeverse/lakefs/cloud/aws"
	"github.com/treeverse/lakefs/cloud/gcp"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/logging"
)

const (
	CloudIDTypeKey = "cloud_id_type"
	CloudIDKey     = "cloud_id"
)

type MetadataProvider interface {
	GetMetadata() map[string]string
}

func BuildMetadataProvider(logger logging.Logger, c *config.Config) MetadataProvider {
	switch c.GetBlockstoreType() {
	case gs.BlockstoreType:
		return gcp.NewMetadataProvider(logger)
	case s3a.BlockstoreType:
		return aws.NewMetadataProvider(logger, c.GetAwsConfig())
	default:
		return nil
	}
}
