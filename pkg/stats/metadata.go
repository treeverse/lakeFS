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
	lg := logger.WithField("trace_flow", true).WithField("method", "NewMetadata")
	lg.Info("trying to fetch md")
	res := &Metadata{}
	authMetadata, err := metadataProvider.GetMetadata(ctx)
	if err != nil {
		logger.WithError(err).Debug("failed to collect account metadata")
	}
	for k, v := range authMetadata {
		lg.Infof("KV: fetching auth md %s", k)
		if k == "installation_id" {
			res.InstallationID = v
		}
		res.Entries = append(res.Entries, MetadataEntry{Name: k, Value: v})
	}
	if cloudMetadataProvider != nil {
		lg.Info("starting cloud md provider: GetMetadata")
		cloudMetadata := cloudMetadataProvider.GetMetadata()
		lg.Info("end cloud md provider: GetMetadata")
		for k, v := range cloudMetadata {
			lg.Infof("GetMetadata: fetching md key %s", k)
			res.Entries = append(res.Entries, MetadataEntry{Name: k, Value: v})
		}
	}
	res.Entries = append(res.Entries, MetadataEntry{Name: BlockstoreTypeKey, Value: blockstoreType})
	lg.Info("finished fetching metadata layer from KV and Cloud")
	return res
}

type noopMetadataProvider struct{}

func (n *noopMetadataProvider) GetMetadata() map[string]string {
	return nil
}

func BuildMetadataProvider(logger logging.Logger, c *config.Config) cloud.MetadataProvider {
	lg := logger.WithField("trace_flow", true)
	lg.Infof("starting BuildMetadataProvider type: %s", block.BlockstoreTypeS3)
	switch c.Blockstore.Type {
	case block.BlockstoreTypeGS:
		return gcp.NewMetadataProvider(logger)
	case block.BlockstoreTypeS3:
		lg.Info("BuildMetadataProvider: getting blockstore params")
		s3Params, err := c.BlockstoreS3Params()
		if err != nil {
			logger.WithError(err).Warn("Failed to create S3 client for MetadataProvider")
			return &noopMetadataProvider{}
		}
		lg.Info("BuildMetadataProvider: creating metadata provider during build")
		provider, err := aws.NewMetadataProvider(logger, s3Params)
		if err != nil {
			logger.WithError(err).Warn("Failed to create S3 client for MetadataProvider")
			return &noopMetadataProvider{}
		}
		lg.Info("BuildMetadataProvider: finished creating metadata provider during build")
		return provider
	case block.BlockstoreTypeAzure:
		return azure.NewMetadataProvider(logger)
	}
	lg.Info("return md provider noop provider")
	return &noopMetadataProvider{}
}
