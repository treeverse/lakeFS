package stats

import (
	"context"

	"github.com/treeverse/lakefs/pkg/cloud"
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

func NewMetadata(ctx context.Context, logger logging.Logger, blockstoreType string, metadataProvider MetadataProvider) *Metadata {
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
	// add cloud metadata
	cloudType, cloudID, cloudDetected := cloud.GetMetadata()
	if cloudDetected {
		res.Entries = append(res.Entries, MetadataEntry{Name: cloudType, Value: cloudID})
	}

	// add blockstore metadata
	res.Entries = append(res.Entries, MetadataEntry{Name: BlockstoreTypeKey, Value: blockstoreType})
	return res
}
