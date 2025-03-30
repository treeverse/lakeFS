package stats

import (
	"context"

	"github.com/treeverse/lakefs/pkg/logging"
)

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

func NewMetadata(ctx context.Context, logger logging.Logger, providers []MetadataProvider) *Metadata {
	res := &Metadata{}
	// collect metadata from providers
	for _, provider := range providers {
		m, err := provider.GetMetadata(ctx)
		if err != nil {
			// do not return at this point, just log the error.
			// we want to collect as much metadata as possible as some providers may return partial metadata.
			logger.WithError(err).Debug("failed to metadata from provider")
		}
		for k, v := range m {
			// keep installation id from provider, if exists
			if k == "installation_id" {
				res.InstallationID = v
			}
			res.Entries = append(res.Entries, MetadataEntry{Name: k, Value: v})
		}
	}
	return res
}
