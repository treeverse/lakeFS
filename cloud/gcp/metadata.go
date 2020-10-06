package gcp

import (
	"cloud.google.com/go/compute/metadata"
	"github.com/treeverse/lakefs/logging"
)

type MetadataProvider struct {
	logger logging.Logger
}

func NewMetadataProvider(logger logging.Logger) *MetadataProvider {
	return &MetadataProvider{logger: logger}
}

func (m *MetadataProvider) GetMetadata() map[string]string {
	projectID, err := metadata.NumericProjectID()
	if err != nil {
		m.logger.Warnf("%v: failed to get Google numeric project ID from instance metadata", err)
		return nil
	}
	return map[string]string{"google_numeric_project_id": projectID}
}
