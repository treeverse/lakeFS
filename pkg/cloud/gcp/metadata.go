package gcp

import (
	"crypto/md5" //nolint:gosec
	"fmt"

	"cloud.google.com/go/compute/metadata"
	"github.com/treeverse/lakefs/pkg/cloud"
	"github.com/treeverse/lakefs/pkg/logging"
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
	return map[string]string{
		cloud.IDKey:     fmt.Sprintf("%x", md5.Sum([]byte(projectID))), //nolint:gosec
		cloud.IDTypeKey: "gcp_project_numerical_id",
	}
}
