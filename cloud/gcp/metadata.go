package gcp

import (
	"crypto/md5"
	"fmt"

	"cloud.google.com/go/compute/metadata"
	"github.com/treeverse/lakefs/cloud"
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
	hashedProjectID := fmt.Sprintf("%x", md5.Sum([]byte(projectID)))
	return map[string]string{cloud.IDKey: hashedProjectID, cloud.IDTypeKey: "gcp_project_numerical_id"}
}
