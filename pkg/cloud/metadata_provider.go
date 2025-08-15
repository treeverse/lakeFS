package cloud

import (
	"context"
	"crypto/md5" //nolint:gosec
	"encoding/hex"

	"github.com/treeverse/lakefs/pkg/config"
)

// MetadataProvider collecting cloud environment metadata.
// Implements the stats.MetadataProvider interface
type MetadataProvider struct {
	metadata map[string]string
}

// NewMetadataProvider creates a new MetadataProvider and initializes metadata
// with cloud type and hashed ID if detected.
func NewMetadataProvider(storageConfig config.StorageConfig) *MetadataProvider {
	metadata := make(map[string]string)
	cloudType, cloudID, cloudDetected := Detect(storageConfig)
	if cloudDetected {
		metadata[cloudType] = hashCloudID(cloudID)
	}
	return &MetadataProvider{
		metadata: metadata,
	}
}

// GetMetadata returns cloud metadata information as a map
// It returns the detected cloud type and a hashed version of the cloud ID
func (p *MetadataProvider) GetMetadata(ctx context.Context) (map[string]string, error) {
	return p.metadata, nil
}

// hashCloudID hashes the cloud ID to protect sensitive information
func hashCloudID(cloudID string) string {
	s := md5.Sum([]byte(cloudID)) //nolint:gosec
	return hex.EncodeToString(s[:])
}
