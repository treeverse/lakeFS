package cloud

import (
	"context"
)

// MetadataProvider collecting cloud environment metadata.
// Implements the stats.MetadataProvider interface
type MetadataProvider struct{}

// NewMetadataProvider creates a new MetadataProvider
func NewMetadataProvider() *MetadataProvider {
	return &MetadataProvider{}
}

// GetMetadata returns cloud metadata information as a map
// It returns the detected cloud type and a hashed version of the cloud ID
func (p *MetadataProvider) GetMetadata(ctx context.Context) (map[string]string, error) {
	metadata := make(map[string]string)

	// Get cloud metadata using the existing function
	cloudType, cloudID, cloudDetected := GetHashedInformation()
	if !cloudDetected {
		return metadata, nil
	}

	// Add the cloud type and ID to the metadata map
	metadata[cloudType] = cloudID

	return metadata, nil
}
