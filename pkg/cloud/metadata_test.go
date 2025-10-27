package cloud

import (
	"errors"
	"fmt"
	"testing"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/params"
	"github.com/treeverse/lakefs/pkg/config"
)

// MockStorageConfig implements the StorageConfig interface for testing cloud detection
type MockStorageConfig struct {
	storageIDs  []string
	storageByID map[string]config.AdapterConfig
	signingKey  config.SecureString
}

// NewMockStorageConfig creates a new MockStorageConfig with default values
func NewMockStorageConfig() *MockStorageConfig {
	return &MockStorageConfig{
		storageIDs:  []string{config.SingleBlockstoreID},
		storageByID: make(map[string]config.AdapterConfig),
		signingKey:  config.SecureString("test-signing-key"),
	}
}

// GetStorageByID returns the storage configuration for the given ID
func (m *MockStorageConfig) GetStorageByID(storageID string) config.AdapterConfig {
	if storage, exists := m.storageByID[storageID]; exists {
		return storage
	}
	// Return a default mock adapter if not found
	return &MockAdapterConfig{id: storageID}
}

// GetStorageIDs returns all available storage IDs
func (m *MockStorageConfig) GetStorageIDs() []string {
	return m.storageIDs
}

// SigningKey returns the signing key for this storage configuration
func (m *MockStorageConfig) SigningKey() config.SecureString {
	return m.signingKey
}

// SetStorageByID allows setting a specific storage configuration for testing
func (m *MockStorageConfig) SetStorageByID(storageID string, adapter config.AdapterConfig) {
	m.storageByID[storageID] = adapter
}

// SetStorageIDs allows setting the list of storage IDs for testing
func (m *MockStorageConfig) SetStorageIDs(storageIDs []string) {
	m.storageByID = make(map[string]config.AdapterConfig)
	for _, id := range storageIDs {
		m.storageByID[id] = &MockAdapterConfig{id: id}
	}
	m.storageIDs = storageIDs
}

// MockAdapterConfig implements the AdapterConfig interface for testing cloud detection
type MockAdapterConfig struct {
	id string
}

func (m *MockAdapterConfig) BlockstoreType() string {
	return block.BlockstoreTypeS3 // Always return s3 for cloud detection testing
}

func (m *MockAdapterConfig) BlockstoreDescription() string {
	return "Mock S3 blockstore for testing"
}

func (m *MockAdapterConfig) BlockstoreLocalParams() (params.Local, error) {
	return params.Local{}, errors.New("not implemented for S3 testing")
}

func (m *MockAdapterConfig) BlockstoreS3Params() (params.S3, error) {
	// Return S3 parameters that would be used for cloud detection
	return params.S3{
		Region:          "us-east-1",
		Profile:         "test-profile",
		CredentialsFile: "/tmp/test-credentials",
		Credentials: params.S3Credentials{
			AccessKeyID:     "test-access-key",
			SecretAccessKey: "test-secret-key",
			SessionToken:    "test-session-token",
		},
	}, nil
}

func (m *MockAdapterConfig) BlockstoreGSParams() (params.GS, error) {
	return params.GS{}, errors.New("not implemented for S3 testing")
}

func (m *MockAdapterConfig) BlockstoreAzureParams() (params.Azure, error) {
	return params.Azure{}, errors.New("not implemented for S3 testing")
}

func (m *MockAdapterConfig) GetDefaultNamespacePrefix() *string {
	prefix := "mock-prefix"
	return &prefix
}

func (m *MockAdapterConfig) IsBackwardsCompatible() bool {
	return true
}

func (m *MockAdapterConfig) ID() string {
	return m.id
}

// TestDetect verifies the cloud detection logic
func TestDetect(t *testing.T) {
	// Reset all detectors before the test
	Reset()

	// Register test detectors in specific order
	const (
		firstCloud  = "first_cloud"
		secondCloud = "second_cloud"
		secondID    = "second-id-456"
	)

	// Register a detector that fails
	RegisterDetector(firstCloud, func(storageConfig config.StorageConfig) (string, error) {
		return "", errors.New("detection failed")
	})

	// Register a detector that succeeds
	RegisterDetector(secondCloud, func(storageConfig config.StorageConfig) (string, error) {
		return secondID, nil
	})

	// Detect and verify the results
	// The second detector should be called since the first one failed
	cloudType, cloudID, detected := Detect(nil)
	if !detected {
		t.Error("Expected cloud to be detected")
	}
	if cloudType != secondCloud {
		t.Errorf("Expected cloud type to be '%s', got %s", secondCloud, cloudType)
	}
	if cloudID != secondID {
		t.Errorf("Expected cloud ID to be '%s', got '%s'", secondID, cloudID)
	}
}

// TestDetectWithDefaultDetectors verifies that only known clouds can be detected
func TestDetectWithDefaultDetectors(t *testing.T) {
	// Reset all detectors before the test
	Reset()

	// Register the default cloud detectors (AWS, GCP, Azure)
	RegisterDefaultDetectors()

	mockStorageConfig := NewMockStorageConfig()

	storageConfigs := []config.StorageConfig{mockStorageConfig, nil}
	for _, storageConfig := range storageConfigs {
		t.Run(fmt.Sprintf("storageConfig=%t", storageConfig != nil), func(t *testing.T) {
			cloudType, cloudID, detected := Detect(storageConfig)

			// If a cloud was detected, verify it's one of the known types
			if detected {
				switch cloudType {
				case AWSCloud, GCPCloud, AzureCloud:
					// Known cloud type detected - test passes
				default:
					t.Errorf("Detected unknown cloud type: %s", cloudType)
				}

				// Verify we got a non-empty cloud ID
				if cloudID == "" {
					t.Error("Cloud was detected but cloud ID is empty")
				}
			}
			// Note: if no cloud was detected, that's also a valid state
			// since the test might run in a non-cloud environment
		})
	}
}

// TestDetectorRegistrationOrder verifies that detectors are called in registration order
func TestDetectorRegistrationOrder(t *testing.T) {
	// Reset all detectors before the test
	Reset()

	const (
		firstCloud  = "first_cloud"
		secondCloud = "second_cloud"
		firstID     = "first-id-123"
		secondID    = "second-id-456"
	)

	// Register first detector that succeeds
	RegisterDetector(firstCloud, func(storageConfig config.StorageConfig) (string, error) {
		return firstID, nil
	})

	// Register second detector that also succeeds
	RegisterDetector(secondCloud, func(storageConfig config.StorageConfig) (string, error) {
		return secondID, nil
	})

	// Run detection - should return the first registered detector's result
	cloudType, cloudID, detected := Detect(nil)

	// Verify the results - should be from the first detector
	if !detected {
		t.Error("Expected cloud to be detected")
	}
	if cloudType != firstCloud {
		t.Errorf("Expected cloud type to be '%s' (first registered), got %s", firstCloud, cloudType)
	}
	if cloudID != firstID {
		t.Errorf("Expected cloud ID to be '%s' (from first detector), got '%s'", firstID, cloudID)
	}
}

// TestRegisterDetectorDuplicate verifies that duplicate detector registrations are handled properly
func TestRegisterDetectorDuplicate(t *testing.T) {
	// Reset all detectors before the test
	Reset()

	const (
		cloudName = "test_cloud"
		firstID   = "first-id"
		secondID  = "second-id"
	)

	// Register a detector
	RegisterDetector(cloudName, func(storageConfig config.StorageConfig) (string, error) {
		return firstID, nil
	})

	// Try to register the same detector again with different implementation
	RegisterDetector(cloudName, func(storageConfig config.StorageConfig) (string, error) {
		return secondID, nil
	})

	// Run detection - should use the first registration
	cloudType, cloudID, detected := Detect(nil)

	// Verify the results - should be from the first detector
	if !detected {
		t.Error("Expected cloud to be detected")
	}
	if cloudType != cloudName {
		t.Errorf("Expected cloud type to be '%s', got %s", cloudName, cloudType)
	}
	if cloudID != firstID {
		t.Errorf("Expected cloud ID to be '%s' (from first registration), got '%s'", firstID, cloudID)
	}
}
