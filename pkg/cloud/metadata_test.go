package cloud

import (
	"errors"
	"testing"

	"github.com/treeverse/lakefs/pkg/config"
)

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

	// Create a mock storage config
	var mockStorageConfig config.StorageConfig

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
	cloudType, cloudID, detected := Detect(mockStorageConfig)
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

	// Create a mock storage config
	var mockStorageConfig config.StorageConfig

	// Run detection
	cloudType, cloudID, detected := Detect(mockStorageConfig)

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

	// Create a mock storage config
	var mockStorageConfig config.StorageConfig

	// Register first detector that succeeds
	RegisterDetector(firstCloud, func(storageConfig config.StorageConfig) (string, error) {
		return firstID, nil
	})

	// Register second detector that also succeeds
	RegisterDetector(secondCloud, func(storageConfig config.StorageConfig) (string, error) {
		return secondID, nil
	})

	// Run detection - should return the first registered detector's result
	cloudType, cloudID, detected := Detect(mockStorageConfig)

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

	// Create a mock storage config
	var mockStorageConfig config.StorageConfig

	// Register a detector
	RegisterDetector(cloudName, func(storageConfig config.StorageConfig) (string, error) {
		return firstID, nil
	})

	// Try to register the same detector again with different implementation
	RegisterDetector(cloudName, func(storageConfig config.StorageConfig) (string, error) {
		return secondID, nil
	})

	// Run detection - should use the first registration
	cloudType, cloudID, detected := Detect(mockStorageConfig)

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
