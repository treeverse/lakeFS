package cloud

import (
	"errors"
	"testing"
)

// TestDetect verifies the cloud detection logic
func TestDetect(t *testing.T) {
	// Reset all detectors before the test
	Reset()

	// Register test detectors in specific order
	const (
		firstCloud  = "first_cloud"
		secondCloud = "second_cloud"
		firstID     = "first-id-123"
		secondID    = "second-id-456"
	)

	// Register a detector that fails
	RegisterDetector(firstCloud, func() (string, error) {
		return "", errors.New("detection failed")
	})

	// Register a detector that succeeds
	RegisterDetector(secondCloud, func() (string, error) {
		return secondID, nil
	})

	// Run detection
	Detect()

	// Verify the results
	if !cloudDetected {
		t.Error("Expected cloud to be detected")
	}
	if cloudType != secondCloud {
		t.Errorf("Expected cloud type to be '%s', got %s", secondCloud, cloudType)
	}
	if cloudID != secondID {
		t.Errorf("Expected cloud ID to be '%s', got '%s'", secondID, cloudID)
	}
}

// TestGetHashedInformation verifies the hashing and caching behavior
func TestGetHashedInformation(t *testing.T) {
	// Reset all detectors before the test
	Reset()

	// Register a test detector
	const (
		testCloud = "test_cloud"
		testID    = "test-id-789"
	)
	RegisterDetector(testCloud, func() (string, error) {
		return testID, nil
	})

	// First call should detect and hash
	cloudType, cloudID, detected := GetHashedInformation()
	if !detected {
		t.Fatal("Expected cloud to be detected")
	}
	if cloudType != testCloud {
		t.Fatalf("Expected cloud type to be '%s', got %s", testCloud, cloudType)
	}
	expectedHash := hashCloudID(testID)
	if cloudID != expectedHash {
		t.Fatalf("Expected hashed ID to be '%s', got '%s'", expectedHash, cloudID)
	}

	// Register a new detector that would return different results
	RegisterDetector("new_cloud", func() (string, error) {
		return "new-id-999", nil
	})

	// Second call should return cached results
	cloudType, cloudID, detected = GetHashedInformation()
	if !detected {
		t.Fatal("Expected cloud to be detected")
	}
	if cloudType != testCloud {
		t.Fatalf("Expected cloud type to still be '%s', got %s", testCloud, cloudType)
	}
	if cloudID != expectedHash {
		t.Fatalf("Expected hashed ID to still be '%s', got '%s'", expectedHash, cloudID)
	}
}

// TestDetectWithDefaultDetectors verifies that only known clouds can be detected
func TestDetectWithDefaultDetectors(t *testing.T) {
	// Reset all detectors before the test
	Reset()

	// Register the default cloud detectors (AWS, GCP, Azure)
	RegisterDefaultDetectors()

	// Run detection
	Detect()

	// If a cloud was detected, verify it's one of the known types
	if cloudDetected {
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
