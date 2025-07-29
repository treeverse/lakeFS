package cloud

import (
	"context"
	"crypto/md5" //nolint:gosec
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/compute/metadata"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armsubscriptions"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// Cloud provider constants
const (
	GCPCloud   = "gcp_project_numerical_id"
	AWSCloud   = "aws_account_id"
	AzureCloud = "azure_subscription_id"
)

var (
	cloudType     string
	cloudID       string
	cloudDetected bool
	once          sync.Once

	// ErrNotInCloud is returned when the code is not running in the respective cloud provider
	ErrNotInCloud = errors.New("not running in cloud provider")

	// detectorsRegistry holds all registered cloud detectors
	detectorsRegistry = make(map[string]DetectorFunc)
	// detectorOrder preserves registration order
	detectorOrder = []string{}
)

// DetectorFunc is a function type that detects a cloud provider and returns its ID
type DetectorFunc func() (string, error)

// RegisterDetector registers a new cloud detector with the given name
func RegisterDetector(name string, detector DetectorFunc) {
	_, exists := detectorsRegistry[name]
	if exists {
		// detector already registered, do nothing
		return
	}
	detectorOrder = append(detectorOrder, name)
	detectorsRegistry[name] = detector
}

// Reset clears all registered detectors, mainly used for testing
func Reset() {
	detectorsRegistry = make(map[string]DetectorFunc)
	detectorOrder = []string{}
}

// GetAWSAccountID retrieves AWS account ID using STS.
func GetAWSAccountID() (string, error) {
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", err
	}

	awsStsClient := sts.NewFromConfig(cfg)
	resp, err := awsStsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", err
	}
	return aws.ToString(resp.Account), nil
}

// GetAzureSubscriptionID retrieves the Azure Subscription ID using the armsubscriptions package.
func GetAzureSubscriptionID() (string, error) {
	if !checkAzureMetadata() {
		return "", ErrNotInCloud
	}

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return "", err
	}
	client, err := armsubscriptions.NewClient(cred, nil)
	if err != nil {
		return "", err
	}

	// List subscriptions and return the first one
	pager := client.NewListPager(nil)
	for pager.More() {
		resp, err := pager.NextPage(context.TODO())
		if err != nil {
			return "", err
		}
		if len(resp.Value) > 0 {
			return *resp.Value[0].SubscriptionID, nil
		}
	}
	return "", fmt.Errorf("no Azure subscription found: %w", ErrNotInCloud)
}

// GetGCPProjectID retrieves the GCP numerical project ID.
func GetGCPProjectID() (string, error) {
	if !metadata.OnGCE() {
		return "", ErrNotInCloud
	}
	return metadata.NumericProjectIDWithContext(context.Background())
}

// checkAzureMetadata detects Azure by querying IMDS.
func checkAzureMetadata() bool {
	client := http.Client{Timeout: 1 * time.Second}
	req, err := http.NewRequest(http.MethodGet, "http://169.254.169.254/metadata/instance?api-version=2021-02-01", nil)
	if err != nil {
		return false
	}
	req.Header.Set("Metadata", "true")

	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()

	return resp.StatusCode == http.StatusOK
}

// init registers the built-in cloud detectors
//
//nolint:gochecknoinits
func init() {
	RegisterDefaultDetectors()
}

// RegisterDefaultDetectors registers the built-in cloud detectors
//
// maintained the order: GCP first, then AWS, then Azure
func RegisterDefaultDetectors() {
	RegisterDetector(GCPCloud, GetGCPProjectID)
	RegisterDetector(AWSCloud, GetAWSAccountID)
	RegisterDetector(AzureCloud, GetAzureSubscriptionID)
}

// Detect runs the cloud detection logic and caches the result.
// Detectors are tried in registration order, and the first one that succeeds is used.
// Any error is ignored, and the next detector is tried.
func Detect() {
	// Iterate through detectors in the order they were registered
	for _, name := range detectorOrder {
		detector := detectorsRegistry[name]
		if id, err := detector(); err == nil {
			cloudType, cloudID, cloudDetected = name, id, true
			return
		}
	}

	// No cloud detected
	cloudType, cloudID, cloudDetected = "", "", false
}

// GetHashedInformation returns the detected cloud type, cloud ID (hashed), and detection status.
func GetHashedInformation() (string, string, bool) {
	once.Do(Detect)
	return GetHashedInformationCached()
}

// GetHashedInformationCached returns the detected cloud type, cloud ID (hashed), and detection status.
func GetHashedInformationCached() (string, string, bool) {
	if !cloudDetected {
		return "", "", false
	}

	hashedID := hashCloudID(cloudID)
	return cloudType, hashedID, cloudDetected
}

// hashCloudID hashes the cloud ID to protect sensitive information
func hashCloudID(cloudID string) string {
	s := md5.Sum([]byte(cloudID)) //nolint:gosec
	return hex.EncodeToString(s[:])
}
