package cloud

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"cloud.google.com/go/compute/metadata"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armsubscriptions"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	lakefsconfig "github.com/treeverse/lakefs/pkg/config"
)

// Cloud provider constants
const (
	GCPCloud   = "gcp_project_numerical_id"
	AWSCloud   = "aws_account_id"
	AzureCloud = "azure_subscription_id"
)

var (
	// ErrNotInCloud is returned when the code is not running in the respective cloud provider
	ErrNotInCloud = errors.New("not running in cloud provider")

	// detectorsRegistry holds all registered cloud detectors
	detectorsRegistry = make(map[string]DetectorFunc)
	// detectorOrder preserves registration order
	detectorOrder = []string{}
)

// DetectorFunc is a function type that detects a cloud provider and returns its ID
type DetectorFunc func(storageConfig lakefsconfig.StorageConfig) (string, error)

// RegisterDetector registers a new cloud detector with the given name
func RegisterDetector(name string, detector DetectorFunc) {
	_, exists := detectorsRegistry[name]
	if exists {
		// TODO(barak): do we like to panic here?
		// detector already registered, do nothing
		return
	}
	detectorsRegistry[name] = detector
	detectorOrder = append(detectorOrder, name)
}

// Reset clears all registered detectors, mainly used for testing
func Reset() {
	detectorsRegistry = make(map[string]DetectorFunc)
	detectorOrder = []string{}
}

// GetAWSAccountID retrieves AWS account ID using STS.
func GetAWSAccountID(storageConfig lakefsconfig.StorageConfig) (string, error) {
	ctx := context.Background()

	// first try to use the default configuration
	cfg, err := config.LoadDefaultConfig(ctx)
	if err == nil {
		awsStsClient := sts.NewFromConfig(cfg)
		resp, err := awsStsClient.GetCallerIdentity(ctx, nil)
		if err == nil {
			return aws.ToString(resp.Account), nil
		}
	}

	// try to use each storage config with s3 configuration
	storageIDs := storageConfig.GetStorageIDs()
	for _, storageID := range storageIDs {
		storageConfig := storageConfig.GetStorageByID(storageID)
		if storageConfig.BlockstoreType() != "s3" {
			continue
		}

		params, err := storageConfig.BlockstoreS3Params()
		if err != nil {
			continue
		}
		var opts []func(*config.LoadOptions) error
		if params.Region != "" {
			opts = append(opts, config.WithRegion(params.Region))
		}
		if params.Profile != "" {
			opts = append(opts, config.WithSharedConfigProfile(params.Profile))
		}
		if params.CredentialsFile != "" {
			opts = append(opts, config.WithSharedCredentialsFiles([]string{params.CredentialsFile}))
		}
		if params.Credentials.AccessKeyID != "" {
			opts = append(opts, config.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(
					params.Credentials.AccessKeyID,
					params.Credentials.SecretAccessKey,
					params.Credentials.SessionToken,
				),
			))
		}
		cfg, err := config.LoadDefaultConfig(ctx, opts...)
		if err != nil {
			continue
		}

		awsStsClient := sts.NewFromConfig(cfg)
		resp, err := awsStsClient.GetCallerIdentity(ctx, nil)
		if err != nil {
			continue
		}
		return aws.ToString(resp.Account), nil
	}

	return "", ErrNotInCloud
}

// GetAzureSubscriptionID retrieves the Azure Subscription ID using the armsubscriptions package.
func GetAzureSubscriptionID(storageConfig lakefsconfig.StorageConfig) (string, error) {
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
func GetGCPProjectID(_ lakefsconfig.StorageConfig) (string, error) {
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

// RegisterDefaultDetectors registers the built-in cloud detectors
//
// maintaine the order: GCP first, then AWS, then Azure
func RegisterDefaultDetectors() {
	RegisterDetector(GCPCloud, GetGCPProjectID)
	RegisterDetector(AWSCloud, GetAWSAccountID)
	RegisterDetector(AzureCloud, GetAzureSubscriptionID)
}

// Detect cloud type and ID. use the storage config if needed
func Detect(storageConfig lakefsconfig.StorageConfig) (string, string, bool) {
	// Iterate through detectors in the order they were registered
	for _, name := range detectorOrder {
		detector := detectorsRegistry[name]
		cloudID, err := detector(storageConfig)
		if err == nil {
			return name, cloudID, true
		}
	}

	// No cloud detected
	return "", "", false
}

// init registers the built-in cloud detectors
//
//nolint:gochecknoinits
func init() {
	RegisterDefaultDetectors()
}
