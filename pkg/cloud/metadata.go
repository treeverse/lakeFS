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
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
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

	// ErrRequestFailed is returned when a request to the cloud provider fails
	ErrRequestFailed = errors.New("request failed")

	// detectorsRegistry holds all registered cloud detectors
	detectorsRegistry = make(map[string]DetectorFunc)
	// detectorOrder preserves registration order
	detectorOrder = []string{}
)

// DetectorFunc is a function type that detects a cloud provider and returns its ID
type DetectorFunc func(storageConfig config.StorageConfig) (string, error)

// RegisterDetector registers a new cloud detector with the given name
func RegisterDetector(name string, detector DetectorFunc) {
	_, exists := detectorsRegistry[name]
	if exists {
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

// DetectAWSAccountID retrieves AWS account ID using STS.
// The implementation uses the storage config to help identify the used account id.
func DetectAWSAccountID(storageConfig config.StorageConfig) (string, error) {
	ctx := context.Background()
	var errs []error

	// try to use each storage config with s3 configuration
	var storageIDs []string
	if storageConfig != nil {
		storageIDs = storageConfig.GetStorageIDs()
	}
	for _, storageID := range storageIDs {
		storageConfig := storageConfig.GetStorageByID(storageID)
		if storageConfig.BlockstoreType() != block.BlockstoreTypeS3 {
			continue
		}

		params, err := storageConfig.BlockstoreS3Params()
		if err != nil {
			errs = append(errs, fmt.Errorf("get s3 params for storage '%s': %w", storageID, err))
			continue
		}
		var opts []func(*awsconfig.LoadOptions) error
		if params.Region != "" {
			opts = append(opts, awsconfig.WithRegion(params.Region))
		}
		if params.Profile != "" {
			opts = append(opts, awsconfig.WithSharedConfigProfile(params.Profile))
		}
		if params.CredentialsFile != "" {
			opts = append(opts, awsconfig.WithSharedCredentialsFiles([]string{params.CredentialsFile}))
		}
		if params.Credentials.AccessKeyID != "" {
			opts = append(opts, awsconfig.WithCredentialsProvider(
				credentials.NewStaticCredentialsProvider(
					params.Credentials.AccessKeyID,
					params.Credentials.SecretAccessKey,
					params.Credentials.SessionToken,
				),
			))
		}
		accountID, err := getAWSAccountID(ctx, opts...)
		if err != nil {
			errs = append(errs, fmt.Errorf("storage '%s': %w", storageID, err))
			continue
		}
		return accountID, nil
	}

	// Fallback to default AWS credentials
	accountID, err := getAWSAccountID(ctx)
	if err != nil {
		errs = append(errs, fmt.Errorf("default: %w", err))
		return "", errors.Join(errs...)
	}
	return accountID, nil
}

// getAWSAccountID retrieves the AWS account ID using STS.
func getAWSAccountID(ctx context.Context, opts ...func(*awsconfig.LoadOptions) error) (string, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return "", err
	}
	awsStsClient := sts.NewFromConfig(cfg)
	resp, err := awsStsClient.GetCallerIdentity(ctx, nil)
	if err != nil {
		return "", err
	}
	return aws.ToString(resp.Account), nil
}

// DetectAzureSubscriptionID retrieves the Azure Subscription ID using the armsubscriptions package.
func DetectAzureSubscriptionID(config.StorageConfig) (string, error) {
	if err := checkAzureMetadata(); err != nil {
		return "", fmt.Errorf("check metadata: %w", err)
	}

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return "", fmt.Errorf("new default credential: %w", err)
	}
	client, err := armsubscriptions.NewClient(cred, nil)
	if err != nil {
		return "", fmt.Errorf("new client: %w", err)
	}

	// List subscriptions and return the first one
	pager := client.NewListPager(nil)
	for pager.More() {
		resp, err := pager.NextPage(context.TODO())
		if err != nil {
			return "", fmt.Errorf("next page: %w", err)
		}
		if len(resp.Value) > 0 {
			return *resp.Value[0].SubscriptionID, nil
		}
	}
	return "", ErrNotInCloud
}

// DetectGCPProjectID retrieves the GCP numerical project ID.
func DetectGCPProjectID(_ config.StorageConfig) (string, error) {
	if !metadata.OnGCE() {
		return "", ErrNotInCloud
	}
	return metadata.NumericProjectIDWithContext(context.Background())
}

// checkAzureMetadata detects Azure by querying IMDS.
func checkAzureMetadata() error {
	client := http.Client{Timeout: 1 * time.Second}
	req, err := http.NewRequest(http.MethodGet, "http://169.254.169.254/metadata/instance?api-version=2021-02-01", nil)
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Metadata", "true")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w, status code: %d", ErrRequestFailed, resp.StatusCode)
	}
	return nil
}

// RegisterDefaultDetectors registers the built-in cloud detectors
//
// maintaine the order: GCP first, then AWS, then Azure
func RegisterDefaultDetectors() {
	RegisterDetector(GCPCloud, DetectGCPProjectID)
	RegisterDetector(AWSCloud, DetectAWSAccountID)
	RegisterDetector(AzureCloud, DetectAzureSubscriptionID)
}

// Detect cloud type and ID. use the storage config if needed
func Detect(storageConfig config.StorageConfig) (string, string, bool) {
	// Iterate through detectors in the order they were registered
	for _, name := range detectorOrder {
		detector := detectorsRegistry[name]
		cloudID, err := detector(storageConfig)
		if err != nil {
			logging.ContextUnavailable().WithError(err).
				WithField("cloud_type", name).
				Trace("Failed to detect cloud type")
			continue
		}
		return name, cloudID, true
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
