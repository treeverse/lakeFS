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
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/aws-sdk-go/aws"
)

var (
	cloudType     string
	cloudID       string
	cloudDetected bool
	once          sync.Once

	// ErrNotInCloud is returned when the code is not running in the respective cloud provider
	ErrNotInCloud = errors.New("not running in cloud provider")
)

// getAWSAccountID retrieves AWS account ID using STS.
func getAWSAccountID() (string, error) {
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
	return aws.StringValue(resp.Account), nil
}

// getAzureSubscriptionID retrieves the Azure Subscription ID using the armsubscriptions package.
func getAzureSubscriptionID() (string, error) {
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

// getGCPProjectID retrieves the GCP numerical project ID.
func getGCPProjectID() (string, error) {
	if !metadata.OnGCE() {
		return "", ErrNotInCloud
	}
	return metadata.NumericProjectID()
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

// Detect runs the cloud detection logic and caches the result.
// kept Google before AWS because they have AWS compatibility which I didn't want to fallback
func Detect() {
	detectionFunc := []struct {
		cloud  string
		detect func() (string, error)
	}{
		{cloud: "gcp_project_numerical_id", detect: getGCPProjectID},
		{cloud: "aws_account_id", detect: getAWSAccountID},
		{cloud: "azure_subscription_id", detect: getAzureSubscriptionID},
	}

	for _, df := range detectionFunc {
		if id, err := df.detect(); err == nil {
			cloudType, cloudID, cloudDetected = df.cloud, id, true
			return
		}
	}

	// No cloud detected
	cloudType, cloudID, cloudDetected = "", "", false
}

// GetMetadata returns the detected cloud type, cloud ID (hashed), and detection status.
func GetMetadata() (string, string, bool) {
	once.Do(Detect)
	if !cloudDetected {
		return "", "", false
	}

	// Hash the cloud ID
	s := md5.Sum([]byte(cloudID)) //nolint:gosec
	hashedID := hex.EncodeToString(s[:])

	return cloudType, hashedID, cloudDetected
}
