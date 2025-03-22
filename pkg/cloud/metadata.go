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
)

var (
	cloudType     string
	cloudID       string
	cloudDetected bool
	once          sync.Once

	// AWS SDK initialization
	awsOnce      sync.Once
	awsStsClient *sts.Client
	awsInitErr   error

	// ErrNotInCloud is returned when the code is not running in the respective cloud provider
	ErrNotInCloud = errors.New("not running in cloud provider")
)

// initAWSClient initializes the AWS STS client once
func initAWSClient() {
	awsOnce.Do(func() {
		ctx := context.Background()
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			awsInitErr = err
			return
		}
		awsStsClient = sts.NewFromConfig(cfg)
	})
}

// getAWSAccountID retrieves AWS account ID using STS.
func getAWSAccountID() (string, error) {
	initAWSClient()
	if awsInitErr != nil {
		return "", awsInitErr
	}

	ctx := context.Background()
	resp, err := awsStsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", err
	}
	return *resp.Account, nil
}

// getAzureSubscriptionID retrieves the Azure Subscription ID using the armsubscriptions package.
func getAzureSubscriptionID() (string, error) {
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
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

// Detect runs the cloud detection logic and caches the result.
func Detect() {
	// AWS Detection
	if accountID, err := getAWSAccountID(); err == nil {
		cloudType, cloudID, cloudDetected = "aws_account_id", accountID, true
		return
	}

	// Azure Detection
	if checkAzureMetadata() {
		if subscriptionID, err := getAzureSubscriptionID(); err == nil {
			cloudType, cloudID, cloudDetected = "azure_subscription_id", subscriptionID, true
			return
		}
	}

	// Google Cloud Detection
	if projectID, err := getGCPProjectID(); err == nil {
		cloudType, cloudID, cloudDetected = "gcp_project_numerical_id", projectID, true
		return
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
