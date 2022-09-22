package testutil

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/logging"
)

const defaultSetupTimeout = 5 * time.Minute

type SetupTestingEnvParams struct {
	Name      string
	StorageNS string

	// Only if non-empty
	AdminAccessKeyID     string
	AdminSecretAccessKey string
}

func SetupTestingEnv(params *SetupTestingEnvParams) (log logging.Logger, apiClient api.ClientWithResponsesInterface, pathStyleSvc *s3.S3, hostStyleSvc *s3.S3, s3Endpoint string) {
	logger := logging.Default()

	viper.SetDefault("setup_lakefs", true)
	viper.SetDefault("setup_lakefs_timeout", defaultSetupTimeout)
	viper.SetDefault("endpoint_url", "http://localhost:8000")
	viper.SetDefault("s3_endpoint", "s3.local.lakefs.io:8000")
	viper.SetDefault("storage_namespace", fmt.Sprintf("s3://%s", params.StorageNS))
	viper.SetDefault("blockstore_type", block.BlockstoreTypeS3)
	viper.SetDefault("version", "dev")
	viper.SetDefault("lakectl_dir", "..")

	viper.AddConfigPath(".")
	viper.SetEnvPrefix(strings.ToUpper(params.Name))
	viper.SetConfigName(strings.ToLower(params.Name))
	viper.AutomaticEnv()

	err := viper.ReadInConfig()
	if err != nil && !errors.As(err, &viper.ConfigFileNotFoundError{}) {
		logger.WithError(err).Fatal("Failed to read configuration")
	}

	ctx := context.Background()

	// initialize the env/repo
	logger = logging.Default()
	logger.WithField("settings", viper.AllSettings()).Info(fmt.Sprintf("Starting %s", params.Name))

	endpointURL := ParseEndpointURL(logger, viper.GetString("endpoint_url"))

	client, err := api.NewClientWithResponses(endpointURL)
	if err != nil {
		logger.WithError(err).Fatal("could not initialize API client")
	}

	if err := waitUntilLakeFSRunning(ctx, logger, client); err != nil {
		logger.WithError(err).Fatal("Waiting for lakeFS")
	}

	setupLakeFS := viper.GetBool("setup_lakefs")
	if setupLakeFS {
		// first setup of lakeFS
		adminUserName := params.Name
		requestBody := api.SetupJSONRequestBody{
			Username: adminUserName,
		}
		if params.AdminAccessKeyID != "" || params.AdminSecretAccessKey != "" {
			requestBody.Key = &api.AccessKeyCredentials{
				AccessKeyId:     params.AdminAccessKeyID,
				SecretAccessKey: params.AdminSecretAccessKey,
			}
		}
		res, err := client.SetupWithResponse(ctx, requestBody)
		if err != nil {
			logger.WithError(err).Fatal("Failed to setup lakeFS")
		}
		if res.StatusCode() != http.StatusOK {
			logger.WithField("status", res.HTTPResponse.Status).Fatal("Failed to setup lakeFS")
		}
		logger.Info("Cluster setup successfully")
		credentialsWithSecret := res.JSON200
		viper.Set("access_key_id", credentialsWithSecret.AccessKeyId)
		viper.Set("secret_access_key", credentialsWithSecret.SecretAccessKey)
	} else {
		viper.Set("access_key_id", params.AdminAccessKeyID)
		viper.Set("secret_access_key", params.AdminSecretAccessKey)
	}

	key := viper.GetString("access_key_id")
	secret := viper.GetString("secret_access_key")
	s3Endpoint = viper.GetString("s3_endpoint")

	client, err = NewClientFromCreds(logger, key, secret, endpointURL)
	if err != nil {
		logger.WithError(err).Fatal("could not initialize API client with security provider")
	}

	pathStyleSvc = SetupTestS3Client(key, secret, s3Endpoint, false)
	hostStyleSvc = SetupTestS3Client(key, secret, s3Endpoint, true)
	return logger, client, pathStyleSvc, hostStyleSvc, s3Endpoint
}

func SetupTestS3Client(key, secret, s3Endpoint string, hostBaseClient bool) *s3.S3 {
	awsSession := session.Must(session.NewSession())
	return s3.New(awsSession,
		aws.NewConfig().
			WithRegion("us-east-1").
			WithEndpoint(s3Endpoint).
			WithDisableSSL(true).
			WithS3ForcePathStyle(hostBaseClient).
			WithCredentials(credentials.NewCredentials(
				&credentials.StaticProvider{
					Value: credentials.Value{
						AccessKeyID:     key,
						SecretAccessKey: secret,
					}})))
}

// ParseEndpointURL parses the given endpoint string
func ParseEndpointURL(logger logging.Logger, endpointURL string) string {
	u, err := url.Parse(endpointURL)
	if err != nil {
		logger.WithError(err).Fatal("could not initialize API client with security provider")
	}
	if u.Path == "" || u.Path == "/" {
		endpointURL = strings.TrimRight(endpointURL, "/") + api.BaseURL
	}

	return endpointURL
}

// NewClientFromCreds creates a client using the credentials of a user
func NewClientFromCreds(logger logging.Logger, accessKeyID string, secretAccessKey string, endpointURL string) (*api.ClientWithResponses, error) {
	basicAuthProvider, err := securityprovider.NewSecurityProviderBasicAuth(accessKeyID, secretAccessKey)
	if err != nil {
		logger.WithError(err).Fatal("could not initialize basic auth security provider")
	}

	return api.NewClientWithResponses(endpointURL, api.WithRequestEditorFn(basicAuthProvider.Intercept))
}

const checkIteration = 5 * time.Second

func waitUntilLakeFSRunning(ctx context.Context, logger logging.Logger, cl api.ClientWithResponsesInterface) error {
	setupCtx, cancel := context.WithTimeout(ctx, viper.GetDuration("setup_lakefs_timeout"))
	defer cancel()
	for {
		_, err := cl.HealthCheckWithResponse(setupCtx)
		if err == nil {
			return nil
		}
		logger.WithError(err).Info("Setup failed")

		select {
		case <-setupCtx.Done():
			return setupCtx.Err()
		case <-time.After(checkIteration):
		}
	}
}
