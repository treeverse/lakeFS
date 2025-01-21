package testutil

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
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

func SetupTestingEnv(params *SetupTestingEnvParams) (logging.Logger, apigen.ClientWithResponsesInterface, *s3.Client, string) {
	logger := logging.ContextUnavailable()
	viper.AddConfigPath(".")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // support nested config
	viper.SetEnvPrefix(strings.ToUpper(params.Name))
	viper.SetConfigName(strings.ToLower(params.Name))
	viper.AutomaticEnv()

	viper.SetDefault("setup_lakefs", true)
	viper.SetDefault("setup_lakefs_timeout", defaultSetupTimeout)
	viper.SetDefault("endpoint_url", "http://localhost:8000")
	viper.SetDefault("s3_endpoint", "s3.local.lakefs.io:8000")
	viper.SetDefault("storage_namespace", fmt.Sprintf("s3://%s", params.StorageNS))
	viper.SetDefault(config.BlockstoreTypeKey, block.BlockstoreTypeS3)
	viper.SetDefault("version", "dev")
	currDir, err := os.Getwd()
	if err != nil {
		logger.WithError(err).Fatal("Failed to get CWD")
	}
	viper.SetDefault("glue_export_hooks_database", "export-hooks-esti")
	viper.SetDefault("glue_export_region", "us-east-1")
	viper.SetDefault("binaries_dir", filepath.Join(currDir, ".."))
	viper.SetDefault("azure_storage_account", "")
	viper.SetDefault("azure_storage_access_key", "")
	viper.SetDefault("large_object_path", "")
	err = viper.ReadInConfig()
	if err != nil && !errors.As(err, &viper.ConfigFileNotFoundError{}) {
		logger.WithError(err).Fatal("Failed to read configuration")
	}

	ctx := context.Background()

	// initialize the env/repo
	logger = logging.ContextUnavailable()
	logger.WithField("settings", viper.AllSettings()).Info(fmt.Sprintf("Starting %s", params.Name))

	endpointURL := ParseEndpointURL(logger, viper.GetString("endpoint_url"))

	client, err := apigen.NewClientWithResponses(endpointURL)
	if err != nil {
		logger.WithError(err).Fatal("could not initialize API client")
	}

	setupLakeFS := viper.GetBool("setup_lakefs")
	if setupLakeFS {
		if err := waitUntilLakeFSRunning(ctx, logger, client); err != nil {
			logger.WithError(err).Fatal("Waiting for lakeFS")
		}

		// first setup of lakeFS
		mockEmail := "test@acme.co"
		commResp, err := client.SetupCommPrefsWithResponse(context.Background(), apigen.SetupCommPrefsJSONRequestBody{
			Email:           &mockEmail,
			FeatureUpdates:  false,
			SecurityUpdates: false,
		})
		if err != nil {
			logger.WithError(err).Fatal("Failed to setup lakeFS")
		}
		if commResp.StatusCode() != http.StatusOK {
			logger.WithField("status", commResp.HTTPResponse.Status).Fatal("Failed to setup lakeFS")
		}
		adminUserName := params.Name
		requestBody := apigen.SetupJSONRequestBody{
			Username: adminUserName,
		}
		if params.AdminAccessKeyID != "" || params.AdminSecretAccessKey != "" {
			requestBody.Key = &apigen.AccessKeyCredentials{
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
	client, err = NewClientFromCreds(logger, key, secret, endpointURL)
	if err != nil {
		logger.WithError(err).Fatal("could not initialize API client with security provider")
	}

	s3Endpoint := viper.GetString("s3_endpoint")
	forcePathStyle := viper.GetBool("force_path_style")
	svc, err := SetupTestS3Client(s3Endpoint, key, secret, forcePathStyle)
	if err != nil {
		logger.WithError(err).Fatal("could not initialize S3 client")
	}
	return logger, client, svc, endpointURL
}

func SetupTestS3Client(endpoint, key, secret string, forcePathStyle bool) (*s3.Client, error) {
	if !strings.HasPrefix(endpoint, "http") {
		endpoint = "http://" + endpoint
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, "")),
	)
	if err != nil {
		return nil, err
	}
	svc := s3.NewFromConfig(cfg, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = forcePathStyle
	})
	return svc, nil
}

// ParseEndpointURL parses the given endpoint string
func ParseEndpointURL(logger logging.Logger, endpointURL string) string {
	u, err := url.Parse(endpointURL)
	if err != nil {
		logger.WithError(err).Fatal("could not initialize API client with security provider")
	}
	if u.Path == "" || u.Path == "/" {
		endpointURL = strings.TrimRight(endpointURL, "/") + apiutil.BaseURL
	}

	return endpointURL
}

// NewClientFromCreds creates a client using the credentials of a user
func NewClientFromCreds(logger logging.Logger, accessKeyID string, secretAccessKey string, endpointURL string) (*apigen.ClientWithResponses, error) {
	basicAuthProvider, err := securityprovider.NewSecurityProviderBasicAuth(accessKeyID, secretAccessKey)
	if err != nil {
		logger.WithError(err).Fatal("could not initialize basic auth security provider")
	}

	return apigen.NewClientWithResponses(endpointURL, apigen.WithRequestEditorFn(basicAuthProvider.Intercept))
}

const checkIteration = 5 * time.Second

func waitUntilLakeFSRunning(ctx context.Context, logger logging.Logger, cl apigen.ClientWithResponsesInterface) error {
	setupCtx, cancel := context.WithTimeout(ctx, viper.GetDuration("setup_lakefs_timeout"))
	defer cancel()
	for {
		resp, err := cl.HealthCheckWithResponse(setupCtx)
		if err != nil {
			logger.WithError(err).Info("Setup failed")
		} else {
			if resp.StatusCode() == http.StatusNoContent {
				return nil
			}
			logger.WithField("status", resp.HTTPResponse.Status).Warning("Bad status on healthcheck")
		}
		select {
		case <-setupCtx.Done():
			return setupCtx.Err()
		case <-time.After(checkIteration):
		}
	}
}
