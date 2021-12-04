package testutil

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/rs/xid"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/api"
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

func LakectlLocation() string {
	return viper.GetString("lakectl_dir") + "/lakectl"
}

func buildLakectl() error {
	_, err := os.Stat(LakectlLocation())
	if err == nil {
		//
		// lakectl binary exists in the expected location. Nothing to do
		//
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		//
		// An error occurred, other than not exist - FAIL
		//
		return err
	}
	//
	// lakectl does not exist - build it
	//
	wd, err := os.Getwd()
	if err != nil {
		return err
	}

	err = os.Chdir(viper.GetString("lakectl_dir"))
	if err != nil {
		return err
	}

	make := exec.Command("make", "build-lakectl")
	err = make.Run()
	if err != nil {
		return err
	}

	return os.Chdir(wd)
}

func SetupTestingEnv(params *SetupTestingEnvParams) (logging.Logger, api.ClientWithResponsesInterface, *s3.S3) {
	logger := logging.Default()

	viper.SetDefault("setup_lakefs", true)
	viper.SetDefault("setup_lakefs_timeout", defaultSetupTimeout)
	viper.SetDefault("endpoint_url", "http://localhost:8000")
	viper.SetDefault("s3_endpoint", "s3.local.lakefs.io:8000")
	viper.SetDefault("access_key_id", "")
	viper.SetDefault("secret_access_key", "")
	viper.SetDefault("storage_namespace", fmt.Sprintf("s3://%s/%s", params.StorageNS, xid.New().String()))
	viper.SetDefault("lakectl_dir", "..")

	viper.AddConfigPath(".")
	viper.SetEnvPrefix(strings.ToUpper(params.Name))
	viper.SetConfigName(strings.ToLower(params.Name))
	viper.AutomaticEnv()

	err := viper.ReadInConfig()
	if err != nil && !errors.As(err, &viper.ConfigFileNotFoundError{}) {
		logger.WithError(err).Fatal("Failed to read configuration")
	}

	err = buildLakectl()
	if err != nil {
		logger.WithError(err).Fatal("Failed to build lakectl")
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
	}

	client, err = NewClientFromCreds(logger, viper.GetString("access_key_id"), viper.GetString("secret_access_key"), endpointURL)
	if err != nil {
		logger.WithError(err).Fatal("could not initialize API client with security provider")
	}

	s3Endpoint := viper.GetString("s3_endpoint")
	awsSession := session.Must(session.NewSession())
	svc := s3.New(awsSession,
		aws.NewConfig().
			WithRegion("us-east-1").
			WithEndpoint(s3Endpoint).
			WithDisableSSL(true).
			WithCredentials(credentials.NewCredentials(
				&credentials.StaticProvider{
					Value: credentials.Value{
						AccessKeyID:     viper.GetString("access_key_id"),
						SecretAccessKey: viper.GetString("secret_access_key"),
					}})))

	return logger, client, svc
}

// Parses the given endpoint string
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

// Creates a client using the credentials of a user
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
